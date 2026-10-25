package storage

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"uuid"

	"chsync/internal/schema"
)

// Role — роль хоста в конфиге.
type Role string

const (
	RoleSource Role = "source"
	RoleTarget Role = "target"
)

// Obj — таблица / вью / словарь на конкретном хосте.
type Obj struct {
	DB, Name, Engine string
	UUID             string // "" — у объекта нет UUID (Ordinary-БД)
	Kind             schema.Kind
	Create           string // DDL как его вернул сервер (с UUID, если preserve_uuid)
	Norm             string // нормализованная форма для сравнения между хостами

	body string
	toks map[string]bool
}

// Key — полное имя объекта db.name.
func (o *Obj) Key() schema.ObjKey { return schema.ObjKey{DB: o.DB, Name: o.Name} }

// Body — DDL без заголовка "CREATE ... db.name" (лениво, кешируется): в нём ищем ссылки на другие объекты.
func (o *Obj) Body() string {
	if o.body == "" {
		o.body = schema.Body(o.Create)
	}
	return o.body
}

// Tokens — множество идентификаторов из тела DDL; быстрый предфильтр перед точной проверкой ссылок.
func (o *Obj) Tokens() map[string]bool {
	if o.toks == nil {
		o.toks = schema.Tokens(o.Body())
	}
	return o.toks
}

// DBInfo — база данных на хосте.
type DBInfo struct {
	Name, Engine, UUID string
	Create, Norm       string
}

// ClusterRow — строка system.clusters. InternalReplication = nil, если колонки нет (старые версии).
type ClusterRow struct {
	Cluster             string `json:"cluster"`
	ShardNum            uint32 `json:"shard_num"`
	ShardWeight         uint32 `json:"shard_weight"`
	InternalReplication *uint8 `json:"internal_replication"`
	ReplicaNum          uint32 `json:"replica_num"`
	HostName            string `json:"host_name"`
	Port                uint16 `json:"port"`
	IsLocal             uint8  `json:"is_local"`
}

// Inventory — всё, что chsync знает о хосте: версия, макросы, БД, объекты, кластеры, окружение.
type Inventory struct {
	Client          *Client
	Role            Role
	Version         string
	Hostname        string
	Macros          map[string]string
	DBs             map[string]*DBInfo
	Objects         map[schema.ObjKey]*Obj
	Functions       map[string]string // имя -> CREATE FUNCTION
	Clusters        []ClusterRow
	StoragePolicies map[string]bool
	Disks           map[string]bool
	ServerSettings  map[string]string // default_replica_* всегда; изменённые — только для check
	Users, Roles    []string
	Warnings        []string
	Err             error // хост недоступен / не прочитан
}

// NewInventory — пустой снимок хоста (все map инициализированы).
func NewInventory(c *Client, role Role) *Inventory {
	return &Inventory{
		Client: c, Role: role, Macros: map[string]string{}, DBs: map[string]*DBInfo{}, Objects: map[schema.ObjKey]*Obj{},
		Functions: map[string]string{}, StoragePolicies: map[string]bool{}, Disks: map[string]bool{},
		ServerSettings: map[string]string{},
	}
}

// Name — хост так, как он записан в конфиге.
func (inv *Inventory) Name() string { return inv.Client.Name() }

// Options — что именно читать с хоста.
type Options struct {
	PreserveUUID   bool                 // показывать UUID в DDL
	DisplaySecrets bool                 // показывать пароли в DDL словарей/движков
	WithAccess     bool                 // читать пользователей и роли (check)
	WithSettings   bool                 // читать все изменённые server_settings (check)
	DBFilter       func(db string) bool // false — не запрашивать SHOW CREATE DATABASE
}

// Host — хост для чтения снимка: клиент и роль.
type Host struct {
	Client *Client
	Role   Role
}

// sysDBs — служебные БД, которые не читаются вообще.
const sysDBs = "'system','INFORMATION_SCHEMA','information_schema','_temporary_and_external_tables'"

// maxParallel — сколько хостов читаем одновременно.
const maxParallel = 8

type kv struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type nameRow struct {
	Name string `json:"name"`
}

// Load читает снимок одного хоста. Ошибка критичных запросов кладётся в inv.Err,
// некритичных (UDF, кластеры) — в inv.Warnings.
func Load(ctx context.Context, h Host, opt Options) *Inventory {
	inv := NewInventory(h.Client, h.Role)
	inv.Err = inv.load(ctx, opt)
	return inv
}

func (inv *Inventory) load(ctx context.Context, opt Options) error {
	c := inv.Client
	show := map[string]string{}
	if opt.PreserveUUID {
		show["show_table_uuid_in_table_create_query_if_not_nil"] = "1"
	}
	if opt.DisplaySecrets {
		show["format_display_secrets_in_show_and_select"] = "1"
	}

	// Версия и hostname.
	vr, err := c.Select[kv](ctx, "SELECT version() AS name, hostName() AS value", nil)
	if err != nil {
		return err
	}
	if len(vr) > 0 {
		inv.Version, inv.Hostname = vr[0].Name, vr[0].Value
	}

	// Макросы: от них зависят ZK-пути Replicated-таблиц.
	mr, err := c.Select[kv](ctx, "SELECT macro AS name, substitution AS value FROM system.macros", nil)
	if err != nil {
		return err
	}
	for _, m := range mr {
		inv.Macros[m.Name] = m.Value
	}

	if err := inv.loadDatabases(ctx, opt, show); err != nil {
		return err
	}
	if err := inv.loadObjects(ctx, show); err != nil {
		return err
	}
	inv.loadOptional(ctx, opt)
	return nil
}

// loadDatabases — базы данных и их DDL (SHOW CREATE — только для БД, прошедших фильтр).
func (inv *Inventory) loadDatabases(ctx context.Context, opt Options, show map[string]string) error {
	type dbRow struct {
		Name   string `json:"name"`
		Engine string `json:"engine"`
		UUID   string `json:"uuid"`
	}
	type stmt struct {
		Statement string `json:"statement"`
	}
	c := inv.Client
	dbs, err := c.Select[dbRow](ctx, "SELECT name, engine, toString(uuid) AS uuid FROM system.databases WHERE name NOT IN ("+sysDBs+")", nil)
	if err != nil {
		return err
	}
	for _, d := range dbs {
		di := &DBInfo{Name: d.Name, Engine: d.Engine, UUID: normUUID(d.UUID)}
		if opt.DBFilter == nil || opt.DBFilter(d.Name) {
			st, err := c.Select[stmt](ctx, "SHOW CREATE DATABASE "+QuoteIdent(d.Name), show)
			if err == nil && len(st) > 0 {
				di.Create = st[0].Statement
			} else {
				di.Create = "CREATE DATABASE " + QuoteIdent(d.Name) + " ENGINE = " + d.Engine
				if err != nil {
					inv.Warnings = append(inv.Warnings, fmt.Sprintf("SHOW CREATE DATABASE %s: %v", d.Name, err))
				}
			}
			di.Norm = schema.Normalize(di.Create)
		}
		inv.DBs[d.Name] = di
	}
	return nil
}

// loadObjects — все таблицы/вью/словари одним запросом.
func (inv *Inventory) loadObjects(ctx context.Context, show map[string]string) error {
	type tRow struct {
		Database string `json:"database"`
		Name     string `json:"name"`
		Engine   string `json:"engine"`
		UUID     string `json:"uuid"`
		Create   string `json:"create"`
	}
	rows, err := inv.Client.Select[tRow](ctx, "SELECT database, name, engine, toString(uuid) AS uuid, create_table_query AS create "+
		"FROM system.tables WHERE is_temporary = 0 AND database NOT IN ("+sysDBs+")", show)
	if err != nil {
		return err
	}
	for _, r := range rows {
		o := &Obj{DB: r.Database, Name: r.Name, Engine: r.Engine, UUID: normUUID(r.UUID), Create: strings.TrimSpace(r.Create)}
		o.Kind = schema.Classify(r.Name, r.Engine, o.Create)
		o.Norm = schema.Normalize(o.Create)
		inv.Objects[o.Key()] = o
	}
	return nil
}

// loadOptional — то, без чего снимок всё равно полезен: UDF, кластеры, политики, диски, настройки, RBAC.
func (inv *Inventory) loadOptional(ctx context.Context, opt Options) {
	c := inv.Client

	// SQL UDF (CREATE FUNCTION) — нужны до таблиц, если используются в DEFAULT.
	if fr, err := c.Select[kv](ctx, "SELECT name, create_query AS value FROM system.functions WHERE origin = 'SQLUserDefined'", nil); err == nil {
		for _, f := range fr {
			inv.Functions[f.Name] = f.Value
		}
	} else {
		inv.Warnings = append(inv.Warnings, "SQL UDF не прочитаны: "+err.Error())
	}

	// Кластеры. internal_replication есть не во всех версиях — при ошибке читаем без неё.
	const clq = "SELECT cluster, shard_num, shard_weight, %s replica_num, host_name, port, is_local FROM system.clusters"
	var err error
	if inv.Clusters, err = c.Select[ClusterRow](ctx, fmt.Sprintf(clq, "internal_replication,"), nil); err != nil {
		if inv.Clusters, err = c.Select[ClusterRow](ctx, fmt.Sprintf(clq, ""), nil); err != nil {
			inv.Warnings = append(inv.Warnings, "system.clusters: "+err.Error())
		}
	}

	// Окружение: политики хранения и диски (нужны для storage_policy/disk в DDL).
	if r, err := c.Select[nameRow](ctx, "SELECT DISTINCT policy_name AS name FROM system.storage_policies", nil); err == nil {
		for _, x := range r {
			inv.StoragePolicies[x.Name] = true
		}
	}
	if r, err := c.Select[nameRow](ctx, "SELECT name FROM system.disks", nil); err == nil {
		for _, x := range r {
			inv.Disks[x.Name] = true
		}
	}

	// default_replica_path/name — путь Replicated-таблиц, созданных без аргументов движка.
	q := "SELECT name, value FROM system.server_settings WHERE name IN ('default_replica_path','default_replica_name')"
	if opt.WithSettings {
		q += " OR changed"
	}
	if r, err := c.Select[kv](ctx, q, nil); err == nil {
		for _, x := range r {
			inv.ServerSettings[x.Name] = x.Value
		}
	}
	if _, ok := inv.ServerSettings["default_replica_path"]; !ok { // очень старые версии
		inv.ServerSettings["default_replica_path"] = "/clickhouse/tables/{uuid}/{shard}"
		inv.ServerSettings["default_replica_name"] = "{replica}"
	}

	if opt.WithAccess {
		for q, dst := range map[string]*[]string{"SELECT name FROM system.users": &inv.Users, "SELECT name FROM system.roles": &inv.Roles} {
			if r, err := c.Select[nameRow](ctx, q, nil); err == nil {
				for _, x := range r {
					*dst = append(*dst, x.Name)
				}
			}
		}
	}
}

// normUUID приводит UUID к каноничному виду; нулевой (у объектов Ordinary-БД) и некорректный — к "".
func normUUID(s string) string {
	u, err := uuid.Parse(s)
	if err != nil || u == uuid.Nil() {
		return ""
	}
	return u.String()
}

// LoadAll читает снимки всех хостов параллельно (не более maxParallel одновременно).
// Порядок результата совпадает с порядком hosts.
func LoadAll(ctx context.Context, hosts []Host, opt Options) []*Inventory {
	out := make([]*Inventory, len(hosts))
	sem := make(chan struct{}, maxParallel)
	var wg sync.WaitGroup
	for i, h := range hosts {
		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()
			out[i] = Load(ctx, h, opt)
		})
	}
	wg.Wait()
	return out
}

// ============================== Кластер ==============================

// ClusterRows — строки кластера name, отсортированные по (shard_num, replica_num).
func (inv *Inventory) ClusterRows(name string) []ClusterRow {
	var out []ClusterRow
	for _, r := range inv.Clusters {
		if r.Cluster == name {
			out = append(out, r)
		}
	}
	slices.SortFunc(out, func(a, b ClusterRow) int {
		return cmp.Or(cmp.Compare(a.ShardNum, b.ShardNum), cmp.Compare(a.ReplicaNum, b.ReplicaNum))
	})
	return out
}

// HasCluster — описан ли кластер в remote_servers хоста.
func (inv *Inventory) HasCluster(name string) bool {
	return slices.ContainsFunc(inv.Clusters, func(r ClusterRow) bool { return r.Cluster == name })
}

// OwnShard — строки шарда, в котором хост нашёл сам себя (is_local=1). nil — не нашёл.
func (inv *Inventory) OwnShard(cluster string) []ClusterRow {
	rows := inv.ClusterRows(cluster)
	if i := slices.IndexFunc(rows, func(r ClusterRow) bool { return r.IsLocal == 1 }); i >= 0 {
		return ShardRows(rows, rows[i].ShardNum)
	}
	return nil
}

// ShardRows — реплики одного шарда из списка строк кластера.
func ShardRows(rows []ClusterRow, shard uint32) []ClusterRow {
	var out []ClusterRow
	for _, r := range rows {
		if r.ShardNum == shard {
			out = append(out, r)
		}
	}
	return out
}

// ClusterFingerprint — топология кластера одной строкой (сравнение между хостами) и число шардов.
func ClusterFingerprint(rows []ClusterRow) (string, int) {
	var b strings.Builder
	shards := map[uint32]bool{}
	for _, r := range rows {
		shards[r.ShardNum] = true
		fmt.Fprintf(&b, "%d(w%d):%s:%d;", r.ShardNum, r.ShardWeight, r.HostName, r.Port)
	}
	return b.String(), len(shards)
}

// Label — имя хоста для отчёта вместе с его макросами shard/replica.
func (inv *Inventory) Label() string {
	if sh, ok := inv.Macros["shard"]; ok {
		return fmt.Sprintf("%s [shard=%s replica=%s]", inv.Name(), sh, inv.Macros["replica"])
	}
	return inv.Name()
}

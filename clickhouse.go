package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ============================== HTTP-клиент ==============================

// Client — минимальный клиент ClickHouse поверх HTTP-интерфейса (8123/8443).
// Без драйверов: прозрачно для аудита и работает через любые балансировщики/chproxy.
type Client struct {
	Name       string // хост так, как он записан в конфиге
	base       string // http(s)://host:port/
	user, pass string
	hc         *http.Client
	logComment string // попадает в system.query_log.log_comment — легко найти запросы chsync
}

// CHError — ошибка ClickHouse (или сети) с кодом исключения, если сервер его вернул.
type CHError struct {
	Host    string
	Code    int
	Message string
}

func (e *CHError) Error() string {
	if e.Code > 0 {
		return fmt.Sprintf("%s: Code %d: %s", e.Host, e.Code, e.Message)
	}
	return e.Host + ": " + e.Message
}

// errCode достаёт код исключения ClickHouse из ошибки (0 — если это не ошибка сервера).
func errCode(err error) int {
	if e, ok := err.(*CHError); ok {
		return e.Code
	}
	return 0
}

// newClient создаёт клиент по строке из конфига: "host", "host:port" или "https://host:8443".
// По умолчанию HTTPS_PROXY из окружения игнорируется — до прод-хостов ходим напрямую.
func newClient(spec string, cc ConnConfig, runID string) (*Client, error) {
	scheme := map[bool]string{true: "https", false: "http"}[cc.Secure]
	hostport := spec
	if strings.HasPrefix(spec, "http://") || strings.HasPrefix(spec, "https://") {
		u, err := url.Parse(spec)
		if err != nil {
			return nil, fmt.Errorf("хост %q: %w", spec, err)
		}
		scheme, hostport = u.Scheme, u.Host
	}
	host, port := hostport, cc.Port
	if h, p, err := net.SplitHostPort(hostport); err == nil {
		host = h
		if port, err = strconv.Atoi(p); err != nil {
			return nil, fmt.Errorf("хост %q: неверный порт", spec)
		}
	}
	tr := &http.Transport{
		MaxIdleConnsPerHost: 4,
		IdleConnTimeout:     60 * time.Second,
		DialContext:         (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
	}
	if cc.UseEnvProxy {
		tr.Proxy = http.ProxyFromEnvironment
	}
	if scheme == "https" {
		tc := &tls.Config{InsecureSkipVerify: cc.InsecureSkipVerify} //nolint:gosec // явная опция конфига
		if cc.CAFile != "" {
			pem, err := os.ReadFile(cc.CAFile)
			if err != nil {
				return nil, fmt.Errorf("ca_file: %w", err)
			}
			tc.RootCAs = x509.NewCertPool()
			if !tc.RootCAs.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("ca_file %s: не найдено сертификатов", cc.CAFile)
			}
		}
		tr.TLSClientConfig = tc
	}
	return &Client{
		Name: spec, base: fmt.Sprintf("%s://%s/", scheme, net.JoinHostPort(host, strconv.Itoa(port))),
		user: cc.User, pass: cc.Password, logComment: "chsync " + runID,
		hc: &http.Client{Transport: tr, Timeout: time.Duration(cc.TimeoutSec) * time.Second},
	}, nil
}

var codeRe = regexp.MustCompile(`Code: (\d+)`)

// Exec выполняет один запрос и возвращает тело ответа. settings уходят параметрами URL.
// wait_end_of_query=1 — чтобы ошибка посреди выполнения пришла HTTP-статусом, а не обрывком тела.
func (c *Client) Exec(ctx context.Context, query string, settings map[string]string) ([]byte, error) {
	v := url.Values{}
	v.Set("wait_end_of_query", "1")
	v.Set("output_format_json_quote_64bit_integers", "0")
	v.Set("log_comment", c.logComment)
	for k, val := range settings {
		v.Set(k, val)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"?"+v.Encode(), strings.NewReader(query))
	if err != nil {
		return nil, &CHError{Host: c.Name, Message: err.Error()}
	}
	req.Header.Set("X-ClickHouse-User", c.user)
	if c.pass != "" {
		req.Header.Set("X-ClickHouse-Key", c.pass) // пароль в заголовке, не в URL
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, &CHError{Host: c.Name, Message: err.Error()}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 256<<20))
	if err != nil {
		return nil, &CHError{Host: c.Name, Message: "read body: " + err.Error()}
	}
	if resp.StatusCode == http.StatusOK {
		return body, nil
	}
	e := &CHError{Host: c.Name, Message: truncate(strings.TrimSpace(string(body)), 2000)}
	if h := resp.Header.Get("X-ClickHouse-Exception-Code"); h != "" {
		e.Code, _ = strconv.Atoi(h)
	} else if m := codeRe.FindStringSubmatch(e.Message); m != nil {
		e.Code, _ = strconv.Atoi(m[1])
	}
	if e.Message == "" {
		e.Message = resp.Status
	}
	return nil, e
}

// Select выполняет SELECT/SHOW в формате JSONEachRow и декодирует каждую строку в T.
func Select[T any](ctx context.Context, c *Client, query string, settings map[string]string) ([]T, error) {
	s := map[string]string{"default_format": "JSONEachRow"}
	for k, v := range settings {
		s[k] = v
	}
	body, err := c.Exec(ctx, query, s)
	if err != nil {
		return nil, err
	}
	var out []T
	sc := bufio.NewScanner(bytes.NewReader(body))
	sc.Buffer(make([]byte, 1<<20), 128<<20) // DDL больших таблиц бывает длинным
	for sc.Scan() {
		if len(bytes.TrimSpace(sc.Bytes())) == 0 {
			continue
		}
		var row T
		if err := json.Unmarshal(sc.Bytes(), &row); err != nil {
			return nil, &CHError{Host: c.Name, Message: fmt.Sprintf("decode %q: %v", truncate(sc.Text(), 200), err)}
		}
		out = append(out, row)
	}
	if err := sc.Err(); err != nil {
		return nil, &CHError{Host: c.Name, Message: err.Error()}
	}
	return out, nil
}

// quoteIdent экранирует идентификатор обратными кавычками: my db -> `my db`.
func quoteIdent(s string) string {
	return "`" + strings.NewReplacer("\\", "\\\\", "`", "\\`").Replace(s) + "`"
}

// quoteString экранирует строковый литерал: it's -> 'it\'s'.
func quoteString(s string) string {
	return "'" + strings.NewReplacer("\\", "\\\\", "'", "\\'").Replace(s) + "'"
}

// ============================== Снимок хоста ==============================

// Obj — таблица / вью / словарь на конкретном хосте.
type Obj struct {
	DB, Name, Engine, UUID string
	Kind                   Kind
	Create                 string // DDL как его вернул сервер (с UUID, если preserve_uuid)
	Norm                   string // нормализованная форма для сравнения между хостами
	body                   string
	toks                   map[string]bool
}

// Key — полное имя объекта db.name.
func (o *Obj) Key() ObjKey { return ObjKey{o.DB, o.Name} }

// Body — DDL без заголовка "CREATE ... db.name" (лениво, кешируется): в нём ищем ссылки на другие объекты.
func (o *Obj) Body() string {
	if o.body == "" {
		o.body = ddlBody(o.Create)
	}
	return o.body
}

// Tokens — множество идентификаторов из тела DDL; быстрый предфильтр перед точной проверкой ссылок.
func (o *Obj) Tokens() map[string]bool {
	if o.toks == nil {
		o.toks = tokens(o.Body())
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
	C               *Client
	Role            string // source | target
	Version         string
	Hostname        string
	Macros          map[string]string
	DBs             map[string]*DBInfo
	Objects         map[ObjKey]*Obj
	Functions       map[string]string // имя -> CREATE FUNCTION
	Clusters        []ClusterRow
	StoragePolicies map[string]bool
	Disks           map[string]bool
	ServerSettings  map[string]string // default_replica_* всегда; изменённые — только для check
	Users, Roles    []string
	Warnings        []string
	Err             error // хост недоступен / не прочитан
}

// invOptions — что именно читать со хоста.
type invOptions struct {
	preserveUUID   bool                 // показывать UUID в DDL
	displaySecrets bool                 // показывать пароли в DDL словарей/движков
	withAccess     bool                 // читать пользователей и роли (check)
	withSettings   bool                 // читать все изменённые server_settings (check)
	dbFilter       func(db string) bool // false — не запрашивать SHOW CREATE DATABASE
}

// sysDBs — служебные БД, которые не читаются вообще.
const sysDBs = "'system','INFORMATION_SCHEMA','information_schema','_temporary_and_external_tables'"

// loadInventory читает снимок одного хоста. Ошибка критичных запросов кладётся в inv.Err,
// некритичных (UDF, кластеры) — в inv.Warnings.
func loadInventory(ctx context.Context, c *Client, role string, opt invOptions) *Inventory {
	inv := &Inventory{
		C: c, Role: role, Macros: map[string]string{}, DBs: map[string]*DBInfo{}, Objects: map[ObjKey]*Obj{},
		Functions: map[string]string{}, StoragePolicies: map[string]bool{}, Disks: map[string]bool{},
		ServerSettings: map[string]string{},
	}
	show := map[string]string{}
	if opt.preserveUUID {
		show["show_table_uuid_in_table_create_query_if_not_nil"] = "1"
	}
	if opt.displaySecrets {
		show["format_display_secrets_in_show_and_select"] = "1"
	}
	type kv struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	}
	type nameRow struct {
		Name string `json:"name"`
	}

	// Версия и hostname.
	vr, err := Select[kv](ctx, c, "SELECT version() AS name, hostName() AS value", nil)
	if err != nil {
		inv.Err = err
		return inv
	}
	if len(vr) > 0 {
		inv.Version, inv.Hostname = vr[0].Name, vr[0].Value
	}

	// Макросы: от них зависят ZK-пути Replicated-таблиц.
	mr, err := Select[kv](ctx, c, "SELECT macro AS name, substitution AS value FROM system.macros", nil)
	if err != nil {
		inv.Err = err
		return inv
	}
	for _, m := range mr {
		inv.Macros[m.Name] = m.Value
	}

	// Базы данных + их DDL (только для БД, прошедших фильтр).
	type dbRow struct {
		Name   string `json:"name"`
		Engine string `json:"engine"`
		UUID   string `json:"uuid"`
	}
	dbs, err := Select[dbRow](ctx, c, "SELECT name, engine, toString(uuid) AS uuid FROM system.databases WHERE name NOT IN ("+sysDBs+")", nil)
	if err != nil {
		inv.Err = err
		return inv
	}
	type stmt struct {
		Statement string `json:"statement"`
	}
	for _, d := range dbs {
		di := &DBInfo{Name: d.Name, Engine: d.Engine, UUID: d.UUID}
		if opt.dbFilter == nil || opt.dbFilter(d.Name) {
			st, err := Select[stmt](ctx, c, "SHOW CREATE DATABASE "+quoteIdent(d.Name), show)
			if err == nil && len(st) > 0 {
				di.Create = st[0].Statement
			} else {
				di.Create = "CREATE DATABASE " + quoteIdent(d.Name) + " ENGINE = " + d.Engine
				if err != nil {
					inv.Warnings = append(inv.Warnings, fmt.Sprintf("SHOW CREATE DATABASE %s: %v", d.Name, err))
				}
			}
			di.Norm = normalizeDDL(di.Create)
		}
		inv.DBs[d.Name] = di
	}

	// Все таблицы/вью/словари одним запросом.
	type tRow struct {
		Database string `json:"database"`
		Name     string `json:"name"`
		Engine   string `json:"engine"`
		UUID     string `json:"uuid"`
		Create   string `json:"create"`
	}
	rows, err := Select[tRow](ctx, c, "SELECT database, name, engine, toString(uuid) AS uuid, create_table_query AS create "+
		"FROM system.tables WHERE is_temporary = 0 AND database NOT IN ("+sysDBs+")", show)
	if err != nil {
		inv.Err = err
		return inv
	}
	for _, r := range rows {
		o := &Obj{DB: r.Database, Name: r.Name, Engine: r.Engine, UUID: r.UUID, Create: strings.TrimSpace(r.Create)}
		o.Kind = classify(r.Name, r.Engine, o.Create)
		o.Norm = normalizeDDL(o.Create)
		inv.Objects[o.Key()] = o
	}

	// SQL UDF (CREATE FUNCTION) — нужны до таблиц, если используются в DEFAULT.
	if fr, err := Select[kv](ctx, c, "SELECT name, create_query AS value FROM system.functions WHERE origin = 'SQLUserDefined'", nil); err == nil {
		for _, f := range fr {
			inv.Functions[f.Name] = f.Value
		}
	} else {
		inv.Warnings = append(inv.Warnings, "SQL UDF не прочитаны: "+err.Error())
	}

	// Кластеры. internal_replication есть не во всех версиях — при ошибке читаем без неё.
	const clq = "SELECT cluster, shard_num, shard_weight, %s replica_num, host_name, port, is_local FROM system.clusters"
	if inv.Clusters, err = Select[ClusterRow](ctx, c, fmt.Sprintf(clq, "internal_replication,"), nil); err != nil {
		if inv.Clusters, err = Select[ClusterRow](ctx, c, fmt.Sprintf(clq, ""), nil); err != nil {
			inv.Warnings = append(inv.Warnings, "system.clusters: "+err.Error())
		}
	}

	// Окружение: политики хранения и диски (нужны для storage_policy/disk в DDL).
	if r, err := Select[nameRow](ctx, c, "SELECT DISTINCT policy_name AS name FROM system.storage_policies", nil); err == nil {
		for _, x := range r {
			inv.StoragePolicies[x.Name] = true
		}
	}
	if r, err := Select[nameRow](ctx, c, "SELECT name FROM system.disks", nil); err == nil {
		for _, x := range r {
			inv.Disks[x.Name] = true
		}
	}

	// default_replica_path/name — путь Replicated-таблиц, созданных без аргументов движка.
	q := "SELECT name, value FROM system.server_settings WHERE name IN ('default_replica_path','default_replica_name')"
	if opt.withSettings {
		q += " OR changed"
	}
	if r, err := Select[kv](ctx, c, q, nil); err == nil {
		for _, x := range r {
			inv.ServerSettings[x.Name] = x.Value
		}
	}
	if _, ok := inv.ServerSettings["default_replica_path"]; !ok { // очень старые версии
		inv.ServerSettings["default_replica_path"] = "/clickhouse/tables/{uuid}/{shard}"
		inv.ServerSettings["default_replica_name"] = "{replica}"
	}

	if opt.withAccess {
		for _, t := range []struct {
			q   string
			dst *[]string
		}{{"SELECT name FROM system.users", &inv.Users}, {"SELECT name FROM system.roles", &inv.Roles}} {
			if r, err := Select[nameRow](ctx, c, t.q, nil); err == nil {
				for _, x := range r {
					*t.dst = append(*t.dst, x.Name)
				}
			}
		}
	}
	return inv
}

// loadAll читает снимки всех хостов параллельно (не более 8 одновременно).
func loadAll(ctx context.Context, clients []*Client, roles []string, opt invOptions) []*Inventory {
	out := make([]*Inventory, len(clients))
	var wg sync.WaitGroup
	sem := make(chan struct{}, 8)
	for i := range clients {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			out[i] = loadInventory(ctx, clients[i], roles[i], opt)
		}(i)
	}
	wg.Wait()
	return out
}

// clusterRows — строки кластера name, отсортированные по (shard_num, replica_num).
func (inv *Inventory) clusterRows(name string) []ClusterRow {
	var out []ClusterRow
	for _, r := range inv.Clusters {
		if r.Cluster == name {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ShardNum != out[j].ShardNum {
			return out[i].ShardNum < out[j].ShardNum
		}
		return out[i].ReplicaNum < out[j].ReplicaNum
	})
	return out
}

// hasCluster — описан ли кластер в remote_servers хоста.
func (inv *Inventory) hasCluster(name string) bool { return len(inv.clusterRows(name)) > 0 }

// ownShard — строки шарда, в котором хост нашёл сам себя (is_local=1). nil — не нашёл.
func (inv *Inventory) ownShard(cluster string) []ClusterRow {
	rows := inv.clusterRows(cluster)
	for _, r := range rows {
		if r.IsLocal == 1 {
			return shardRows(rows, r.ShardNum)
		}
	}
	return nil
}

// shardRows — реплики одного шарда из списка строк кластера.
func shardRows(rows []ClusterRow, shard uint32) []ClusterRow {
	var out []ClusterRow
	for _, r := range rows {
		if r.ShardNum == shard {
			out = append(out, r)
		}
	}
	return out
}

// clusterFingerprint — топология кластера одной строкой (сравнение между хостами) и число шардов.
func clusterFingerprint(rows []ClusterRow) (string, int) {
	var b strings.Builder
	shards := map[uint32]bool{}
	for _, r := range rows {
		shards[r.ShardNum] = true
		fmt.Fprintf(&b, "%d(w%d):%s:%d;", r.ShardNum, r.ShardWeight, r.HostName, r.Port)
	}
	return b.String(), len(shards)
}

// label — имя хоста для отчёта вместе с его макросами shard/replica.
func (inv *Inventory) label() string {
	if sh, ok := inv.Macros["shard"]; ok {
		return fmt.Sprintf("%s [shard=%s replica=%s]", inv.C.Name, sh, inv.Macros["replica"])
	}
	return inv.C.Name
}

// zkChildren возвращает детей узлов ZK: path -> []name (пустой срез — узла нет или он пуст).
// Сначала пакетно через path IN (...); если версия так не умеет — по одному пути.
func zkChildren(ctx context.Context, c *Client, paths []string) (map[string][]string, string) {
	type row struct {
		Path string `json:"path"`
		Name string `json:"name"`
	}
	out := map[string][]string{}
	paths = uniq(paths)
	for _, p := range paths {
		out[p] = nil
	}
	var lastErr string
	for i := 0; i < len(paths); i += 100 {
		chunk := paths[i:minInt(i+100, len(paths))]
		q := make([]string, len(chunk))
		for j, p := range chunk {
			q[j] = quoteString(p)
		}
		rows, err := Select[row](ctx, c, "SELECT path, name FROM system.zookeeper WHERE path IN ("+strings.Join(q, ",")+")", nil)
		if err == nil {
			for _, r := range rows {
				out[r.Path] = append(out[r.Path], r.Name)
			}
			continue
		}
		for _, p := range chunk { // старые версии падают на несуществующем пути в IN
			rows, err := Select[row](ctx, c, "SELECT path, name FROM system.zookeeper WHERE path = "+quoteString(p), nil)
			switch {
			case err != nil && strings.Contains(err.Error(), "No node"):
			case err != nil:
				lastErr = err.Error()
				delete(out, p) // состояние узла неизвестно
			default:
				for _, r := range rows {
					out[r.Path] = append(out[r.Path], r.Name)
				}
			}
		}
	}
	return out, truncate(lastErr, 300)
}

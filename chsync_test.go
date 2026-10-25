package main

import (
	"reflect"
	"strings"
	"testing"
)

// ============================== Разбор DDL ==============================

const mvInner = "CREATE MATERIALIZED VIEW BD1.mv_inner UUID '6690d772-d953-42e6-8928-e5e3945401cb' TO INNER UUID 'a836af67-5ddc-4fea-94f9-ef8e15dbc902' (`id` UInt64, `s` Float64) ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') ORDER BY id SETTINGS index_granularity = 8192 AS SELECT id, sum(v) AS s FROM BD1.dev_table_p GROUP BY id"

// SHOW CREATE (многострочный, без UUID) и create_table_query (с UUID) должны нормализоваться одинаково.
func TestNormalizeAndHeader(t *testing.T) {
	a := "CREATE TABLE BD1.t UUID 'fd8ce2e3-a28b-4333-acf1-f63fb72d4cf3' (`id` UInt64)  ENGINE = MergeTree ORDER BY id"
	b := "CREATE TABLE IF NOT EXISTS BD1.t\n(\n    `id` UInt64\n)\nENGINE = MergeTree\nORDER BY id"
	if normalizeDDL(a) != normalizeDDL(b) {
		t.Fatalf("norm differ:\n%s\n%s", normalizeDDL(a), normalizeDDL(b))
	}
	cases := map[string]string{
		a:       "CREATE TABLE IF NOT EXISTS BD1.t UUID 'fd8ce2e3",
		b:       b, // уже есть IF NOT EXISTS — без изменений
		mvInner: "CREATE MATERIALIZED VIEW IF NOT EXISTS BD1.mv_inner UUID",
		"CREATE TABLE `my db`.`we.ird` UUID 'fd8ce2e3-a28b-4333-acf1-f63fb72d4cf3' (`id` UInt64) ENGINE = Log": "CREATE TABLE IF NOT EXISTS `my db`.`we.ird` UUID",
		"CREATE FUNCTION lab_norm AS x -> lower(trimBoth(x))":                                                  "CREATE FUNCTION IF NOT EXISTS lab_norm AS x -> lower(trimBoth(x))",
		"CREATE DATABASE BD1 UUID '0d97477c-9272-4a2a-a3a0-df7d0d6edea0'\nENGINE = Atomic":                     "CREATE DATABASE IF NOT EXISTS BD1 UUID",
	}
	for in, prefix := range cases {
		if got := addIfNotExists(in); !strings.HasPrefix(got, prefix) {
			t.Errorf("addIfNotExists(%q) = %q", in, got)
		}
	}
	if got := stripUUID(mvInner); strings.Contains(got, "UUID") {
		t.Fatal(got)
	}
	if headerUUID(a) != "fd8ce2e3-a28b-4333-acf1-f63fb72d4cf3" || innerUUID(mvInner) != "a836af67-5ddc-4fea-94f9-ef8e15dbc902" {
		t.Fatal("uuid")
	}
}

// Разбор аргументов Replicated: явный путь, путь по умолчанию, экранирование, MV с inner-движком.
func TestParseReplicated(t *testing.T) {
	ri := parseReplicated(mvInner)
	if ri == nil || ri.PathTmpl != "/clickhouse/tables/{uuid}/{shard}" || ri.ReplicaTmpl != "{replica}" || ri.UUID != "a836af67-5ddc-4fea-94f9-ef8e15dbc902" {
		t.Fatalf("%+v", ri)
	}
	ri = parseReplicated("CREATE TABLE BD1.u (`uid` UInt64, `ver` UInt32) ENGINE = ReplicatedReplacingMergeTree('/ch/{shard}/BD1/u', '{replica}', ver) ORDER BY uid")
	if ri.PathTmpl != "/ch/{shard}/BD1/u" || ri.Defaults {
		t.Fatalf("%+v", ri)
	}
	for _, s := range []string{
		"CREATE TABLE a.b (x UInt8) ENGINE = ReplicatedReplacingMergeTree(ver) ORDER BY x",
		"CREATE TABLE a.b (x UInt8) ENGINE = ReplicatedMergeTree ORDER BY x",
	} {
		if ri := parseReplicated(s); ri == nil || !ri.Defaults {
			t.Fatalf("defaults: %s", s)
		}
	}
	if parseReplicated("CREATE TABLE a.b (x UInt8) ENGINE = MergeTree ORDER BY x") != nil {
		t.Fatal("non-replicated")
	}
	ri = parseReplicated(`CREATE TABLE a.b (x UInt8) ENGINE = ReplicatedMergeTree('/p/it''s/{shard}', 'r\'1') ORDER BY x`)
	if ri.PathTmpl != "/p/it's/{shard}" || ri.ReplicaTmpl != "r'1" {
		t.Fatalf("%+v", ri)
	}
}

func TestSplitArgsAndUnquote(t *testing.T) {
	s := "Distributed('{cluster}', 'BD1', 'dev_table_p', cityHash64(id, concat('a,b', \"x\")), 'pol')"
	args, _ := splitArgs(s, strings.Index(s, "("))
	want := []string{"'{cluster}'", "'BD1'", "'dev_table_p'", "cityHash64(id, concat('a,b', \"x\"))", "'pol'"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("%q", args)
	}
	if args, _ = splitArgs("f()", 1); len(args) != 0 {
		t.Fatalf("%q", args)
	}
	for in, out := range map[string]string{`'a\nb'`: "a\nb", "`x y`": "x y", `'it\'s'`: "it's", "ident": "ident"} {
		if got, _ := unquote(in); got != out {
			t.Errorf("unquote(%s) = %q", in, got)
		}
	}
}

func TestExpandMacros(t *testing.T) {
	out, miss := expandMacros("/clickhouse/tables/{uuid}/{shard}/{layer}", map[string]string{"shard": "04"}, map[string]string{"uuid": "U"})
	if out != "/clickhouse/tables/U/04/{layer}" || !reflect.DeepEqual(miss, []string{"layer"}) {
		t.Fatal(out, miss)
	}
	if !templateHasMacro("/x/{shard}/y", []string{"shard"}) || templateHasMacro("/x/all/{database}", []string{"shard"}) {
		t.Fatal("templateHasMacro")
	}
}

// Ссылки: квалифицированные, неквалифицированные в той же БД, явные (Distributed, словарь), dictGet.
func TestRefs(t *testing.T) {
	o := &Obj{DB: "BD2", Name: "v", Kind: KView, Create: "CREATE VIEW BD2.v (`a` UInt8) AS SELECT a FROM BD1.`BMS_events_p` JOIN dim USING a"}
	body := ddlBody(o.Create)
	switch {
	case !refersTo(o, ObjKey{"BD1", "BMS_events_p"}, body):
		t.Fatal("qualified backticked")
	case !refersTo(o, ObjKey{"BD2", "dim"}, body):
		t.Fatal("unqualified same db")
	case refersTo(o, ObjKey{"BD3", "dim"}, body):
		t.Fatal("unqualified other db must not match")
	case refersTo(o, ObjKey{"BD1", "BMS_events"}, body):
		t.Fatal("prefix must not match")
	}
	d := &Obj{DB: "BD1", Name: "d", Kind: KDistributed, Create: "CREATE TABLE BD1.d (`x` UInt8) ENGINE = Distributed('c', 'BD9', 'loc_p', rand())"}
	if r := explicitRefs(d); len(r) != 1 || r[0] != (ObjKey{"BD9", "loc_p"}) {
		t.Fatal(r)
	}
	dict := &Obj{DB: "BD2", Name: "dd", Kind: KDictionary, Create: "CREATE DICTIONARY BD2.dd (`k` UInt64) PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'dim_p' DB 'BD2')) LIFETIME(MIN 0 MAX 300) LAYOUT(HASHED())"}
	if r := explicitRefs(dict); len(r) != 1 || r[0] != (ObjKey{"BD2", "dim_p"}) {
		t.Fatal(r)
	}
	tb := &Obj{DB: "BD2", Name: "f", Kind: KTable, Create: "CREATE TABLE BD2.f (`k` UInt64, `l` String DEFAULT dictGet('BD2.dim_dict', 'label', k)) ENGINE = Log"}
	if !tb.Tokens()["dim_dict"] || !refersTo(tb, ObjKey{"BD2", "dim_dict"}, tb.Body()) {
		t.Fatal("dictGet ref")
	}
}

func TestFilter(t *testing.T) {
	f := newFilter(FilterConfig{ExcludeDatabases: []string{"test"}, ExcludeNameRegex: []string{"(?i)bms"}, ExcludeEngines: []string{"kafka"}}, nil)
	for db, skip := range map[string]bool{"system": true, "INFORMATION_SCHEMA": true, "TEST": true, "BD1": false, "bms_db": true, "xBmSy": true} {
		if (f.SkipDB(db) != "") != skip {
			t.Errorf("SkipDB(%s)", db)
		}
	}
	if f.SkipObject(&Obj{DB: "BD1", Name: "dev_BMS_p"}) == "" || f.SkipObject(&Obj{DB: "BD1", Name: "dev_p"}) != "" {
		t.Error("SkipObject name")
	}
	if f.SkipObject(&Obj{DB: "BD1", Name: "q", Engine: "Kafka"}) == "" {
		t.Error("engine")
	}
	f2 := newFilter(FilterConfig{IncludeDatabases: []string{"BD1"}}, []string{"BD2"})
	if f2.SkipDB("BD1") == "" || f2.SkipDB("BD2") != "" {
		t.Error("--databases overrides include")
	}
}

// Порядок создания: таблица → Distributed → VIEW → VIEW над VIEW; цикл не должен зависать.
func TestTopoSortOrderAndCycle(t *testing.T) {
	objs := map[ObjKey]*SrcObj{}
	add := func(name string, k Kind, deps ...string) {
		o := &SrcObj{Obj: &Obj{DB: "a", Name: name, Kind: k}}
		for _, d := range deps {
			o.Deps = append(o.Deps, ObjKey{"a", d})
		}
		objs[o.Key()] = o
	}
	add("v2", KView, "v1")
	add("v1", KView, "dist")
	add("dist", KDistributed, "t_p")
	add("t_p", KReplicated)
	add("mv", KMatView, "t_p")
	add("c1", KView, "c2")
	add("c2", KView, "c1")
	var keys []ObjKey
	for k := range objs {
		keys = append(keys, k)
	}
	pos := map[string]int{}
	order := topoSort(objs, keys)
	for i, k := range order {
		pos[k.Name] = i
	}
	if len(order) != 7 || !(pos["t_p"] < pos["dist"] && pos["dist"] < pos["v1"] && pos["v1"] < pos["v2"] && pos["t_p"] < pos["mv"]) {
		t.Fatalf("order %v", order)
	}
}

// ============================== Проверки плана ==============================

// fakeInv — снимок хоста без сети. replicas — сколько реплик у его шарда в remote_servers (0 — кластера нет).
func fakeInv(name, shard, replica string, replicas int) *Inventory {
	inv := &Inventory{
		C: &Client{Name: name}, Macros: map[string]string{"shard": shard, "replica": replica},
		DBs: map[string]*DBInfo{}, Objects: map[ObjKey]*Obj{}, Functions: map[string]string{},
		StoragePolicies: map[string]bool{"default": true}, Disks: map[string]bool{"default": true},
		ServerSettings: map[string]string{"default_replica_path": "/clickhouse/tables/{shard}/{database}/{table}", "default_replica_name": "{replica}"},
	}
	for i := 1; i <= replicas; i++ {
		inv.Clusters = append(inv.Clusters, ClusterRow{Cluster: "c", ShardNum: 4, ReplicaNum: uint32(i), HostName: name, IsLocal: uint8(map[bool]int{true: 1}[i == 1])})
	}
	return inv
}

// planOne прогоняет validate для одного объекта из DDL create на target tgt.
func planOne(t *testing.T, cfg *Config, create string, tgt *Inventory, others []*Inventory, zk map[string][]string) *Item {
	t.Helper()
	cfg.Cluster = "c"
	cfg.applyDefaults()
	src := fakeInv("s1", "01", "r1", 2)
	o := &Obj{DB: "BD1", Name: "t_p", Engine: "ReplicatedMergeTree", Create: create, Kind: KReplicated}
	if !strings.Contains(create, "Replicated") {
		o.Engine, o.Kind = "MergeTree", KTable
	}
	so := &SrcObj{Obj: o, Source: "s1", Hosts: []string{"s1"}}
	p := &planner{cfg: cfg, srcByID: map[string]*Inventory{"s1": src}, targets: append([]*Inventory{tgt}, others...),
		u: &Union{Objects: map[ObjKey]*SrcObj{o.Key(): so}}}
	it := &Item{}
	p.validate(so, tgt, it, p.replicaTarget(so, tgt), zk, "")
	return it
}

const (
	uuidT = "CREATE TABLE BD1.t_p UUID 'fd8ce2e3-a28b-4333-acf1-f63fb72d4cf3' (`id` UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') ORDER BY id"
	zp    = "/clickhouse/tables/fd8ce2e3-a28b-4333-acf1-f63fb72d4cf3/04/replicas"
)

func TestValidateReplicated(t *testing.T) {
	tgt := func() *Inventory { return fakeInv("t1", "04", "r7", 2) }
	sib := fakeInv("t2", "04", "r8", 2)
	f := false
	cases := []struct {
		name   string
		cfg    *Config
		create string
		tgt    *Inventory
		others []*Inventory
		zk     map[string][]string
		want   Action
		reason string
	}{
		{"нормальный случай", &Config{}, uuidT, tgt(), nil, map[string][]string{}, ACreate, ""},
		{"shard совпал со старым", &Config{}, uuidT, fakeInv("t1", "01", "r7", 2), nil, nil, ABlocked, "коллизия"},
		{"{uuid} без preserve_uuid", &Config{DDL: DDLConfig{PreserveUUID: &f}}, stripUUID(uuidT), tgt(), nil, nil, ABlocked, "разные UUID"},
		{"своя реплика осталась в ZK", &Config{}, uuidT, tgt(), nil, map[string][]string{zp: {"r7"}}, ABlocked, "SYSTEM DROP REPLICA"},
		{"соседняя реплика нового шарда", &Config{}, uuidT, tgt(), []*Inventory{sib}, map[string][]string{zp: {"r8"}}, ACreate, ""},
		{"чужая реплика в ZK", &Config{}, uuidT, tgt(), []*Inventory{sib}, map[string][]string{zp: {"old9"}}, ABlocked, "чужими"},
		{"неизвестный макрос", &Config{}, strings.Replace(uuidT, "{shard}", "{shard}/{layer}", 1), tgt(), nil, nil, ABlocked, "{layer}"},
		{"нет storage policy", &Config{}, uuidT + " SETTINGS storage_policy = 'hot_cold'", tgt(), nil, nil, ABlocked, "hot_cold"},
	}
	for _, c := range cases {
		it := planOne(t, c.cfg, c.create, c.tgt, c.others, c.zk)
		if it.Action != c.want || !strings.Contains(it.Reason, c.reason) {
			t.Errorf("%s: %v %q", c.name, it.Action, it.Reason)
		}
	}

	// Путь без {shard}: решение по global_replicated.
	glob := "CREATE TABLE BD1.t_p (`id` UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/all/BD1/t_p', '{replica}') ORDER BY id"
	for pol, want := range map[string]Action{"block": ABlocked, "skip": ASkip, "create": ACreate} {
		if it := planOne(t, &Config{DDL: DDLConfig{GlobalReplicated: pol}}, glob, tgt(), nil, nil); it.Action != want {
			t.Errorf("global %s: %v", pol, it.Action)
		}
	}

	// Схема как на проде: CREATE без аргументов → путь из default_replica_path, сохранён литералом.
	stored := "CREATE TABLE BD1.t_p UUID 'fd8ce2e3-a28b-4333-acf1-f63fb72d4cf3' (`id` UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/BD1/old_name_p', '{replica}') ORDER BY id"
	if it := planOne(t, &Config{}, stored, tgt(), nil, nil); it.Action != ACreate || it.ExpZK != "/clickhouse/tables/04/BD1/old_name_p" {
		t.Errorf("prod-style: %v %s %s", it.Action, it.Reason, it.ExpZK)
	}
	// Аргументы не сохранены вовсе → default_replica_path target.
	if it := planOne(t, &Config{}, "CREATE TABLE BD1.t_p (`id` UInt64) ENGINE = ReplicatedMergeTree ORDER BY id", fakeInv("t1", "05", "r9", 2), nil, nil); it.ExpZK != "/clickhouse/tables/05/BD1/t_p" || it.ExpRepl != "r9" {
		t.Errorf("defaults: %v %s %s", it.Action, it.Reason, it.ExpZK)
	}
}

// Нереплицируемая MergeTree: предупреждение только если у шарда target больше одной реплики.
func TestPlainMergeTreeWarning(t *testing.T) {
	plain := "CREATE TABLE BD1.t_p (`id` UInt64) ENGINE = MergeTree ORDER BY id"
	hasWarn := func(it *Item) bool {
		return strings.Contains(strings.Join(it.Warn, ";"), "нереплицируемая")
	}
	if it := planOne(t, &Config{}, plain, fakeInv("t1", "04", "r7", 2), nil, nil); !hasWarn(it) {
		t.Error("rf=2: ожидалось предупреждение")
	}
	if it := planOne(t, &Config{}, plain, fakeInv("t1", "04", "r7", 1), nil, nil); hasWarn(it) {
		t.Error("rf=1: предупреждение лишнее")
	}
}

func TestReplicationLine(t *testing.T) {
	one := replicationLine(fakeInv("t1", "04", "r7", 1), "c")
	two := replicationLine(fakeInv("t1", "04", "r7", 2), "c")
	if !strings.Contains(one, "1 реплик") || !strings.Contains(one, "без реплик") || !strings.Contains(two, "2 реплик") || strings.Contains(two, "без реплик") {
		t.Fatalf("%s | %s", one, two)
	}
}

func TestConfigValidate(t *testing.T) {
	c := &Config{Source: SourceConfig{Hosts: []string{"a"}}, Target: TargetConfig{Hosts: []string{"a"}, ReplicasPerShard: -1}}
	c.applyDefaults()
	err := c.validate()
	if err == nil || !strings.Contains(err.Error(), "и в source, и в target") || !strings.Contains(err.Error(), "replicas_per_shard") {
		t.Fatal(err)
	}
}

package plan

import (
	"strings"
	"testing"

	"chsync/internal/config"
	"chsync/internal/schema"
	"chsync/internal/storage"
)

// Порядок создания: таблица → Distributed → VIEW → VIEW над VIEW; цикл не должен зависать.
func TestTopoSortOrderAndCycle(t *testing.T) {
	objs := map[schema.ObjKey]*SrcObj{}
	add := func(name string, k schema.Kind, deps ...string) {
		o := &SrcObj{Obj: &storage.Obj{DB: "a", Name: name, Kind: k}}
		for _, d := range deps {
			o.Deps = append(o.Deps, schema.ObjKey{DB: "a", Name: d})
		}
		objs[o.Key()] = o
	}
	add("v2", schema.KView, "v1")
	add("v1", schema.KView, "dist")
	add("dist", schema.KDistributed, "t_p")
	add("t_p", schema.KReplicated)
	add("mv", schema.KMatView, "t_p")
	add("c1", schema.KView, "c2")
	add("c2", schema.KView, "c1")
	var keys []schema.ObjKey
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

// fakeInv — снимок хоста без сети. replicas — сколько реплик у его шарда в remote_servers (0 — кластера нет).
func fakeInv(t *testing.T, name, shard, replica string, replicas int) *storage.Inventory {
	t.Helper()
	c, err := storage.New(name, config.Connection{Port: 8123}, "test")
	if err != nil {
		t.Fatal(err)
	}
	inv := storage.NewInventory(c, storage.RoleTarget)
	inv.Macros = map[string]string{"shard": shard, "replica": replica}
	inv.StoragePolicies["default"] = true
	inv.Disks["default"] = true
	inv.ServerSettings = map[string]string{"default_replica_path": "/clickhouse/tables/{shard}/{database}/{table}", "default_replica_name": "{replica}"}
	for i := 1; i <= replicas; i++ {
		var local uint8
		if i == 1 {
			local = 1
		}
		inv.Clusters = append(inv.Clusters, storage.ClusterRow{Cluster: "c", ShardNum: 4, ReplicaNum: uint32(i), HostName: name, IsLocal: local})
	}
	return inv
}

// planOne прогоняет validate для одного объекта из DDL create на target tgt.
func planOne(t *testing.T, mod func(*config.Config), create string, tgt *storage.Inventory, others []*storage.Inventory, zk map[string][]string) *Item {
	t.Helper()
	cfg := config.Default()
	cfg.Cluster = "c"
	if mod != nil {
		mod(&cfg)
	}
	src := fakeInv(t, "s1", "01", "r1", 2)
	o := &storage.Obj{DB: "BD1", Name: "t_p", Engine: "ReplicatedMergeTree", Create: create, Kind: schema.KReplicated}
	if !strings.Contains(create, "Replicated") {
		o.Engine, o.Kind = "MergeTree", schema.KTable
	}
	so := &SrcObj{Obj: o, Source: "s1", Hosts: []string{"s1"}}
	u := &Union{Objects: map[schema.ObjKey]*SrcObj{o.Key(): so}, Sources: []*storage.Inventory{src}}
	p := NewPlanner(&cfg, nil, u, append([]*storage.Inventory{tgt}, others...))
	it := &Item{}
	p.validate(so, tgt, it, p.replicaTarget(so, tgt), zkState{children: zk})
	return it
}

const (
	uuidT = "CREATE TABLE BD1.t_p UUID 'fd8ce2e3-a28b-4333-acf1-f63fb72d4cf3' (`id` UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') ORDER BY id"
	zp    = "/clickhouse/tables/fd8ce2e3-a28b-4333-acf1-f63fb72d4cf3/04/replicas"
)

func TestValidateReplicated(t *testing.T) {
	tgt := func() *storage.Inventory { return fakeInv(t, "t1", "04", "r7", 2) }
	sib := fakeInv(t, "t2", "04", "r8", 2)
	noUUID := func(c *config.Config) { c.DDL.PreserveUUID = false }
	cases := []struct {
		name   string
		mod    func(*config.Config)
		create string
		tgt    *storage.Inventory
		others []*storage.Inventory
		zk     map[string][]string
		want   Action
		reason string
	}{
		{"нормальный случай", nil, uuidT, tgt(), nil, map[string][]string{}, ACreate, ""},
		{"shard совпал со старым", nil, uuidT, fakeInv(t, "t1", "01", "r7", 2), nil, nil, ABlocked, "коллизия"},
		{"{uuid} без preserve_uuid", noUUID, schema.StripUUID(uuidT), tgt(), nil, nil, ABlocked, "разные UUID"},
		{"своя реплика осталась в ZK", nil, uuidT, tgt(), nil, map[string][]string{zp: {"r7"}}, ABlocked, "SYSTEM DROP REPLICA"},
		{"соседняя реплика нового шарда", nil, uuidT, tgt(), []*storage.Inventory{sib}, map[string][]string{zp: {"r8"}}, ACreate, ""},
		{"чужая реплика в ZK", nil, uuidT, tgt(), []*storage.Inventory{sib}, map[string][]string{zp: {"old9"}}, ABlocked, "чужими"},
		{"неизвестный макрос", nil, strings.Replace(uuidT, "{shard}", "{shard}/{layer}", 1), tgt(), nil, nil, ABlocked, "{layer}"},
		{"нет storage policy", nil, uuidT + " SETTINGS storage_policy = 'hot_cold'", tgt(), nil, nil, ABlocked, "hot_cold"},
	}
	for _, c := range cases {
		it := planOne(t, c.mod, c.create, c.tgt, c.others, c.zk)
		if it.Action != c.want || !strings.Contains(it.Reason, c.reason) {
			t.Errorf("%s: %v %q", c.name, it.Action, it.Reason)
		}
	}

	// Путь без {shard}: решение по global_replicated.
	glob := "CREATE TABLE BD1.t_p (`id` UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/all/BD1/t_p', '{replica}') ORDER BY id"
	for pol, want := range map[config.GlobalPolicy]Action{config.GlobalBlock: ABlocked, config.GlobalSkip: ASkip, config.GlobalCreate: ACreate} {
		if it := planOne(t, func(c *config.Config) { c.DDL.GlobalReplicated = pol }, glob, tgt(), nil, nil); it.Action != want {
			t.Errorf("global %s: %v", pol, it.Action)
		}
	}

	// Схема как на проде: CREATE без аргументов → путь из default_replica_path, сохранён литералом.
	stored := "CREATE TABLE BD1.t_p UUID 'fd8ce2e3-a28b-4333-acf1-f63fb72d4cf3' (`id` UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/BD1/old_name_p', '{replica}') ORDER BY id"
	if it := planOne(t, nil, stored, tgt(), nil, nil); it.Action != ACreate || it.ExpZK != "/clickhouse/tables/04/BD1/old_name_p" {
		t.Errorf("prod-style: %v %s %s", it.Action, it.Reason, it.ExpZK)
	}
	// Аргументы не сохранены вовсе → default_replica_path target.
	if it := planOne(t, nil, "CREATE TABLE BD1.t_p (`id` UInt64) ENGINE = ReplicatedMergeTree ORDER BY id", fakeInv(t, "t1", "05", "r9", 2), nil, nil); it.ExpZK != "/clickhouse/tables/05/BD1/t_p" || it.ExpRepl != "r9" {
		t.Errorf("defaults: %v %s %s", it.Action, it.Reason, it.ExpZK)
	}
}

// Нереплицируемая MergeTree: предупреждение только если у шарда target больше одной реплики.
func TestPlainMergeTreeWarning(t *testing.T) {
	plain := "CREATE TABLE BD1.t_p (`id` UInt64) ENGINE = MergeTree ORDER BY id"
	hasWarn := func(it *Item) bool {
		return strings.Contains(strings.Join(it.Warn, ";"), "нереплицируемая")
	}
	if it := planOne(t, nil, plain, fakeInv(t, "t1", "04", "r7", 2), nil, nil); !hasWarn(it) {
		t.Error("rf=2: ожидалось предупреждение")
	}
	if it := planOne(t, nil, plain, fakeInv(t, "t1", "04", "r7", 1), nil, nil); hasWarn(it) {
		t.Error("rf=1: предупреждение лишнее")
	}
}

func TestReplicationLine(t *testing.T) {
	one := ReplicationLine(fakeInv(t, "t1", "04", "r7", 1), "c")
	two := ReplicationLine(fakeInv(t, "t1", "04", "r7", 2), "c")
	if !strings.Contains(one, "1 реплик") || !strings.Contains(one, "без реплик") || !strings.Contains(two, "2 реплик") || strings.Contains(two, "без реплик") {
		t.Fatalf("%s | %s", one, two)
	}
}

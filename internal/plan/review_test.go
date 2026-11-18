package plan

import (
	"context"
	"errors"
	"strings"
	"testing"

	"chsync/internal/config"
	"chsync/internal/filter"
	"chsync/internal/schema"
	"chsync/internal/storage"
)

func inv(t *testing.T, name, shard, replica string, shardNum uint32) *storage.Inventory {
	i := fakeInv(t, name, shard, replica, 0)
	for r := uint32(1); r <= 2; r++ {
		var local uint8
		if r == 1 {
			local = 1
		}
		i.Clusters = append(i.Clusters, storage.ClusterRow{Cluster: "c", ShardNum: shardNum, ReplicaNum: r, HostName: name, IsLocal: local})
	}
	i.DBs["BD1"] = &storage.DBInfo{Name: "BD1", Engine: "Atomic", Create: "CREATE DATABASE BD1 ENGINE = Atomic", Norm: "CREATE DATABASE BD1 ENGINE = Atomic"}
	i.DBs["BD2"] = &storage.DBInfo{Name: "BD2", Engine: "Atomic", Create: "CREATE DATABASE BD2 ENGINE = Atomic", Norm: "CREATE DATABASE BD2 ENGINE = Atomic"}
	return i
}

func obj(i *storage.Inventory, db, name, engine, create string) {
	o := &storage.Obj{DB: db, Name: name, Engine: engine, Create: create}
	o.Kind = schema.Classify(name, engine, create)
	o.Norm = schema.Normalize(create)
	i.Objects[o.Key()] = o
}

// --databases BD2, а VIEW в BD2 читает BD1.t (BD1.t уже есть на target)
func TestIncludeFilterDoesNotCascade(t *testing.T) {
	cfg := config.Default()
	cfg.Cluster = "c"
	src := inv(t, "s1", "01", "r1", 1)
	obj(src, "BD1", "t", "MergeTree", "CREATE TABLE BD1.t (`id` UInt64) ENGINE = MergeTree ORDER BY id")
	obj(src, "BD2", "v", "View", "CREATE VIEW BD2.v (`id` UInt64) AS SELECT id FROM BD1.t")
	f := filter.New(cfg.Filter, []string{"BD2"})
	u := BuildUnion(&cfg, f, []*storage.Inventory{src})
	if s := u.Objects[schema.ObjKey{DB: "BD2", Name: "v"}].Skip; s != "" {
		t.Fatalf("BD2.v не должен исключаться каскадом от объекта вне --databases: %q", s)
	}
	// BD1.t на target нет — VIEW блокируется с понятной причиной; есть — создаётся.
	for _, has := range []bool{false, true} {
		tg := inv(t, "t4", "04", "r4", 4)
		if has {
			obj(tg, "BD1", "t", "MergeTree", "CREATE TABLE BD1.t (`id` UInt64) ENGINE = MergeTree ORDER BY id")
		}
		tp := NewPlanner(&cfg, f, u, []*storage.Inventory{tg}).PlanTarget(context.Background(), tg)
		for _, it := range tp.Items {
			if it.Name == "BD2.v" && (it.Action == ACreate) != has {
				t.Errorf("BD1.t на target: %v → BD2.v %v %q", has, it.Action, it.Reason)
			}
		}
	}
}

// target t5 (remote_servers шард 5) по ошибке с макросом shard=04, как у t4 (шард 4)
func TestTargetMacrosMatchRemoteServers(t *testing.T) {
	cfg := config.Default()
	cfg.Cluster = "c"
	src := inv(t, "s1", "01", "r1", 1)
	ddl := "CREATE TABLE BD1.t UUID 'fd8ce2e3-a28b-4333-acf1-f63fb72d4cf3' (`id` UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/BD1/t', '{replica}') ORDER BY id"
	obj(src, "BD1", "t", "ReplicatedMergeTree", ddl)
	t4 := inv(t, "t4", "04", "r4", 4)
	t5 := inv(t, "t5", "04", "r5", 5) // ошибка в макросе
	f := filter.New(cfg.Filter, nil)
	u := BuildUnion(&cfg, f, []*storage.Inventory{src})
	p := NewPlanner(&cfg, f, u, []*storage.Inventory{t4, t5})
	so := u.Objects[schema.ObjKey{DB: "BD1", Name: "t"}]
	// t4 уже создан: в ZK есть реплика r4. t5 не должен встать к ней «соседом».
	it := &Item{}
	p.validate(so, t5, it, p.replicaTarget(so, t5), zkState{checked: true, children: map[string][]string{"/clickhouse/tables/04/BD1/t/replicas": {"r4"}}})
	if it.Action != ABlocked || !strings.Contains(it.Reason, "разные шарды") {
		t.Errorf("макрос shard совпал у разных шардов remote_servers: %v %q", it.Action, it.Reason)
	}
	// Реплики одного шарда remote_servers с разными макросами shard.
	t4b := inv(t, "t4b", "44", "r4b", 4)
	it = &Item{}
	p2 := NewPlanner(&cfg, f, u, []*storage.Inventory{t4, t4b})
	p2.validate(so, t4, it, p2.replicaTarget(so, t4), zkState{})
	if it.Action != ABlocked || !strings.Contains(it.Reason, "того же шарда") {
		t.Errorf("разные макросы у реплик одного шарда: %v %q", it.Action, it.Reason)
	}
}

// Проверка ZK включена, но состояние пути неизвестно: создавать вслепую нельзя. С выключенной проверкой — можно.
func TestZKStateUnknownBlocks(t *testing.T) {
	blind := zkState{checked: true, children: map[string][]string{}, err: errors.New("timeout")}
	if it := planOne(t, nil, uuidT, fakeInv(t, "t1", "04", "r7", 2), nil, nil); it.Action != ACreate {
		t.Errorf("check_zookeeper выключен: %v %q", it.Action, it.Reason)
	}
	cfg := config.Default()
	cfg.Cluster = "c"
	src := inv(t, "s1", "01", "r1", 1)
	obj(src, "BD1", "t", "ReplicatedMergeTree", uuidT)
	f := filter.New(cfg.Filter, nil)
	u := BuildUnion(&cfg, f, []*storage.Inventory{src})
	tg := inv(t, "t4", "04", "r4", 4)
	p := NewPlanner(&cfg, f, u, []*storage.Inventory{tg})
	so := u.Objects[schema.ObjKey{DB: "BD1", Name: "t"}]
	it := &Item{}
	p.validate(so, tg, it, p.replicaTarget(so, tg), blind)
	if it.Action != ABlocked || !strings.Contains(it.Reason, "проверить не удалось") {
		t.Errorf("состояние ZK неизвестно: %v %q", it.Action, it.Reason)
	}
}

// БД заблокирована (DDL различается между источниками), таблицы в ней
func TestTablesInBlockedDatabase(t *testing.T) {
	cfg := config.Default()
	cfg.Cluster = "c"
	s1 := inv(t, "s1", "01", "r1", 1)
	s2 := inv(t, "s2", "02", "r2", 2)
	s2.DBs["BD1"].Norm = "CREATE DATABASE BD1 ENGINE = Atomic COMMENT 'x'"
	for _, s := range []*storage.Inventory{s1, s2} {
		obj(s, "BD1", "t", "MergeTree", "CREATE TABLE BD1.t (`id` UInt64) ENGINE = MergeTree ORDER BY id")
	}
	tg := inv(t, "t4", "04", "r4", 4)
	delete(tg.DBs, "BD1")
	f := filter.New(cfg.Filter, nil)
	u := BuildUnion(&cfg, f, []*storage.Inventory{s1, s2})
	tp := NewPlanner(&cfg, f, u, []*storage.Inventory{tg}).PlanTarget(context.Background(), tg)
	for _, it := range tp.Items {
		if (it.Name == "BD1" || it.Name == "BD1.t") && it.Action != ABlocked {
			t.Errorf("%s %s: %v — в заблокированной БД таблицы создаваться не должны", it.Kind, it.Name, it.Action)
		}
	}
}

// Исключённая таблица shop.bms и VIEW в той же БД с КОЛОНКОЙ bms из другой таблицы
func TestColumnNameIsNotTableRef(t *testing.T) {
	cfg := config.Default()
	cfg.Cluster = "c"
	cfg.Filter.ExcludeNameRegex = []string{"(?i)^bms$"}
	src := inv(t, "s1", "01", "r1", 1)
	obj(src, "BD1", "bms", "MergeTree", "CREATE TABLE BD1.bms (`id` UInt64) ENGINE = MergeTree ORDER BY id")
	obj(src, "BD1", "t", "MergeTree", "CREATE TABLE BD1.t (`id` UInt64, `bms` String) ENGINE = MergeTree ORDER BY id")
	obj(src, "BD1", "v", "View", "CREATE VIEW BD1.v (`id` UInt64, `bms` String) AS SELECT id, bms FROM BD1.t")
	u := BuildUnion(&cfg, filter.New(cfg.Filter, nil), []*storage.Inventory{src})
	if s := u.Objects[schema.ObjKey{DB: "BD1", Name: "v"}].Skip; s != "" {
		t.Errorf("колонка bms — не ссылка на таблицу bms: %q", s)
	}
}

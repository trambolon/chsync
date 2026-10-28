// Package check — предполётные проверки (команда check): связь и версии, макросы, remote_servers,
// контроль репликации шардов, связность кластера, ZooKeeper, окружение, ключи шардирования.
// Ничего не меняет.
package check

import (
	"context"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"

	"chsync/internal/config"
	"chsync/internal/filter"
	"chsync/internal/storage"
)

// checker — состояние проверки: хосты по ролям и счётчики ошибок/предупреждений.
type checker struct {
	cfg               *config.Config
	f                 *filter.Filter // фильтр из конфига (без --databases) — для статистики по схеме
	w                 io.Writer
	srcs, tgts, alive []*storage.Inventory
	errors, warns     int
}

func (c *checker) ok(f string, a ...any)   { fmt.Fprintf(c.w, "  ✓ "+f+"\n", a...) }
func (c *checker) warn(f string, a ...any) { c.warns++; fmt.Fprintf(c.w, "  ! "+f+"\n", a...) }
func (c *checker) fail(f string, a ...any) { c.errors++; fmt.Fprintf(c.w, "  ✗ "+f+"\n", a...) }
func (c *checker) h(f string, a ...any)    { fmt.Fprintf(c.w, "\n── "+f+"\n", a...) }

// Run — предполётная проверка всех хостов. Возвращает число ошибок.
func Run(ctx context.Context, cfg *config.Config, invs []*storage.Inventory, w io.Writer) int {
	c := &checker{cfg: cfg, f: filter.New(cfg.Filter, nil), w: w}
	c.checkHosts(invs)
	c.checkMacros()
	if cfg.Cluster != "" {
		c.checkTopology()
		c.checkReplication(ctx)
		c.checkConnectivity(ctx)
	} else {
		c.h("Кластер")
		c.warn("cluster не задан в конфиге — проверки remote_servers и репликации пропущены")
	}
	c.checkZooKeeper(ctx)
	c.checkEnvironment()
	c.checkShardingKeys()
	fmt.Fprintf(w, "\nИТОГО: ошибок %d, предупреждений %d\n", c.errors, c.warns)
	return c.errors
}

// checkHosts — доступность хостов и одинаковость версий ClickHouse.
func (c *checker) checkHosts(invs []*storage.Inventory) {
	c.h("Подключение и версии")
	versions := map[string][]string{}
	for _, inv := range invs {
		if inv.Err != nil {
			c.fail("%-8s %s: %v", inv.Role, inv.Name(), inv.Err)
			continue
		}
		c.alive = append(c.alive, inv)
		if inv.Role == storage.RoleSource {
			c.srcs = append(c.srcs, inv)
		} else {
			c.tgts = append(c.tgts, inv)
		}
		versions[inv.Version] = append(versions[inv.Version], inv.Name())
		c.ok("%-8s %-30s version=%s hostName()=%s macros=%v  (%s)", inv.Role, inv.Name(), inv.Version, inv.Hostname, inv.Macros, inv.Client.Endpoint())
	}
	if len(versions) > 1 {
		for _, v := range slices.Sorted(maps.Keys(versions)) {
			c.warn("версия %s: %s", v, strings.Join(versions[v], ", "))
		}
		c.warn("версии ClickHouse различаются — DDL может форматироваться по-разному, ставьте ту же версию на новые хосты")
	}
}

// checkMacros — макросы shard/replica: есть везде, пары уникальны, новый shard не совпадает со старым.
func (c *checker) checkMacros() {
	c.h("Макросы")
	pairs := map[string]string{}
	srcShards := map[string]bool{}
	for _, inv := range c.srcs {
		srcShards[inv.Macros["shard"]] = true
	}
	for _, inv := range c.alive {
		sh, okS := inv.Macros["shard"]
		rp, okR := inv.Macros["replica"]
		if !okS || !okR {
			c.fail("%s: нет макроса shard и/или replica", inv.Name())
			continue
		}
		if other, dup := pairs[sh+"/"+rp]; dup {
			c.fail("пара shard=%s replica=%s одинакова на %s и %s", sh, rp, other, inv.Name())
		}
		pairs[sh+"/"+rp] = inv.Name()
	}
	tgtShards := map[string][]string{}
	for _, inv := range c.tgts {
		sh := inv.Macros["shard"]
		tgtShards[sh] = append(tgtShards[sh], inv.Name())
		if srcShards[sh] {
			c.fail("%s: shard=%s уже используется старым шардом — новые таблицы станут репликами старого шарда!", inv.Name(), sh)
		}
	}
	want := c.cfg.Target.ReplicasPerShard
	for _, sh := range slices.Sorted(maps.Keys(tgtShards)) {
		hs := tgtShards[sh]
		if want > 0 && len(hs) != want {
			c.fail("новый шард %s: в target.hosts %d хост(ов) (%s), ожидается replicas_per_shard=%d", sh, len(hs), strings.Join(hs, ", "), want)
		} else {
			c.ok("новый шард %s: %s", sh, strings.Join(hs, ", "))
		}
	}
	repsByName := map[string][]string{}
	for _, inv := range c.alive {
		repsByName[inv.Macros["replica"]] = append(repsByName[inv.Macros["replica"]], inv.Name())
	}
	for _, rp := range slices.Sorted(maps.Keys(repsByName)) {
		if hs := repsByName[rp]; len(hs) > 1 {
			c.warn("replica=%s повторяется на %s — безопасно, только если ВСЕ ZK-пути содержат {shard}; таблицы с общим путём сломаются", rp, strings.Join(hs, ", "))
		}
	}
}

// checkTopology — описание кластера в remote_servers на каждом хосте: число шардов, нашёл ли хост себя,
// одинакова ли топология везде, веса шардов.
func (c *checker) checkTopology() {
	cl := c.cfg.Cluster
	c.h("Кластер %q в remote_servers", cl)
	fps := map[string][]string{}
	for _, inv := range c.alive {
		rows := inv.ClusterRows(cl)
		if len(rows) == 0 {
			c.fail("%s: кластер %s не найден", inv.Name(), cl)
			continue
		}
		fp, n := storage.ClusterFingerprint(rows)
		fps[fp] = append(fps[fp], inv.Name())
		line := fmt.Sprintf("%-8s %-30s видит %d шард(ов)", inv.Role, inv.Name(), n)
		exp := c.cfg.Target.ExpectShards
		switch {
		case inv.OwnShard(cl) == nil:
			c.fail("%s — хост НЕ нашёл себя в кластере (is_local=0 везде: host/port в remote_servers не совпадают)", line)
		case inv.Role == storage.RoleTarget && exp > 0 && n != exp:
			c.fail("%s, ожидалось %d", line, exp)
		case inv.Role == storage.RoleSource && exp > 0 && n != exp:
			c.warn("%s — старый конфиг (нормально ДО шага переключения remote_servers)", line)
		default:
			c.ok("%s", line)
		}
	}
	switch {
	case len(fps) > 1:
		c.warn("топология кластера отличается между хостами (%d вариантов):", len(fps))
		for i, fp := range slices.Sorted(maps.Keys(fps)) {
			fmt.Fprintf(c.w, "      вариант %d: %s\n        %s\n", i+1, strings.Join(fps[fp], ", "),
				strings.ReplaceAll(strings.TrimSuffix(fp, ";"), ";", "  "))
		}
	case len(fps) == 1:
		c.ok("топология одинакова на всех хостах")
	}
}

// checkZooKeeper — target видит ZooKeeper/Keeper (без него Replicated-таблицы будут readonly).
func (c *checker) checkZooKeeper(ctx context.Context) {
	c.h("ZooKeeper/Keeper на target")
	type cnt struct {
		N uint64 `ch:"n"`
	}
	for _, inv := range c.tgts {
		if _, err := inv.Client.Select[cnt](ctx, "SELECT count() AS n FROM system.zookeeper WHERE path = '/'", nil); err != nil {
			c.fail("%s: %v", inv.Name(), truncate(err.Error(), 300))
		} else {
			c.ok("%s: доступен", inv.Name())
		}
	}
}

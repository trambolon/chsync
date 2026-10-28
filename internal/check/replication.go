package check

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"chsync/internal/schema"
	"chsync/internal/storage"
	"chsync/internal/xstrings"
)

var truncate = xstrings.Truncate

// clusterQuerySettings — запросы через clusterAllReplicas: не пропускать недоступные реплики, не висеть дольше 30 с.
var clusterQuerySettings = map[string]string{"skip_unavailable_shards": "0", "max_execution_time": "30"}

// replicaMacros — макросы одной реплики, прочитанные через clusterAllReplicas.
type replicaMacros struct {
	I uint32 `ch:"i"` // shardNum() — порядковый номер реплики в system.clusters
	P uint16 `ch:"p"` // tcpPort() — НЕзащищённый native-порт (9000), даже если в remote_servers 9440
	H string `ch:"h"` // hostName()
	S string `ch:"s"` // getMacro('shard')
	R string `ch:"r"` // getMacro('replica')
}

// sameReplica — та ли это реплика, что в строке system.clusters: совпал порт (remote_servers без TLS)
// или имя хоста (с учётом FQDN: ch-s1-r1 ↔ ch-s1-r1.prod.local). При remote_servers на 9440
// порт не совпадает никогда — tcpPort() возвращает tcp_port, а не tcp_port_secure.
func sameReplica(r storage.ClusterRow, x replicaMacros) bool {
	if r.Port == x.P {
		return true
	}
	a, b := strings.ToLower(r.HostName), strings.ToLower(x.H)
	return a != "" && b != "" && (a == b || strings.HasPrefix(a, b+".") || strings.HasPrefix(b, a+"."))
}

// checkReplication — контроль репликации шардов. Сводит вместе то, что определяет репликацию:
//   - remote_servers: сколько реплик у каждого шарда и internal_replication;
//   - макросы shard на каждой реплике: у реплик одного шарда он должен совпадать, у разных — различаться
//     (иначе по ZK это разные наборы реплик, а Distributed считает их копиями — или наоборот);
//   - target.replicas_per_shard: ожидаемое число реплик новых шардов;
//   - движки таблиц: Replicated при internal_replication=false даёт двойную запись,
//     нереплицируемая MergeTree при нескольких репликах — данные только на одной из них.
func (c *checker) checkReplication(ctx context.Context) {
	cl := c.cfg.Cluster
	c.h("Репликация шардов")
	// По чьему remote_servers смотрим: первый target, видящий кластер (у него уже новая топология).
	i := slices.IndexFunc(slices.Concat(c.tgts, c.srcs), func(inv *storage.Inventory) bool { return inv.HasCluster(cl) })
	if i < 0 {
		c.fail("кластер %s не найден ни на одном хосте", cl)
		return
	}
	ref := slices.Concat(c.tgts, c.srcs)[i]
	rows := ref.ClusterRows(cl)

	// Один host:port в нескольких шардах — опечатка в remote_servers (clusterAllReplicas такой хост схлопывает).
	addrShards := map[string][]uint32{}
	for _, r := range rows {
		a := fmt.Sprintf("%s:%d", r.HostName, r.Port)
		addrShards[a] = append(addrShards[a], r.ShardNum)
	}
	for _, a := range slices.Sorted(maps.Keys(addrShards)) {
		if sns := addrShards[a]; len(sns) > 1 {
			c.fail("хост %s указан в remote_servers %d раз (шарды %v) — опечатка: Distributed будет читать его данные несколько раз", a, len(sns), sns)
		}
	}

	macroOf := c.readReplicaMacros(ctx, ref, rows)

	// Какие шарды новые: те, в которых target-хосты нашли себя.
	newShard := map[uint32]bool{}
	for _, t := range c.tgts {
		if own := t.OwnShard(cl); own != nil {
			newShard[own[0].ShardNum] = true
		}
	}
	nRepl, nPlain, plainEx := c.tableEngineStats()

	want := c.cfg.Target.ReplicasPerShard
	macroShards := map[string][]uint32{} // значение макроса shard -> номера шардов remote_servers
	var shardNums []uint32
	for _, r := range rows {
		if len(shardNums) == 0 || shardNums[len(shardNums)-1] != r.ShardNum {
			shardNums = append(shardNums, r.ShardNum)
		}
	}
	multiReplica := false
	for _, sn := range shardNums {
		var ms []string
		for i, r := range rows {
			if m, ok := macroOf[i]; ok && r.ShardNum == sn {
				ms = append(ms, m.S)
			}
		}
		ms = xstrings.Uniq(ms)
		for _, m := range ms {
			macroShards[m] = append(macroShards[m], sn)
		}
		reps := storage.ShardRows(rows, sn)
		n := len(reps)
		multiReplica = multiReplica || n > 1
		ir, irs := -1, "?"
		if reps[0].InternalReplication != nil {
			ir = int(*reps[0].InternalReplication)
			irs = fmt.Sprint(ir)
		}
		tag := ""
		if newShard[sn] {
			tag = " (новый)"
		}
		line := fmt.Sprintf("шард %d%s: %d реплик(и), macro shard=%s, internal_replication=%s", sn, tag, n, strings.Join(ms, ","), irs)
		switch {
		case len(ms) > 1:
			c.fail("%s — у реплик одного шарда разные макросы shard: по ZK они НЕ реплицируют друг друга, а Distributed считает их копиями (SELECT вернёт неполные данные)", line)
		case n > 1 && ir == 0 && nRepl > 0:
			c.fail("%s — при internal_replication=false Distributed сам пишет в каждую реплику, а Replicated-таблицы (%d шт.) ещё и реплицируют: двойная запись. Нужно <internal_replication>true</internal_replication>", line, nRepl)
		case newShard[sn] && want > 0 && n != want:
			c.fail("%s, ожидается replicas_per_shard=%d", line, want)
		case want > 0 && n != want:
			c.warn("%s — у старого шарда не %d реплик(и)", line, want)
		case n == 1:
			c.warn("%s — без реплик: нет отказоустойчивости. Replicated-таблицы работают (одна реплика в ZK), вторую реплику можно добавить позже без пересоздания", line)
		default:
			c.ok("%s", line)
		}
	}
	for _, m := range slices.Sorted(maps.Keys(macroShards)) {
		if sns := macroShards[m]; len(sns) > 1 {
			c.fail("macro shard=%s у реплик разных шардов remote_servers %v — по ZK это один набор реплик: данные будут общими, через Distributed задвоятся", m, sns)
		}
	}
	if nPlain > 0 && multiReplica {
		c.warn("%d Distributed над нереплицируемыми MergeTree (например %s): при internal_replication=true данные попадут только на одну реплику шарда", nPlain, plainEx)
	}
	c.ok("в схеме источников: Replicated-таблиц %d, Distributed над нереплицируемыми MergeTree %d", nRepl, nPlain)
}

// readReplicaMacros — макросы каждой реплики кластера: индекс строки rows -> макросы.
// В clusterAllReplicas каждая реплика — отдельный «шард», shardNum() = её порядковый номер
// в system.clusters; дополнительно сверяем порт или имя хоста. При любом сомнении возвращает пустую map.
func (c *checker) readReplicaMacros(ctx context.Context, ref *storage.Inventory, rows []storage.ClusterRow) map[int]replicaMacros {
	macroOf := map[int]replicaMacros{}
	res, err := ref.Client.Select[replicaMacros](ctx, "SELECT shardNum() AS i, tcpPort() AS p, hostName() AS h, getMacro('shard') AS s, getMacro('replica') AS r "+
		"FROM clusterAllReplicas("+storage.QuoteString(c.cfg.Cluster)+", system.one)", clusterQuerySettings)
	if err != nil {
		c.warn("макросы реплик не прочитаны (%s) — сверка remote_servers ↔ макросы пропущена", truncate(err.Error(), 200))
		return macroOf
	}
	for _, x := range res {
		i := int(x.I) - 1
		if i < 0 || i >= len(rows) || !sameReplica(rows[i], x) {
			c.warn("реплику %s (hostName()=%s) не удалось сопоставить со строкой system.clusters — сверка макросов пропущена", fmt.Sprint(x.I), x.H)
			return map[int]replicaMacros{}
		}
		macroOf[i] = x
	}
	if len(macroOf) > 0 && len(macroOf) < len(rows) {
		c.warn("макросы прочитаны у %d реплик из %d — сверка макросов неполная", len(macroOf), len(rows))
	}
	return macroOf
}

// tableEngineStats считает по источникам (с учётом фильтров): сколько Replicated-таблиц (включая
// inner-движки MV) и сколько Distributed смотрят на нереплицируемую MergeTree.
func (c *checker) tableEngineStats() (nRepl, nPlain int, example string) {
	seen := map[schema.ObjKey]bool{}
	for _, inv := range c.srcs {
		for k, o := range inv.Objects {
			if seen[k] || c.f.SkipObject(k, o.Engine) != "" {
				continue
			}
			seen[k] = true
			switch o.Kind {
			case schema.KReplicated:
				nRepl++
			case schema.KMatView:
				if schema.ParseReplicated(o.Create) != nil {
					nRepl++
				}
			case schema.KDistributed:
				for _, lk := range schema.ExplicitRefs(o.Kind, o.DB, o.Create) {
					if lo := inv.Objects[lk]; lo != nil && lo.Kind == schema.KTable && strings.HasSuffix(lo.Engine, "MergeTree") {
						nPlain++
						if example == "" {
							example = k.String() + " → " + lk.String()
						}
					}
				}
			}
		}
	}
	return nRepl, nPlain, example
}

// checkConnectivity — каждый target достаёт до всех реплик кластера с кредами кластера.
func (c *checker) checkConnectivity(ctx context.Context) {
	cl := c.cfg.Cluster
	c.h("Связность внутри кластера (clusterAllReplicas с кредами кластера)")
	type hostRow struct {
		H string `ch:"h"`
	}
	for _, inv := range c.tgts {
		rows, err := inv.Client.Select[hostRow](ctx, "SELECT hostName() AS h FROM clusterAllReplicas("+storage.QuoteString(cl)+", system.one)", clusterQuerySettings)
		want := len(inv.ClusterRows(cl))
		switch {
		case err != nil:
			c.fail("%s: %v", inv.Name(), truncate(err.Error(), 400))
		case len(rows) != want:
			c.fail("%s: ответили %d реплик из %d", inv.Name(), len(rows), want)
		default:
			c.ok("%s: ответили все %d реплик", inv.Name(), want)
		}
	}
}

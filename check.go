package main

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
)

// ============================== check ==============================

// checker — состояние предполётной проверки: хосты по ролям и счётчики ошибок/предупреждений.
type checker struct {
	ctx               context.Context
	cfg               *Config
	w                 io.Writer
	srcs, tgts, alive []*Inventory
	errors, warns     int
}

func (c *checker) ok(f string, a ...any)   { fmt.Fprintf(c.w, "  ✓ "+f+"\n", a...) }
func (c *checker) warn(f string, a ...any) { c.warns++; fmt.Fprintf(c.w, "  ! "+f+"\n", a...) }
func (c *checker) fail(f string, a ...any) { c.errors++; fmt.Fprintf(c.w, "  ✗ "+f+"\n", a...) }
func (c *checker) h(f string, a ...any)    { fmt.Fprintf(c.w, "\n── "+f+"\n", a...) }

// runCheck — предполётная проверка всех хостов. Ничего не меняет. Возвращает число ошибок.
func runCheck(ctx context.Context, cfg *Config, invs []*Inventory, w io.Writer) int {
	c := &checker{ctx: ctx, cfg: cfg, w: w}
	c.checkHosts(invs)
	c.checkMacros()
	if cfg.Cluster != "" {
		c.checkTopology()
		c.checkReplication()
		c.checkConnectivity()
	} else {
		c.h("Кластер")
		c.warn("cluster не задан в конфиге — проверки remote_servers и репликации пропущены")
	}
	c.checkZooKeeper()
	c.checkEnvironment()
	c.checkShardingKeys()
	fmt.Fprintf(w, "\nИТОГО: ошибок %d, предупреждений %d\n", c.errors, c.warns)
	return c.errors
}

// checkHosts — доступность хостов и одинаковость версий ClickHouse.
func (c *checker) checkHosts(invs []*Inventory) {
	c.h("Подключение и версии")
	versions := map[string][]string{}
	for _, inv := range invs {
		if inv.Err != nil {
			c.fail("%-8s %s: %v", inv.Role, inv.C.Name, inv.Err)
			continue
		}
		c.alive = append(c.alive, inv)
		if inv.Role == "source" {
			c.srcs = append(c.srcs, inv)
		} else {
			c.tgts = append(c.tgts, inv)
		}
		versions[inv.Version] = append(versions[inv.Version], inv.C.Name)
		c.ok("%-8s %-30s version=%s hostName()=%s macros=%v", inv.Role, inv.C.Name, inv.Version, inv.Hostname, inv.Macros)
	}
	if len(versions) > 1 {
		for _, v := range sortedKeys(versions) {
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
			c.fail("%s: нет макроса shard и/или replica", inv.C.Name)
			continue
		}
		if other, dup := pairs[sh+"/"+rp]; dup {
			c.fail("пара shard=%s replica=%s одинакова на %s и %s", sh, rp, other, inv.C.Name)
		}
		pairs[sh+"/"+rp] = inv.C.Name
	}
	tgtShards := map[string][]string{}
	for _, inv := range c.tgts {
		sh := inv.Macros["shard"]
		tgtShards[sh] = append(tgtShards[sh], inv.C.Name)
		if srcShards[sh] {
			c.fail("%s: shard=%s уже используется старым шардом — новые таблицы станут репликами старого шарда!", inv.C.Name, sh)
		}
	}
	want := c.cfg.Target.ReplicasPerShard
	for _, sh := range sortedKeys(tgtShards) {
		hs := tgtShards[sh]
		if want > 0 && len(hs) != want {
			c.fail("новый шард %s: в target.hosts %d хост(ов) (%s), ожидается replicas_per_shard=%d", sh, len(hs), strings.Join(hs, ", "), want)
		} else {
			c.ok("новый шард %s: %s", sh, strings.Join(hs, ", "))
		}
	}
	repsByName := map[string][]string{}
	for _, inv := range c.alive {
		repsByName[inv.Macros["replica"]] = append(repsByName[inv.Macros["replica"]], inv.C.Name)
	}
	for _, rp := range sortedKeys(repsByName) {
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
		rows := inv.clusterRows(cl)
		if len(rows) == 0 {
			c.fail("%s: кластер %s не найден", inv.C.Name, cl)
			continue
		}
		fp, n := clusterFingerprint(rows)
		fps[fp] = append(fps[fp], inv.C.Name)
		line := fmt.Sprintf("%-8s %-30s видит %d шард(ов)", inv.Role, inv.C.Name, n)
		exp := c.cfg.Target.ExpectShards
		switch {
		case inv.ownShard(cl) == nil:
			c.fail("%s — хост НЕ нашёл себя в кластере (is_local=0 везде: host/port в remote_servers не совпадают)", line)
		case inv.Role == "target" && exp > 0 && n != exp:
			c.fail("%s, ожидалось %d", line, exp)
		case inv.Role == "source" && exp > 0 && n != exp:
			c.warn("%s — старый конфиг (нормально ДО шага переключения remote_servers)", line)
		default:
			c.ok("%s", line)
		}
	}
	if len(fps) > 1 {
		c.warn("топология кластера отличается между хостами (%d вариантов):", len(fps))
		for i, fp := range sortedKeys(fps) {
			fmt.Fprintf(c.w, "      вариант %d: %s\n        %s\n", i+1, strings.Join(fps[fp], ", "),
				strings.ReplaceAll(strings.TrimSuffix(fp, ";"), ";", "  "))
		}
	} else if len(fps) == 1 {
		c.ok("топология одинакова на всех хостах")
	}
}

// checkReplication — контроль репликации шардов. Сводит вместе то, что определяет репликацию:
//   - remote_servers: сколько реплик у каждого шарда и internal_replication;
//   - макросы shard на каждой реплике: у реплик одного шарда он должен совпадать, у разных — различаться
//     (иначе по ZK это разные наборы реплик, а Distributed считает их копиями — или наоборот);
//   - target.replicas_per_shard: ожидаемое число реплик новых шардов;
//   - движки таблиц: Replicated при internal_replication=false даёт двойную запись,
//     нереплицируемая MergeTree при нескольких репликах — данные только на одной из них.
func (c *checker) checkReplication() {
	cl := c.cfg.Cluster
	c.h("Репликация шардов")
	var ref *Inventory // по чьему remote_servers смотрим: первый target, видящий кластер
	for _, inv := range append(append([]*Inventory{}, c.tgts...), c.srcs...) {
		if inv.hasCluster(cl) {
			ref = inv
			break
		}
	}
	if ref == nil {
		c.fail("кластер %s не найден ни на одном хосте", cl)
		return
	}
	rows := ref.clusterRows(cl)

	// Один host:port в нескольких шардах — опечатка в remote_servers (clusterAllReplicas такой хост схлопывает).
	addrShards := map[string][]uint32{}
	for _, r := range rows {
		a := fmt.Sprintf("%s:%d", r.HostName, r.Port)
		addrShards[a] = append(addrShards[a], r.ShardNum)
	}
	for _, a := range sortedKeys(addrShards) {
		if sns := addrShards[a]; len(sns) > 1 {
			c.fail("хост %s указан в remote_servers %d раз (шарды %v) — опечатка: Distributed будет читать его данные несколько раз", a, len(sns), sns)
		}
	}

	// Макросы каждой реплики кластера. В clusterAllReplicas каждая реплика — отдельный «шард»,
	// shardNum() = её порядковый номер в system.clusters; дополнительно сверяем порт.
	type rr struct {
		I uint32 `json:"i"`
		P uint16 `json:"p"`
		S string `json:"s"`
		R string `json:"r"`
	}
	macroOf := map[int]rr{}
	res, err := Select[rr](c.ctx, ref.C, "SELECT shardNum() AS i, tcpPort() AS p, getMacro('shard') AS s, getMacro('replica') AS r "+
		"FROM clusterAllReplicas("+quoteString(cl)+", system.one)", map[string]string{"skip_unavailable_shards": "0", "max_execution_time": "30"})
	if err != nil {
		c.warn("макросы реплик не прочитаны (%s) — сверка remote_servers ↔ макросы пропущена", truncate(err.Error(), 200))
	} else {
		for _, x := range res {
			if i := int(x.I) - 1; i >= 0 && i < len(rows) && rows[i].Port == x.P {
				macroOf[i] = x
			} else {
				c.warn("порядок реплик в clusterAllReplicas не совпал с system.clusters — сверка макросов пропущена")
				macroOf = map[int]rr{}
				break
			}
		}
		if len(macroOf) > 0 && len(macroOf) < len(rows) {
			c.warn("макросы прочитаны у %d реплик из %d — сверка макросов неполная", len(macroOf), len(rows))
		}
	}

	// Какие шарды новые: те, в которых target-хосты нашли себя.
	newShard := map[uint32]bool{}
	for _, t := range c.tgts {
		if own := t.ownShard(cl); own != nil {
			newShard[own[0].ShardNum] = true
		}
	}
	nRepl, nPlain, plainEx := c.tableEngineStats()

	want := c.cfg.Target.ReplicasPerShard
	macroShards := map[string][]uint32{} // значение макроса shard -> номера шардов remote_servers
	var shardNums []uint32
	multiReplica := false
	for i, r := range rows {
		if i == 0 || r.ShardNum != rows[i-1].ShardNum {
			shardNums = append(shardNums, r.ShardNum)
		}
	}
	for _, sn := range shardNums {
		var ms []string
		for i, r := range rows {
			if m, ok := macroOf[i]; ok && r.ShardNum == sn {
				ms = append(ms, m.S)
			}
		}
		ms = uniq(ms)
		for _, m := range ms {
			macroShards[m] = append(macroShards[m], sn)
		}
		reps := shardRows(rows, sn)
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
	for _, m := range sortedKeys(macroShards) {
		if sns := macroShards[m]; len(sns) > 1 {
			c.fail("macro shard=%s у реплик разных шардов remote_servers %v — по ZK это один набор реплик: данные будут общими, через Distributed задвоятся", m, sns)
		}
	}
	if nPlain > 0 && multiReplica {
		c.warn("%d Distributed над нереплицируемыми MergeTree (например %s): при internal_replication=true данные попадут только на одну реплику шарда", nPlain, plainEx)
	}
	c.ok("в схеме источников: Replicated-таблиц %d, Distributed над нереплицируемыми MergeTree %d", nRepl, nPlain)
}

// tableEngineStats считает по источникам (с учётом фильтров): сколько Replicated-таблиц (включая
// inner-движки MV) и сколько Distributed смотрят на нереплицируемую MergeTree.
func (c *checker) tableEngineStats() (nRepl, nPlain int, example string) {
	f := newFilter(c.cfg.Filter, nil)
	seen := map[ObjKey]bool{}
	for _, inv := range c.srcs {
		for k, o := range inv.Objects {
			if seen[k] || f.SkipObject(o) != "" {
				continue
			}
			seen[k] = true
			switch o.Kind {
			case KReplicated:
				nRepl++
			case KMatView:
				if parseReplicated(o.Create) != nil {
					nRepl++
				}
			case KDistributed:
				for _, lk := range explicitRefs(o) {
					if lo := inv.Objects[lk]; lo != nil && lo.Kind == KTable && strings.HasSuffix(lo.Engine, "MergeTree") {
						nPlain++
						if example == "" {
							example = k.String() + " → " + lk.String()
						}
					}
				}
			}
		}
	}
	return
}

// checkConnectivity — каждый target достаёт до всех реплик кластера с кредами кластера.
func (c *checker) checkConnectivity() {
	cl := c.cfg.Cluster
	c.h("Связность внутри кластера (clusterAllReplicas с кредами кластера)")
	type rr struct {
		H string `json:"h"`
	}
	for _, inv := range c.tgts {
		rows, err := Select[rr](c.ctx, inv.C, "SELECT hostName() AS h FROM clusterAllReplicas("+quoteString(cl)+", system.one)",
			map[string]string{"skip_unavailable_shards": "0", "max_execution_time": "30"})
		want := len(inv.clusterRows(cl))
		switch {
		case err != nil:
			c.fail("%s: %v", inv.C.Name, truncate(err.Error(), 400))
		case len(rows) != want:
			c.fail("%s: ответили %d реплик из %d", inv.C.Name, len(rows), want)
		default:
			c.ok("%s: ответили все %d реплик", inv.C.Name, want)
		}
	}
}

// checkZooKeeper — target видит ZooKeeper/Keeper (без него Replicated-таблицы будут readonly).
func (c *checker) checkZooKeeper() {
	c.h("ZooKeeper/Keeper на target")
	type cnt struct {
		N uint64 `json:"n"`
	}
	for _, inv := range c.tgts {
		if _, err := Select[cnt](c.ctx, inv.C, "SELECT count() AS n FROM system.zookeeper WHERE path = '/'", nil); err != nil {
			c.fail("%s: %v", inv.C.Name, truncate(err.Error(), 300))
		} else {
			c.ok("%s: доступен", inv.C.Name)
		}
	}
}

// hostSpecificSettings — server_settings, которые по смыслу различаются между хостами.
var hostSpecificSettings = map[string]bool{
	"interserver_http_host": true, "interserver_https_host": true, "display_name": true, "listen_host": true, "config-file": true,
}

// checkEnvironment — у target есть всё, что есть у источников: политики хранения, диски, пользователи,
// роли, SQL-функции; изменённые server_settings совпадают с первым источником.
func (c *checker) checkEnvironment() {
	c.h("Окружение target vs источники")
	setDiff := func(what string, get func(*Inventory) []string, isErr bool) {
		need := map[string]bool{}
		for _, inv := range c.srcs {
			for _, x := range get(inv) {
				need[x] = true
			}
		}
		for _, inv := range c.tgts {
			have := map[string]bool{}
			for _, x := range get(inv) {
				have[x] = true
			}
			var miss []string
			for _, x := range sortedKeys(need) {
				if !have[x] {
					miss = append(miss, x)
				}
			}
			switch {
			case len(miss) == 0:
				c.ok("%s: %s — все есть", inv.C.Name, what)
			case isErr:
				c.fail("%s: нет %s: %s", inv.C.Name, what, strings.Join(miss, ", "))
			default:
				c.warn("%s: нет %s: %s", inv.C.Name, what, strings.Join(miss, ", "))
			}
		}
	}
	setDiff("storage policies", func(i *Inventory) []string { return sortedKeys(i.StoragePolicies) }, true)
	setDiff("disks", func(i *Inventory) []string { return sortedKeys(i.Disks) }, false)
	setDiff("пользователей (SQL RBAC не реплицируется сам, если не настроен replicated user_directories)", func(i *Inventory) []string { return i.Users }, false)
	setDiff("ролей", func(i *Inventory) []string { return i.Roles }, false)
	setDiff("SQL-функций (будут созданы chsync)", func(i *Inventory) []string { return sortedKeys(i.Functions) }, false)

	if len(c.srcs) == 0 {
		return
	}
	ref := c.srcs[0]
	for _, inv := range c.tgts {
		var diffs []string
		for _, k := range sortedKeys(ref.ServerSettings) {
			if tv, ok := inv.ServerSettings[k]; !hostSpecificSettings[k] && (!ok || tv != ref.ServerSettings[k]) {
				diffs = append(diffs, fmt.Sprintf("%s: %q vs %q", k, ref.ServerSettings[k], tv))
			}
		}
		if len(diffs) == 0 {
			c.ok("%s: изменённые server_settings совпадают с %s", inv.C.Name, ref.C.Name)
		} else {
			c.warn("%s: server_settings отличаются от %s (%d): %s", inv.C.Name, ref.C.Name, len(diffs), truncate(strings.Join(diffs, "; "), 1200))
		}
	}
}

// checkShardingKeys — ключи шардирования всех Distributed: от них зависит стратегия перебалансировки
// (rand() — можно просто поднять веса; ключ — при новых шардах меняется отображение ключ→шард).
func (c *checker) checkShardingKeys() {
	if len(c.srcs) == 0 {
		return
	}
	c.h("Distributed: ключи шардирования (определяют стратегию перебалансировки)")
	var random, nokey int
	var keyed []string
	seen := map[ObjKey]bool{}
	f := newFilter(c.cfg.Filter, nil)
	for _, inv := range c.srcs {
		for k, o := range inv.Objects {
			if o.Kind != KDistributed || seen[k] || f.SkipObject(o) != "" {
				continue
			}
			seen[k] = true
			_, a, _ := engineArgs(o.Create, "Distributed")
			switch {
			case len(a) < 4:
				nokey++
			case strings.HasPrefix(strings.ToLower(a[3]), "rand"):
				random++
			default:
				keyed = append(keyed, fmt.Sprintf("%s  BY %s", k, a[3]))
			}
		}
	}
	sort.Strings(keyed)
	c.ok("rand(): %d, без ключа (вставка только в 1 шард): %d, по ключу: %d", random, nokey, len(keyed))
	if len(keyed) == 0 {
		return
	}
	c.warn("при добавлении шардов с весом > 0 отображение ключ→шард меняется для старых данных. Если запросы полагаются на локальность ключа " +
		"(optimize_skip_unused_shards, distributed_group_by_no_merge, JOIN/IN по локальным таблицам, Replacing/Collapsing по ключу) — нужен решард, см. RUNBOOK:")
	for i, s := range keyed {
		if i == 40 {
			fmt.Fprintf(c.w, "      … ещё %d\n", len(keyed)-40)
			break
		}
		fmt.Fprintf(c.w, "      %s\n", s)
	}
}

// ============================== diff ==============================

// runDiff сравнивает схему на всех хостах: «везде одинаково» или нет. Ничего не меняет.
// Учитывает те же исключения, что и plan (включая каскад). Возвращает число расхождений.
func runDiff(cfg *Config, f *Filter, invs []*Inventory, w io.Writer, verbose bool) int {
	var alive []*Inventory
	for _, inv := range invs {
		if inv.Err != nil {
			fmt.Fprintf(w, "  ✗ %s недоступен: %v\n", inv.C.Name, inv.Err)
			continue
		}
		alive = append(alive, inv)
	}
	c2 := *cfg // расхождения тут — предмет отчёта, а не блокировка
	c2.Source.OnPartial, c2.Source.OnConflict = "warn", "first"
	u := buildUnion(&c2, f, alive)

	type entry struct {
		kind   Kind
		engine string
		norms  map[string]string // хост -> нормализованный DDL
	}
	all := map[string]*entry{}
	add := func(key string, k Kind, eng, host, norm string) {
		if all[key] == nil {
			all[key] = &entry{kind: k, engine: eng, norms: map[string]string{}}
		}
		all[key].norms[host] = norm
	}
	for _, inv := range alive {
		for name, d := range inv.DBs {
			if f.SkipDB(name) == "" {
				add("database "+name, KDatabase, d.Engine, inv.C.Name, d.Norm)
			}
		}
		for name, cq := range inv.Functions {
			if f.SkipFunction(name) == "" {
				add("function "+name, KFunction, "", inv.C.Name, normalizeDDL(cq))
			}
		}
		for k, o := range inv.Objects {
			if so := u.Objects[k]; o.Kind == KInner || f.SkipObject(o) != "" || (so != nil && so.Skip != "") {
				continue
			}
			add(k.String(), o.Kind, o.Engine, inv.C.Name, o.Norm)
		}
	}

	drift := 0
	for _, key := range sortedKeys(all) {
		e := all[key]
		var missing []string
		for _, inv := range alive {
			if _, ok := e.norms[inv.C.Name]; !ok {
				missing = append(missing, inv.C.Name)
			}
		}
		variants := map[string][]string{}
		for h, n := range e.norms {
			variants[n] = append(variants[n], h)
		}
		if len(missing) == 0 && len(variants) == 1 {
			if verbose {
				fmt.Fprintf(w, "  OK     %-11s %s\n", e.kind, key)
			}
			continue
		}
		drift++
		fmt.Fprintf(w, "  DRIFT  %-11s %-50s %s\n", e.kind, key, e.engine)
		if len(missing) > 0 {
			fmt.Fprintf(w, "         нет на (%d): %s\n", len(missing), strings.Join(missing, ", "))
		}
		if len(variants) < 2 {
			continue
		}
		// Самый частый вариант — эталон A, остальные показываем окрестностью первого различия.
		norms := sortedKeys(variants)
		sort.SliceStable(norms, func(i, j int) bool { return len(variants[norms[i]]) > len(variants[norms[j]]) })
		for i, n := range norms {
			hs := variants[n]
			sort.Strings(hs)
			fmt.Fprintf(w, "         вариант %c (%d): %s\n", 'A'+i, len(hs), strings.Join(hs, ", "))
			if i > 0 {
				a, b := diffSnippet(norms[0], n)
				fmt.Fprintf(w, "           A: …%s…\n           %c: …%s…\n", a, 'A'+i, b)
			}
		}
	}
	fmt.Fprintf(w, "\nИТОГО: хостов %d, объектов %d, расхождений %d\n", len(alive), len(all), drift)
	return drift
}

// diffSnippet — окрестность первого различия двух строк (60 символов до, 80 после).
func diffSnippet(a, b string) (string, string) {
	i := 0
	for i < len(a) && i < len(b) && a[i] == b[i] {
		i++
	}
	from := maxInt(0, i-60)
	return a[from:minInt(len(a), i+80)], b[from:minInt(len(b), i+80)]
}

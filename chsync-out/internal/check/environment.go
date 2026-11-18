package check

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"chsync/internal/schema"
	"chsync/internal/storage"
)

// hostSpecificSettings — server_settings, которые по смыслу различаются между хостами.
var hostSpecificSettings = map[string]bool{
	"interserver_http_host": true, "interserver_https_host": true, "display_name": true, "listen_host": true, "config-file": true,
}

// checkEnvironment — у target есть всё, что есть у источников: политики хранения, диски, пользователи,
// роли, SQL-функции; изменённые server_settings совпадают с первым источником.
func (c *checker) checkEnvironment() {
	c.h("Окружение target vs источники")
	keys := func(m map[string]bool) []string { return slices.Collect(maps.Keys(m)) }
	c.setDiff("storage policies", func(i *storage.Inventory) []string { return keys(i.StoragePolicies) }, true)
	c.setDiff("disks", func(i *storage.Inventory) []string { return keys(i.Disks) }, false)
	c.setDiff("пользователей (SQL RBAC не реплицируется сам, если не настроен replicated user_directories)", func(i *storage.Inventory) []string { return i.Users }, false)
	c.setDiff("ролей", func(i *storage.Inventory) []string { return i.Roles }, false)
	c.setDiff("SQL-функций (будут созданы chsync)", func(i *storage.Inventory) []string { return slices.Collect(maps.Keys(i.Functions)) }, false)

	if len(c.srcs) == 0 {
		return
	}
	ref := c.srcs[0]
	for _, inv := range c.tgts {
		var diffs []string
		for _, k := range slices.Sorted(maps.Keys(ref.ServerSettings)) {
			if tv, ok := inv.ServerSettings[k]; !hostSpecificSettings[k] && (!ok || tv != ref.ServerSettings[k]) {
				diffs = append(diffs, fmt.Sprintf("%s: %q vs %q", k, ref.ServerSettings[k], tv))
			}
		}
		if len(diffs) == 0 {
			c.ok("%s: изменённые server_settings совпадают с %s", inv.Name(), ref.Name())
		} else {
			c.warn("%s: server_settings отличаются от %s (%d): %s", inv.Name(), ref.Name(), len(diffs), truncate(strings.Join(diffs, "; "), 1200))
		}
	}
}

// setDiff — чего из того, что есть на источниках, не хватает на каждом target. isErr — ошибка, иначе предупреждение.
func (c *checker) setDiff(what string, get func(*storage.Inventory) []string, isErr bool) {
	need := map[string]bool{}
	for _, inv := range c.srcs {
		for _, x := range get(inv) {
			need[x] = true
		}
	}
	for _, inv := range c.tgts {
		have := get(inv)
		var miss []string
		for _, x := range slices.Sorted(maps.Keys(need)) {
			if !slices.Contains(have, x) {
				miss = append(miss, x)
			}
		}
		switch {
		case len(miss) == 0:
			c.ok("%s: %s — все есть", inv.Name(), what)
		case isErr:
			c.fail("%s: нет %s: %s", inv.Name(), what, strings.Join(miss, ", "))
		default:
			c.warn("%s: нет %s: %s", inv.Name(), what, strings.Join(miss, ", "))
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
	seen := map[schema.ObjKey]bool{}
	for _, inv := range c.srcs {
		for k, o := range inv.Objects {
			if o.Kind != schema.KDistributed || seen[k] || c.f.SkipObject(k, o.Engine) != "" {
				continue
			}
			seen[k] = true
			_, a, _ := schema.EngineArgs(o.Create, "Distributed")
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
	slices.Sort(keyed)
	c.ok("rand(): %d, без ключа (вставка только в 1 шард): %d, по ключу: %d", random, nokey, len(keyed))
	if len(keyed) == 0 {
		return
	}
	c.warn("при добавлении шардов с весом > 0 отображение ключ→шард меняется для старых данных. Если запросы полагаются на локальность ключа " +
		"(optimize_skip_unused_shards, distributed_group_by_no_merge, JOIN/IN по локальным таблицам, Replacing/Collapsing по ключу) — нужен решард, см. RUNBOOK:")
	const maxShown = 40
	for i, s := range keyed {
		if i == maxShown {
			fmt.Fprintf(c.w, "      … ещё %d\n", len(keyed)-maxShown)
			break
		}
		fmt.Fprintf(c.w, "      %s\n", s)
	}
}

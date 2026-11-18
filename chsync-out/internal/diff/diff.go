// Package diff — сравнение схемы на всех хостах (команда diff): «везде одинаково» или нет. Ничего не меняет.
package diff

import (
	"cmp"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"

	"chsync/internal/config"
	"chsync/internal/filter"
	"chsync/internal/plan"
	"chsync/internal/schema"
	"chsync/internal/storage"
)

// entry — один объект (БД, функция, таблица...) на всех хостах.
type entry struct {
	kind   schema.Kind
	engine string
	norms  map[string]string // хост -> нормализованный DDL
}

// Run сравнивает схему на всех хостах. Учитывает те же исключения, что и plan (включая каскад).
// Возвращает число расхождений.
func Run(cfg *config.Config, f *filter.Filter, invs []*storage.Inventory, w io.Writer, verbose bool) int {
	var alive []*storage.Inventory
	for _, inv := range invs {
		if inv.Err != nil {
			fmt.Fprintf(w, "  ✗ %s недоступен: %v\n", inv.Name(), inv.Err)
			continue
		}
		alive = append(alive, inv)
	}
	c2 := *cfg // расхождения тут — предмет отчёта, а не блокировка
	c2.Source.OnPartial, c2.Source.OnConflict = config.PartialWarn, config.ConflictFirst
	u := plan.BuildUnion(&c2, f, alive)

	all := map[string]*entry{}
	add := func(key string, k schema.Kind, eng, host, norm string) {
		if all[key] == nil {
			all[key] = &entry{kind: k, engine: eng, norms: map[string]string{}}
		}
		all[key].norms[host] = norm
	}
	for _, inv := range alive {
		for name, d := range inv.DBs {
			if f.SkipDB(name) == "" {
				add("database "+name, schema.KDatabase, d.Engine, inv.Name(), d.Norm)
			}
		}
		for name, cq := range inv.Functions {
			if f.SkipFunction(name) == "" {
				add("function "+name, schema.KFunction, "", inv.Name(), schema.Normalize(cq))
			}
		}
		for k, o := range inv.Objects {
			if so := u.Objects[k]; o.Kind == schema.KInner || f.SkipObject(k, o.Engine) != "" || (so != nil && so.Skip != "") {
				continue
			}
			add(k.String(), o.Kind, o.Engine, inv.Name(), o.Norm)
		}
	}

	drift := 0
	for _, key := range slices.Sorted(maps.Keys(all)) {
		if report(w, key, all[key], alive, verbose) {
			drift++
		}
	}
	fmt.Fprintf(w, "\nИТОГО: хостов %d, объектов %d, расхождений %d\n", len(alive), len(all), drift)
	return drift
}

// report печатает объект, если он отсутствует где-то или его DDL различается. Возвращает true при расхождении.
func report(w io.Writer, key string, e *entry, alive []*storage.Inventory, verbose bool) bool {
	var missing []string
	for _, inv := range alive {
		if _, ok := e.norms[inv.Name()]; !ok {
			missing = append(missing, inv.Name())
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
		return false
	}
	fmt.Fprintf(w, "  DRIFT  %-11s %-50s %s\n", e.kind, key, e.engine)
	if len(missing) > 0 {
		fmt.Fprintf(w, "         нет на (%d): %s\n", len(missing), strings.Join(missing, ", "))
	}
	if len(variants) < 2 {
		return true
	}
	// Самый частый вариант — эталон A, остальные показываем окрестностью первого различия.
	norms := slices.Sorted(maps.Keys(variants))
	slices.SortStableFunc(norms, func(a, b string) int { return cmp.Compare(len(variants[b]), len(variants[a])) })
	for i, n := range norms {
		hs := variants[n]
		slices.Sort(hs)
		fmt.Fprintf(w, "         вариант %c (%d): %s\n", 'A'+i, len(hs), strings.Join(hs, ", "))
		if i > 0 {
			a, b := snippet(norms[0], n)
			fmt.Fprintf(w, "           A: …%s…\n           %c: …%s…\n", a, 'A'+i, b)
		}
	}
	return true
}

// snippet — окрестность первого различия двух строк (60 символов до, 80 после).
func snippet(a, b string) (string, string) {
	i := 0
	for i < len(a) && i < len(b) && a[i] == b[i] {
		i++
	}
	from := max(0, i-60)
	return a[from:min(len(a), i+80)], b[from:min(len(b), i+80)]
}

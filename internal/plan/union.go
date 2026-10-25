// Package plan — сердце chsync: объединение схем источников, порядок создания объектов,
// план по каждому target, выполнение CREATE (apply) и сверка результата (verify).
package plan

import (
	"fmt"
	"slices"
	"strings"

	"chsync/internal/config"
	"chsync/internal/filter"
	"chsync/internal/schema"
	"chsync/internal/storage"
)

// SrcObj — объект в объединённой схеме источников.
type SrcObj struct {
	*storage.Obj
	Source   string              // источник, с которого взят DDL
	Hosts    []string            // источники, на которых объект есть
	Variants map[string][]string // нормализованный DDL -> источники (больше одного — дрейф схемы)
	Skip     string              // причина исключения (фильтр или каскад)
	Block    string              // причина блокировки (конфликт DDL, секреты и т.п.)
	Warn     []string
	Deps     []schema.ObjKey // от чего зависит: создаётся только после них
}

// SrcDB — база данных в объединённой схеме.
type SrcDB struct {
	*storage.DBInfo
	Source     string
	Variants   map[string][]string
	Skip       string
	Block      string
	TablesSkip string // почему таблицы этой БД не переносим (Replicated/MySQL/... движок БД)
}

// SrcFunc — SQL-функция в объединённой схеме.
type SrcFunc struct {
	Name, Create, Norm, Source string
	Skip, Block                string
}

// Union — объединённая схема всех источников и порядок создания объектов.
type Union struct {
	DBs      map[string]*SrcDB
	Objects  map[schema.ObjKey]*SrcObj
	Funcs    map[string]*SrcFunc
	Order    []schema.ObjKey // топологический порядок неисключённых объектов
	Warnings []string
	Sources  []*storage.Inventory
}

// tableSyncDBEngines — движки БД, таблицы которых переносим поштучно.
var tableSyncDBEngines = map[string]bool{"Atomic": true, "Ordinary": true, "Lazy": true, "Memory": true}

// BuildUnion объединяет схемы источников: находит расхождения между ними (дрейф), применяет фильтры
// и каскадное исключение, строит зависимости и порядок создания. Недоступные источники пропускаются.
func BuildUnion(cfg *config.Config, f *filter.Filter, srcs []*storage.Inventory) *Union {
	u := &Union{DBs: map[string]*SrcDB{}, Objects: map[schema.ObjKey]*SrcObj{}, Funcs: map[string]*SrcFunc{}, Sources: srcs}
	alive := slices.DeleteFunc(slices.Clone(srcs), func(inv *storage.Inventory) bool { return inv.Err != nil })
	conflictBlocks := cfg.Source.OnConflict == config.ConflictError

	u.mergeDatabases(alive, f, conflictBlocks, cfg.DDL.PreserveUUID)
	if cfg.DDL.SyncFunctions {
		u.mergeFunctions(alive, f, conflictBlocks)
	}
	u.mergeObjects(alive, f, cfg)
	u.cascadeExclude(cfg.Filter.CascadeExclude)
	u.buildDeps()
	return u
}

// mergeDatabases — 1. базы данных.
func (u *Union) mergeDatabases(alive []*storage.Inventory, f *filter.Filter, conflictBlocks, preserve bool) {
	for _, inv := range alive {
		for name, d := range inv.DBs {
			sd, ok := u.DBs[name]
			if !ok {
				sd = &SrcDB{DBInfo: d, Source: inv.Name(), Variants: map[string][]string{}, Skip: f.SkipDB(name)}
				switch {
				case strings.HasPrefix(d.Engine, "Replicated"):
					sd.TablesSkip = "таблицами управляет движок БД Replicated (синхронизирует сам)"
				case !tableSyncDBEngines[d.Engine]:
					sd.TablesSkip = "движок БД " + d.Engine + ": таблицы не синхронизируются"
				}
				u.DBs[name] = sd
			}
			if d.Norm != "" {
				sd.Variants[d.Norm] = append(sd.Variants[d.Norm], inv.Name())
			}
		}
	}
	for name, sd := range u.DBs {
		if sd.Skip != "" {
			continue
		}
		if len(sd.Variants) > 1 {
			msg := "DDL базы различается между источниками: " + variantsSummary(sd.Variants)
			if conflictBlocks {
				sd.Block = msg
			} else {
				u.Warnings = append(u.Warnings, "БД "+name+": "+msg+" (взят "+sd.Source+")")
			}
		}
		if schema.HasHiddenSecrets(sd.Create) {
			sd.Block = "в DDL замаскированы секреты [HIDDEN] (см. ddl.display_secrets)"
		}
		if !preserve {
			c := *sd.DBInfo // копия, чтобы не портить снимок источника
			c.Create = schema.StripUUID(c.Create)
			sd.DBInfo = &c
		}
	}
}

// mergeFunctions — 2. SQL-функции.
func (u *Union) mergeFunctions(alive []*storage.Inventory, f *filter.Filter, conflictBlocks bool) {
	vars := map[string]map[string][]string{}
	for _, inv := range alive {
		for name, cq := range inv.Functions {
			n := schema.Normalize(cq)
			if _, ok := u.Funcs[name]; !ok {
				u.Funcs[name] = &SrcFunc{Name: name, Create: cq, Norm: n, Source: inv.Name(), Skip: f.SkipFunction(name)}
				vars[name] = map[string][]string{}
			}
			vars[name][n] = append(vars[name][n], inv.Name())
		}
	}
	for name, v := range vars {
		if len(v) > 1 && conflictBlocks {
			u.Funcs[name].Block = "определение функции различается: " + variantsSummary(v)
		}
	}
}

// mergeObjects — 3. таблицы, вью, словари.
func (u *Union) mergeObjects(alive []*storage.Inventory, f *filter.Filter, cfg *config.Config) {
	for _, inv := range alive {
		for k, o := range inv.Objects {
			if o.Kind == schema.KInner {
				continue // создастся вместе со своей MV
			}
			so, ok := u.Objects[k]
			if !ok {
				so = &SrcObj{Obj: o, Source: inv.Name(), Variants: map[string][]string{}}
				u.Objects[k] = so
			}
			so.Hosts = append(so.Hosts, inv.Name())
			so.Variants[o.Norm] = append(so.Variants[o.Norm], inv.Name())
		}
	}
	for k, so := range u.Objects {
		if r := f.SkipObject(k, so.Engine); r != "" {
			so.Skip = r
			continue
		}
		if sd := u.DBs[k.DB]; sd != nil && sd.TablesSkip != "" {
			so.Skip = sd.TablesSkip
			continue
		}
		if len(so.Hosts) < len(alive) {
			msg := fmt.Sprintf("есть только на %d из %d источников: %s", len(so.Hosts), len(alive), strings.Join(so.Hosts, ", "))
			if cfg.Source.OnPartial == config.PartialSkip {
				so.Skip = msg
				continue
			}
			so.Warn = append(so.Warn, msg)
		}
		if len(so.Variants) > 1 {
			msg := "DDL различается между источниками: " + variantsSummary(so.Variants)
			if cfg.Source.OnConflict == config.ConflictError {
				so.Block = msg
			} else {
				so.Warn = append(so.Warn, msg+" (взят "+so.Source+")")
			}
		}
		switch {
		case so.Create == "":
			so.Block = "сервер не вернул DDL (create_table_query пуст)"
		case schema.HasHiddenSecrets(so.Create):
			so.Block = "в DDL замаскированы секреты [HIDDEN]: включите display_secrets_in_show_and_select на источнике и ddl.display_secrets=true, или создайте объект вручную"
		}
		if !cfg.DDL.PreserveUUID {
			c := *so.Obj // копия, чтобы не портить снимок источника
			c.Create = schema.StripUUID(c.Create)
			so.Obj = &c
		}
	}
}

// variantsSummary описывает расхождение: "2 варианта: [h1,h2] vs [h3]".
func variantsSummary(v map[string][]string) string {
	parts := make([]string, 0, len(v))
	for _, hosts := range v {
		parts = append(parts, "["+strings.Join(hosts, ",")+"]")
	}
	slices.Sort(parts)
	return fmt.Sprintf("%d варианта: %s", len(v), strings.Join(parts, " vs "))
}

package plan

import (
	"context"
	"fmt"
	"iter"
	"maps"
	"slices"
	"strings"

	"chsync/internal/config"
	"chsync/internal/filter"
	"chsync/internal/schema"
	"chsync/internal/storage"
	"chsync/internal/xstrings"
)

// Action — что будет с объектом на target.
type Action int

const (
	ACreate  Action = iota // будет создан
	AExists                // уже есть и совпадает
	ADrift                 // есть, но отличается (не трогаем)
	ABlocked               // создать нельзя, причина в Reason
	ASkip                  // исключён фильтром/каскадом
	AExtra                 // есть на target, нет на источниках
)

func (a Action) String() string {
	return [...]string{"CREATE", "EXISTS", "DRIFT", "BLOCKED", "SKIP", "EXTRA"}[a]
}

// Item — строка плана.
type Item struct {
	Kind    schema.Kind
	Name    string // db.name, имя БД или функции
	DB      string // БД, в контексте которой выполняется CREATE
	Engine  string
	Action  Action
	Reason  string
	Warn    []string
	Info    string // для Replicated: ожидаемые zk-путь и реплика
	SQL     string
	Source  string
	ExpZK   string // ожидаемые zookeeper_path / replica_name — проверяются после apply
	ExpRepl string
	key     schema.ObjKey
}

// TargetPlan — план для одного target-хоста.
type TargetPlan struct {
	Inv    *storage.Inventory
	Items  []*Item
	Counts map[Action]int
}

func (tp *TargetPlan) add(it *Item) {
	tp.Items = append(tp.Items, it)
	tp.Counts[it.Action]++
}

// ByAction — элементы плана с действием a (в порядке плана, т.е. в порядке создания).
func (tp *TargetPlan) ByAction(a Action) iter.Seq[*Item] {
	return func(yield func(*Item) bool) {
		for _, it := range tp.Items {
			if it.Action == a && !yield(it) {
				return
			}
		}
	}
}

// Creates — элементы плана, которые нужно создать (в порядке создания).
func (tp *TargetPlan) Creates() []*Item { return slices.Collect(tp.ByAction(ACreate)) }

// Planner строит планы по target-хостам на основе объединённой схемы.
type Planner struct {
	cfg     *config.Config
	f       *filter.Filter
	u       *Union
	srcByID map[string]*storage.Inventory
	targets []*storage.Inventory // все target (в т.ч. неактивные) — для распознавания соседних реплик
}

// NewPlanner создаёт планировщик. targets — все target из конфига: снимки неактивных
// (вне --targets) нужны, чтобы узнать соседнюю реплику нового шарда в ZK.
func NewPlanner(cfg *config.Config, f *filter.Filter, u *Union, targets []*storage.Inventory) *Planner {
	p := &Planner{cfg: cfg, f: f, u: u, targets: targets, srcByID: map[string]*storage.Inventory{}}
	for _, s := range u.Sources {
		p.srcByID[s.Name()] = s
	}
	return p
}

// prep добавляет IF NOT EXISTS, если это включено в конфиге.
func (p *Planner) prep(sql string) string {
	if p.cfg.DDL.IfNotExists {
		return schema.AddIfNotExists(sql)
	}
	return sql
}

// PlanTarget сравнивает объединённую схему со снимком target и решает судьбу каждого объекта.
func (p *Planner) PlanTarget(ctx context.Context, t *storage.Inventory) *TargetPlan {
	tp := &TargetPlan{Inv: t, Counts: map[Action]int{}}
	p.planDatabases(tp)
	p.planFunctions(tp)
	p.planObjects(ctx, tp)

	// Исключённые и лишние (есть на target, нет на источниках) — для полноты отчёта.
	for _, k := range slices.SortedFunc(maps.Keys(p.u.Objects), schema.ObjKey.Compare) {
		if so := p.u.Objects[k]; so.Skip != "" {
			tp.add(&Item{Kind: so.Kind, Name: k.String(), Engine: so.Engine, Action: ASkip, Reason: so.Skip})
		}
	}
	for _, k := range slices.SortedFunc(maps.Keys(t.Objects), schema.ObjKey.Compare) {
		o := t.Objects[k]
		if _, ok := p.u.Objects[k]; !ok && o.Kind != schema.KInner && p.f.SkipObject(k, o.Engine) == "" {
			tp.add(&Item{Kind: o.Kind, Name: k.String(), Engine: o.Engine, Action: AExtra, Reason: "есть на target, нет на источниках"})
		}
	}
	return tp
}

// planDatabases — 1. базы данных.
func (p *Planner) planDatabases(tp *TargetPlan) {
	for _, n := range slices.Sorted(maps.Keys(p.u.DBs)) {
		sd := p.u.DBs[n]
		it := &Item{Kind: schema.KDatabase, Name: n, Engine: sd.Engine, Source: sd.Source}
		td, exists := tp.Inv.DBs[n]
		switch {
		case sd.Skip != "":
			it.Action, it.Reason = ASkip, sd.Skip
		case exists && td.Engine != sd.Engine:
			it.Action, it.Reason = ADrift, fmt.Sprintf("движок БД на target %s, на источнике %s", td.Engine, sd.Engine)
		case exists:
			it.Action = AExists
		case sd.Block != "":
			it.Action, it.Reason = ABlocked, sd.Block
		default:
			it.Action, it.SQL = ACreate, p.prep(sd.Create)
			if sd.TablesSkip != "" {
				it.Warn = append(it.Warn, sd.TablesSkip)
			}
			if sd.Engine == "Ordinary" {
				it.Warn = append(it.Warn, "Ordinary-БД: может понадобиться ddl.settings.allow_deprecated_database_ordinary = 1")
			}
		}
		tp.add(it)
	}
}

// planFunctions — 2. SQL-функции.
func (p *Planner) planFunctions(tp *TargetPlan) {
	for _, n := range slices.Sorted(maps.Keys(p.u.Funcs)) {
		sf := p.u.Funcs[n]
		it := &Item{Kind: schema.KFunction, Name: n, Source: sf.Source}
		tc, exists := tp.Inv.Functions[n]
		switch {
		case sf.Skip != "":
			it.Action, it.Reason = ASkip, sf.Skip
		case exists && schema.Normalize(tc) == sf.Norm:
			it.Action = AExists
		case exists:
			it.Action, it.Reason = ADrift, "определение функции отличается"
		case sf.Block != "":
			it.Action, it.Reason = ABlocked, sf.Block
		default:
			it.Action, it.SQL = ACreate, p.prep(sf.Create)
		}
		tp.add(it)
	}
}

// planObjects — 3. таблицы, вью, словари в порядке создания.
// Сначала раскрываем ZK-пути всех Replicated, чтобы проверить их в ZK одним пакетом.
func (p *Planner) planObjects(ctx context.Context, tp *TargetPlan) {
	t := tp.Inv
	repl := map[schema.ObjKey]replTarget{}
	var paths []string
	for _, k := range p.u.Order {
		so := p.u.Objects[k]
		if _, exists := t.Objects[k]; exists || so.Block != "" {
			continue
		}
		if rt := p.replicaTarget(so, t); rt.ri != nil {
			repl[k] = rt
			if rt.err == "" {
				paths = append(paths, rt.path+"/replicas")
			}
		}
	}
	zk := zkState{}
	if p.cfg.DDL.CheckZooKeeper && len(paths) > 0 {
		zk.children, zk.err = storage.ZKChildren(ctx, t.Client, paths)
	}

	state := map[schema.ObjKey]Action{}
	for _, k := range p.u.Order {
		so := p.u.Objects[k]
		it := &Item{Kind: so.Kind, Name: k.String(), DB: k.DB, Engine: so.Engine, Source: so.Source, key: k}
		it.Warn = append(it.Warn, so.Warn...)
		to, exists := t.Objects[k]
		switch {
		case exists && to.Norm != so.Norm:
			it.Action, it.Reason = ADrift, "DDL на target отличается от источника (не трогаем, см. diff)"
		case exists:
			it.Action = AExists
		case so.Block != "":
			it.Action, it.Reason = ABlocked, so.Block
		default:
			for _, d := range so.Deps { // зависимость не будет создана — и этот объект тоже
				if st := state[d]; st == ABlocked || st == ASkip {
					it.Action, it.Reason = ABlocked, "зависит от "+d.String()+" ("+st.String()+")"
					break
				}
			}
			if it.Action != ABlocked {
				p.validate(so, t, it, repl[k], zk)
			}
			if it.Action == ACreate {
				it.SQL = p.prep(so.Create)
			}
		}
		state[k] = it.Action
		tp.add(it)
	}
}

// zkState — результат пакетной проверки ZK-путей на target.
type zkState struct {
	children map[string][]string // path/replicas -> имена реплик; пути нет в map — состояние неизвестно
	err      error               // часть путей проверить не удалось
}

// replTarget — ZK-путь и имя реплики, которые получит Replicated-объект на конкретном target.
type replTarget struct {
	ri                 *schema.ReplInfo
	tmplPath, tmplRepl string // шаблоны (из DDL или default_replica_* target)
	path, repl         string // раскрытые макросами target
	err                string // раскрыть нельзя — объект блокируется
}

// replicaTarget раскрывает ZK-путь Replicated-таблицы (или inner-движка MV) макросами target.
func (p *Planner) replicaTarget(so *SrcObj, t *storage.Inventory) replTarget {
	var rt replTarget
	if so.Kind != schema.KReplicated && so.Kind != schema.KMatView {
		return rt
	}
	if rt.ri = schema.ParseReplicated(so.Create); rt.ri == nil {
		return rt
	}
	rt.tmplPath, rt.tmplRepl = rt.ri.PathTmpl, rt.ri.ReplicaTmpl
	if rt.ri.Defaults {
		rt.tmplPath, rt.tmplRepl = t.ServerSettings["default_replica_path"], t.ServerSettings["default_replica_name"]
	}
	if strings.Contains(rt.tmplPath, "{uuid}") && rt.ri.UUID == "" {
		rt.err = "ZK-путь содержит {uuid}, но UUID объекта не определён (Ordinary-БД?)"
		if !p.cfg.DDL.PreserveUUID {
			rt.err = "ZK-путь содержит {uuid}, а preserve_uuid=false: реплики нового шарда получат разные UUID и НЕ будут реплицироваться между собой"
		}
		return rt
	}
	special := specialMacros(so, rt.ri)
	var m1, m2 []string
	rt.path, m1 = schema.ExpandMacros(rt.tmplPath, t.Macros, special)
	rt.repl, m2 = schema.ExpandMacros(rt.tmplRepl, t.Macros, special)
	if miss := xstrings.Uniq(append(m1, m2...)); len(miss) > 0 {
		rt.err = "на target не определены макросы: {" + strings.Join(miss, "}, {") + "}"
	}
	return rt
}

// specialMacros — макросы, которые сервер раскрывает сам: {database}, {table}, {uuid}.
func specialMacros(so *SrcObj, ri *schema.ReplInfo) map[string]string {
	return map[string]string{"database": so.DB, "table": so.Name, "uuid": ri.UUID}
}

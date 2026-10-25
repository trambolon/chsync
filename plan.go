package main

import (
	"container/heap"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

// ============================== Объединение источников ==============================

// SrcObj — объект в объединённой схеме источников.
type SrcObj struct {
	*Obj
	Source   string              // источник, с которого взят DDL
	Hosts    []string            // источники, на которых объект есть
	Variants map[string][]string // нормализованный DDL -> источники (больше одного — дрейф схемы)
	Skip     string              // причина исключения (фильтр или каскад)
	Block    string              // причина блокировки (конфликт DDL, секреты и т.п.)
	Warn     []string
	Deps     []ObjKey // от чего зависит: создаётся только после них
}

// SrcDB — база данных в объединённой схеме.
type SrcDB struct {
	*DBInfo
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
	Objects  map[ObjKey]*SrcObj
	Funcs    map[string]*SrcFunc
	Order    []ObjKey // топологический порядок неисключённых объектов
	Warnings []string
	Sources  []*Inventory
}

// tableSyncDBEngines — движки БД, таблицы которых переносим поштучно.
var tableSyncDBEngines = map[string]bool{"Atomic": true, "Ordinary": true, "Lazy": true, "Memory": true}

// buildUnion объединяет схемы источников: находит расхождения между ними (дрейф), применяет фильтры
// и каскадное исключение, строит зависимости и порядок создания.
func buildUnion(cfg *Config, f *Filter, srcs []*Inventory) *Union {
	u := &Union{DBs: map[string]*SrcDB{}, Objects: map[ObjKey]*SrcObj{}, Funcs: map[string]*SrcFunc{}, Sources: srcs}
	preserve := boolOr(cfg.DDL.PreserveUUID, true)
	conflictBlocks := cfg.Source.OnConflict == "error"
	nSrc := 0
	for _, inv := range srcs {
		if inv.Err == nil {
			nSrc++
		}
	}

	// 1. Базы данных.
	for _, inv := range srcs {
		if inv.Err != nil {
			continue
		}
		for name, d := range inv.DBs {
			sd, ok := u.DBs[name]
			if !ok {
				sd = &SrcDB{DBInfo: d, Source: inv.C.Name, Variants: map[string][]string{}, Skip: f.SkipDB(name)}
				switch {
				case strings.HasPrefix(d.Engine, "Replicated"):
					sd.TablesSkip = "таблицами управляет движок БД Replicated (синхронизирует сам)"
				case !tableSyncDBEngines[d.Engine]:
					sd.TablesSkip = "движок БД " + d.Engine + ": таблицы не синхронизируются"
				}
				u.DBs[name] = sd
			}
			if d.Norm != "" {
				sd.Variants[d.Norm] = append(sd.Variants[d.Norm], inv.C.Name)
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
		if hasHiddenSecrets(sd.Create) {
			sd.Block = "в DDL замаскированы секреты [HIDDEN] (см. ddl.display_secrets)"
		}
		if !preserve {
			sd.Create = stripUUID(sd.Create)
		}
	}

	// 2. SQL-функции.
	if boolOr(cfg.DDL.SyncFunctions, true) {
		vars := map[string]map[string][]string{}
		for _, inv := range srcs {
			if inv.Err != nil {
				continue
			}
			for name, cq := range inv.Functions {
				n := normalizeDDL(cq)
				if _, ok := u.Funcs[name]; !ok {
					u.Funcs[name] = &SrcFunc{Name: name, Create: cq, Norm: n, Source: inv.C.Name, Skip: f.SkipFunction(name)}
					vars[name] = map[string][]string{}
				}
				vars[name][n] = append(vars[name][n], inv.C.Name)
			}
		}
		for name, v := range vars {
			if len(v) > 1 && conflictBlocks {
				u.Funcs[name].Block = "определение функции различается: " + variantsSummary(v)
			}
		}
	}

	// 3. Таблицы, вью, словари.
	for _, inv := range srcs {
		if inv.Err != nil {
			continue
		}
		for k, o := range inv.Objects {
			if o.Kind == KInner {
				continue // создастся вместе со своей MV
			}
			so, ok := u.Objects[k]
			if !ok {
				so = &SrcObj{Obj: o, Source: inv.C.Name, Variants: map[string][]string{}}
				u.Objects[k] = so
			}
			so.Hosts = append(so.Hosts, inv.C.Name)
			so.Variants[o.Norm] = append(so.Variants[o.Norm], inv.C.Name)
		}
	}
	for k, so := range u.Objects {
		if r := f.SkipObject(so.Obj); r != "" {
			so.Skip = r
			continue
		}
		if sd := u.DBs[k.DB]; sd != nil && sd.TablesSkip != "" {
			so.Skip = sd.TablesSkip
			continue
		}
		if len(so.Hosts) < nSrc {
			msg := fmt.Sprintf("есть только на %d из %d источников: %s", len(so.Hosts), nSrc, strings.Join(so.Hosts, ", "))
			if cfg.Source.OnPartial == "skip" {
				so.Skip = msg
				continue
			}
			so.Warn = append(so.Warn, msg)
		}
		if len(so.Variants) > 1 {
			msg := "DDL различается между источниками: " + variantsSummary(so.Variants)
			if conflictBlocks {
				so.Block = msg
			} else {
				so.Warn = append(so.Warn, msg+" (взят "+so.Source+")")
			}
		}
		switch {
		case so.Create == "":
			so.Block = "сервер не вернул DDL (create_table_query пуст)"
		case hasHiddenSecrets(so.Create):
			so.Block = "в DDL замаскированы секреты [HIDDEN]: включите display_secrets_in_show_and_select на источнике и ddl.display_secrets=true, или создайте объект вручную"
		}
		if !preserve {
			c := *so.Obj // копия, чтобы не портить снимок источника
			c.Create = stripUUID(c.Create)
			so.Obj = &c
		}
	}

	u.cascadeExclude(boolOr(cfg.Filter.CascadeExclude, true))
	u.buildDeps()
	return u
}

// variantsSummary описывает расхождение: "2 варианта: [h1,h2] vs [h3]".
func variantsSummary(v map[string][]string) string {
	var parts []string
	for _, hosts := range v {
		parts = append(parts, "["+strings.Join(hosts, ",")+"]")
	}
	sort.Strings(parts)
	return fmt.Sprintf("%d варианта: %s", len(v), strings.Join(parts, " vs "))
}

// nameIndex — индекс «имя объекта -> ключи» по объектам, удовлетворяющим pred.
func nameIndex(objs map[ObjKey]*SrcObj, pred func(*SrcObj) bool) map[string][]ObjKey {
	idx := map[string][]ObjKey{}
	for k, o := range objs {
		if pred(o) {
			idx[k.Name] = append(idx[k.Name], k)
		}
	}
	return idx
}

// refs — на какие объекты из индекса idx ссылается o (с учётом допустимых видов kindOK).
func (u *Union) refs(o *SrcObj, idx map[string][]ObjKey, kindOK func(*SrcObj) bool) []ObjKey {
	found := map[ObjKey]bool{}
	for _, k := range explicitRefs(o.Obj) {
		if p, ok := u.Objects[k]; ok && k != o.Key() && kindOK(p) && containsKey(idx[k.Name], k) {
			found[k] = true
		}
	}
	body := o.Body()
	for tok := range o.Tokens() {
		for _, k := range idx[tok] {
			if k != o.Key() && !found[k] && kindOK(u.Objects[k]) && refersTo(o.Obj, k, body) {
				found[k] = true
			}
		}
	}
	out := make([]ObjKey, 0, len(found))
	for k := range found {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}

func containsKey(ks []ObjKey, k ObjKey) bool {
	for _, x := range ks {
		if x == k {
			return true
		}
	}
	return false
}

// dependsKinds — от объектов каких видов может зависеть объект вида k при CREATE.
// Обычным таблицам нужны только словари (dictGet в DEFAULT/MATERIALIZED).
func dependsKinds(k Kind) func(*SrcObj) bool {
	switch k {
	case KDistributed, KProxy, KView, KMatView, KOtherView, KDictionary:
		return func(*SrcObj) bool { return true }
	case KTable, KReplicated, KStreaming:
		return func(p *SrcObj) bool { return p.Kind == KDictionary }
	}
	return func(*SrcObj) bool { return false }
}

// cascadeExclude исключает объекты, ссылающиеся на исключённые (MV/VIEW/Distributed над bms-таблицей):
// иначе их CREATE упадёт. Повторяет, пока исключения распространяются. enabled=false — только предупреждает.
func (u *Union) cascadeExclude(enabled bool) {
	for changed := true; changed; {
		changed = false
		excl := nameIndex(u.Objects, func(o *SrcObj) bool { return o.Skip != "" })
		for _, o := range u.Objects {
			if o.Skip != "" {
				continue
			}
			r := u.refs(o, excl, dependsKinds(o.Kind))
			if len(r) == 0 {
				continue
			}
			msg := "ссылается на исключённый объект " + r[0].String()
			if enabled {
				o.Skip, changed = "cascade: "+msg, true
			} else if !containsStr(o.Warn, msg+" (CREATE может упасть)") {
				o.Warn = append(o.Warn, msg+" (CREATE может упасть)")
			}
		}
	}
}

// buildDeps вычисляет зависимости неисключённых объектов и порядок создания.
func (u *Union) buildDeps() {
	cand := nameIndex(u.Objects, func(o *SrcObj) bool { return o.Skip == "" })
	var keys []ObjKey
	for k, o := range u.Objects {
		if o.Skip != "" {
			continue
		}
		keys = append(keys, k)
		if o.Kind != KDictionary { // словари грузятся лениво: источник при CREATE не нужен
			o.Deps = u.refs(o, cand, dependsKinds(o.Kind))
		}
	}
	u.Order = topoSort(u.Objects, keys)
}

// pq — очередь с приоритетом для топосортировки: сначала вид объекта, потом имя (стабильный порядок).
type pqItem struct {
	k   ObjKey
	pri int
}
type pq []pqItem

func (p pq) Len() int { return len(p) }
func (p pq) Less(i, j int) bool {
	if p[i].pri != p[j].pri {
		return p[i].pri < p[j].pri
	}
	return p[i].k.String() < p[j].k.String()
}
func (p pq) Swap(i, j int)       { p[i], p[j] = p[j], p[i] }
func (p *pq) Push(x interface{}) { *p = append(*p, x.(pqItem)) }
func (p *pq) Pop() interface{} {
	old := *p
	x := old[len(old)-1]
	*p = old[:len(old)-1]
	return x
}

// topoSort — топологическая сортировка (Кан) с приоритетом по виду объекта.
// При цикле берёт оставшийся объект с наименьшим приоритетом и помечает его предупреждением.
func topoSort(objs map[ObjKey]*SrcObj, keys []ObjKey) []ObjKey {
	indeg := map[ObjKey]int{}
	dependents := map[ObjKey][]ObjKey{}
	inSet := map[ObjKey]bool{}
	for _, k := range keys {
		inSet[k] = true
	}
	for _, k := range keys {
		for _, d := range objs[k].Deps {
			if inSet[d] {
				indeg[k]++
				dependents[d] = append(dependents[d], k)
			}
		}
	}
	h := &pq{}
	for _, k := range keys {
		if indeg[k] == 0 {
			heap.Push(h, pqItem{k, objs[k].Kind.priority()})
		}
	}
	var out []ObjKey
	done := map[ObjKey]bool{}
	for len(out) < len(keys) {
		if h.Len() == 0 { // цикл
			var rest []pqItem
			for _, k := range keys {
				if !done[k] {
					rest = append(rest, pqItem{k, objs[k].Kind.priority()})
				}
			}
			sort.Sort(pq(rest))
			heap.Push(h, rest[0])
			objs[rest[0].k].Warn = append(objs[rest[0].k].Warn, "циклическая зависимость в DDL — порядок выбран по типу объекта")
		}
		it := heap.Pop(h).(pqItem)
		if done[it.k] {
			continue
		}
		done[it.k] = true
		out = append(out, it.k)
		for _, d := range dependents[it.k] {
			if indeg[d]--; indeg[d] == 0 && !done[d] {
				heap.Push(h, pqItem{d, objs[d].Kind.priority()})
			}
		}
	}
	return out
}

// ============================== План для одного target ==============================

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
	Kind    Kind
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
	key     ObjKey
}

// TargetPlan — план для одного target-хоста.
type TargetPlan struct {
	Inv    *Inventory
	Items  []*Item
	Counts map[Action]int
}

func (tp *TargetPlan) add(it *Item) {
	tp.Items = append(tp.Items, it)
	tp.Counts[it.Action]++
}

// creates — элементы плана, которые нужно создать (в порядке создания).
func (tp *TargetPlan) creates() []*Item {
	var out []*Item
	for _, it := range tp.Items {
		if it.Action == ACreate {
			out = append(out, it)
		}
	}
	return out
}

// planner строит планы по target-хостам на основе объединённой схемы.
type planner struct {
	cfg     *Config
	f       *Filter
	u       *Union
	srcByID map[string]*Inventory
	targets []*Inventory // все target (в т.ч. неактивные) — для распознавания соседних реплик
}

// planTarget сравнивает объединённую схему со снимком target и решает судьбу каждого объекта.
func (p *planner) planTarget(ctx context.Context, t *Inventory) *TargetPlan {
	tp := &TargetPlan{Inv: t, Counts: map[Action]int{}}
	prep := func(sql string) string {
		if boolOr(p.cfg.DDL.IfNotExists, true) {
			return addIfNotExists(sql)
		}
		return sql
	}

	// 1. Базы данных.
	for _, n := range sortedKeys(p.u.DBs) {
		sd := p.u.DBs[n]
		it := &Item{Kind: KDatabase, Name: n, Engine: sd.Engine, Source: sd.Source}
		td, exists := t.DBs[n]
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
			it.Action, it.SQL = ACreate, prep(sd.Create)
			if sd.TablesSkip != "" {
				it.Warn = append(it.Warn, sd.TablesSkip)
			}
			if sd.Engine == "Ordinary" {
				it.Warn = append(it.Warn, "Ordinary-БД: может понадобиться ddl.settings.allow_deprecated_database_ordinary = 1")
			}
		}
		tp.add(it)
	}

	// 2. SQL-функции.
	for _, n := range sortedKeys(p.u.Funcs) {
		sf := p.u.Funcs[n]
		it := &Item{Kind: KFunction, Name: n, Source: sf.Source}
		tc, exists := t.Functions[n]
		switch {
		case sf.Skip != "":
			it.Action, it.Reason = ASkip, sf.Skip
		case exists && normalizeDDL(tc) == sf.Norm:
			it.Action = AExists
		case exists:
			it.Action, it.Reason = ADrift, "определение функции отличается"
		case sf.Block != "":
			it.Action, it.Reason = ABlocked, sf.Block
		default:
			it.Action, it.SQL = ACreate, prep(sf.Create)
		}
		tp.add(it)
	}

	// 3. Объекты. Сначала раскрываем ZK-пути всех Replicated, чтобы проверить их в ZK одним пакетом.
	repl := map[ObjKey]replTarget{}
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
	var zk map[string][]string
	var zkErr string
	if boolOr(p.cfg.DDL.CheckZooKeeper, true) && len(paths) > 0 {
		zk, zkErr = zkChildren(ctx, t.C, paths)
	}

	state := map[ObjKey]Action{}
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
				p.validate(so, t, it, repl[k], zk, zkErr)
			}
			if it.Action == ACreate {
				it.SQL = prep(so.Create)
			}
		}
		state[k] = it.Action
		tp.add(it)
	}

	// 4. Исключённые и лишние (есть на target, нет на источниках) — для полноты отчёта.
	for k, so := range p.u.Objects {
		if so.Skip != "" {
			tp.add(&Item{Kind: so.Kind, Name: k.String(), Engine: so.Engine, Action: ASkip, Reason: so.Skip})
		}
	}
	for k, o := range t.Objects {
		if _, ok := p.u.Objects[k]; !ok && o.Kind != KInner && p.f.SkipObject(o) == "" {
			tp.add(&Item{Kind: o.Kind, Name: k.String(), Engine: o.Engine, Action: AExtra, Reason: "есть на target, нет на источниках"})
		}
	}
	return tp
}

// replTarget — ZK-путь и имя реплики, которые получит Replicated-объект на конкретном target.
type replTarget struct {
	ri                 *ReplInfo
	tmplPath, tmplRepl string // шаблоны (из DDL или default_replica_* target)
	path, repl         string // раскрытые макросами target
	err                string // раскрыть нельзя — объект блокируется
}

// replicaTarget раскрывает ZK-путь Replicated-таблицы (или inner-движка MV) макросами target.
func (p *planner) replicaTarget(so *SrcObj, t *Inventory) replTarget {
	var rt replTarget
	if so.Kind != KReplicated && so.Kind != KMatView {
		return rt
	}
	if rt.ri = parseReplicated(so.Create); rt.ri == nil {
		return rt
	}
	rt.tmplPath, rt.tmplRepl = rt.ri.PathTmpl, rt.ri.ReplicaTmpl
	if rt.ri.Defaults {
		rt.tmplPath, rt.tmplRepl = t.ServerSettings["default_replica_path"], t.ServerSettings["default_replica_name"]
	}
	if strings.Contains(rt.tmplPath, "{uuid}") && rt.ri.UUID == "" {
		rt.err = "ZK-путь содержит {uuid}, но UUID объекта не определён (Ordinary-БД?)"
		if !boolOr(p.cfg.DDL.PreserveUUID, true) {
			rt.err = "ZK-путь содержит {uuid}, а preserve_uuid=false: реплики нового шарда получат разные UUID и НЕ будут реплицироваться между собой"
		}
		return rt
	}
	special := map[string]string{"database": so.DB, "table": so.Name, "uuid": rt.ri.UUID}
	var m1, m2 []string
	rt.path, m1 = expandMacros(rt.tmplPath, t.Macros, special)
	rt.repl, m2 = expandMacros(rt.tmplRepl, t.Macros, special)
	if miss := uniq(append(m1, m2...)); len(miss) > 0 {
		rt.err = "на target не определены макросы: {" + strings.Join(miss, "}, {") + "}"
	}
	return rt
}

// validate проверяет, можно ли безопасно создать объект на target. По умолчанию — CREATE;
// при проблеме — BLOCKED с причиной (или SKIP для глобальных таблиц при global_replicated=skip).
func (p *planner) validate(so *SrcObj, t *Inventory, it *Item, rt replTarget, zk map[string][]string, zkErr string) {
	it.Action = ACreate
	block := func(r string) { it.Action, it.Reason = ABlocked, r }

	if rt.ri != nil && !p.validateReplicated(so, t, it, rt, zk, zkErr) {
		return
	}
	switch so.Kind {
	case KDistributed:
		_, a, ok := engineArgs(so.Create, "Distributed")
		if !ok || len(a) == 0 {
			break
		}
		cl, _ := unquote(a[0])
		cl, _ = expandMacros(cl, t.Macros, nil)
		if !t.hasCluster(cl) {
			block("кластер '" + cl + "' не найден в system.clusters на target (remote_servers не раскатан?)")
			return
		}
		if n := p.cfg.Target.ExpectShards; n > 0 {
			if _, got := clusterFingerprint(t.clusterRows(cl)); got != n {
				it.Warn = append(it.Warn, fmt.Sprintf("кластер %s на target содержит %d шард(ов), ожидалось %d", cl, got, n))
			}
		}
		if len(a) >= 3 { // локальная таблица должна быть хоть где-то
			db, _ := unquote(a[1])
			tb, _ := unquote(a[2])
			if db == "" || strings.Contains(db, "(") {
				db = so.DB
			}
			lk := ObjKey{db, tb}
			_, inU := p.u.Objects[lk]
			_, onT := t.Objects[lk]
			if !inU && !onT {
				it.Warn = append(it.Warn, "локальная таблица "+lk.String()+" не найдена ни на источниках, ни на target")
			}
		}
	case KStreaming:
		it.Warn = append(it.Warn, so.Engine+": потребление начнётся сразу после создания MV (consumer group общий — будет ребаланс партиций)")
	case KTable:
		// Нереплицируемая MergeTree на шарде с несколькими репликами: данные между ними не синхронизируются.
		if strings.HasSuffix(so.Engine, "MergeTree") && len(t.ownShard(p.cfg.Cluster)) != 1 {
			it.Warn = append(it.Warn, "нереплицируемая "+so.Engine+": данные между репликами шарда не синхронизируются (как и на старых шардах)")
		}
	}
	for _, sp := range settingValues(storagePolicyRe, so.Create) {
		if !t.StoragePolicies[sp] {
			block("storage_policy '" + sp + "' отсутствует на target")
			return
		}
	}
	for _, d := range settingValues(diskSettingRe, so.Create) {
		if !t.Disks[d] {
			block("disk '" + d + "' отсутствует на target")
			return
		}
	}
}

// validateReplicated — самое опасное место: куда в ZK встанет новая реплика. Возвращает false,
// если объект заблокирован или пропущен. Ловит:
//   - путь без макроса шарда (общая таблица на весь кластер) — решение по ddl.global_replicated;
//   - совпадение пути с путём старого шарда (опечатка в макросе shard) — хост стал бы репликой старого шарда;
//   - свою реплику, оставшуюся в ZK от прошлой попытки;
//   - чужие реплики по этому пути (остатки выведенного шарда).
func (p *planner) validateReplicated(so *SrcObj, t *Inventory, it *Item, rt replTarget, zk map[string][]string, zkErr string) bool {
	block := func(r string) bool { it.Action, it.Reason = ABlocked, r; return false }
	if rt.err != "" {
		return block(rt.err)
	}
	it.Info = "zk=" + rt.path + " replica=" + rt.repl
	it.ExpZK, it.ExpRepl = rt.path, rt.repl
	children, zkKnown := zk[rt.path+"/replicas"]
	if zkErr != "" {
		it.Warn = append(it.Warn, "проверка ZK не выполнена: "+zkErr)
	}

	if !templateHasMacro(rt.tmplPath, p.cfg.DDL.ShardMacros) {
		msg := "ZK-путь '" + rt.tmplPath + "' без макроса шарда: это ОБЩИЙ набор реплик на все хосты кластера (полная копия на каждом)"
		switch p.cfg.DDL.GlobalReplicated {
		case "block":
			return block(msg + ". Решите явно: ddl.global_replicated = \"create\" (подключить к общему набору, данные скачаются целиком) или \"skip\"")
		case "skip":
			it.Action, it.Reason = ASkip, msg
			return false
		}
		it.Warn = append(it.Warn, msg+": хост присоединится к существующему набору реплик и скачает все данные")
		if zkKnown && containsStr(children, rt.repl) {
			return block("реплика '" + rt.repl + "' уже есть в " + rt.path + "/replicas (конфликт макроса {replica}?)")
		}
		return true
	}

	// Тот же путь, раскрытый макросами каждого источника, не должен совпасть с путём на target.
	special := map[string]string{"database": so.DB, "table": so.Name, "uuid": rt.ri.UUID}
	var collide []string
	for _, h := range so.Hosts {
		src := p.srcByID[h]
		tmpl := rt.tmplPath
		if rt.ri.Defaults {
			tmpl = src.ServerSettings["default_replica_path"]
		}
		if sp, _ := expandMacros(tmpl, src.Macros, special); sp == rt.path {
			collide = append(collide, h)
		}
	}
	if len(collide) > 0 {
		return block("развёрнутый ZK-путь " + rt.path + " совпадает с путём на источнике " + strings.Join(collide, ",") +
			" — коллизия макроса шарда! Хост стал бы репликой старого шарда")
	}
	if !zkKnown || len(children) == 0 {
		return true
	}

	// В ZK уже есть реплики по этому пути: допустимы только соседи по новому шарду (тот же макрос shard).
	siblings := map[string]bool{}
	for _, ot := range p.targets {
		if ot != t && ot.Err == nil && ot.Macros["shard"] == t.Macros["shard"] {
			if r, _ := expandMacros(rt.tmplRepl, ot.Macros, special); r != "" {
				siblings[r] = true
			}
		}
	}
	var foreign []string
	for _, ch := range children {
		if ch == rt.repl {
			return block("в ZK уже есть реплика " + rt.path + "/replicas/" + rt.repl + ", а таблицы на хосте нет (остаток прошлой попытки?). " +
				"Проверьте и удалите: SYSTEM DROP REPLICA '" + rt.repl + "' FROM ZKPATH '" + rt.path + "'")
		}
		if !siblings[ch] {
			foreign = append(foreign, ch)
		}
	}
	if len(foreign) > 0 {
		return block("ZK-путь " + rt.path + " уже занят чужими репликами " + strings.Join(foreign, ",") + " (остатки выведенного шарда или коллизия макросов)")
	}
	it.Warn = append(it.Warn, "в ZK уже есть соседняя реплика нового шарда: "+strings.Join(children, ","))
	return true
}

// ============================== Apply и verify ==============================

// applyResult — итог выполнения на одном хосте.
type applyResult struct {
	OK       int
	Failures []string
}

// applyTarget выполняет CREATE из плана напрямую на хосте (без ON CLUSTER), несколькими проходами:
// если объект упал из-за ещё не созданной зависимости, он повторяется, пока проход что-то создаёт.
func applyTarget(ctx context.Context, cfg *Config, tp *TargetPlan, logw io.Writer, stopOnError bool) applyResult {
	var res applyResult
	pending := tp.creates()
	lastErr := map[*Item]error{}
	for pass := 1; len(pending) > 0; pass++ {
		var next []*Item
		progress := 0
		for _, it := range pending {
			s := cfg.ddlSettings()
			if it.DB != "" {
				s["database"] = it.DB // неквалифицированные имена в DDL — относительно своей БД
			}
			t0 := time.Now()
			_, err := tp.Inv.C.Exec(ctx, it.SQL, s)
			dur := time.Since(t0).Round(time.Millisecond)
			if err == nil {
				progress++
				res.OK++
				fmt.Fprintf(logw, "%s OK    pass=%d %-6s [%s] %s\n", time.Now().Format("15:04:05"), pass, dur, it.Kind, it.Name)
				logf("  ✓ %-11s %s (%s)", it.Kind, it.Name, dur)
				continue
			}
			lastErr[it] = err
			fmt.Fprintf(logw, "%s FAIL  pass=%d %-6s [%s] %s\n      %v\n      SQL: %s\n", time.Now().Format("15:04:05"), pass, dur, it.Kind, it.Name, err, it.SQL)
			if stopOnError {
				res.Failures = append(res.Failures, fmt.Sprintf("%s: %v%s", it.Name, err, hintFor(err)))
				logf("  ✗ %-11s %s: %v", it.Kind, it.Name, err)
				return res
			}
			next = append(next, it)
		}
		if progress == 0 || len(next) == 0 { // прогресса нет — оставшиеся окончательно упали
			for _, it := range next {
				msg := fmt.Sprintf("%s: %v%s", it.Name, lastErr[it], hintFor(lastErr[it]))
				res.Failures = append(res.Failures, msg)
				logf("  ✗ %-11s %s", it.Kind, msg)
			}
			break
		}
		logf("  … проход %d: %d объектов отложено (ждут зависимостей), повтор", pass, len(next))
		pending = next
	}
	return res
}

// hintFor — подсказка к частым ошибкам CREATE по коду исключения.
func hintFor(err error) string {
	switch errCode(err) {
	case 57:
		return "\n      подсказка: таблица/UUID уже существует. Если пересоздаёте после DROP — используйте DROP ... SYNC"
	case 253:
		return "\n      подсказка: реплика уже есть в ZK — остаток прошлой попытки? SYSTEM DROP REPLICA '<r>' FROM ZKPATH '<path>'"
	case 36, 344:
		return "\n      подсказка: возможно, нужна настройка в [ddl.settings] (allow_experimental_*/allow_suspicious_*)"
	case 60, 81:
		return "\n      подсказка: нет зависимой таблицы/БД — исключена фильтром или не создалась"
	}
	return ""
}

// verifyTarget перечитывает target после apply: всё ли создано, совпадает ли DDL с источником,
// встали ли реплики по ожидаемым ZK-путям и не в readonly ли они.
func verifyTarget(ctx context.Context, u *Union, tp *TargetPlan, opt invOptions) []string {
	inv := loadInventory(ctx, tp.Inv.C, "target", opt)
	if inv.Err != nil {
		return []string{"не удалось перечитать target: " + inv.Err.Error()}
	}
	type repRow struct {
		Database   string `json:"database"`
		Table      string `json:"table"`
		ZKPath     string `json:"zookeeper_path"`
		Replica    string `json:"replica_name"`
		IsReadonly uint8  `json:"is_readonly"`
	}
	var issues []string
	reps := map[ObjKey]repRow{}
	if rows, err := Select[repRow](ctx, inv.C, "SELECT database, table, zookeeper_path, replica_name, is_readonly FROM system.replicas", nil); err == nil {
		for _, r := range rows {
			reps[ObjKey{r.Database, r.Table}] = r
		}
	} else {
		issues = append(issues, "system.replicas: "+err.Error())
	}
	for _, it := range tp.creates() {
		switch it.Kind {
		case KDatabase:
			if _, ok := inv.DBs[it.Name]; !ok {
				issues = append(issues, "БД "+it.Name+" не создана")
			}
			continue
		case KFunction:
			if _, ok := inv.Functions[it.Name]; !ok {
				issues = append(issues, "функция "+it.Name+" не создана")
			}
			continue
		}
		o, ok := inv.Objects[it.key]
		if !ok {
			issues = append(issues, it.Name+": не создан")
			continue
		}
		if so := u.Objects[it.key]; so != nil && o.Norm != so.Norm {
			issues = append(issues, it.Name+": DDL после создания отличается от источника (разные версии ClickHouse?)")
		}
		if it.ExpZK == "" {
			continue
		}
		rk := it.key
		if it.Kind == KMatView && o.UUID != "" { // реплицируется inner-таблица MV
			rk = ObjKey{it.key.DB, ".inner_id." + o.UUID}
		}
		r, ok := reps[rk]
		switch {
		case !ok:
			issues = append(issues, it.Name+": нет в system.replicas")
		case r.ZKPath != it.ExpZK || r.Replica != it.ExpRepl:
			issues = append(issues, fmt.Sprintf("%s: zk=%s replica=%s, ожидалось zk=%s replica=%s", it.Name, r.ZKPath, r.Replica, it.ExpZK, it.ExpRepl))
		case r.IsReadonly == 1:
			issues = append(issues, it.Name+": реплика в readonly (проблема с ZooKeeper/Keeper?)")
		}
	}
	return issues
}

// ============================== Отчёты ==============================

var unsafeFileRe = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

// fileSafe превращает имя хоста в безопасное имя файла: 10.0.0.1:8123 -> 10.0.0.1_8123.
func fileSafe(s string) string { return unsafeFileRe.ReplaceAllString(s, "_") }

// printUnion печатает предупреждения по источникам: недоступные, расхождения DDL баз.
func printUnion(w io.Writer, u *Union) {
	for _, s := range u.Warnings {
		fmt.Fprintln(w, "  ! "+s)
	}
	for _, inv := range u.Sources {
		if inv.Err != nil {
			fmt.Fprintf(w, "  ✗ источник %s недоступен: %v\n", inv.C.Name, inv.Err)
		}
		for _, s := range inv.Warnings {
			fmt.Fprintf(w, "  ! %s: %s\n", inv.C.Name, s)
		}
	}
}

// replicationLine — строка о репликации шарда target: сколько реплик и с каким internal_replication.
func replicationLine(t *Inventory, cluster string) string {
	rows := t.ownShard(cluster)
	if cluster == "" || len(rows) == 0 {
		return "шард target в кластере не найден (cluster не задан или remote_servers без этого хоста)"
	}
	var hosts []string
	for _, r := range rows {
		hosts = append(hosts, fmt.Sprintf("%s:%d", r.HostName, r.Port))
	}
	ir := "?"
	if rows[0].InternalReplication != nil {
		ir = fmt.Sprint(*rows[0].InternalReplication)
	}
	s := fmt.Sprintf("шард %d в кластере %s: %d реплик(и) (%s), internal_replication=%s, weight=%d",
		rows[0].ShardNum, cluster, len(rows), strings.Join(hosts, ", "), ir, rows[0].ShardWeight)
	if len(rows) == 1 {
		s += " — без реплик: Replicated-таблицы работают с одной репликой, вторую можно добавить позже без пересоздания"
	}
	return s
}

// printPlan печатает план одного target: CREATE/BLOCKED/DRIFT/EXTRA подробно, EXISTS/SKIP — сводкой (или всё при -v).
func printPlan(w io.Writer, tp *TargetPlan, cluster string, verbose bool) {
	inv := tp.Inv
	fmt.Fprintf(w, "\n══ TARGET %s  (version %s, host %s)\n", inv.label(), inv.Version, inv.Hostname)
	fmt.Fprintf(w, "   %s\n", replicationLine(inv, cluster))
	for _, a := range []Action{ACreate, ABlocked, ADrift, AExtra, AExists, ASkip} {
		var items []*Item
		for _, it := range tp.Items {
			if it.Action == a {
				items = append(items, it)
			}
		}
		switch {
		case len(items) == 0:
			continue
		case a == AExists && !verbose:
			fmt.Fprintf(w, "  %-8s %d объектов уже есть и совпадают (подробно: -v)\n", a, len(items))
			continue
		case a == ASkip && !verbose:
			reasons := map[string]int{}
			for _, it := range items {
				reasons[shortReason(it.Reason)]++
			}
			fmt.Fprintf(w, "  %-8s %d объектов (подробно: -v)\n", a, len(items))
			for _, r := range sortedKeys(reasons) {
				fmt.Fprintf(w, "           · %d: %s\n", reasons[r], r)
			}
			continue
		}
		for _, it := range items {
			fmt.Fprintf(w, "  %-8s %-11s %-50s %s\n", a, it.Kind, it.Name, it.Engine)
			for _, s := range []string{it.Info, it.Reason} {
				if s != "" {
					fmt.Fprintf(w, "           ↳ %s\n", s)
				}
			}
			for _, wr := range it.Warn {
				fmt.Fprintf(w, "           ! %s\n", wr)
			}
		}
	}
	c := tp.Counts
	fmt.Fprintf(w, "  ИТОГО: create=%d blocked=%d drift=%d extra=%d exists=%d skip=%d\n",
		c[ACreate], c[ABlocked], c[ADrift], c[AExtra], c[AExists], c[ASkip])
}

// shortReason — укрупнённая причина пропуска для сводки.
func shortReason(r string) string {
	switch {
	case strings.HasPrefix(r, "cascade:"):
		return "cascade (ссылается на исключённый объект)"
	case strings.HasPrefix(r, "есть только на"):
		return "есть не на всех источниках (on_partial=skip)"
	}
	return r
}

// writeSQL пишет план target в .sql — для ревью/CAB или ручного прогона через clickhouse-client --multiquery.
func writeSQL(dir string, tp *TargetPlan, srcNames []string) (string, error) {
	path := filepath.Join(dir, fileSafe(tp.Inv.C.Name)+".sql")
	var b strings.Builder
	fmt.Fprintf(&b, "-- chsync plan for %s\n-- generated %s\n-- sources: %s\n", tp.Inv.label(), time.Now().Format(time.RFC3339), strings.Join(srcNames, ", "))
	fmt.Fprintf(&b, "-- create=%d blocked=%d\n-- Выполняется напрямую на хосте (без ON CLUSTER).\n\n", tp.Counts[ACreate], tp.Counts[ABlocked])
	curDB := ""
	for _, it := range tp.Items {
		switch it.Action {
		case ACreate:
			if it.DB != "" && it.DB != curDB {
				fmt.Fprintf(&b, "USE %s;\n\n", quoteIdent(it.DB))
				curDB = it.DB
			}
			fmt.Fprintf(&b, "-- [%s] %s  (source: %s)\n", it.Kind, it.Name, it.Source)
			if it.Info != "" {
				fmt.Fprintf(&b, "-- %s\n", it.Info)
			}
			for _, w := range it.Warn {
				fmt.Fprintf(&b, "-- WARN: %s\n", w)
			}
			fmt.Fprintf(&b, "%s;\n\n", it.SQL)
		case ABlocked:
			fmt.Fprintf(&b, "-- BLOCKED [%s] %s: %s\n\n", it.Kind, it.Name, it.Reason)
		}
	}
	return path, os.WriteFile(path, []byte(b.String()), 0o644)
}

package plan

import (
	"cmp"
	"container/heap"
	"slices"

	"chsync/internal/schema"
)

// nameIndex — индекс «имя объекта -> ключи» по объектам, удовлетворяющим pred.
func nameIndex(objs map[schema.ObjKey]*SrcObj, pred func(*SrcObj) bool) map[string][]schema.ObjKey {
	idx := map[string][]schema.ObjKey{}
	for k, o := range objs {
		if pred(o) {
			idx[k.Name] = append(idx[k.Name], k)
		}
	}
	return idx
}

// refs — на какие объекты из индекса idx ссылается o (с учётом допустимых видов kindOK).
func (u *Union) refs(o *SrcObj, idx map[string][]schema.ObjKey, kindOK func(*SrcObj) bool) []schema.ObjKey {
	found := map[schema.ObjKey]bool{}
	for _, k := range schema.ExplicitRefs(o.Kind, o.DB, o.Create) {
		if p, ok := u.Objects[k]; ok && k != o.Key() && kindOK(p) && slices.Contains(idx[k.Name], k) {
			found[k] = true
		}
	}
	body := o.Body()
	for tok := range o.Tokens() {
		for _, k := range idx[tok] {
			if k != o.Key() && !found[k] && kindOK(u.Objects[k]) && schema.RefersTo(o.DB, k, body) {
				found[k] = true
			}
		}
	}
	out := make([]schema.ObjKey, 0, len(found))
	for k := range found {
		out = append(out, k)
	}
	slices.SortFunc(out, schema.ObjKey.Compare)
	return out
}

// dependsKinds — от объектов каких видов может зависеть объект вида k при CREATE.
// Обычным таблицам нужны только словари (dictGet в DEFAULT/MATERIALIZED).
func dependsKinds(k schema.Kind) func(*SrcObj) bool {
	switch k {
	case schema.KDistributed, schema.KProxy, schema.KView, schema.KMatView, schema.KOtherView, schema.KDictionary:
		return func(*SrcObj) bool { return true }
	case schema.KTable, schema.KReplicated, schema.KStreaming:
		return func(p *SrcObj) bool { return p.Kind == schema.KDictionary }
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
			} else if w := msg + " (CREATE может упасть)"; !slices.Contains(o.Warn, w) {
				o.Warn = append(o.Warn, w)
			}
		}
	}
}

// buildDeps вычисляет зависимости неисключённых объектов и порядок создания.
func (u *Union) buildDeps() {
	cand := nameIndex(u.Objects, func(o *SrcObj) bool { return o.Skip == "" })
	var keys []schema.ObjKey
	for k, o := range u.Objects {
		if o.Skip != "" {
			continue
		}
		keys = append(keys, k)
		if o.Kind != schema.KDictionary { // словари грузятся лениво: источник при CREATE не нужен
			o.Deps = u.refs(o, cand, dependsKinds(o.Kind))
		}
	}
	u.Order = topoSort(u.Objects, keys)
}

// pqItem / pq — очередь с приоритетом для топосортировки: сначала вид объекта, потом имя (стабильный порядок).
type pqItem struct {
	k   schema.ObjKey
	pri int
}

func (a pqItem) compare(b pqItem) int { return cmp.Or(cmp.Compare(a.pri, b.pri), a.k.Compare(b.k)) }

type pq []pqItem

func (p pq) Len() int           { return len(p) }
func (p pq) Less(i, j int) bool { return p[i].compare(p[j]) < 0 }
func (p pq) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p *pq) Push(x any)        { *p = append(*p, x.(pqItem)) }
func (p *pq) Pop() any {
	old := *p
	x := old[len(old)-1]
	*p = old[:len(old)-1]
	return x
}

// topoSort — топологическая сортировка (Кан) с приоритетом по виду объекта.
// При цикле берёт оставшийся объект с наименьшим приоритетом и помечает его предупреждением.
func topoSort(objs map[schema.ObjKey]*SrcObj, keys []schema.ObjKey) []schema.ObjKey {
	indeg := map[schema.ObjKey]int{}
	dependents := map[schema.ObjKey][]schema.ObjKey{}
	inSet := map[schema.ObjKey]bool{}
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
	item := func(k schema.ObjKey) pqItem { return pqItem{k, objs[k].Kind.Priority()} }
	h := &pq{}
	for _, k := range keys {
		if indeg[k] == 0 {
			heap.Push(h, item(k))
		}
	}
	out := make([]schema.ObjKey, 0, len(keys))
	done := map[schema.ObjKey]bool{}
	for len(out) < len(keys) {
		if h.Len() == 0 { // цикл: берём «самый ранний» из оставшихся
			var first *pqItem
			for _, k := range keys {
				if it := item(k); !done[k] && (first == nil || it.compare(*first) < 0) {
					first = &it
				}
			}
			heap.Push(h, *first)
			objs[first.k].Warn = append(objs[first.k].Warn, "циклическая зависимость в DDL — порядок выбран по типу объекта")
		}
		it := heap.Pop(h).(pqItem)
		if done[it.k] {
			continue
		}
		done[it.k] = true
		out = append(out, it.k)
		for _, d := range dependents[it.k] {
			if indeg[d]--; indeg[d] == 0 && !done[d] {
				heap.Push(h, item(d))
			}
		}
	}
	return out
}

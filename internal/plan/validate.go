package plan

import (
	"fmt"
	"slices"
	"strings"

	"chsync/internal/config"
	"chsync/internal/schema"
	"chsync/internal/storage"
)

// validate проверяет, можно ли безопасно создать объект на target. По умолчанию — CREATE;
// при проблеме — BLOCKED с причиной (или SKIP для глобальных таблиц при global_replicated=skip).
func (p *Planner) validate(so *SrcObj, t *storage.Inventory, it *Item, rt replTarget, zk zkState) {
	it.Action = ACreate
	block := func(r string) { it.Action, it.Reason = ABlocked, r }

	if rt.ri != nil && !p.validateReplicated(so, t, it, rt, zk) {
		return
	}
	switch so.Kind {
	case schema.KDistributed:
		if r := p.checkDistributed(so, t, it); r != "" {
			block(r)
			return
		}
	case schema.KStreaming:
		it.Warn = append(it.Warn, so.Engine+": потребление начнётся сразу после создания MV (consumer group общий — будет ребаланс партиций)")
	case schema.KTable:
		// Нереплицируемая MergeTree на шарде с несколькими репликами: данные между ними не синхронизируются.
		if strings.HasSuffix(so.Engine, "MergeTree") && len(t.OwnShard(p.cfg.Cluster)) != 1 {
			it.Warn = append(it.Warn, "нереплицируемая "+so.Engine+": данные между репликами шарда не синхронизируются (как и на старых шардах)")
		}
	}
	for _, sp := range schema.StoragePolicies(so.Create) {
		if !t.StoragePolicies[sp] {
			block("storage_policy '" + sp + "' отсутствует на target")
			return
		}
	}
	for _, d := range schema.Disks(so.Create) {
		if !t.Disks[d] {
			block("disk '" + d + "' отсутствует на target")
			return
		}
	}
}

// checkDistributed — кластер Distributed-таблицы описан на target; число шардов; локальная таблица существует.
// Возвращает причину блокировки или "".
func (p *Planner) checkDistributed(so *SrcObj, t *storage.Inventory, it *Item) string {
	_, a, ok := schema.EngineArgs(so.Create, "Distributed")
	if !ok || len(a) == 0 {
		return ""
	}
	cl, _ := schema.Unquote(a[0])
	cl, _ = schema.ExpandMacros(cl, t.Macros, nil)
	if !t.HasCluster(cl) {
		return "кластер '" + cl + "' не найден в system.clusters на target (remote_servers не раскатан?)"
	}
	if n := p.cfg.Target.ExpectShards; n > 0 {
		if _, got := storage.ClusterFingerprint(t.ClusterRows(cl)); got != n {
			it.Warn = append(it.Warn, fmt.Sprintf("кластер %s на target содержит %d шард(ов), ожидалось %d", cl, got, n))
		}
	}
	if len(a) >= 3 { // локальная таблица должна быть хоть где-то
		db, _ := schema.Unquote(a[1])
		tb, _ := schema.Unquote(a[2])
		if db == "" || strings.Contains(db, "(") {
			db = so.DB
		}
		lk := schema.ObjKey{DB: db, Name: tb}
		_, inU := p.u.Objects[lk]
		_, onT := t.Objects[lk]
		if !inU && !onT {
			it.Warn = append(it.Warn, "локальная таблица "+lk.String()+" не найдена ни на источниках, ни на target")
		}
	}
	return ""
}

// validateReplicated — самое опасное место: куда в ZK встанет новая реплика. Возвращает false,
// если объект заблокирован или пропущен. Ловит:
//   - путь без макроса шарда (общая таблица на весь кластер) — решение по ddl.global_replicated;
//   - совпадение пути с путём старого шарда (опечатка в макросе shard) — хост стал бы репликой старого шарда;
//   - свою реплику, оставшуюся в ZK от прошлой попытки;
//   - чужие реплики по этому пути (остатки выведенного шарда).
func (p *Planner) validateReplicated(so *SrcObj, t *storage.Inventory, it *Item, rt replTarget, zk zkState) bool {
	block := func(r string) bool { it.Action, it.Reason = ABlocked, r; return false }
	if rt.err != "" {
		return block(rt.err)
	}
	it.Info = "zk=" + rt.path + " replica=" + rt.repl
	it.ExpZK, it.ExpRepl = rt.path, rt.repl
	children, zkKnown := zk.children[rt.path+"/replicas"]
	if zk.err != nil {
		it.Warn = append(it.Warn, "проверка ZK не выполнена: "+zk.err.Error())
	}

	if !schema.TemplateHasMacro(rt.tmplPath, p.cfg.DDL.ShardMacros) {
		msg := "ZK-путь '" + rt.tmplPath + "' без макроса шарда: это ОБЩИЙ набор реплик на все хосты кластера (полная копия на каждом)"
		switch p.cfg.DDL.GlobalReplicated {
		case config.GlobalBlock:
			return block(msg + ". Решите явно: ddl.global_replicated = \"create\" (подключить к общему набору, данные скачаются целиком) или \"skip\"")
		case config.GlobalSkip:
			it.Action, it.Reason = ASkip, msg
			return false
		}
		it.Warn = append(it.Warn, msg+": хост присоединится к существующему набору реплик и скачает все данные")
		if zkKnown && slices.Contains(children, rt.repl) {
			return block("реплика '" + rt.repl + "' уже есть в " + rt.path + "/replicas (конфликт макроса {replica}?)")
		}
		return true
	}

	// Тот же путь, раскрытый макросами каждого источника, не должен совпасть с путём на target.
	special := specialMacros(so, rt.ri)
	var collide []string
	for _, h := range so.Hosts {
		src := p.srcByID[h]
		tmpl := rt.tmplPath
		if rt.ri.Defaults {
			tmpl = src.ServerSettings["default_replica_path"]
		}
		if sp, _ := schema.ExpandMacros(tmpl, src.Macros, special); sp == rt.path {
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
	var unread []string // target, снимок которых не прочитан: их реплика могла бы быть «соседом»
	for _, ot := range p.targets {
		switch {
		case ot == t:
		case ot.Err != nil:
			unread = append(unread, ot.Name())
		case ot.Macros["shard"] == t.Macros["shard"]:
			if r, _ := schema.ExpandMacros(rt.tmplRepl, ot.Macros, special); r != "" {
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
		msg := "ZK-путь " + rt.path + " уже занят чужими репликами " + strings.Join(foreign, ",") + " (остатки выведенного шарда или коллизия макросов)"
		if len(unread) > 0 {
			msg += ". Внимание: не прочитаны снимки target " + strings.Join(unread, ", ") +
				" — если это реплика того же нового шарда, её не удалось опознать как соседнюю (проверьте доступ к хосту)"
		}
		return block(msg)
	}
	it.Warn = append(it.Warn, "в ZK уже есть соседняя реплика нового шарда: "+strings.Join(children, ","))
	return true
}

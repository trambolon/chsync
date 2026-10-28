package plan

import (
	"context"
	"fmt"

	"chsync/internal/schema"
	"chsync/internal/storage"
)

// Verify перечитывает target после apply: всё ли создано, совпадает ли DDL с источником,
// встали ли реплики по ожидаемым ZK-путям и не в readonly ли они. Возвращает список проблем.
func Verify(ctx context.Context, u *Union, tp *TargetPlan, opt storage.Options) []string {
	inv := storage.Load(ctx, storage.Host{Client: tp.Inv.Client, Role: storage.RoleTarget}, opt)
	if inv.Err != nil {
		return []string{"не удалось перечитать target: " + inv.Err.Error()}
	}
	type repRow struct {
		Database   string `ch:"database"`
		Table      string `ch:"table"`
		ZKPath     string `ch:"zookeeper_path"`
		Replica    string `ch:"replica_name"`
		IsReadonly uint8  `ch:"is_readonly"`
	}
	var issues []string
	reps := map[schema.ObjKey]repRow{}
	rows, err := inv.Client.Select[repRow](ctx, "SELECT database, table, zookeeper_path, replica_name, is_readonly FROM system.replicas", nil)
	if err != nil {
		issues = append(issues, "system.replicas: "+err.Error())
	}
	for _, r := range rows {
		reps[schema.ObjKey{DB: r.Database, Name: r.Table}] = r
	}
	for _, it := range tp.Creates() {
		switch it.Kind {
		case schema.KDatabase:
			if _, ok := inv.DBs[it.Name]; !ok {
				issues = append(issues, "БД "+it.Name+" не создана")
			}
			continue
		case schema.KFunction:
			if _, ok := inv.Functions[it.Name]; !ok {
				issues = append(issues, "функция "+it.Name+" не создана")
			}
			continue
		}
		o, ok := inv.Objects[it.key]
		if !ok {
			issues = append(issues, it.Name+": не создан. Если CREATE прошёл без ошибки — скорее всего, на хосте есть отсоединённая "+
				"таблица с этим именем (DETACH ... PERMANENTLY): CREATE IF NOT EXISTS её молча пропускает. Проверьте system.detached_tables")
			continue
		}
		if so := u.Objects[it.key]; so != nil && o.Norm != so.Norm {
			issues = append(issues, it.Name+": DDL после создания отличается от источника (разные версии ClickHouse?)")
		}
		if it.ExpZK == "" {
			continue
		}
		rk := it.key
		if it.Kind == schema.KMatView && o.UUID != "" { // реплицируется inner-таблица MV
			rk = schema.ObjKey{DB: it.key.DB, Name: ".inner_id." + o.UUID}
		}
		switch r, ok := reps[rk]; {
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

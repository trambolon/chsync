package plan

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"chsync/internal/config"
	"chsync/internal/storage"
)

// ApplyResult — итог выполнения на одном хосте.
type ApplyResult struct {
	OK       int
	Failures []string
}

// Apply выполняет CREATE из плана напрямую на хосте (без ON CLUSTER), несколькими проходами:
// если объект упал из-за ещё не созданной зависимости, он повторяется, пока проход что-то создаёт.
//
// Журнал: успешные CREATE — Info, промежуточные ошибки (будет повтор) — Debug, окончательные — Error.
// Атрибут sql содержит полный текст запроса (в консоль не выводится, в apply.log — да).
func Apply(ctx context.Context, cfg *config.Config, tp *TargetPlan, log *slog.Logger, stopOnError bool) ApplyResult {
	var res ApplyResult
	pending := tp.Creates()
	lastErr := map[*Item]error{}
	for pass := 1; len(pending) > 0; pass++ {
		var next []*Item
		progress := 0
		for _, it := range pending {
			if ctx.Err() != nil {
				res.Failures = append(res.Failures, fmt.Sprintf("прервано: %v", context.Cause(ctx)))
				return res
			}
			s := cfg.DDL.SettingsParams()
			if it.DB != "" {
				s["database"] = it.DB // неквалифицированные имена в DDL — относительно своей БД
			}
			t0 := time.Now()
			_, err := tp.Inv.Client.Exec(ctx, it.SQL, s)
			dur := time.Since(t0).Round(time.Millisecond)
			attrs := []any{"pass", pass, "kind", it.Kind, "object", it.Name, "dur", dur}
			if err == nil {
				progress++
				res.OK++
				log.Info("✓ создан", attrs...)
				continue
			}
			lastErr[it] = err
			if stopOnError {
				res.Failures = append(res.Failures, fmt.Sprintf("%s: %v%s", it.Name, err, hintFor(err)))
				log.Error("✗ не создан", append(attrs, "err", err, "sql", it.SQL)...)
				return res
			}
			log.Debug("отложен, будет повтор", append(attrs, "err", err, "sql", it.SQL)...)
			next = append(next, it)
		}
		if progress == 0 || len(next) == 0 { // прогресса нет — оставшиеся окончательно упали
			for _, it := range next {
				res.Failures = append(res.Failures, fmt.Sprintf("%s: %v%s", it.Name, lastErr[it], hintFor(lastErr[it])))
				log.Error("✗ не создан", "kind", it.Kind, "object", it.Name, "err", lastErr[it], "sql", it.SQL)
			}
			break
		}
		log.Info("… проход завершён, повтор отложенных (ждут зависимостей)", "pass", pass, "deferred", len(next))
		pending = next
	}
	return res
}

// hintFor — подсказка к частым ошибкам CREATE по коду исключения ClickHouse.
func hintFor(err error) string {
	switch storage.Code(err) {
	case 57:
		return "\n      подсказка: таблица/UUID уже существует. Если пересоздаёте после DROP — используйте DROP ... SYNC"
	case 253:
		return "\n      подсказка: реплика уже есть в ZK — остаток прошлой попытки? SYSTEM DROP REPLICA '<r>' FROM ZKPATH '<path>'"
	case 36, 344:
		return "\n      подсказка: возможно, нужна настройка в [ddl.settings] (allow_experimental_*/allow_suspicious_*)"
	case 60, 81:
		return "\n      подсказка: нет зависимой таблицы/БД — исключена фильтром или не создалась"
	case 497:
		return "\n      подсказка: не хватает прав (для MATERIALIZED VIEW нужны SELECT на источник и INSERT на целевую таблицу)"
	}
	return ""
}

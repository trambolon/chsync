package app

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"chsync/internal/config"
	"chsync/internal/filter"
	"chsync/internal/logging"
	"chsync/internal/plan"
	"chsync/internal/report"
	"chsync/internal/storage"
)

// planArgs — параметры сценария plan/apply.
type planArgs struct {
	cfg    *config.Config
	f      *filter.Filter
	invs   []*storage.Inventory
	active map[string]bool
	out    io.Writer // stdout + файл отчёта
	run    *report.Run
	log    *slog.Logger
	in     io.Reader // для подтверждения
	errw   io.Writer

	dry, yes, allowBlocked, stopOnError, verbose bool
	invOpt                                       storage.Options
}

// runPlanApply строит план по каждому активному target, пишет SQL-скрипты и, если не dry run,
// выполняет CREATE и проверяет результат. Возвращает код выхода.
func runPlanApply(ctx context.Context, a planArgs) int {
	out := a.out
	var srcs, tgts []*storage.Inventory
	var srcNames []string
	for _, inv := range a.invs {
		if inv.Role != storage.RoleSource {
			tgts = append(tgts, inv)
			continue
		}
		if inv.Err != nil {
			fmt.Fprintf(out, "✗ источник %s недоступен: %v\n", inv.Name(), inv.Err)
			return ExitError
		}
		srcs = append(srcs, inv)
		srcNames = append(srcNames, inv.Name())
	}
	u := plan.BuildUnion(a.cfg, a.f, srcs)
	plan.PrintUnion(out, u)
	p := plan.NewPlanner(a.cfg, a.f, u, tgts)

	// 1. План по каждому target.
	var plans []*plan.TargetPlan
	totalCreate, totalBlocked := 0, 0
	for _, t := range tgts {
		if !a.active[t.Name()] {
			continue
		}
		if t.Err != nil {
			fmt.Fprintf(out, "\n✗ target %s недоступен: %v\n", t.Name(), t.Err)
			return ExitError
		}
		tp := p.PlanTarget(ctx, t)
		plan.PrintPlan(out, tp, a.cfg.Cluster, a.verbose)
		if err := writeSQL(a.run, tp, srcNames); err != nil {
			fmt.Fprintln(out, err)
			return ExitError
		}
		fmt.Fprintf(out, "  SQL: %s\n", a.run.Path(t.Name()+".sql"))
		plans = append(plans, tp)
		totalCreate += tp.Counts[plan.ACreate]
		totalBlocked += tp.Counts[plan.ABlocked]
	}
	fmt.Fprintf(out, "\nВСЕГО по %d target: create=%d blocked=%d\n", len(plans), totalCreate, totalBlocked)

	// 2. Решаем, выполнять ли.
	switch {
	case a.dry:
		fmt.Fprintln(out, "DRY RUN: ничего не выполнено. Для выполнения: chsync apply --execute")
		if totalBlocked > 0 {
			return ExitDiff
		}
		return ExitOK
	case totalBlocked > 0 && !a.allowBlocked:
		fmt.Fprintln(out, "Есть BLOCKED объекты — apply остановлен. Разберите причины или запустите с --allow-blocked (они будут пропущены).")
		return ExitDiff
	case totalCreate == 0:
		fmt.Fprintln(out, "Нечего создавать — target уже синхронизированы.")
		return ExitOK
	case !a.yes && !confirm(a.in, a.errw, fmt.Sprintf("Выполнить %d CREATE на %d хостах? Введите yes: ", totalCreate, len(plans))):
		fmt.Fprintln(out, "Отменено.")
		return ExitError
	}

	// 3. Выполняем и сверяем, хост за хостом.
	code := ExitOK
	for _, tp := range plans {
		if len(tp.Creates()) == 0 {
			continue
		}
		res, logPath, err := applyOne(ctx, a, tp)
		if err != nil {
			fmt.Fprintln(out, err)
			return ExitError
		}
		issues := plan.Verify(ctx, u, tp, a.invOpt)
		fmt.Fprintf(out, "\n══ APPLY %s: ok=%d failed=%d  (лог: %s)\n", tp.Inv.Label(), res.OK, len(res.Failures), logPath)
		for _, s := range res.Failures {
			fmt.Fprintln(out, "  ✗ "+s)
			code = ExitError
		}
		for _, s := range issues {
			fmt.Fprintln(out, "  ! verify: "+s)
			code = ExitError
		}
		if len(issues) == 0 {
			fmt.Fprintln(out, "  ✓ verify: созданные объекты совпадают с источником, реплики в порядке")
		}
		if ctx.Err() != nil {
			fmt.Fprintf(out, "прервано: %v\n", context.Cause(ctx))
			return ExitError
		}
		if code != ExitOK && a.stopOnError {
			break
		}
	}
	fmt.Fprintln(out, "\nДальше: chsync diff — убедиться, что схема везде одинакова.")
	return code
}

// applyOne выполняет план одного target; журнал — в консоль и в <host>.apply.log.
func applyOne(ctx context.Context, a planArgs, tp *plan.TargetPlan) (plan.ApplyResult, string, error) {
	name := tp.Inv.Name() + ".apply.log"
	lf, err := a.run.Create(name)
	if err != nil {
		return plan.ApplyResult{}, "", err
	}
	defer lf.Close()
	log := logging.Tee(a.log.Handler(), logging.NewFileHandler(lf)).With("host", tp.Inv.Name())
	a.log.Info("APPLY", "host", tp.Inv.Label(), "objects", len(tp.Creates()))
	return plan.Apply(ctx, a.cfg, tp, log, a.stopOnError), a.run.Path(name), nil
}

// writeSQL сохраняет план target как <host>.sql в каталоге запуска.
func writeSQL(run *report.Run, tp *plan.TargetPlan, sources []string) error {
	f, err := run.Create(tp.Inv.Name() + ".sql")
	if err != nil {
		return err
	}
	if err := plan.WriteSQL(f, tp, sources, time.Now()); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// confirm спрашивает подтверждение в терминале; без терминала (cron, pipe) — отказ, нужен --yes.
func confirm(in io.Reader, errw io.Writer, prompt string) bool {
	f, ok := in.(*os.File)
	if !ok {
		fmt.Fprintln(errw, "stdin не терминал — используйте --yes")
		return false
	}
	if fi, err := f.Stat(); err != nil || fi.Mode()&os.ModeCharDevice == 0 {
		fmt.Fprintln(errw, "stdin не терминал — используйте --yes")
		return false
	}
	fmt.Fprint(errw, prompt)
	line, _ := bufio.NewReader(f).ReadString('\n')
	return strings.TrimSpace(line) == "yes"
}

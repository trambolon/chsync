// Package app — командная строка chsync: разбор команд и флагов, чтение снимков хостов
// и запуск сценария (check, plan, apply, diff).
package app

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"slices"
	"time"

	"chsync/internal/check"
	"chsync/internal/config"
	"chsync/internal/diff"
	"chsync/internal/filter"
	"chsync/internal/logging"
	"chsync/internal/report"
	"chsync/internal/storage"
	"chsync/internal/xstrings"
)

// version задаётся при сборке: -ldflags "-X chsync/internal/app.version=1.2.0".
var version string

// Коды выхода.
const (
	ExitOK     = 0 // всё хорошо
	ExitError  = 1 // ошибки (хост недоступен, CREATE упал, check нашёл проблемы)
	ExitDiff   = 2 // расхождения (diff) или BLOCKED-объекты (plan/apply)
	ExitConfig = 3 // ошибка конфига или командной строки
)

// Stdio — потоки ввода-вывода команды (подменяются в тестах).
type Stdio struct {
	In       io.Reader
	Out, Err io.Writer
}

// OSStdio — стандартные потоки процесса.
func OSStdio() Stdio { return Stdio{In: os.Stdin, Out: os.Stdout, Err: os.Stderr} }

// Version — версия сборки: из -ldflags, иначе из информации о сборке модуля (go install ...@v1.2.0).
func Version() string {
	if version != "" {
		return version
	}
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
		return bi.Main.Version
	}
	return "dev"
}

// options — флаги команд check/plan/apply/diff.
type options struct {
	cfgPath      string
	targets      string
	databases    string
	scope        string
	execute      bool
	dryRun       bool
	yes          bool
	allowBlocked bool
	stopOnError  bool
	verbose      bool
}

// Run выполняет команду chsync с аргументами args (без имени программы) и возвращает код выхода.
func Run(ctx context.Context, args []string, sio Stdio) int {
	if len(args) == 0 {
		usage(sio.Err)
		return ExitConfig
	}
	cmd := args[0]
	switch cmd {
	case "version", "--version", "-version":
		fmt.Fprintln(sio.Out, "chsync", Version())
		return ExitOK
	case "init":
		fmt.Fprint(sio.Out, config.Example)
		return ExitOK
	case "help", "-h", "--help":
		usage(sio.Out)
		return ExitOK
	case "check", "plan", "apply", "diff":
	default:
		fmt.Fprintf(sio.Err, "неизвестная команда %q\n\n", cmd)
		usage(sio.Err)
		return ExitConfig
	}

	opts, err := parseFlags(cmd, args[1:], sio.Err)
	switch {
	case errors.Is(err, flag.ErrHelp):
		return ExitOK
	case err != nil:
		fmt.Fprintln(sio.Err, err)
		return ExitConfig
	}
	cfg, err := config.Load(opts.cfgPath)
	if err != nil {
		fmt.Fprintln(sio.Err, err)
		return ExitConfig
	}
	return runCommand(ctx, cmd, cfg, opts, sio)
}

// parseFlags разбирает флаги команды. Лишние позиционные аргументы — ошибка (скорее всего, опечатка).
func parseFlags(cmd string, args []string, stderr io.Writer) (*options, error) {
	o := &options{}
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { usage(stderr) }
	fs.StringVar(&o.cfgPath, "c", "chsync.toml", "")
	fs.StringVar(&o.targets, "targets", "", "")
	fs.StringVar(&o.databases, "databases", "", "")
	fs.StringVar(&o.scope, "scope", "all", "")
	fs.BoolVar(&o.execute, "execute", false, "")
	fs.BoolVar(&o.dryRun, "dry-run", false, "")
	fs.BoolVar(&o.yes, "yes", false, "")
	fs.BoolVar(&o.allowBlocked, "allow-blocked", false, "")
	fs.BoolVar(&o.stopOnError, "stop-on-error", false, "")
	fs.BoolVar(&o.verbose, "v", false, "")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if fs.NArg() > 0 {
		return nil, fmt.Errorf("лишние аргументы: %v (флаги пишутся после команды: chsync %s -c chsync.toml)", fs.Args(), cmd)
	}
	if !slices.Contains([]string{"all", "sources", "targets"}, o.scope) {
		return nil, fmt.Errorf("--scope %q: допустимо all|sources|targets", o.scope)
	}
	return o, nil
}

// runCommand читает снимки нужных хостов, пишет отчёт в каталог запуска и выполняет сценарий команды.
func runCommand(ctx context.Context, cmd string, cfg *config.Config, opts *options, sio Stdio) int {
	log := logging.NewConsole(sio.Err, opts.verbose)

	// --targets ограничивает, на каких хостах строить план и применять. Снимки остальных target
	// всё равно читаются в plan/apply: они нужны, чтобы узнать «соседнюю» реплику нового шарда в ZK.
	active, err := activeTargets(cfg, opts.targets)
	if err != nil {
		fmt.Fprintln(sio.Err, err)
		return ExitConfig
	}
	run, err := report.Open(cfg.OutputDir, cmd, time.Now())
	if err != nil {
		fmt.Fprintln(sio.Err, err)
		return ExitError
	}
	defer run.Close()

	// Журнал всех запросов к ClickHouse: файл queries.log — всегда, консоль — с -v.
	qf, err := run.Create("queries.log")
	if err != nil {
		fmt.Fprintln(sio.Err, err)
		return ExitError
	}
	defer qf.Close()
	queryLog := logging.Tee(log.Handler(), logging.NewFileHandler(qf))

	hosts, err := selectHosts(cmd, cfg, opts.scope, active, storage.ClientOptions{RunID: run.ID, Version: Version(), Log: queryLog})
	if err != nil {
		fmt.Fprintln(sio.Err, err)
		return ExitConfig
	}
	defer func() {
		for _, h := range hosts {
			h.Client.Close()
		}
	}()
	f := filter.New(cfg.Filter, xstrings.SplitList(opts.databases))
	invOpt := storage.Options{
		PreserveUUID:   cfg.DDL.PreserveUUID,
		DisplaySecrets: cfg.DDL.DisplaySecrets,
		WithAccess:     cmd == "check",
		WithSettings:   cmd == "check",
		DBFilter:       func(db string) bool { return f.SkipDB(db) == "" },
	}
	log.Info("читаю схему", "hosts", len(hosts))
	invs := storage.LoadAll(ctx, hosts, invOpt)

	// Всё, что печатаем, дублируем в файл отчёта.
	rf, err := run.Create(cmd + ".txt")
	if err != nil {
		fmt.Fprintln(sio.Err, err)
		return ExitError
	}
	defer rf.Close()
	out := io.MultiWriter(sio.Out, rf)

	code := ExitOK
	switch cmd {
	case "check":
		if check.Run(ctx, cfg, invs, out) > 0 {
			code = ExitError
		}
	case "diff":
		if diff.Run(cfg, f, invs, out, opts.verbose) > 0 {
			code = ExitDiff
		}
	default:
		dry := (cfg.DryRun && !opts.execute) || opts.dryRun || cmd == "plan"
		code = runPlanApply(ctx, planArgs{
			cfg: cfg, f: f, invs: invs, active: active, out: out, run: run, log: log, in: sio.In, errw: sio.Err,
			dry: dry, yes: opts.yes, allowBlocked: opts.allowBlocked, stopOnError: opts.stopOnError, verbose: opts.verbose, invOpt: invOpt,
		})
	}
	log.Info("отчёт сохранён", "dir", run.Dir, "queries", run.Path("queries.log"))
	return code
}

// activeTargets — какие target активны в этом запуске: все или только перечисленные в --targets.
func activeTargets(cfg *config.Config, only string) (map[string]bool, error) {
	list := xstrings.SplitList(only)
	active := make(map[string]bool, len(cfg.Target.Hosts))
	for _, h := range cfg.Target.Hosts {
		active[h] = len(list) == 0
	}
	for _, h := range list {
		if !slices.Contains(cfg.Target.Hosts, h) {
			return nil, fmt.Errorf("--targets: хост %s не найден в target.hosts конфига", h)
		}
		active[h] = true
	}
	return active, nil
}

// selectHosts — какие хосты читать: plan/apply — все target (для соседей), check/diff — только активные;
// diff --scope sources|targets — только одну из групп.
func selectHosts(cmd string, cfg *config.Config, scope string, active map[string]bool, co storage.ClientOptions) ([]storage.Host, error) {
	var hosts []storage.Host
	add := func(list []string, role storage.Role, onlyActive bool) error {
		for _, h := range list {
			if onlyActive && !active[h] {
				continue
			}
			c, err := storage.New(h, cfg.Connection, co)
			if err != nil {
				return err
			}
			hosts = append(hosts, storage.Host{Client: c, Role: role})
		}
		return nil
	}
	isPlan := cmd == "plan" || cmd == "apply"
	if cmd != "diff" || scope != "targets" {
		if err := add(cfg.Source.Hosts, storage.RoleSource, false); err != nil {
			return nil, err
		}
	}
	if cmd != "diff" || scope != "sources" {
		if err := add(cfg.Target.Hosts, storage.RoleTarget, !isPlan); err != nil {
			return nil, err
		}
	}
	return hosts, nil
}

// usage печатает справку по командам и флагам.
func usage(w io.Writer) {
	fmt.Fprintf(w, `chsync %s — синхронизация DDL ClickHouse на новые шарды

Команды:
  check    предполётные проверки: связь, версии, макросы, remote_servers, репликация, ZK, окружение
  plan     построить план и SQL-скрипты для каждого target (ничего не меняет)
  apply    создать недостающие объекты на target (нужен --execute или dry_run=false)
  diff     сравнить схему на всех хостах (source + target), exit 2 при расхождениях
  init     напечатать пример конфига
  version

Флаги:
  -c FILE            конфиг (по умолчанию ./chsync.toml)
  --targets a,b      ограничить список target-хостов (поэтапный прогон)
  --databases a,b    ограничить список БД (поверх include_databases)
  --execute          реально выполнять DDL (apply)
  --dry-run          принудительно dry run
  --yes              не спрашивать подтверждение
  --allow-blocked    apply, даже если есть BLOCKED объекты (они будут пропущены)
  --stop-on-error    остановиться на первой ошибке
  --scope S          diff: all|sources|targets (по умолчанию all)
  -v                 подробный вывод

Коды выхода: 0 — ок, 1 — ошибки, 2 — расхождения/блокировки, 3 — ошибка конфига
`, Version())
}

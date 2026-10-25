// chsync — перенос схемы ClickHouse (DDL) со старых шардов на новые хосты кластера.
//
// Как работает:
//  1. Читает DDL всех объектов с хостов-источников (достаточно одной реплики с каждого старого шарда).
//  2. Объединяет их, применяет фильтры (bms, test, system, ...) и строит граф зависимостей.
//  3. Для каждого target-хоста строит план: что создать, что уже есть, что заблокировано и почему.
//  4. apply выполняет CREATE ... IF NOT EXISTS напрямую на target (без ON CLUSTER) и сверяет результат.
//
// chsync никогда не делает DROP / ALTER / RENAME / INSERT. Повторный запуск безопасен.
//
// Файлы проекта:
//
//	main.go       — CLI, конфиг, сценарий plan/apply, мелкие утилиты
//	clickhouse.go — HTTP-клиент ClickHouse и снимок хоста (Inventory)
//	ddl.go        — разбор DDL: тип объекта, UUID, аргументы движка, ZK-пути, ссылки; фильтры
//	plan.go       — объединение источников, порядок создания, план, apply, verify
//	check.go      — предполётные проверки (check), контроль репликации и сравнение схем (diff)
package main

import (
	"bufio"
	"context"
	_ "embed"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
)

var version = "1.1.0"

//go:embed chsync.example.toml
var exampleConfig string

// ============================== Конфиг ==============================

// Config — содержимое chsync.toml.
type Config struct {
	Cluster   string `toml:"cluster"`    // имя кластера из <remote_servers>
	OutputDir string `toml:"output_dir"` // куда писать отчёты и SQL
	DryRun    *bool  `toml:"dry_run"`    // nil = true

	Connection ConnConfig   `toml:"connection"`
	Source     SourceConfig `toml:"source"`
	Target     TargetConfig `toml:"target"`
	Filter     FilterConfig `toml:"filter"`
	DDL        DDLConfig    `toml:"ddl"`
}

// ConnConfig — параметры подключения, общие для всех хостов (HTTP-интерфейс 8123/8443).
type ConnConfig struct {
	Port               int    `toml:"port"`
	Secure             bool   `toml:"secure"`
	User               string `toml:"user"`
	Password           string `toml:"password"`
	PasswordEnv        string `toml:"password_env"`
	TimeoutSec         int    `toml:"timeout_sec"`
	CAFile             string `toml:"ca_file"`
	InsecureSkipVerify bool   `toml:"insecure_skip_verify"`
	UseEnvProxy        bool   `toml:"use_env_proxy"`
}

// SourceConfig — откуда берём DDL и как разрешаем расхождения между источниками.
type SourceConfig struct {
	Hosts      []string `toml:"hosts"`
	OnConflict string   `toml:"on_conflict"` // error | first — DDL объекта различается между источниками
	OnPartial  string   `toml:"on_partial"`  // warn | skip  — объект есть не на всех источниках
}

// TargetConfig — куда разворачиваем схему и какую топологию ожидаем.
type TargetConfig struct {
	Hosts            []string `toml:"hosts"`
	ExpectShards     int      `toml:"expect_shards"`      // сколько шардов должен видеть target (0 — не проверять)
	ReplicasPerShard int      `toml:"replicas_per_shard"` // ожидаемый rf новых шардов (0 — не проверять)
}

// FilterConfig — что не переносим.
type FilterConfig struct {
	IncludeDatabases []string `toml:"include_databases"`
	ExcludeDatabases []string `toml:"exclude_databases"`
	ExcludeNameRegex []string `toml:"exclude_name_regex"`
	ExcludeObjects   []string `toml:"exclude_objects"`
	ExcludeEngines   []string `toml:"exclude_engines"`
	CascadeExclude   *bool    `toml:"cascade_exclude"` // nil = true
}

// DDLConfig — как формировать и проверять DDL.
type DDLConfig struct {
	PreserveUUID     *bool                  `toml:"preserve_uuid"`  // nil = true
	IfNotExists      *bool                  `toml:"if_not_exists"`  // nil = true
	SyncFunctions    *bool                  `toml:"sync_functions"` // nil = true
	DisplaySecrets   bool                   `toml:"display_secrets"`
	GlobalReplicated string                 `toml:"global_replicated"` // block | skip | create
	ShardMacros      []string               `toml:"shard_macros"`
	CheckZooKeeper   *bool                  `toml:"check_zookeeper"` // nil = true
	Settings         map[string]interface{} `toml:"settings"`
}

// boolOr возвращает значение необязательного bool-параметра или def, если он не задан.
func boolOr(p *bool, def bool) bool {
	if p == nil {
		return def
	}
	return *p
}

// loadConfig читает TOML, отвергает неизвестные ключи (опечатки), ставит умолчания и проверяет значения.
func loadConfig(path string) (*Config, error) {
	var c Config
	md, err := toml.DecodeFile(path, &c)
	if err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}
	if und := md.Undecoded(); len(und) > 0 {
		var ks []string
		for _, k := range und {
			ks = append(ks, k.String())
		}
		return nil, fmt.Errorf("config %s: неизвестные ключи: %s", path, strings.Join(ks, ", "))
	}
	c.applyDefaults()
	return &c, c.validate()
}

// applyDefaults заполняет незаданные параметры значениями по умолчанию.
func (c *Config) applyDefaults() {
	def := func(s *string, v string) {
		if *s == "" {
			*s = v
		}
	}
	def(&c.OutputDir, "./chsync-out")
	def(&c.Connection.User, "default")
	def(&c.Source.OnConflict, "error")
	def(&c.Source.OnPartial, "warn")
	def(&c.DDL.GlobalReplicated, "block")
	if c.Connection.Port == 0 {
		c.Connection.Port = 8123
		if c.Connection.Secure {
			c.Connection.Port = 8443
		}
	}
	if c.Connection.TimeoutSec == 0 {
		c.Connection.TimeoutSec = 120
	}
	if c.Connection.Password == "" && c.Connection.PasswordEnv != "" {
		c.Connection.Password = os.Getenv(c.Connection.PasswordEnv)
	}
	if len(c.DDL.ShardMacros) == 0 {
		c.DDL.ShardMacros = []string{"shard"}
	}
}

// validate собирает все ошибки конфига сразу, чтобы не чинить их по одной.
func (c *Config) validate() error {
	var errs []string
	bad := func(f string, a ...any) { errs = append(errs, fmt.Sprintf(f, a...)) }
	if len(c.Source.Hosts) == 0 {
		bad("source.hosts пуст")
	}
	if len(c.Target.Hosts) == 0 {
		bad("target.hosts пуст")
	}
	for _, h := range c.Target.Hosts {
		if containsStr(c.Source.Hosts, h) {
			bad("хост %s указан и в source, и в target", h)
		}
	}
	oneOf := func(name, v string, allowed ...string) {
		if !containsStr(allowed, v) {
			bad("%s: допустимо %s", name, strings.Join(allowed, "|"))
		}
	}
	oneOf("source.on_conflict", c.Source.OnConflict, "error", "first")
	oneOf("source.on_partial", c.Source.OnPartial, "warn", "skip")
	oneOf("ddl.global_replicated", c.DDL.GlobalReplicated, "block", "skip", "create")
	if c.Target.ReplicasPerShard < 0 {
		bad("target.replicas_per_shard не может быть отрицательным")
	}
	for _, r := range c.Filter.ExcludeNameRegex {
		if _, err := regexp.Compile(r); err != nil {
			bad("filter.exclude_name_regex %q: %v", r, err)
		}
	}
	for _, o := range c.Filter.ExcludeObjects {
		if !strings.Contains(o, ".") {
			bad("filter.exclude_objects %q: нужен формат db.name", o)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("ошибки конфига:\n  - %s", strings.Join(errs, "\n  - "))
	}
	return nil
}

// ddlSettings возвращает настройки из [ddl.settings], которые передаются с каждым CREATE на target.
func (c *Config) ddlSettings() map[string]string {
	out := map[string]string{}
	for k, v := range c.DDL.Settings {
		switch t := v.(type) {
		case bool:
			out[k] = map[bool]string{true: "1", false: "0"}[t]
		default:
			out[k] = fmt.Sprint(t)
		}
	}
	return out
}

// ============================== CLI ==============================

// usage печатает справку по командам и флагам.
func usage() {
	fmt.Fprintf(os.Stderr, `chsync %s — синхронизация DDL ClickHouse на новые шарды

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
`, version)
}

// main разбирает команду и флаги, читает снимки хостов и запускает нужный сценарий.
func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(3)
	}
	cmd := os.Args[1]
	switch cmd {
	case "version", "--version", "-version":
		fmt.Println("chsync", version)
		return
	case "init":
		fmt.Print(exampleConfig)
		return
	case "help", "-h", "--help":
		usage()
		return
	case "check", "plan", "apply", "diff":
	default:
		fmt.Fprintf(os.Stderr, "неизвестная команда %q\n\n", cmd)
		usage()
		os.Exit(3)
	}

	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	fs.Usage = usage
	cfgPath := fs.String("c", "chsync.toml", "")
	onlyTargets := fs.String("targets", "", "")
	onlyDBs := fs.String("databases", "", "")
	execute := fs.Bool("execute", false, "")
	forceDry := fs.Bool("dry-run", false, "")
	yes := fs.Bool("yes", false, "")
	allowBlocked := fs.Bool("allow-blocked", false, "")
	stopOnError := fs.Bool("stop-on-error", false, "")
	scope := fs.String("scope", "all", "")
	verbose := fs.Bool("v", false, "")
	_ = fs.Parse(os.Args[2:])

	cfg, err := loadConfig(*cfgPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(3)
	}

	// --targets ограничивает, на каких хостах строить план и применять. Снимки остальных target
	// всё равно читаются: они нужны, чтобы узнать «соседнюю» реплику нового шарда в ZK.
	active := map[string]bool{}
	for _, h := range cfg.Target.Hosts {
		active[h] = *onlyTargets == ""
	}
	for _, h := range splitList(*onlyTargets) {
		if !containsStr(cfg.Target.Hosts, h) {
			fmt.Fprintf(os.Stderr, "--targets: хост %s не найден в target.hosts конфига\n", h)
			os.Exit(3)
		}
		active[h] = true
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	runID := time.Now().Format("20060102-150405")
	outDir := filepath.Join(cfg.OutputDir, runID+"-"+cmd)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	f := newFilter(cfg.Filter, splitList(*onlyDBs))

	// Какие хосты читать: plan/apply — все target (для соседей), check/diff — только активные.
	var clients []*Client
	var roles []string
	addHosts := func(hosts []string, role string, onlyActive bool) {
		for _, h := range hosts {
			if onlyActive && !active[h] {
				continue
			}
			c, err := newClient(h, cfg.Connection, runID)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(3)
			}
			clients, roles = append(clients, c), append(roles, role)
		}
	}
	isPlan := cmd == "plan" || cmd == "apply"
	if !(cmd == "diff" && *scope == "targets") {
		addHosts(cfg.Source.Hosts, "source", false)
	}
	if !(cmd == "diff" && *scope == "sources") {
		addHosts(cfg.Target.Hosts, "target", !isPlan)
	}

	opt := invOptions{
		preserveUUID:   boolOr(cfg.DDL.PreserveUUID, true),
		displaySecrets: cfg.DDL.DisplaySecrets,
		withAccess:     cmd == "check",
		withSettings:   cmd == "check",
		dbFilter:       func(db string) bool { return f.SkipDB(db) == "" },
	}
	logf("читаю схему с %d хостов...", len(clients))
	invs := loadAll(ctx, clients, roles, opt)

	// Всё, что печатаем, дублируем в файл отчёта.
	rf, err := os.Create(filepath.Join(outDir, cmd+".txt"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer rf.Close()
	out := io.MultiWriter(os.Stdout, rf)

	code := 0
	switch cmd {
	case "check":
		if runCheck(ctx, cfg, invs, out) > 0 {
			code = 1
		}
	case "diff":
		if runDiff(cfg, f, invs, out, *verbose) > 0 {
			code = 2
		}
	default:
		dry := boolOr(cfg.DryRun, true)
		if *execute {
			dry = false
		}
		if *forceDry || cmd == "plan" {
			dry = true
		}
		code = runPlanApply(ctx, planArgs{cfg: cfg, f: f, invs: invs, active: active, out: out, outDir: outDir,
			dry: dry, yes: *yes, allowBlocked: *allowBlocked, stopOnError: *stopOnError, verbose: *verbose, opt: opt})
	}
	logf("отчёт: %s", outDir)
	os.Exit(code)
}

// planArgs — параметры сценария plan/apply (чтобы не тащить 12 аргументов в функцию).
type planArgs struct {
	cfg                                          *Config
	f                                            *Filter
	invs                                         []*Inventory
	active                                       map[string]bool
	out                                          io.Writer
	outDir                                       string
	dry, yes, allowBlocked, stopOnError, verbose bool
	opt                                          invOptions
}

// runPlanApply строит план по каждому активному target, пишет SQL-скрипты и, если не dry run,
// выполняет CREATE и проверяет результат. Возвращает код выхода.
func runPlanApply(ctx context.Context, a planArgs) int {
	out := a.out
	var srcs, tgts []*Inventory
	srcByID := map[string]*Inventory{}
	var srcNames []string
	for _, inv := range a.invs {
		if inv.Role != "source" {
			tgts = append(tgts, inv)
			continue
		}
		if inv.Err != nil {
			fmt.Fprintf(out, "✗ источник %s недоступен: %v\n", inv.C.Name, inv.Err)
			return 1
		}
		srcs = append(srcs, inv)
		srcByID[inv.C.Name] = inv
		srcNames = append(srcNames, inv.C.Name)
	}
	u := buildUnion(a.cfg, a.f, srcs)
	printUnion(out, u)
	p := &planner{cfg: a.cfg, f: a.f, u: u, srcByID: srcByID, targets: tgts}

	// 1. План по каждому target.
	var plans []*TargetPlan
	totalCreate, totalBlocked := 0, 0
	for _, t := range tgts {
		if !a.active[t.C.Name] {
			continue
		}
		if t.Err != nil {
			fmt.Fprintf(out, "\n✗ target %s недоступен: %v\n", t.C.Name, t.Err)
			return 1
		}
		tp := p.planTarget(ctx, t)
		printPlan(out, tp, a.cfg.Cluster, a.verbose)
		path, err := writeSQL(a.outDir, tp, srcNames)
		if err != nil {
			fmt.Fprintln(out, err)
			return 1
		}
		fmt.Fprintf(out, "  SQL: %s\n", path)
		plans = append(plans, tp)
		totalCreate += tp.Counts[ACreate]
		totalBlocked += tp.Counts[ABlocked]
	}
	fmt.Fprintf(out, "\nВСЕГО по %d target: create=%d blocked=%d\n", len(plans), totalCreate, totalBlocked)

	// 2. Решаем, выполнять ли.
	switch {
	case a.dry:
		fmt.Fprintln(out, "DRY RUN: ничего не выполнено. Для выполнения: chsync apply --execute")
		if totalBlocked > 0 {
			return 2
		}
		return 0
	case totalBlocked > 0 && !a.allowBlocked:
		fmt.Fprintln(out, "Есть BLOCKED объекты — apply остановлен. Разберите причины или запустите с --allow-blocked (они будут пропущены).")
		return 2
	case totalCreate == 0:
		fmt.Fprintln(out, "Нечего создавать — target уже синхронизированы.")
		return 0
	case !a.yes && !confirm(fmt.Sprintf("Выполнить %d CREATE на %d хостах? Введите yes: ", totalCreate, len(plans))):
		fmt.Fprintln(out, "Отменено.")
		return 1
	}

	// 3. Выполняем и сверяем, хост за хостом.
	code := 0
	for _, tp := range plans {
		if len(tp.creates()) == 0 {
			continue
		}
		logPath := filepath.Join(a.outDir, fileSafe(tp.Inv.C.Name)+".apply.log")
		lf, err := os.Create(logPath)
		if err != nil {
			fmt.Fprintln(out, err)
			return 1
		}
		logf("APPLY %s: %d объектов", tp.Inv.label(), len(tp.creates()))
		res := applyTarget(ctx, a.cfg, tp, lf, a.stopOnError)
		lf.Close()
		issues := verifyTarget(ctx, u, tp, a.opt)
		fmt.Fprintf(out, "\n══ APPLY %s: ok=%d failed=%d  (лог: %s)\n", tp.Inv.label(), res.OK, len(res.Failures), logPath)
		for _, s := range res.Failures {
			fmt.Fprintln(out, "  ✗ "+s)
			code = 1
		}
		for _, s := range issues {
			fmt.Fprintln(out, "  ! verify: "+s)
			code = 1
		}
		if len(issues) == 0 {
			fmt.Fprintln(out, "  ✓ verify: созданные объекты совпадают с источником, реплики в порядке")
		}
		if ctx.Err() != nil {
			fmt.Fprintln(out, "прервано")
			return 1
		}
		if code != 0 && a.stopOnError {
			break
		}
	}
	fmt.Fprintln(out, "\nДальше: chsync diff — убедиться, что схема везде одинакова.")
	return code
}

// ============================== Утилиты ==============================

// logf пишет строку прогресса в stderr с отметкой времени.
func logf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "%s "+format+"\n", append([]any{time.Now().Format("15:04:05")}, a...)...)
}

// confirm спрашивает подтверждение в терминале; без терминала (cron, pipe) — отказ, нужен --yes.
func confirm(prompt string) bool {
	fi, err := os.Stdin.Stat()
	if err != nil || fi.Mode()&os.ModeCharDevice == 0 {
		fmt.Fprintln(os.Stderr, "stdin не терминал — используйте --yes")
		return false
	}
	fmt.Fprint(os.Stderr, prompt)
	line, _ := bufio.NewReader(os.Stdin).ReadString('\n')
	return strings.TrimSpace(line) == "yes"
}

// splitList разбивает "a, b,c" в ["a","b","c"], пропуская пустые элементы.
func splitList(s string) []string {
	var out []string
	for _, x := range strings.Split(s, ",") {
		if x = strings.TrimSpace(x); x != "" {
			out = append(out, x)
		}
	}
	return out
}

// containsStr — есть ли строка s в срезе.
func containsStr(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

// uniq возвращает отсортированные уникальные значения.
func uniq(in []string) []string {
	m := map[string]bool{}
	var out []string
	for _, s := range in {
		if !m[s] {
			m[s] = true
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}

// sortedKeys возвращает ключи map в отсортированном порядке (для стабильного вывода).
func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// truncate обрезает длинные сообщения об ошибках для отчёта.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// minInt / maxInt — встроенные min/max появились только в Go 1.21, а в Debian 12 — Go 1.19.
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

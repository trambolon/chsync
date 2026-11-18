// Command demogen drives the real ddlSaver flows with in-memory fakes and
// emits a JSON transcript for the documentation page.
//
// Nothing here reimplements the tool: every line of output comes from
// app.Deps (the same entry points cmd/ddl-saver calls), snapshot.Run,
// recovery's planner/executor/guard and the real slog handler. Only
// ClickHouse and GitLab are faked, behind the interfaces the production
// code already depends on.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"strings"
	"time"

	"gitlab.example.com/infra/ddl-saver/internal/app"
	"gitlab.example.com/infra/ddl-saver/internal/clickhouse"
	"gitlab.example.com/infra/ddl-saver/internal/config"
	"gitlab.example.com/infra/ddl-saver/internal/gitlab"
	"gitlab.example.com/infra/ddl-saver/internal/model"
)

// ---------- transcript model ----------

type Line struct {
	Kind string `json:"k"` // out | log | warn | err
	Text string `json:"t"`
}

type Step struct {
	Title   string            `json:"title"`
	Cmd     string            `json:"cmd,omitempty"`
	Note    string            `json:"note,omitempty"`
	Lines   []Line            `json:"lines,omitempty"`
	Tree    []string          `json:"tree,omitempty"`
	Files   map[string]string `json:"files,omitempty"`
	Sent    []string          `json:"sent,omitempty"`
	Commits []string          `json:"commits,omitempty"`
	Exit    *int              `json:"exit,omitempty"`
}

type Scenario struct {
	ID       string `json:"id"`
	Title    string `json:"title"`
	Subtitle string `json:"subtitle"`
	Steps    []Step `json:"steps"`
}

type Doc struct {
	Scenarios []Scenario `json:"scenarios"`
	Config    string     `json:"config"`
	GoVersion string     `json:"goVersion"`
}

// ---------- log capture ----------

const logMark = "\x00L\x00"

// markWriter prefixes each slog record so the transcript can tell log lines
// from stdout while preserving their real interleaved order.
type markWriter struct{ buf *bytes.Buffer }

func (m markWriter) Write(p []byte) (int, error) {
	m.buf.WriteString(logMark)
	m.buf.Write(p)
	return len(p), nil
}

var fixedTime = time.Date(2026, 10, 25, 21, 0, 0, 0, time.FixedZone("MSK", 3*3600))

func newLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(markWriter{buf}, &slog.HandlerOptions{
		Level: slog.LevelInfo,
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.Time(slog.TimeKey, fixedTime)
			}
			return a
		},
	}))
}

func parse(buf *bytes.Buffer) []Line {
	var out []Line
	for _, raw := range strings.Split(buf.String(), "\n") {
		if strings.TrimSpace(raw) == "" {
			continue
		}
		if rest, ok := cutMark(raw); ok {
			kind := "log"
			switch {
			case strings.Contains(rest, "level=WARN"):
				kind = "warn"
			case strings.Contains(rest, "level=ERROR"):
				kind = "err"
			}
			out = append(out, Line{Kind: kind, Text: rest})
			continue
		}
		out = append(out, Line{Kind: "out", Text: raw})
	}
	buf.Reset()
	return out
}

func cutMark(s string) (string, bool) {
	if i := strings.Index(s, logMark); i >= 0 {
		return s[:i] + s[i+len(logMark):], true
	}
	return s, false
}

// ---------- fake ClickHouse ----------

type fakeCH struct {
	topo    clickhouse.Topology
	dbs     []clickhouse.ListedDatabase
	objects []clickhouse.ListedObject
	// live holds what system.tables/system.databases would report:
	// "db" for a database, "db.name" for an object.
	live map[string]bool
	// execErr, when set, is returned by every Exec.
	execErr error
	sent    []string
}

func (f *fakeCH) Ping(context.Context) error { return nil }
func (f *fakeCH) Close() error               { return nil }
func (f *fakeCH) Endpoint() string           { return "ch-1.prod.internal:9000" }
func (f *fakeCH) Topology(context.Context) (*clickhouse.Topology, error) {
	t := f.topo
	return &t, nil
}
func (f *fakeCH) ListDatabases(context.Context) ([]clickhouse.ListedDatabase, error) {
	return f.dbs, nil
}
func (f *fakeCH) ListObjects(context.Context) ([]clickhouse.ListedObject, error) {
	return f.objects, nil
}
func (f *fakeCH) ObjectExists(_ context.Context, ref model.ObjectRef) (bool, error) {
	return f.live[ref.String()], nil
}
func (f *fakeCH) ListRefs(context.Context) ([]model.ObjectRef, error) {
	var out []model.ObjectRef
	for k, v := range f.live {
		if !v {
			continue
		}
		if i := strings.IndexByte(k, '.'); i >= 0 {
			out = append(out, model.ObjectRef{Database: k[:i], Name: k[i+1:]})
		} else {
			out = append(out, model.ObjectRef{Database: k})
		}
	}
	return out, nil
}
func (f *fakeCH) Exec(_ context.Context, sql string) error {
	if f.execErr != nil {
		return f.execErr
	}
	f.sent = append(f.sent, sql)
	return nil
}

// syncLive rebuilds the live set from the declared databases and objects,
// the way a real cluster's system tables would read.
func (f *fakeCH) syncLive() {
	f.live = map[string]bool{}
	for _, d := range f.dbs {
		f.live[d.Name] = true
	}
	for _, o := range f.objects {
		f.live[o.Ref.String()] = true
	}
}

// ---------- fake GitLab ----------

type memRepo struct {
	files map[string]string
	msgs  []string
}

func newRepo() *memRepo { return &memRepo{files: map[string]string{}} }

func (r *memRepo) ListTree(_ context.Context, _, base string) ([]string, error) {
	var out []string
	for p := range r.files {
		if strings.HasPrefix(p, base+"/") {
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out, nil
}
func (r *memRepo) GetFiles(_ context.Context, _ string, paths []string) (map[string]string, error) {
	out := map[string]string{}
	for _, p := range paths {
		out[p] = r.files[p]
	}
	return out, nil
}
func (r *memRepo) Commit(_ context.Context, _, msg string, actions []gitlab.Action) error {
	r.msgs = append(r.msgs, msg)
	for _, a := range actions {
		if a.Op == gitlab.OpDelete {
			delete(r.files, a.Path)
		} else {
			r.files[a.Path] = a.Content
		}
	}
	return nil
}
func (r *memRepo) tree() []string {
	var out []string
	for p := range r.files {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

// ---------- harness ----------

type harness struct {
	deps *app.Deps
	ch   *fakeCH
	repo *memRepo
	buf  *bytes.Buffer
}

func newHarness(ch *fakeCH, onDeleted string) *harness {
	cfg := &config.Config{
		App: config.App{LogLevel: "info", LogFormat: "text"},
		Scheduler: config.Scheduler{
			Spec: "0 */6 * * *", Timezone: "Europe/Moscow", RunOnStart: true,
		},
		Clusters: []config.Cluster{{
			Name: "prod-analytics",
			Connection: config.Connection{Endpoints: []config.Endpoint{
				{Host: "ch-1.prod.internal", Port: 9000},
				{Host: "ch-2.prod.internal", Port: 9000},
			}},
			Auth: config.Auth{User: "ddl_bot"},
			Filter: config.Filter{
				SkipDatabases:   []string{"system", "information_schema"},
				SkipTablesRegex: []string{`^\.inner\.`, "bms", "john111"},
			},
		}},
		GitLab: config.GitLab{
			URL: "https://gitlab.company.com", Token: "glpat-xxx",
			ProjectPath: "infra/clickhouse-schemas", Branch: "main",
			CommitAuthor: "DDL Saver Bot <ddl-saver@company.com>",
			OnDeleted:    onDeleted,
		},
	}
	// Mirror examples/config.toml, so the video shows what a user of the
	// shipped example actually gets.
	cfg.Recovery.DeferInvalidObjects = true
	cfg.ApplyDefaults()
	cfg.Recovery.RetryBackoff = 0 // keep the transcript instant

	buf := &bytes.Buffer{}
	repo := newRepo()
	ch.syncLive()

	h := &harness{ch: ch, repo: repo, buf: buf}
	h.deps = &app.Deps{
		Config:  cfg,
		Log:     newLogger(buf),
		Connect: func(context.Context, *config.Cluster) (clickhouse.Client, error) { return ch, nil },
		Repo:    repo,
		Out:     buf,
		Confirm: func(prompt string) (bool, error) {
			fmt.Fprint(buf, "\n"+prompt+"y\n")
			return true, nil
		},
		Status: app.NewStatus(),
		Now:    func() time.Time { return fixedTime },
	}
	return h
}

func (h *harness) step(title, cmd, note string, fn func() *int) Step {
	h.ch.sent = nil
	before := len(h.repo.msgs)
	code := fn()
	return Step{
		Title: title, Cmd: cmd, Note: note,
		Lines:   parse(h.buf),
		Sent:    append([]string(nil), h.ch.sent...),
		Commits: append([]string(nil), h.repo.msgs[before:]...),
		Exit:    code,
	}
}

func ptr(i int) *int { return &i }

// ---------- DDL fixtures ----------

const (
	ddlDatabase = "CREATE DATABASE events ENGINE = Atomic"

	ddlClicksRaw = "CREATE TABLE events.clicks_raw (`ts` DateTime, `uid` UInt64) " +
		"ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/events/clicks_raw', '{replica}') " +
		"PARTITION BY toYYYYMM(ts) ORDER BY ts"

	ddlClicksRawV2 = "CREATE TABLE events.clicks_raw (`ts` DateTime, `uid` UInt64, `country` LowCardinality(String)) " +
		"ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/events/clicks_raw', '{replica}') " +
		"PARTITION BY toYYYYMM(ts) ORDER BY ts"

	ddlClicksAgg = "CREATE TABLE events.clicks_agg (`d` Date, `hits` UInt64) " +
		"ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{shard}/events/clicks_agg', '{replica}') ORDER BY d"

	ddlClicksMV = "CREATE MATERIALIZED VIEW events.clicks_mv TO events.clicks_agg AS " +
		"SELECT toDate(ts) AS d, count() AS hits FROM events.clicks_raw GROUP BY d"

	ddlBare = "CREATE TABLE events.clicks_raw (`ts` DateTime, `uid` UInt64) " +
		"ENGINE = ReplicatedMergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts"
)

func table(db, name, engine, ddl string) clickhouse.ListedObject {
	return clickhouse.ListedObject{
		Ref: model.ObjectRef{Database: db, Name: name}, Kind: model.KindTable,
		Engine: engine, DDL: ddl,
	}
}

func mv(db, name, ddl string) clickhouse.ListedObject {
	return clickhouse.ListedObject{
		Ref: model.ObjectRef{Database: db, Name: name}, Kind: model.KindMaterializedView,
		Engine: "MaterializedView", DDL: ddl,
	}
}

func eventsDB() []clickhouse.ListedDatabase {
	return []clickhouse.ListedDatabase{{Name: "events", Engine: "Atomic", DDL: ddlDatabase}}
}

func distributedTopo() clickhouse.Topology {
	return clickhouse.Topology{
		ClusterMacro: "analytics", ShardCount: 2, ReplicaCount: 2, IsDistributed: true,
		DefaultReplicaPath: "/clickhouse/tables/{uuid}/{shard}",
		DefaultReplicaName: "{replica}", ReplicaDefaultsKnown: true,
	}
}

// ---------- scenario 1: full lifecycle ----------

func scenarioLifecycle() Scenario {
	ch := &fakeCH{
		topo: distributedTopo(),
		dbs:  eventsDB(),
		objects: []clickhouse.ListedObject{
			table("events", "clicks_raw", "ReplicatedMergeTree", ddlClicksRaw),
			table("events", "clicks_agg", "ReplicatedSummingMergeTree", ddlClicksAgg),
			mv("events", "clicks_mv", ddlClicksMV),
		},
	}
	h := newHarness(ch, "deprecate")
	ctx := context.Background()
	var steps []Step

	steps = append(steps, h.step(
		"Проверка конфигурации",
		"ddl-saver validate -config config.toml -connect",
		"Разбирает TOML, подставляет ${ENV}, собирает все ошибки сразу и — с -connect — проверяет доступность кластера.",
		func() *int {
			report, err := h.deps.Validate(ctx, true)
			fmt.Fprint(h.buf, report)
			if err != nil {
				fmt.Fprintln(h.buf, "ошибка:", err)
				return ptr(1)
			}
			return ptr(0)
		}))

	steps = append(steps, h.step(
		"Сухой прогон",
		"ddl-saver snapshot -config config.toml -dry-run",
		"Считает дифф и показывает, что было бы сделано. Ни одного коммита.",
		func() *int {
			if err := h.deps.SnapshotAll(ctx, app.SnapshotOpts{DryRun: true}); err != nil {
				return ptr(1)
			}
			return ptr(0)
		}))

	first := h.step(
		"Первый снапшот",
		"ddl-saver snapshot -config config.toml",
		"ON CLUSTER '{cluster}' добавляется, потому что топология распределённая. Путь ZooKeeper сохраняется дословно.",
		func() *int {
			if err := h.deps.SnapshotAll(ctx, app.SnapshotOpts{}); err != nil {
				return ptr(1)
			}
			return ptr(0)
		})
	first.Tree = h.repo.tree()
	first.Files = map[string]string{
		"schemas/prod-analytics/events/tables/clicks_raw.sql": h.repo.files["schemas/prod-analytics/events/tables/clicks_raw.sql"],
		"schemas/prod-analytics/manifest.json":                h.repo.files["schemas/prod-analytics/manifest.json"],
	}
	steps = append(steps, first)

	steps = append(steps, h.step(
		"Повторный прогон без изменений",
		"ddl-saver snapshot -config config.toml",
		"Желаемое состояние пересчитывается с нуля каждый раз. Совпало — коммита нет. Таймстемп манифеста исключён из сравнения именно поэтому.",
		func() *int {
			if err := h.deps.SnapshotAll(ctx, app.SnapshotOpts{}); err != nil {
				return ptr(1)
			}
			return ptr(0)
		}))

	// Schema change upstream.
	ch.objects[0].DDL = ddlClicksRawV2
	ch.syncLive()
	changed := h.step(
		"Изменение схемы в ClickHouse",
		"ddl-saver snapshot -config config.toml",
		"В clicks_raw добавили колонку country. В коммит-сообщении считаются только .sql-файлы, затронутые этим коммитом.",
		func() *int {
			if err := h.deps.SnapshotAll(ctx, app.SnapshotOpts{}); err != nil {
				return ptr(1)
			}
			return ptr(0)
		})
	changed.Files = map[string]string{
		"schemas/prod-analytics/events/tables/clicks_raw.sql": h.repo.files["schemas/prod-analytics/events/tables/clicks_raw.sql"],
	}
	steps = append(steps, changed)

	// Disaster: the MV and its source table are gone.
	ch.objects = ch.objects[1:2] // only clicks_agg survives
	ch.syncLive()

	steps = append(steps, h.step(
		"Авария: объект потерян, строим план",
		"ddl-saver recover -config config.toml -cluster prod-analytics -object events.clicks_mv -dry-run",
		"clicks_mv и clicks_raw удалены; clicks_agg жив. Планировщик строит замыкание вверх, живой объект закрывает зависимость и попадает в план как skip.",
		func() *int {
			code, err := h.deps.Recover(ctx, app.RecoverOpts{
				Cluster: "prod-analytics", Objects: []string{"events.clicks_mv"}, DryRun: true,
			})
			if err != nil {
				fmt.Fprintln(h.buf, "ошибка:", err)
			}
			return ptr(code)
		}))

	steps = append(steps, h.step(
		"Применение восстановления",
		"ddl-saver recover -config config.toml -cluster prod-analytics -object events.clicks_mv",
		"Порядок топологический: таблица раньше MV. Перед каждым CREATE существование перепроверяется заново.",
		func() *int {
			code, err := h.deps.Recover(ctx, app.RecoverOpts{
				Cluster: "prod-analytics", Objects: []string{"events.clicks_mv"},
			})
			if err != nil {
				fmt.Fprintln(h.buf, "ошибка:", err)
			}
			return ptr(code)
		}))

	return Scenario{
		ID: "lifecycle", Title: "Полный цикл",
		Subtitle: "validate → snapshot → изменение схемы → потеря объекта → recover",
		Steps:    steps,
	}
}

// ---------- scenario 2: safety ----------

func scenarioSafety() Scenario {
	base := func() (*harness, *fakeCH) {
		ch := &fakeCH{
			topo: distributedTopo(),
			dbs:  eventsDB(),
			objects: []clickhouse.ListedObject{
				table("events", "clicks_raw", "ReplicatedMergeTree", ddlClicksRaw),
				table("events", "clicks_agg", "ReplicatedSummingMergeTree", ddlClicksAgg),
				mv("events", "clicks_mv", ddlClicksMV),
			},
		}
		h := newHarness(ch, "deprecate")
		_ = h.deps.SnapshotAll(context.Background(), app.SnapshotOpts{})
		h.buf.Reset()
		return h, ch
	}
	ctx := context.Background()
	var steps []Step

	// 1. Tampered repository file.
	h, ch := base()
	tampered := "DROP TABLE events.clicks_raw"
	h.repo.files["schemas/prod-analytics/events/tables/clicks_raw.sql"] = tampered
	ch.objects = ch.objects[1:2]
	ch.syncLive()
	st := h.step(
		"Подменённый файл в репозитории",
		"ddl-saver recover -config config.toml -cluster prod-analytics -object events.clicks_mv",
		"Кто-то смёрджил MR, заменив DDL на DROP. Guard проверяет каждый оператор ещё на этапе планирования — до запроса подтверждения. План не строится целиком, а не «применяется безопасная часть».",
		func() *int {
			code, err := h.deps.Recover(ctx, app.RecoverOpts{
				Cluster: "prod-analytics", Objects: []string{"events.clicks_mv"},
			})
			if err != nil {
				fmt.Fprintln(h.buf, "fatal:", err)
			}
			return ptr(code)
		})
	st.Files = map[string]string{
		"schemas/prod-analytics/events/tables/clicks_raw.sql  (подменён)": tampered,
	}
	steps = append(steps, st)

	// 2. What actually reaches ClickHouse.
	h, ch = base()
	ch.objects = ch.objects[1:2]
	ch.syncLive()
	steps = append(steps, h.step(
		"Что реально уходит в ClickHouse",
		"ddl-saver recover -config config.toml -cluster prod-analytics -object events.clicks_mv -yes",
		"IF NOT EXISTS отсутствует в create_table_query и дописывается guard'ом. Путь ZooKeeper, макросы и ON CLUSTER проходят нетронутыми.",
		func() *int {
			code, err := h.deps.Recover(ctx, app.RecoverOpts{
				Cluster: "prod-analytics", Objects: []string{"events.clicks_mv"}, Yes: true,
			})
			if err != nil {
				fmt.Fprintln(h.buf, "ошибка:", err)
			}
			return ptr(code)
		}))

	// 3. Stale ZooKeeper path.
	h, ch = base()
	ch.objects = ch.objects[1:2]
	ch.syncLive()
	ch.execErr = errors.New("code: 253, message: Replica " +
		"/clickhouse/tables/01/events/clicks_raw/replicas/replica_1 already exists")
	steps = append(steps, h.step(
		"Занятый путь в ZooKeeper",
		"ddl-saver recover -config config.toml -cluster prod-analytics -object events.clicks_mv -yes",
		"Код 253 классифицируется как постоянный: одна попытка вместо четырёх, ретраи не тратятся, в отчёте — подсказка, как освободить путь. Зависимый MV откладывается, а не падает тем же стеком.",
		func() *int {
			code, err := h.deps.Recover(ctx, app.RecoverOpts{
				Cluster: "prod-analytics", Objects: []string{"events.clicks_mv"}, Yes: true,
			})
			if err != nil {
				fmt.Fprintln(h.buf, "ошибка:", err)
			}
			return ptr(code)
		}))

	return Scenario{
		ID: "safety", Title: "Безопасность",
		Subtitle: "граница доверия репозитория, IF NOT EXISTS, постоянные ошибки",
		Steps:    steps,
	}
}

// ---------- scenario 3: deletion & deprecation ----------

func scenarioDeprecation() Scenario {
	ctx := context.Background()
	var steps []Step

	ch := &fakeCH{
		topo: distributedTopo(),
		dbs:  eventsDB(),
		objects: []clickhouse.ListedObject{
			table("events", "keep_me", "ReplicatedMergeTree",
				"CREATE TABLE events.keep_me (`id` UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/events/keep_me', '{replica}') ORDER BY id"),
			table("events", "drop_me", "ReplicatedMergeTree",
				"CREATE TABLE events.drop_me (`id` UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/events/drop_me', '{replica}') ORDER BY id"),
		},
	}
	h := newHarness(ch, "deprecate")

	bl := h.step("Базовый снапшот", "ddl-saver snapshot -config config.toml",
		"Две таблицы и файл самой базы.",
		func() *int { _ = h.deps.SnapshotAll(ctx, app.SnapshotOpts{}); return ptr(0) })
	bl.Tree = h.repo.tree()
	steps = append(steps, bl)

	// Table dropped.
	dropped := ch.objects[1]
	ch.objects = ch.objects[:1]
	ch.syncLive()
	dep := h.step("Таблица удалена из ClickHouse", "ddl-saver snapshot -config config.toml",
		"on_deleted = \"deprecate\" (по умолчанию): файл переезжает в _deprecated/ тем же коммитом, а манифест фиксирует since. Ничего не уничтожается.",
		func() *int { _ = h.deps.SnapshotAll(ctx, app.SnapshotOpts{}); return ptr(0) })
	dep.Tree = h.repo.tree()
	dep.Files = map[string]string{
		"schemas/prod-analytics/manifest.json": h.repo.files["schemas/prod-analytics/manifest.json"],
	}
	steps = append(steps, dep)

	stable := h.step("Следующий прогон", "ddl-saver snapshot -config config.toml",
		"Архив исключён из диффа, а since переносится из прошлого манифеста. Поэтому депрекация стабильна и не создаёт коммит каждый раз.",
		func() *int { _ = h.deps.SnapshotAll(ctx, app.SnapshotOpts{}); return ptr(0) })
	steps = append(steps, stable)

	// Object returns.
	ch.objects = append(ch.objects, dropped)
	ch.syncLive()
	undep := h.step("Таблицу пересоздали", "ddl-saver snapshot -config config.toml",
		"Живой объект вытесняет архивную копию: файл возвращается на место, копия из _deprecated/ удаляется, запись в манифесте снимается.",
		func() *int { _ = h.deps.SnapshotAll(ctx, app.SnapshotOpts{}); return ptr(0) })
	undep.Tree = h.repo.tree()
	steps = append(steps, undep)

	// Whole database dropped.
	ch.objects = nil
	ch.dbs = nil
	ch.syncLive()
	wholeDB := h.step("Удалена вся база", "ddl-saver snapshot -config config.toml",
		"Отдельного случая для базы нет: переезжают все её файлы по одному, включая _database.sql. Поэтому «удалили базу» обрабатывается тем же кодом, что и «удалили таблицу».",
		func() *int { _ = h.deps.SnapshotAll(ctx, app.SnapshotOpts{}); return ptr(0) })
	wholeDB.Tree = h.repo.tree()
	steps = append(steps, wholeDB)

	// prune variant, from a fresh baseline.
	ch2 := &fakeCH{
		topo: distributedTopo(),
		dbs:  eventsDB(),
		objects: []clickhouse.ListedObject{
			table("events", "keep_me", "ReplicatedMergeTree",
				"CREATE TABLE events.keep_me (`id` UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/events/keep_me', '{replica}') ORDER BY id"),
			table("events", "drop_me", "ReplicatedMergeTree",
				"CREATE TABLE events.drop_me (`id` UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/events/drop_me', '{replica}') ORDER BY id"),
		},
	}
	h2 := newHarness(ch2, "prune")
	_ = h2.deps.SnapshotAll(ctx, app.SnapshotOpts{})
	h2.buf.Reset()
	ch2.objects = ch2.objects[:1]
	ch2.syncLive()
	pr := h2.step("Та же ситуация при on_deleted = \"prune\"", "ddl-saver snapshot -config config.toml",
		"Альтернативная политика: файл удаляется, история git его помнит. Третий вариант — \"keep\": файл остаётся на месте, в лог идёт предупреждение.",
		func() *int { _ = h2.deps.SnapshotAll(ctx, app.SnapshotOpts{}); return ptr(0) })
	pr.Tree = h2.repo.tree()
	steps = append(steps, pr)

	return Scenario{
		ID: "deprecation", Title: "Удаление и депрекация",
		Subtitle: "_deprecated/, запись since, авто-возврат, удалённая база",
		Steps:    steps,
	}
}

// ---------- scenario 4: topologies & replicated engines ----------

func scenarioTopology() Scenario {
	ctx := context.Background()
	var steps []Step

	// Single node.
	single := &fakeCH{
		topo: clickhouse.Topology{
			DefaultReplicaPath: "/clickhouse/tables/{uuid}/{shard}",
			DefaultReplicaName: "{replica}", ReplicaDefaultsKnown: true,
		},
		dbs:     eventsDB(),
		objects: []clickhouse.ListedObject{table("events", "clicks_raw", "ReplicatedMergeTree", ddlClicksRaw)},
	}
	hs := newHarness(single, "deprecate")
	st := hs.step("Одна нода: один шард, одна реплика", "ddl-saver snapshot -config config.toml",
		"Макроса cluster нет, IsDistributed = false — ON CLUSTER не добавляется нигде, включая файл базы. При этом движок остаётся Replicated: это ортогональные вещи.",
		func() *int { _ = hs.deps.SnapshotAll(ctx, app.SnapshotOpts{}); return ptr(0) })
	st.Files = map[string]string{
		"schemas/prod-analytics/events/tables/clicks_raw.sql": hs.repo.files["schemas/prod-analytics/events/tables/clicks_raw.sql"],
		"schemas/prod-analytics/events/_database.sql":         hs.repo.files["schemas/prod-analytics/events/_database.sql"],
	}
	steps = append(steps, st)

	// Distributed.
	dist := &fakeCH{
		topo:    distributedTopo(),
		dbs:     eventsDB(),
		objects: []clickhouse.ListedObject{table("events", "clicks_raw", "ReplicatedMergeTree", ddlClicksRaw)},
	}
	hd := newHarness(dist, "deprecate")
	dt := hd.step("Распределённый кластер: 2 шарда × 2 реплики", "ddl-saver snapshot -config config.toml",
		"Тот же DDL, та же команда. ON CLUSTER вставляется после имени объекта — не внутрь аргументов движка — и в виде макроса, чтобы файл оставался переносимым.",
		func() *int { _ = hd.deps.SnapshotAll(ctx, app.SnapshotOpts{}); return ptr(0) })
	dt.Files = map[string]string{
		"schemas/prod-analytics/events/tables/clicks_raw.sql": hd.repo.files["schemas/prod-analytics/events/tables/clicks_raw.sql"],
	}
	steps = append(steps, dt)

	// Four replica-path notice variants, each through the real recover flow.
	type variant struct {
		title, note string
		ddl         string
		topo        clickhouse.Topology
	}
	uuidTopo := distributedTopo()
	nonUUID := distributedTopo()
	nonUUID.DefaultReplicaPath = "/clickhouse/tables/{shard}/{database}/{table}"
	unknown := distributedTopo()
	unknown.ReplicaDefaultsKnown = false
	unknown.DefaultReplicaPath = ""
	unknown.DefaultReplicaName = ""

	for _, v := range []variant{
		{
			title: "Путь ZooKeeper задан в DDL",
			note:  "Путь зафиксирован, поэтому устаревшая регистрация от прошлой инкарнации таблицы заблокирует CREATE.",
			ddl:   ddlClicksRaw, topo: uuidTopo,
		},
		{
			title: "Без аргументов, серверный дефолт на {uuid}",
			note:  "ddlSaver читает default_replica_path из system.server_settings. Дефолт на {uuid} — самая дружелюбная форма для DR: конфликта путей быть не может.",
			ddl:   ddlBare, topo: uuidTopo,
		},
		{
			title: "Без аргументов, дефолт без {uuid}",
			note:  "Путь разрешается детерминированно, поэтому риск устаревшей регистрации возвращается, хотя в DDL пути нет.",
			ddl:   ddlBare, topo: nonUUID,
		},
		{
			title: "Без аргументов, дефолты прочитать не удалось",
			note:  "system.server_settings появилась в ClickHouse 22.10, и прав на неё может не быть. Инструмент говорит «не знаю» вместо догадки.",
			ddl:   ddlBare, topo: unknown,
		},
	} {
		ch := &fakeCH{
			topo:    v.topo,
			dbs:     eventsDB(),
			objects: []clickhouse.ListedObject{table("events", "clicks_raw", "ReplicatedMergeTree", v.ddl)},
		}
		h := newHarness(ch, "deprecate")
		_ = h.deps.SnapshotAll(ctx, app.SnapshotOpts{})
		h.buf.Reset()
		ch.objects = nil
		ch.syncLive()
		steps = append(steps, h.step(v.title,
			"ddl-saver recover -config config.toml -cluster prod-analytics -object events.clicks_raw -dry-run",
			v.note,
			func() *int {
				code, err := h.deps.Recover(ctx, app.RecoverOpts{
					Cluster: "prod-analytics", Objects: []string{"events.clicks_raw"}, DryRun: true,
				})
				if err != nil {
					fmt.Fprintln(h.buf, "ошибка:", err)
				}
				return ptr(code)
			}))
	}

	return Scenario{
		ID: "topology", Title: "Топологии и Replicated",
		Subtitle: "ON CLUSTER по топологии, путь ZooKeeper, default_replica_path",
		Steps:    steps,
	}
}

func main() {
	cfgBytes, err := os.ReadFile("examples/config.toml")
	if err != nil {
		cfgBytes = []byte("")
	}
	doc := Doc{
		Scenarios: []Scenario{
			scenarioLifecycle(),
			scenarioSafety(),
			scenarioDeprecation(),
			scenarioTopology(),
			scenarioInstall(),
			scenarioFilters(),
			scenarioRecoverExtras(),
			scenarioDaemon(),
			scenarioMore(),
		},
		Config:    string(cfgBytes),
		GoVersion: "1.26",
	}
	enc := json.NewEncoder(io.Writer(os.Stdout))
	enc.SetIndent("", "  ")
	if err := enc.Encode(doc); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

package main

// Extra chapters for the video manual. Same rule as the rest of demogen:
// every line comes from the real packages; only ClickHouse and GitLab are
// faked behind their interfaces.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"gitlab.example.com/infra/ddl-saver/internal/app"
	"gitlab.example.com/infra/ddl-saver/internal/clickhouse"
	"gitlab.example.com/infra/ddl-saver/internal/config"
	"gitlab.example.com/infra/ddl-saver/internal/observability"
)

// ---------- install / config errors ----------

func scenarioInstall() Scenario {
	var steps []Step

	// 1. Missing environment variables: Load wraps ExpandEnv's error, and
	// main logs it with slog.Error("fatal", "error", err).
	raw := `[[clusters]]
name = "prod-analytics"
  [clusters.auth]
  user     = "${CH_PROD_USER}"
  password = "${CH_PROD_PASSWORD}"

[gitlab]
token = "${GITLAB_TOKEN}"
`
	buf := newLogBuf()
	_, err := config.ExpandEnv(raw, func(string) (string, bool) { return "", false })
	newLogger(buf).Error("fatal", "error", fmt.Errorf("config %s: %w", "config.toml", err))
	steps = append(steps, Step{
		Title: "Забыли экспортировать переменные",
		Cmd:   "ddl-saver validate -config config.toml",
		Lines: parse(buf),
		Exit:  ptr(1),
	})

	// 2. Several config mistakes at once: Validate collects them all.
	cfg := &config.Config{
		Clusters: []config.Cluster{
			{Name: "prod-analytics", Connection: config.Connection{Scheme: "http",
				Endpoints: []config.Endpoint{{Host: "ch-1.prod.internal"}}}, Auth: config.Auth{User: "ddl_reader"}},
			{Name: "prod-analytics", Auth: config.Auth{Type: "tls_certificate"}},
		},
		GitLab:   config.GitLab{URL: "https://gitlab.company.com", Token: "t", ProjectPath: "clickhouse-schemas"},
		Recovery: config.Recovery{CreateStrategy: "drop_and_create"},
	}
	cfg.ApplyDefaults()
	cfg.Clusters[0].Filter.SkipTablesRegex = []string{"bms", "john[111"}
	verr := cfg.Validate()
	var lines []Line
	for _, l := range strings.Split(verr.Error(), "\n") {
		lines = append(lines, Line{Kind: "err", Text: l})
	}
	steps = append(steps, Step{
		Title: "Несколько ошибок в конфиге",
		Cmd:   "ddl-saver validate -config config.toml",
		Lines: lines,
		Exit:  ptr(1),
	})
	return Scenario{ID: "install", Title: "Установка", Steps: steps}
}

func newLogBuf() *bytes.Buffer { return &bytes.Buffer{} }

// ---------- filters ----------

func scenarioFilters() Scenario {
	f, err := clickhouse.NewFilter(
		[]string{"system", "information_schema"},
		[]string{`^\.inner\.`, "bms", "john111"},
		nil)
	if err != nil {
		panic(err)
	}
	var decisions []Line
	for _, n := range []string{
		"clicks_raw", "bms_events", "events_bms", "BMS_audit",
		"john111_tmp", "john1110", "john11", ".inner.clicks_mv",
	} {
		verdict := "snapshot"
		if !f.Match("events", n, "MergeTree") {
			verdict = "skip"
		}
		decisions = append(decisions, Line{Kind: "out", Text: fmt.Sprintf("%-18s %s", n, verdict)})
	}

	ch := &fakeCH{
		topo: distributedTopo(),
		dbs:  eventsDB(),
		objects: []clickhouse.ListedObject{
			table("events", "clicks_raw", "ReplicatedMergeTree", ddlClicksRaw),
			table("events", "bms_events", "MergeTree", "CREATE TABLE events.bms_events (`id` UInt64) ENGINE = MergeTree ORDER BY id"),
			table("events", "john111_tmp", "MergeTree", "CREATE TABLE events.john111_tmp (`id` UInt64) ENGINE = MergeTree ORDER BY id"),
			table("events", "BMS_audit", "MergeTree", "CREATE TABLE events.BMS_audit (`id` UInt64) ENGINE = MergeTree ORDER BY id"),
		},
	}
	h := newHarness(ch, "deprecate")
	st := h.step("Снапшот с фильтром", "ddl-saver snapshot -config config.toml", "",
		func() *int { _ = h.deps.SnapshotAll(context.Background(), app.SnapshotOpts{}); return ptr(0) })
	st.Tree = h.repo.tree()

	return Scenario{ID: "filters", Title: "Фильтры", Steps: []Step{
		{Title: "Решение фильтра по именам", Lines: decisions},
		st,
	}}
}

// ---------- recovery extras ----------

func recoveryBaseline() (*harness, *fakeCH) {
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

func scenarioRecoverExtras() Scenario {
	ctx := context.Background()
	var steps []Step

	// Whole database gone.
	h, ch := recoveryBaseline()
	ch.dbs, ch.objects = nil, nil
	ch.syncLive()
	steps = append(steps, h.step("База удалена целиком",
		"ddl-saver recover -config config.toml -cluster prod-analytics -object events.clicks_mv -yes", "",
		func() *int {
			code, err := h.deps.Recover(ctx, app.RecoverOpts{
				Cluster: "prod-analytics", Objects: []string{"events.clicks_mv"}, Yes: true})
			if err != nil {
				fmt.Fprintln(h.buf, "ошибка:", err)
			}
			return ptr(code)
		}))

	// -with-dependents.
	h, ch = recoveryBaseline()
	ch.objects = ch.objects[1:2] // only clicks_agg survives
	ch.syncLive()
	steps = append(steps, h.step("Таблица вместе с зависимыми",
		"ddl-saver recover -config config.toml -cluster prod-analytics -object events.clicks_raw -with-dependents -dry-run", "",
		func() *int {
			code, err := h.deps.Recover(ctx, app.RecoverOpts{
				Cluster: "prod-analytics", Objects: []string{"events.clicks_raw"},
				WithDependents: true, DryRun: true})
			if err != nil {
				fmt.Fprintln(h.buf, "ошибка:", err)
			}
			return ptr(code)
		}))

	// Glob + pinned ref.
	h, ch = recoveryBaseline()
	ch.objects = nil
	ch.syncLive()
	ch.live["events"] = true
	steps = append(steps, h.step("Шаблон и ревизия из истории",
		"ddl-saver recover -config config.toml -cluster prod-analytics -object 'events.clicks_*' -ref 4f2a91c -dry-run", "",
		func() *int {
			code, err := h.deps.Recover(ctx, app.RecoverOpts{
				Cluster: "prod-analytics", Objects: []string{"events.clicks_*"},
				Ref: "4f2a91c", DryRun: true})
			if err != nil {
				fmt.Fprintln(h.buf, "ошибка:", err)
			}
			return ptr(code)
		}))

	// Typo in a pattern: fails loudly.
	h, _ = recoveryBaseline()
	steps = append(steps, h.step("Шаблон ни с чем не совпал",
		"ddl-saver recover -config config.toml -cluster prod-analytics -object 'events.clikcs_*'", "",
		func() *int {
			code, err := h.deps.Recover(ctx, app.RecoverOpts{
				Cluster: "prod-analytics", Objects: []string{"events.clikcs_*"}})
			if err != nil {
				fmt.Fprintln(h.buf, "fatal:", err)
			}
			return ptr(code)
		}))

	return Scenario{ID: "recoverx", Title: "Восстановление: варианты", Steps: steps}
}

// ---------- daemon: status + real health endpoints ----------

func scenarioDaemon() Scenario {
	ctx := context.Background()
	ch := &fakeCH{
		topo: distributedTopo(),
		dbs:  eventsDB(),
		objects: []clickhouse.ListedObject{
			table("events", "clicks_raw", "ReplicatedMergeTree", ddlClicksRaw),
		},
	}
	h := newHarness(ch, "deprecate")
	h.deps.Config.Clusters = append(h.deps.Config.Clusters, config.Cluster{
		Name:       "staging",
		Connection: config.Connection{Endpoints: []config.Endpoint{{Host: "ch-stg.internal", Port: 9000}}},
		Auth:       config.Auth{User: "ddl_reader"},
	})
	h.deps.Connect = func(_ context.Context, cl *config.Cluster) (clickhouse.Client, error) {
		if cl.Name == "staging" {
			return nil, errors.New("all endpoints failed:\n  ch-stg.internal:9000: dial tcp 10.0.3.15:9000: connect: connection refused")
		}
		return ch, nil
	}

	tick := h.step("Плановый прогон: один кластер недоступен", "", "",
		func() *int {
			if err := h.deps.SnapshotAll(ctx, app.SnapshotOpts{}); err != nil {
				h.deps.Log.Error("scheduled snapshot finished with errors", "error", err)
			}
			return nil
		})

	// Real health server on a free port, real HTTP responses.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	hctx, cancel := context.WithCancel(ctx)
	defer cancel()
	observability.ServeHealth(hctx, observability.HealthOptions{
		Addr:   addr,
		Ready:  h.deps.Status.Ready,
		Status: func() any { return h.deps.Status.Snapshot() },
	}, h.deps.Log)

	get := func(path string) (int, string) {
		for i := 0; i < 100; i++ {
			resp, err := http.Get("http://" + addr + path)
			if err != nil {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return resp.StatusCode, string(b)
		}
		return 0, ""
	}
	startLines := parse(h.buf) // "health endpoints listening"
	_ = startLines

	var health []Line
	for _, p := range []string{"/health/live", "/health/ready"} {
		code, body := get(p)
		health = append(health, Line{Kind: "out", Text: fmt.Sprintf("$ curl -i :8080%s", p)})
		health = append(health, Line{Kind: "out", Text: fmt.Sprintf("HTTP/1.1 %d %s", code, http.StatusText(code))})
		for _, bl := range strings.Split(strings.TrimRight(body, "\n"), "\n") {
			kind := "out"
			if code >= 500 {
				kind = "err"
			}
			health = append(health, Line{Kind: kind, Text: bl})
		}
		health = append(health, Line{Kind: "out", Text: ""})
	}
	_, statusBody := get("/status")

	// Pretty JSON exactly as the endpoint serves it.
	var probe map[string]any
	_ = json.Unmarshal([]byte(statusBody), &probe)

	return Scenario{ID: "daemon", Title: "Демон", Steps: []Step{
		tick,
		{Title: "Health-эндпоинты", Lines: health},
		{Title: "GET /status", Files: map[string]string{"/status": statusBody}},
	}}
}

// ---------- keep policy + missing macro ----------

func scenarioMore() Scenario {
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
	h := newHarness(ch, "keep")
	_ = h.deps.SnapshotAll(ctx, app.SnapshotOpts{})
	h.buf.Reset()
	ch.objects = ch.objects[:1]
	ch.syncLive()
	k := h.step("on_deleted = keep", "ddl-saver snapshot -config config.toml", "",
		func() *int { _ = h.deps.SnapshotAll(ctx, app.SnapshotOpts{}); return ptr(0) })
	k.Tree = h.repo.tree()
	steps = append(steps, k)

	// Distributed snapshot replayed onto a node without the cluster macro.
	h2, ch2 := recoveryBaseline()
	ch2.objects = ch2.objects[1:2]
	ch2.syncLive()
	ch2.execErr = errors.New("Code: 36. DB::Exception: No macro 'cluster' in config while processing substitutions in '{cluster}'")
	steps = append(steps, h2.step("Снапшот кластера на одиночной ноде",
		"ddl-saver recover -config config.toml -cluster prod-analytics -object events.clicks_raw -yes", "",
		func() *int {
			code, err := h2.deps.Recover(ctx, app.RecoverOpts{
				Cluster: "prod-analytics", Objects: []string{"events.clicks_raw"}, Yes: true})
			if err != nil {
				fmt.Fprintln(h2.buf, "ошибка:", err)
			}
			return ptr(code)
		}))
	return Scenario{ID: "more", Title: "Дополнительно", Steps: steps}
}

package app

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"encoding/json"
	"gitlab.example.com/infra/ddl-saver/internal/clickhouse"
	"gitlab.example.com/infra/ddl-saver/internal/config"
	"gitlab.example.com/infra/ddl-saver/internal/gitlab"
	"gitlab.example.com/infra/ddl-saver/internal/model"
	"gitlab.example.com/infra/ddl-saver/internal/snapshot"
)

// --- fakes -----------------------------------------------------------------

type fakeCH struct {
	live      map[string]bool // "db" or "db.name"
	executed  []string
	failExec  error
	connectOK bool
}

func (f *fakeCH) Ping(context.Context) error { return nil }
func (f *fakeCH) Close() error               { return nil }
func (f *fakeCH) Endpoint() string           { return "ch:9000" }
func (f *fakeCH) Topology(context.Context) (*clickhouse.Topology, error) {
	return &clickhouse.Topology{}, nil
}
func (f *fakeCH) ListDatabases(context.Context) ([]clickhouse.ListedDatabase, error) {
	return nil, nil
}
func (f *fakeCH) ListObjects(context.Context) ([]clickhouse.ListedObject, error) {
	return []clickhouse.ListedObject{{
		Ref: model.ObjectRef{Database: "events", Name: "live_t"}, Kind: model.KindTable,
		Engine: "MergeTree", DDL: "CREATE TABLE events.live_t (id UInt64) ENGINE = MergeTree() ORDER BY id",
	}}, nil
}
func (f *fakeCH) ObjectExists(_ context.Context, ref model.ObjectRef) (bool, error) {
	return f.live[ref.String()], nil
}
func (f *fakeCH) ListRefs(context.Context) ([]model.ObjectRef, error) {
	var out []model.ObjectRef
	for k := range f.live {
		r, err := model.ParseRef(k, "")
		if err != nil {
			out = append(out, model.ObjectRef{Database: k})
			continue
		}
		out = append(out, r)
	}
	return out, nil
}
func (f *fakeCH) Exec(_ context.Context, sql string) error {
	if f.failExec != nil {
		return f.failExec
	}
	f.executed = append(f.executed, sql)
	return nil
}

type memRepo struct {
	files   map[string]string
	commits int
}

func (r *memRepo) ListTree(_ context.Context, _, base string) ([]string, error) {
	var out []string
	for p := range r.files {
		if strings.HasPrefix(p, base+"/") {
			out = append(out, p)
		}
	}
	return out, nil
}
func (r *memRepo) GetFiles(_ context.Context, _ string, paths []string) (map[string]string, error) {
	out := map[string]string{}
	for _, p := range paths {
		out[p] = r.files[p]
	}
	return out, nil
}
func (r *memRepo) Commit(_ context.Context, _, _ string, actions []gitlab.Action) error {
	r.commits++
	for _, a := range actions {
		if a.Op == gitlab.OpDelete {
			delete(r.files, a.Path)
		} else {
			r.files[a.Path] = a.Content
		}
	}
	return nil
}

func testConfig() *config.Config {
	c := &config.Config{
		Clusters: []config.Cluster{{
			Name:       "prod",
			Connection: config.Connection{Endpoints: []config.Endpoint{{Host: "ch"}}},
			Auth:       config.Auth{User: "u"},
		}},
		GitLab: config.GitLab{URL: "https://gl", Token: "t", ProjectPath: "infra/schemas"},
	}
	c.ApplyDefaults()
	c.Recovery.RetryBackoff = 0
	return c
}

func seededRepo() *memRepo {
	return &memRepo{files: map[string]string{
		"schemas/prod/events/_database.sql":                    "CREATE DATABASE events ENGINE = Atomic",
		"schemas/prod/events/tables/clicks_raw.sql":            "CREATE TABLE events.clicks_raw (ts DateTime) ENGINE = MergeTree() ORDER BY ts",
		"schemas/prod/events/tables/clicks_agg.sql":            "CREATE TABLE events.clicks_agg (d Date) ENGINE = MergeTree() ORDER BY d",
		"schemas/prod/events/materialized_views/clicks_mv.sql": "CREATE MATERIALIZED VIEW events.clicks_mv TO events.clicks_agg AS SELECT toDate(ts) d FROM events.clicks_raw",
	}}
}

type harness struct {
	deps    *Deps
	ch      *fakeCH
	repo    *memRepo
	out     *bytes.Buffer
	asked   []string
	answers []bool
}

func newHarness(live map[string]bool) *harness {
	h := &harness{
		ch:   &fakeCH{live: live},
		repo: seededRepo(),
		out:  &bytes.Buffer{},
	}
	h.deps = &Deps{
		Config:  testConfig(),
		Log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		Connect: func(context.Context, *config.Cluster) (clickhouse.Client, error) { return h.ch, nil },
		Repo:    h.repo,
		Out:     h.out,
		Confirm: func(prompt string) (bool, error) {
			h.asked = append(h.asked, prompt)
			if len(h.answers) == 0 {
				return false, nil
			}
			a := h.answers[0]
			h.answers = h.answers[1:]
			return a, nil
		},
		Status: NewStatus(),
		Now:    func() time.Time { return time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC) },
	}
	return h
}

// --- recover ---------------------------------------------------------------

func TestRecover_RequiresCluster(t *testing.T) {
	t.Parallel()
	h := newHarness(nil)
	code, err := h.deps.Recover(context.Background(), RecoverOpts{})
	if code != ExitFatal || err == nil || !strings.Contains(err.Error(), "-cluster") {
		t.Fatalf("code=%d err=%v", code, err)
	}
}

func TestRecover_DryRunPrintsPlanAndExecutesNothing(t *testing.T) {
	t.Parallel()
	h := newHarness(map[string]bool{"events": true, "events.clicks_raw": true})
	code, err := h.deps.Recover(context.Background(), RecoverOpts{
		Cluster: "prod", Objects: []string{"events.clicks_mv"}, DryRun: true,
	})
	if err != nil || code != ExitOK {
		t.Fatalf("code=%d err=%v", code, err)
	}
	out := h.out.String()
	for _, want := range []string{"Recovery plan", "events.clicks_agg", "events.clicks_mv", "create", "skip"} {
		if !strings.Contains(out, want) {
			t.Errorf("plan output missing %q:\n%s", want, out)
		}
	}
	if len(h.ch.executed) != 0 || len(h.asked) != 0 {
		t.Errorf("dry run must neither execute nor prompt: exec=%v asked=%v", h.ch.executed, h.asked)
	}
}

func TestRecover_DeclinedConfirmationAbortsWithExit1(t *testing.T) {
	t.Parallel()
	h := newHarness(map[string]bool{"events": true})
	h.answers = []bool{false}
	code, err := h.deps.Recover(context.Background(), RecoverOpts{Cluster: "prod", Objects: []string{"events.clicks_raw"}})
	if err != nil || code != ExitFatal {
		t.Fatalf("code=%d err=%v", code, err)
	}
	if len(h.asked) != 1 || len(h.ch.executed) != 0 || !strings.Contains(h.out.String(), "Aborted") {
		t.Errorf("asked=%v executed=%v out=%q", h.asked, h.ch.executed, h.out.String())
	}
}

func TestRecover_ConfirmedAppliesInOrderWithIfNotExists(t *testing.T) {
	t.Parallel()
	h := newHarness(map[string]bool{}) // database missing too
	h.answers = []bool{true}
	code, err := h.deps.Recover(context.Background(), RecoverOpts{Cluster: "prod", Objects: []string{"events.clicks_mv"}})
	if err != nil || code != ExitOK {
		t.Fatalf("code=%d err=%v out=%s", code, err, h.out.String())
	}
	ex := h.ch.executed
	if len(ex) != 4 {
		t.Fatalf("expected db + 2 tables + mv, got %v", ex)
	}
	if !strings.HasPrefix(ex[0], "CREATE DATABASE IF NOT EXISTS") {
		t.Errorf("database must come first: %s", ex[0])
	}
	if !strings.Contains(ex[3], "MATERIALIZED VIEW IF NOT EXISTS events.clicks_mv") {
		t.Errorf("mv must come last: %s", ex[3])
	}
	for _, s := range ex {
		if !strings.Contains(s, "IF NOT EXISTS") {
			t.Errorf("every statement must carry IF NOT EXISTS: %s", s)
		}
	}
	if !strings.Contains(h.out.String(), "created=4") {
		t.Errorf("report: %s", h.out.String())
	}
}

func TestRecover_YesSkipsPromptAndNoTTYNeedsYes(t *testing.T) {
	t.Parallel()
	h := newHarness(map[string]bool{"events": true})
	code, err := h.deps.Recover(context.Background(), RecoverOpts{Cluster: "prod", Objects: []string{"events.clicks_raw"}, Yes: true})
	if err != nil || code != ExitOK || len(h.asked) != 0 {
		t.Fatalf("code=%d err=%v asked=%v", code, err, h.asked)
	}

	h2 := newHarness(map[string]bool{"events": true})
	h2.deps.Confirm = nil // no terminal
	code, err = h2.deps.Recover(context.Background(), RecoverOpts{Cluster: "prod", Objects: []string{"events.clicks_raw"}})
	if code != ExitFatal || err == nil || !strings.Contains(err.Error(), "-yes") {
		t.Fatalf("headless without -yes must refuse: code=%d err=%v", code, err)
	}
	if len(h2.ch.executed) != 0 {
		t.Error("nothing may execute without confirmation")
	}
}

func TestRecover_TamperedRepoIsFatalBeforePrompt(t *testing.T) {
	t.Parallel()
	h := newHarness(map[string]bool{"events": true})
	h.repo.files["schemas/prod/events/tables/clicks_raw.sql"] = "DROP TABLE events.clicks_raw"
	h.answers = []bool{true}
	code, err := h.deps.Recover(context.Background(), RecoverOpts{Cluster: "prod", Objects: []string{"events.clicks_mv"}})
	if code != ExitFatal || err == nil || !strings.Contains(err.Error(), "not safe to execute") {
		t.Fatalf("code=%d err=%v", code, err)
	}
	if len(h.asked) != 0 || len(h.ch.executed) != 0 {
		t.Errorf("must fail before prompting or executing: asked=%v exec=%v", h.asked, h.ch.executed)
	}
}

func TestRecover_ExecutionFailureIsExit2(t *testing.T) {
	t.Parallel()
	h := newHarness(map[string]bool{"events": true})
	h.ch.failExec = errors.New("ZooKeeper unavailable")
	code, err := h.deps.Recover(context.Background(), RecoverOpts{Cluster: "prod", Objects: []string{"events.clicks_raw"}, Yes: true})
	if err != nil || code != ExitPartial {
		t.Fatalf("code=%d err=%v", code, err)
	}
	if !strings.Contains(h.out.String(), "failed") || !strings.Contains(h.out.String(), "ZooKeeper") {
		t.Errorf("report should name the failure:\n%s", h.out.String())
	}
}

func TestRecover_NothingToCreate(t *testing.T) {
	t.Parallel()
	h := newHarness(map[string]bool{"events": true, "events.clicks_raw": true})
	code, err := h.deps.Recover(context.Background(), RecoverOpts{Cluster: "prod", Objects: []string{"events.clicks_raw"}})
	if err != nil || code != ExitOK || !strings.Contains(h.out.String(), "Nothing to create") || len(h.asked) != 0 {
		t.Fatalf("code=%d err=%v out=%s", code, err, h.out.String())
	}
}

func TestRecover_UnknownClusterAndEmptyRepo(t *testing.T) {
	t.Parallel()
	h := newHarness(nil)
	if code, err := h.deps.Recover(context.Background(), RecoverOpts{Cluster: "staging"}); code != ExitFatal || err == nil {
		t.Errorf("unknown cluster: code=%d err=%v", code, err)
	}
	h.repo.files = map[string]string{}
	if code, err := h.deps.Recover(context.Background(), RecoverOpts{Cluster: "prod"}); code != ExitFatal || err == nil || !strings.Contains(err.Error(), "no objects") {
		t.Errorf("empty repo: code=%d err=%v", code, err)
	}
}

// --- snapshot --------------------------------------------------------------

func TestSnapshotAll_RecordsStatusAndJoinsErrors(t *testing.T) {
	t.Parallel()
	h := newHarness(nil)
	h.deps.Config.Clusters = append(h.deps.Config.Clusters, config.Cluster{Name: "broken"})
	h.deps.Connect = func(_ context.Context, cl *config.Cluster) (clickhouse.Client, error) {
		if cl.Name == "broken" {
			return nil, errors.New("connection refused")
		}
		return h.ch, nil
	}

	err := h.deps.SnapshotAll(context.Background(), SnapshotOpts{})
	if !errors.Is(err, ErrSnapshotPartial) || !strings.Contains(err.Error(), "broken: ") {
		t.Fatalf("err = %v", err)
	}
	if h.repo.commits != 1 {
		t.Errorf("the healthy cluster must still be committed: commits=%d", h.repo.commits)
	}
	st := h.deps.Status.Snapshot()
	if st["prod"].LastError != "" || st["prod"].LastSuccess == nil {
		t.Errorf("prod status: %+v", st["prod"])
	}
	if !strings.Contains(st["broken"].LastError, "connection refused") {
		t.Errorf("broken status: %+v", st["broken"])
	}
	if h.deps.Status.Ready() {
		t.Error("readiness must be false while a cluster is failing")
	}

	// Filter to the healthy one: no error, readiness stays false because
	// 'broken' still holds its last failure.
	if err := h.deps.SnapshotAll(context.Background(), SnapshotOpts{Cluster: "prod"}); err != nil {
		t.Fatal(err)
	}
}

func TestStatus_ReadyBeforeFirstRun(t *testing.T) {
	t.Parallel()
	var nilStatus *Status
	if !NewStatus().Ready() || !nilStatus.Ready() {
		t.Error("no runs yet must be ready (and a nil Status must be safe)")
	}
}

// /status is an API: a cluster that never succeeded must not report a zero
// timestamp, and nested stats use the same snake_case as the outer object.
func TestStatusJSON_Shape(t *testing.T) {
	t.Parallel()
	s := NewStatus()
	at := time.Date(2026, 10, 25, 21, 0, 0, 0, time.UTC)
	s.record("ok", at, at, snapshot.Stats{Objects: 3, Commits: 1}, nil)
	s.record("down", at, at, snapshot.Stats{}, errors.New("connection refused"))

	b, err := json.Marshal(s.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if _, ok := got["down"]["last_success"]; ok {
		t.Errorf("never-succeeded cluster must omit last_success: %s", b)
	}
	if _, ok := got["ok"]["last_success"]; !ok {
		t.Errorf("succeeded cluster must report last_success: %s", b)
	}
	stats, _ := got["ok"]["last_stats"].(map[string]any)
	if stats["objects"] != float64(3) || stats["commits"] != float64(1) {
		t.Errorf("last_stats must be snake_case: %v", stats)
	}
	if _, pascal := stats["Objects"]; pascal {
		t.Errorf("PascalCase key leaked into /status: %v", stats)
	}
}

type failingRepo struct {
	*memRepo
	err error
}

func (r failingRepo) ListTree(context.Context, string, string) ([]string, error) { return nil, r.err }

func TestValidate_ChecksEveryClusterAndGitLab(t *testing.T) {
	h := newHarness(nil)
	h.deps.Config.Clusters = append(h.deps.Config.Clusters,
		config.Cluster{Name: "staging"}, config.Cluster{Name: "dev"})
	h.deps.Connect = func(_ context.Context, cl *config.Cluster) (clickhouse.Client, error) {
		if cl.Name == "staging" {
			return nil, errors.New("connection refused")
		}
		return h.ch, nil
	}

	// without -connect nothing is contacted
	h.deps.Repo = failingRepo{h.repo, errors.New("must not be called")}
	if report, err := h.deps.Validate(context.Background(), false); err != nil || !strings.Contains(report, "3 cluster(s)") {
		t.Fatalf("offline validate: %q %v", report, err)
	}

	h.deps.Repo = failingRepo{h.repo, errors.New("401 Unauthorized")}
	report, err := h.deps.Validate(context.Background(), true)
	if err == nil {
		t.Fatal("want an error")
	}
	// one run names every problem, and still reports what works
	for _, want := range []string{
		"cluster prod reachable", "cluster staging FAILED: connection refused",
		"cluster dev reachable", "gitlab project infra/schemas (branch main) FAILED: 401 Unauthorized",
	} {
		if !strings.Contains(report, want) {
			t.Errorf("report lacks %q:\n%s", want, report)
		}
	}
	for _, want := range []string{"staging", "401 Unauthorized"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error lacks %q: %v", want, err)
		}
	}

	h.deps.Repo = h.repo
	h.deps.Connect = func(context.Context, *config.Cluster) (clickhouse.Client, error) { return h.ch, nil }
	report, err = h.deps.Validate(context.Background(), true)
	if err != nil || !strings.Contains(report, "reachable: 4 file(s) under schemas/") {
		t.Fatalf("all good: %q %v", report, err)
	}
	if h.repo.commits != 0 {
		t.Error("validate must never write")
	}
}

type emptyCH struct{ *fakeCH }

func (emptyCH) ListObjects(context.Context) ([]clickhouse.ListedObject, error) { return nil, nil }

// The daemon must never archive a repository because ClickHouse suddenly
// shows nothing; only an explicit one-shot flag may.
func TestSnapshotAll_EmptyClusterNeedsExplicitFlag(t *testing.T) {
	h := newHarness(nil)
	h.deps.Connect = func(context.Context, *config.Cluster) (clickhouse.Client, error) { return emptyCH{h.ch}, nil }
	before := len(h.repo.files)

	err := h.deps.SnapshotAll(context.Background(), SnapshotOpts{})
	if !errors.Is(err, snapshot.ErrEmptyCluster) || !errors.Is(err, ErrSnapshotPartial) {
		t.Fatalf("want ErrEmptyCluster, got %v", err)
	}
	if h.repo.commits != 0 || len(h.repo.files) != before {
		t.Fatal("repository changed")
	}
	if h.deps.Status.Ready() {
		t.Error("readiness must go red so the alert fires")
	}

	if err := h.deps.SnapshotAll(context.Background(), SnapshotOpts{Cluster: "prod", AllowEmpty: true}); err != nil {
		t.Fatal(err)
	}
	if _, ok := h.repo.files["schemas/prod/_deprecated/events/tables/clicks_raw.sql"]; !ok {
		t.Error("with -allow-empty the files are archived")
	}
}

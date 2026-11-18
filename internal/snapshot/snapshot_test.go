package snapshot

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"gitlab.example.com/infra/ddl-saver/internal/clickhouse"
	"gitlab.example.com/infra/ddl-saver/internal/gitlab"
	"gitlab.example.com/infra/ddl-saver/internal/model"
)

type fakeSource struct {
	topo      clickhouse.Topology
	databases []clickhouse.ListedDatabase
	objects   []clickhouse.ListedObject
}

func (f *fakeSource) ListDatabases(context.Context) ([]clickhouse.ListedDatabase, error) {
	return f.databases, nil
}

func (f *fakeSource) Topology(context.Context) (*clickhouse.Topology, error) {
	t := f.topo
	return &t, nil
}
func (f *fakeSource) ListObjects(context.Context) ([]clickhouse.ListedObject, error) {
	return f.objects, nil
}

// fakeRepo is an in-memory Repo that applies commits.
type fakeRepo struct {
	files    map[string]string
	commits  []string // messages
	actioned [][]gitlab.Action
}

func newFakeRepo() *fakeRepo { return &fakeRepo{files: map[string]string{}} }

func (r *fakeRepo) ListTree(_ context.Context, _, basePath string) ([]string, error) {
	var out []string
	for p := range r.files {
		if strings.HasPrefix(p, basePath+"/") {
			out = append(out, p)
		}
	}
	return out, nil
}

func (r *fakeRepo) GetFiles(_ context.Context, _ string, paths []string) (map[string]string, error) {
	out := map[string]string{}
	for _, p := range paths {
		out[p] = r.files[p]
	}
	return out, nil
}

func (r *fakeRepo) Commit(_ context.Context, _, message string, actions []gitlab.Action) error {
	r.commits = append(r.commits, message)
	r.actioned = append(r.actioned, actions)
	for _, a := range actions {
		if a.Op == gitlab.OpDelete {
			delete(r.files, a.Path)
		} else {
			r.files[a.Path] = a.Content
		}
	}
	return nil
}

func listedTable(db, name, ddl string) clickhouse.ListedObject {
	return clickhouse.ListedObject{
		Ref:    model.ObjectRef{Database: db, Name: name},
		Kind:   model.KindTable,
		Engine: "MergeTree",
		DDL:    ddl,
	}
}

func opts() Options {
	return Options{
		Cluster: "prod", BasePath: "schemas", Branch: "main",
		BatchSize: 100, Now: func() time.Time { return time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC) },
	}
}

func TestRun_InitialSnapshot(t *testing.T) {
	src := &fakeSource{
		topo:    clickhouse.Topology{IsDistributed: true, ShardCount: 2},
		objects: []clickhouse.ListedObject{listedTable("events", "clicks", "CREATE TABLE events.clicks (id UInt64) ENGINE = MergeTree()")},
	}
	repo := newFakeRepo()
	st, err := Run(context.Background(), src, repo, opts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if st.Objects != 1 || st.Creates != 1 || st.Commits != 1 { // manifest excluded from Creates
		t.Errorf("stats %+v", st)
	}
	ddl := repo.files["schemas/prod/events/tables/clicks.sql"]
	if !strings.Contains(ddl, "ON CLUSTER '{cluster}'") {
		t.Errorf("distributed DDL should be normalized: %q", ddl)
	}
	if _, ok := repo.files["schemas/prod/manifest.json"]; !ok {
		t.Error("manifest not committed")
	}
	if !strings.Contains(repo.commits[0], "snapshot prod (+1 ~0 -0)") {
		t.Errorf("commit message: %q", repo.commits[0])
	}
}

func TestRun_SecondRunIsNoop(t *testing.T) {
	src := &fakeSource{objects: []clickhouse.ListedObject{listedTable("events", "clicks", "CREATE TABLE events.clicks (id UInt64)")}}
	repo := newFakeRepo()
	if _, err := Run(context.Background(), src, repo, opts(), nil); err != nil {
		t.Fatal(err)
	}
	st, err := Run(context.Background(), src, repo, opts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !st.Unchanged || st.Commits != 0 {
		t.Errorf("second run must be a no-op: %+v", st)
	}
}

func TestRun_UpdateOnDDLChange(t *testing.T) {
	src := &fakeSource{objects: []clickhouse.ListedObject{listedTable("events", "clicks", "CREATE TABLE events.clicks (id UInt64)")}}
	repo := newFakeRepo()
	if _, err := Run(context.Background(), src, repo, opts(), nil); err != nil {
		t.Fatal(err)
	}
	src.objects[0].DDL = "CREATE TABLE events.clicks (id UInt64, ts DateTime)"
	st, err := Run(context.Background(), src, repo, opts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if st.Updates != 1 { // manifest excluded from Updates
		t.Errorf("stats %+v", st)
	}
	if !strings.Contains(repo.commits[len(repo.commits)-1], "(+0 ~1 -0)") {
		t.Errorf("commit message: %q", repo.commits[len(repo.commits)-1])
	}
}

func TestRun_DryRunCommitsNothing(t *testing.T) {
	src := &fakeSource{objects: []clickhouse.ListedObject{listedTable("events", "clicks", "CREATE TABLE events.clicks (id UInt64)")}}
	repo := newFakeRepo()
	o := opts()
	o.DryRun = true
	st, err := Run(context.Background(), src, repo, o, nil)
	if err != nil {
		t.Fatal(err)
	}
	if st.Commits != 0 || len(repo.files) != 0 {
		t.Errorf("dry run must not commit: %+v files=%v", st, repo.files)
	}
	if st.Creates == 0 {
		t.Error("dry run should still report what it would do")
	}
}

func TestRun_Batching(t *testing.T) {
	src := &fakeSource{}
	for i := 0; i < 7; i++ {
		src.objects = append(src.objects,
			listedTable("events", fmt.Sprintf("t%02d", i), fmt.Sprintf("CREATE TABLE events.t%02d (id UInt64)", i)))
	}
	repo := newFakeRepo()
	o := opts()
	o.BatchSize = 3
	st, err := Run(context.Background(), src, repo, o, nil)
	if err != nil {
		t.Fatal(err)
	}
	if st.Commits != 3 { // 8 actions (7 ddl + manifest) / 3
		t.Errorf("stats %+v commits %v", st, repo.commits)
	}
	if !strings.Contains(repo.commits[0], "[1/3]") {
		t.Errorf("batch numbering missing: %v", repo.commits)
	}
}

func TestRun_FilterApplied(t *testing.T) {
	f, _ := clickhouse.NewFilter(nil, []string{`^\.inner`}, nil)
	src := &fakeSource{objects: []clickhouse.ListedObject{
		listedTable("events", "clicks", "CREATE TABLE events.clicks (id UInt64)"),
		listedTable("events", ".inner.mv1", "CREATE TABLE ..."),
	}}
	repo := newFakeRepo()
	o := opts()
	o.Filter = f
	st, err := Run(context.Background(), src, repo, o, nil)
	if err != nil {
		t.Fatal(err)
	}
	if st.Objects != 1 {
		t.Errorf("filter not applied: %+v", st)
	}
	if _, ok := repo.files["schemas/prod/events/tables/.inner.mv1.sql"]; ok {
		t.Error("hidden inner table must be filtered out")
	}
}

// --- on_deleted policy lifecycle ----------------------------------------

func twoTableSource() *fakeSource {
	return &fakeSource{objects: []clickhouse.ListedObject{
		listedTable("events", "keep", "CREATE TABLE events.keep (id UInt64)"),
		listedTable("events", "drop_me", "CREATE TABLE events.drop_me (id UInt64)"),
	}}
}

const livePath = "schemas/prod/events/tables/drop_me.sql"
const depPath = "schemas/prod/_deprecated/events/tables/drop_me.sql"

func TestRun_DeprecateMovesFile(t *testing.T) {
	src := twoTableSource()
	repo := newFakeRepo()
	if _, err := Run(context.Background(), src, repo, opts(), nil); err != nil {
		t.Fatal(err)
	}
	originalContent := repo.files[livePath]
	src.objects = src.objects[:1] // drop_me disappears (same as a dropped DB, per file)

	st, err := Run(context.Background(), src, repo, opts(), nil) // default policy = deprecate
	if err != nil {
		t.Fatal(err)
	}
	if st.Deprecated != 1 || st.Deletes != 0 {
		t.Errorf("stats %+v", st)
	}
	if _, ok := repo.files[livePath]; ok {
		t.Error("live file must be gone after deprecation")
	}
	if got := repo.files[depPath]; got != originalContent {
		t.Errorf("archived content mismatch:\n%q\nwant\n%q", got, originalContent)
	}
	if !strings.Contains(repo.commits[len(repo.commits)-1], "deprecated 1") {
		t.Errorf("commit message: %q", repo.commits[len(repo.commits)-1])
	}
	// Manifest records the deprecation.
	if !strings.Contains(repo.files["schemas/prod/manifest.json"], `"events.drop_me"`) ||
		!strings.Contains(repo.files["schemas/prod/manifest.json"], `"since"`) {
		t.Errorf("manifest should record deprecation:\n%s", repo.files["schemas/prod/manifest.json"])
	}
}

func TestRun_DeprecateIsStableAcrossRuns(t *testing.T) {
	src := twoTableSource()
	repo := newFakeRepo()
	o := opts()
	if _, err := Run(context.Background(), src, repo, o, nil); err != nil {
		t.Fatal(err)
	}
	src.objects = src.objects[:1]
	if _, err := Run(context.Background(), src, repo, o, nil); err != nil {
		t.Fatal(err)
	}
	// Later runs must not touch the archive again — even with a different clock.
	o.Now = func() time.Time { return time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC) }
	st, err := Run(context.Background(), src, repo, o, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !st.Unchanged || st.Commits != 0 {
		t.Errorf("post-deprecation run must be a no-op: %+v", st)
	}
}

func TestRun_UndeprecateWhenObjectReturns(t *testing.T) {
	src := twoTableSource()
	repo := newFakeRepo()
	if _, err := Run(context.Background(), src, repo, opts(), nil); err != nil {
		t.Fatal(err)
	}
	dropped := src.objects[1]
	src.objects = src.objects[:1]
	if _, err := Run(context.Background(), src, repo, opts(), nil); err != nil {
		t.Fatal(err)
	}

	src.objects = append(src.objects, dropped) // the table is recreated
	st, err := Run(context.Background(), src, repo, opts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if st.Undeprecated != 1 || st.Creates != 1 {
		t.Errorf("stats %+v", st)
	}
	if _, ok := repo.files[depPath]; ok {
		t.Error("archived copy must be removed when the object is back")
	}
	if _, ok := repo.files[livePath]; !ok {
		t.Error("live file must be recreated")
	}
	if strings.Contains(repo.files["schemas/prod/manifest.json"], `"events.drop_me"`+`: {`) &&
		strings.Contains(repo.files["schemas/prod/manifest.json"], `"since"`) {
		// crude but sufficient: no deprecation record should remain
		if strings.Contains(repo.files["schemas/prod/manifest.json"], `"deprecated"`) {
			t.Errorf("deprecation record should be cleared:\n%s", repo.files["schemas/prod/manifest.json"])
		}
	}

	st, err = Run(context.Background(), src, repo, opts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !st.Unchanged {
		t.Errorf("run after un-deprecation must be a no-op: %+v", st)
	}
}

func TestRun_KeepPolicy(t *testing.T) {
	src := twoTableSource()
	repo := newFakeRepo()
	o := opts()
	o.OnDeleted = PolicyKeep
	if _, err := Run(context.Background(), src, repo, o, nil); err != nil {
		t.Fatal(err)
	}
	src.objects = src.objects[:1]
	st, err := Run(context.Background(), src, repo, o, nil)
	if err != nil {
		t.Fatal(err)
	}
	if st.Skipped != 1 || st.Deprecated != 0 || st.Deletes != 0 {
		t.Errorf("stats %+v", st)
	}
	if _, ok := repo.files[livePath]; !ok {
		t.Error("file must survive with on_deleted=keep")
	}
}

func TestRun_PrunePolicy(t *testing.T) {
	src := twoTableSource()
	repo := newFakeRepo()
	o := opts()
	o.OnDeleted = PolicyPrune
	if _, err := Run(context.Background(), src, repo, o, nil); err != nil {
		t.Fatal(err)
	}
	src.objects = src.objects[:1]
	st, err := Run(context.Background(), src, repo, o, nil)
	if err != nil {
		t.Fatal(err)
	}
	if st.Deletes != 1 || st.Deprecated != 0 {
		t.Errorf("stats %+v", st)
	}
	if _, ok := repo.files[livePath]; ok {
		t.Error("file must be deleted with on_deleted=prune")
	}
	if !strings.Contains(repo.commits[len(repo.commits)-1], "-1)") {
		t.Errorf("commit message: %q", repo.commits[len(repo.commits)-1])
	}
}

func TestRun_UnmanagedFilesNeverTouched(t *testing.T) {
	src := twoTableSource()
	repo := newFakeRepo()
	repo.files["schemas/prod/README.md"] = "# docs"
	repo.files["schemas/prod/events/notes.txt"] = "scratch"
	for _, policy := range []Policy{PolicyKeep, PolicyDeprecate, PolicyPrune} {
		o := opts()
		o.OnDeleted = policy
		if _, err := Run(context.Background(), src, repo, o, nil); err != nil {
			t.Fatal(err)
		}
		if _, ok := repo.files["schemas/prod/README.md"]; !ok {
			t.Fatalf("policy %s deleted an unmanaged file", policy)
		}
		if _, ok := repo.files["schemas/prod/events/notes.txt"]; !ok {
			t.Fatalf("policy %s deleted an unmanaged nested file", policy)
		}
	}
}

// TestSourceCannotWrite is a compile-time + runtime guard: the snapshot path
// consumes a read-only interface. If someone ever widens Source with a
// method that can execute DDL, this test is where the review conversation
// starts.
func TestSourceCannotWrite(t *testing.T) {
	var s Source = &fakeSource{}
	if _, ok := s.(interface {
		Exec(context.Context, string) error
	}); ok {
		t.Fatal("snapshot.Source must never expose a way to execute SQL")
	}
	if _, ok := s.(interface {
		ExecDDL(context.Context, string) error
	}); ok {
		t.Fatal("snapshot.Source must never expose a way to execute SQL")
	}
}

func TestRun_WritesDatabaseDDL(t *testing.T) {
	t.Parallel()
	src := &fakeSource{
		topo: clickhouse.Topology{IsDistributed: true},
		databases: []clickhouse.ListedDatabase{
			{Name: "events", Engine: "Atomic", DDL: "CREATE DATABASE events ENGINE = Atomic"},
			{Name: "unused", Engine: "Atomic", DDL: "CREATE DATABASE unused"}, // no objects -> no file
		},
		objects: []clickhouse.ListedObject{listedTable("events", "clicks", "CREATE TABLE events.clicks (id UInt64)")},
	}
	repo := newFakeRepo()
	if _, err := Run(context.Background(), src, repo, opts(), nil); err != nil {
		t.Fatal(err)
	}
	ddl, ok := repo.files["schemas/prod/events/_database.sql"]
	if !ok {
		t.Fatalf("database DDL not written; files: %v", keys(repo.files))
	}
	if !strings.Contains(ddl, "ON CLUSTER '{cluster}'") {
		t.Errorf("database DDL should be normalized like objects: %q", ddl)
	}
	if _, ok := repo.files["schemas/prod/unused/_database.sql"]; ok {
		t.Error("databases with no snapshotted objects must not get a file")
	}
	if !strings.Contains(repo.files["schemas/prod/manifest.json"], `"events": {`) {
		t.Error("manifest should record the database")
	}

	// A dropped database: its table AND its _database.sql are deprecated.
	// It was the only one, so the cluster is now empty — which needs the
	// operator's explicit go-ahead (TestRun_EmptyClusterGuard).
	src.objects = nil
	src.databases = src.databases[1:]
	o := opts()
	o.AllowEmpty = true
	st, err := Run(context.Background(), src, repo, o, nil)
	if err != nil {
		t.Fatal(err)
	}
	if st.Deprecated != 2 {
		t.Errorf("both files should move: %+v", st)
	}
	if _, ok := repo.files["schemas/prod/_deprecated/events/_database.sql"]; !ok {
		t.Errorf("database file should be archived; files: %v", keys(repo.files))
	}
}

func keys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// Single node (one shard, one replica, no cluster macro): nothing may carry
// ON CLUSTER, and a ReplicatedMergeTree ZooKeeper path must survive the
// round trip byte-for-byte without causing checksum churn.
func TestRun_SingleNodeReplicatedTable(t *testing.T) {
	t.Parallel()
	const zk = "'/clickhouse/tables/{shard}/events/clicks', '{replica}'"
	src := &fakeSource{
		topo: clickhouse.Topology{}, // no cluster macro => IsDistributed false
		databases: []clickhouse.ListedDatabase{
			{Name: "events", Engine: "Atomic", DDL: "CREATE DATABASE events ENGINE = Atomic"},
		},
		objects: []clickhouse.ListedObject{{
			Ref: model.ObjectRef{Database: "events", Name: "clicks"}, Kind: model.KindTable,
			Engine: "ReplicatedMergeTree",
			DDL:    "CREATE TABLE events.clicks (`ts` DateTime) ENGINE = ReplicatedMergeTree(" + zk + ") ORDER BY ts",
		}},
	}
	repo := newFakeRepo()
	if _, err := Run(context.Background(), src, repo, opts(), nil); err != nil {
		t.Fatal(err)
	}
	for p, c := range repo.files {
		if strings.HasSuffix(p, ".sql") && strings.Contains(c, "ON CLUSTER") {
			t.Errorf("single node must not produce ON CLUSTER in %s:\n%s", p, c)
		}
	}
	if !strings.Contains(repo.files["schemas/prod/events/tables/clicks.sql"], zk) {
		t.Errorf("ZooKeeper path not preserved:\n%s", repo.files["schemas/prod/events/tables/clicks.sql"])
	}

	st, err := Run(context.Background(), src, repo, opts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !st.Unchanged {
		t.Errorf("replicated DDL caused churn on an unchanged cluster: %+v", st)
	}
}

// The Replicated engine filter must admit the whole family via the
// suffix rule, so replicated clusters are not silently under-snapshotted.
func TestRun_FilterAdmitsReplicatedFamily(t *testing.T) {
	t.Parallel()
	f, err := clickhouse.NewFilter(nil, nil, []string{"MergeTree", "SummingMergeTree"})
	if err != nil {
		t.Fatal(err)
	}
	for _, engine := range []string{
		"MergeTree", "ReplicatedMergeTree", "ReplicatedSummingMergeTree", "ReplicatedReplacingMergeTree",
	} {
		if !f.Match("events", "t", engine) {
			t.Errorf("engine %s should be admitted by the MergeTree allow-list", engine)
		}
	}
	if f.Match("events", "t", "Log") {
		t.Error("Log must not be admitted")
	}
}

// A ClickHouse user that lost its grants sees zero tables and no error.
// Believing that would archive (or prune) the whole repository.
func TestRun_EmptyClusterGuard(t *testing.T) {
	t.Parallel()
	for _, policy := range []Policy{PolicyDeprecate, PolicyPrune, PolicyKeep} {
		src := twoTableSource()
		repo := newFakeRepo()
		o := opts()
		o.OnDeleted = policy
		if _, err := Run(context.Background(), src, repo, o, nil); err != nil {
			t.Fatal(err)
		}
		before := fmt.Sprint(repo.files)
		commits := len(repo.commits)

		src.objects, src.databases = nil, nil
		_, err := Run(context.Background(), src, repo, o, nil)
		if !errors.Is(err, ErrEmptyCluster) {
			t.Fatalf("%s: want ErrEmptyCluster, got %v", policy, err)
		}
		if !strings.Contains(err.Error(), "-allow-empty") {
			t.Errorf("error must name the way out: %v", err)
		}
		if len(repo.commits) != commits || fmt.Sprint(repo.files) != before {
			t.Fatalf("%s: repository changed despite the guard", policy)
		}
	}

	// A brand-new cluster with nothing in it and nothing in the repo is fine.
	st, err := Run(context.Background(), &fakeSource{}, newFakeRepo(), opts(), nil)
	if err != nil || st.Objects != 0 {
		t.Fatalf("empty cluster, empty repo: %+v %v", st, err)
	}
	// Unmanaged files alone do not trip it either.
	repo := newFakeRepo()
	repo.files["schemas/prod/README.md"] = "notes"
	if _, err := Run(context.Background(), &fakeSource{}, repo, opts(), nil); err != nil {
		t.Fatalf("only unmanaged files: %v", err)
	}
}

// Batches are separate commits. If the second one fails, a deprecated file
// must already be in the archive — never deleted from the live layout first.
func TestRun_ArchiveWrittenBeforeLiveCopyIsDeleted(t *testing.T) {
	t.Parallel()
	// "Analytics" sorts before "_deprecated", so path order alone would
	// delete the live file in an earlier batch than the archive write.
	src := &fakeSource{objects: []clickhouse.ListedObject{
		listedTable("Analytics", "keep", "CREATE TABLE Analytics.keep (id UInt64)"),
		listedTable("Analytics", "gone", "CREATE TABLE Analytics.gone (id UInt64)"),
	}}
	repo := newFakeRepo()
	o := opts()
	o.BatchSize = 1
	if _, err := Run(context.Background(), src, repo, o, nil); err != nil {
		t.Fatal(err)
	}
	repo.actioned = nil
	src.objects = src.objects[:1]
	if _, err := Run(context.Background(), src, repo, o, nil); err != nil {
		t.Fatal(err)
	}
	const live, arch = "schemas/prod/Analytics/tables/gone.sql", "schemas/prod/_deprecated/Analytics/tables/gone.sql"
	wrote, deleted := -1, -1
	for i, batch := range repo.actioned {
		for _, a := range batch {
			switch {
			case a.Path == arch && a.Op == gitlab.OpCreate:
				wrote = i
			case a.Path == live && a.Op == gitlab.OpDelete:
				deleted = i
			}
		}
	}
	if wrote < 0 || deleted < 0 || wrote >= deleted {
		t.Fatalf("archive written in batch %d, live copy deleted in batch %d", wrote, deleted)
	}
	sawDelete := false
	for _, batch := range repo.actioned {
		for _, a := range batch {
			if a.Op == gitlab.OpDelete {
				sawDelete = true
			} else if sawDelete {
				t.Fatalf("write %s scheduled after a delete", a.Path)
			}
		}
	}
}

func TestRun_ImplicitInnerTablesAreNotSnapshotted(t *testing.T) {
	t.Parallel()
	src := &fakeSource{objects: []clickhouse.ListedObject{
		listedTable("events", "clicks", "CREATE TABLE events.clicks (id UInt64)"),
		listedTable("events", ".inner_id.6f2a0c1e-0000-4000-8000-000000000001", "CREATE TABLE events.`.inner_id.6f2a0c1e` (id UInt64)"),
		listedTable("events", ".inner.old_mv", "CREATE TABLE events.`.inner.old_mv` (id UInt64)"),
	}}
	repo := newFakeRepo()
	o := opts()
	o.Filter = nil // not a matter of configuration
	st, err := Run(context.Background(), src, repo, o, nil)
	if err != nil {
		t.Fatal(err)
	}
	if st.Objects != 1 {
		t.Errorf("objects = %d, want 1", st.Objects)
	}
	for p := range repo.files {
		if strings.Contains(p, ".inner") {
			t.Errorf("inner table snapshotted: %s", p)
		}
	}
}

// Objects inside a Replicated database must not carry ON CLUSTER: the
// database replicates DDL itself and the server rejects the clause.
func TestRun_ReplicatedDatabaseObjectsGetNoOnCluster(t *testing.T) {
	t.Parallel()
	src := &fakeSource{
		topo: clickhouse.Topology{IsDistributed: true},
		databases: []clickhouse.ListedDatabase{
			{Name: "repl", Engine: "Replicated", DDL: "CREATE DATABASE repl ENGINE = Replicated('/ch/db/repl', '{shard}', '{replica}')"},
			{Name: "plain", Engine: "Atomic", DDL: "CREATE DATABASE plain ENGINE = Atomic"},
		},
		objects: []clickhouse.ListedObject{
			listedTable("repl", "t", "CREATE TABLE repl.t (id UInt64)"),
			listedTable("plain", "t", "CREATE TABLE plain.t (id UInt64)"),
		},
	}
	repo := newFakeRepo()
	if _, err := Run(context.Background(), src, repo, opts(), nil); err != nil {
		t.Fatal(err)
	}
	if ddl := repo.files["schemas/prod/repl/tables/t.sql"]; strings.Contains(ddl, "ON CLUSTER") {
		t.Errorf("table in a Replicated database got ON CLUSTER: %q", ddl)
	}
	if ddl := repo.files["schemas/prod/plain/tables/t.sql"]; !strings.Contains(ddl, "ON CLUSTER '{cluster}'") {
		t.Errorf("ordinary table lost ON CLUSTER: %q", ddl)
	}
	if ddl := repo.files["schemas/prod/repl/_database.sql"]; !strings.Contains(ddl, "ON CLUSTER '{cluster}'") {
		t.Errorf("the database itself is still created ON CLUSTER: %q", ddl)
	}
}

// One table with a hostile name must not take the cluster's snapshot down
// (GitLab rejects a commit containing a ".." path) or write outside its
// directory.
func TestRun_UnsafeNamesStayInsideTheirDirectory(t *testing.T) {
	t.Parallel()
	src := &fakeSource{objects: []clickhouse.ListedObject{
		listedTable("events", "../../../etc/passwd", "CREATE TABLE events.`../../../etc/passwd` (id UInt64)"),
		listedTable("events", "2024/q1", "CREATE TABLE events.`2024/q1` (id UInt64)"),
		listedTable("_deprecated", "t", "CREATE TABLE _deprecated.t (id UInt64)"),
	}}
	repo := newFakeRepo()
	if _, err := Run(context.Background(), src, repo, opts(), nil); err != nil {
		t.Fatal(err)
	}
	for p := range repo.files {
		if p == "schemas/prod/manifest.json" {
			continue
		}
		rel := strings.TrimPrefix(p, "schemas/prod/")
		if rel == p || strings.Contains(p, "..") && !strings.Contains(p, "%2F") || len(strings.Split(rel, "/")) != 3 {
			t.Errorf("unexpected path %q", p)
		}
		if strings.HasPrefix(rel, "_deprecated/") {
			t.Errorf("a live object landed in the archive: %q", p)
		}
	}
	// stable: the second run finds every file where it expects it
	st, err := Run(context.Background(), src, repo, opts(), nil)
	if err != nil || !st.Unchanged {
		t.Fatalf("second run should be a no-op: %+v %v", st, err)
	}
	// and dropping one archives it under its encoded name, then brings it back
	all := src.objects
	src.objects = all[1:]
	if st, _ := Run(context.Background(), src, repo, opts(), nil); st.Deprecated != 1 {
		t.Fatalf("deprecate: %+v", st)
	}
	src.objects = all
	if st, _ := Run(context.Background(), src, repo, opts(), nil); st.Undeprecated != 1 || st.Creates != 1 {
		t.Fatalf("undeprecate: %+v", st)
	}
}

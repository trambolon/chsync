package recovery

import (
	"context"
	"strings"
	"testing"
)

// Real SHOW CREATE output shapes for the Replicated* family. The ZooKeeper
// path and its macros are part of the DDL and must survive the snapshot →
// repo → recovery round trip byte-for-byte: they are what binds the table
// to its replica set.
const (
	replPathDDL = "CREATE TABLE events.clicks (`ts` DateTime, `uid` UInt64) " +
		"ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/events/clicks', '{replica}') " +
		"PARTITION BY toYYYYMM(ts) ORDER BY ts SETTINGS index_granularity = 8192"

	replUUIDDDL = "CREATE TABLE events.clicks (`id` UInt64) " +
		"ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') ORDER BY id"

	replSummingDDL = "CREATE TABLE events.clicks (`d` Date, `c` UInt64) " +
		"ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{shard}/events/agg', '{replica}') ORDER BY d"

	// Argument-less form: legal when the server defines
	// default_replica_path / default_replica_name.
	bareReplDDL = "CREATE TABLE events.clicks (`ts` DateTime) ENGINE = ReplicatedMergeTree ORDER BY ts"
)

func replObj(sql string) *DDLObject {
	return &DDLObject{Ref: ObjectRef{Database: "events", Name: "clicks"}, Kind: KindTable, SQL: sql}
}

// replPlan plans a single replicated object against the given server
// replica defaults.
func replPlan(t *testing.T, ddl string, rd ReplicaDefaults) *Plan {
	t.Helper()
	p := NewPlanner([]*DDLObject{replObj(ddl)})
	refs, _ := p.ExpandTargets(nil)
	plan, err := p.Plan(context.Background(), "prod", "", refs, fakeLive{},
		PlanOpts{DeferInvalid: true, ReplicaDefaults: rd})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func findNotice(t *testing.T, plan *Plan, substr string) string {
	t.Helper()
	for _, n := range plan.Notices {
		if strings.Contains(n, substr) {
			return n
		}
	}
	t.Fatalf("no notice containing %q in %v", substr, plan.Notices)
	return ""
}

// The guard rewrites the statement head to inject IF NOT EXISTS. The
// ZooKeeper path lives in a string literal full of braces and slashes —
// exactly the sort of thing a sloppy rewrite corrupts.
func TestGuard_PreservesZooKeeperPath(t *testing.T) {
	t.Parallel()
	for name, ddl := range map[string]string{
		"shard/replica macros": replPathDDL,
		"uuid path":            replUUIDDDL,
		"summing family":       replSummingDDL,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got, err := GuardDDL(ObjectRef{Database: "events", Name: "clicks"}, KindTable, ddl)
			if err != nil {
				t.Fatalf("guard rejected replicated DDL: %v", err)
			}
			if !strings.Contains(got, "CREATE TABLE IF NOT EXISTS events.clicks") {
				t.Errorf("injection landed wrong:\n%s", got)
			}
			// Everything from ENGINE onwards must be untouched.
			engine := ddl[strings.Index(ddl, "ENGINE"):]
			if !strings.Contains(got, engine) {
				t.Errorf("engine clause altered.\n got: %s\nwant: %s", got, engine)
			}
		})
	}
}

// The guard must accept argument-less Replicated DDL untouched apart from
// the IF NOT EXISTS injection.
func TestGuard_BareReplicatedUnchanged(t *testing.T) {
	t.Parallel()
	got, err := GuardDDL(ObjectRef{Database: "events", Name: "clicks"}, KindTable, bareReplDDL)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(got, "ENGINE = ReplicatedMergeTree ORDER BY ts") {
		t.Errorf("engine clause altered: %s", got)
	}
	if !strings.Contains(got, "CREATE TABLE IF NOT EXISTS events.clicks") {
		t.Errorf("injection wrong: %s", got)
	}
}

// A plain replicated table has no object dependencies: the ZooKeeper path
// is not a reference to another table, and must not be mistaken for one.
func TestExtractDeps_ZooKeeperPathIsNotADependency(t *testing.T) {
	t.Parallel()
	for _, ddl := range []string{replPathDDL, replUUIDDDL, replSummingDDL, bareReplDDL} {
		_, cands, external, _ := ExtractDeps("events", ddl)
		if len(cands) != 0 {
			t.Errorf("replicated table gained phantom deps %v from:\n%s", cands, ddl)
		}
		if len(external) != 0 {
			t.Errorf("replicated table gained phantom external refs %v", external)
		}
	}
}

// Before the operator confirms, the plan must say what binds each object to
// ZooKeeper — a stale path is the most common restore failure.
func TestPlan_WarnsAboutExplicitZooKeeperPath(t *testing.T) {
	t.Parallel()
	plan := replPlan(t, replPathDDL, ReplicaDefaults{})
	n := findNotice(t, plan, "pin an explicit ZooKeeper path")
	for _, want := range []string{"events.clicks", "must be free", "already exists"} {
		if !strings.Contains(n, want) {
			t.Errorf("notice should mention %q: %s", want, n)
		}
	}
}

// Argument-less Replicated DDL takes the server's default_replica_path.
// When that default is UUID-based, the recreated table gets a FRESH path,
// so the stale-path warning would be wrong — the plan must say something
// different, and flag that the table starts empty instead.
func TestPlan_BareReplicated_UUIDDefaultIsNotAStalePathHazard(t *testing.T) {
	t.Parallel()
	plan := replPlan(t, bareReplDDL, ReplicaDefaults{
		Known: true, Path: "/clickhouse/tables/{uuid}/{shard}", Name: "{replica}",
	})
	n := findNotice(t, plan, "UUID-based")
	for _, want := range []string{"events.clicks", "fresh path", "starts empty", "/clickhouse/tables/{uuid}/{shard}"} {
		if !strings.Contains(n, want) {
			t.Errorf("notice should mention %q: %s", want, n)
		}
	}
	if strings.Contains(n, "must be free") {
		t.Errorf("a UUID-based default must not be described as a stale-path hazard: %s", n)
	}
}

// A server default WITHOUT {uuid} resolves deterministically, so the
// stale-path hazard is back even though the DDL carries no path.
func TestPlan_BareReplicated_NonUUIDDefaultIsAHazard(t *testing.T) {
	t.Parallel()
	plan := replPlan(t, bareReplDDL, ReplicaDefaults{
		Known: true, Path: "/clickhouse/tables/{shard}/{database}/{table}", Name: "{replica}",
	})
	n := findNotice(t, plan, "not UUID-based")
	if !strings.Contains(n, "block CREATE") {
		t.Errorf("notice should warn about the stale path: %s", n)
	}
}

// If the defaults could not be read (old server, or no access to
// system.server_settings), say so rather than guessing either way.
func TestPlan_BareReplicated_UnknownDefaultsSaysSo(t *testing.T) {
	t.Parallel()
	plan := replPlan(t, bareReplDDL, ReplicaDefaults{})
	n := findNotice(t, plan, "could not be read")
	for _, want := range []string{"default_replica_path", "22.10", "these CREATEs fail"} {
		if !strings.Contains(n, want) {
			t.Errorf("notice should mention %q: %s", want, n)
		}
	}
}

func TestPlan_NoReplicatedWarningForPlainTables(t *testing.T) {
	t.Parallel()
	p := NewPlanner(testRepo()) // plain MergeTree fixtures
	refs, _ := p.ExpandTargets([]string{"events.clicks_raw"})
	plan, err := p.Plan(context.Background(), "prod", "", refs, fakeLive{}, PlanOpts{DeferInvalid: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range plan.Notices {
		if strings.Contains(n, "ZooKeeper") {
			t.Errorf("unexpected replicated notice: %s", n)
		}
	}
}

func TestAnalyzeEngine(t *testing.T) {
	t.Parallel()
	tests := []struct {
		sql                  string
		replicated, explicit bool
	}{
		{replPathDDL, true, true},
		{replSummingDDL, true, true},
		{replUUIDDDL, true, true},
		// argument-less forms: legal when the server sets default_replica_path
		{bareReplDDL, true, false},
		{"CREATE TABLE t (id UInt64) ENGINE = ReplicatedMergeTree", true, false},
		{"CREATE TABLE t (id UInt64) ENGINE = ReplicatedMergeTree ORDER BY id", true, false},
		{"CREATE TABLE t (id UInt64) ENGINE=ReplicatedReplacingMergeTree PARTITION BY id ORDER BY id", true, false},
		// empty parens also fall back to the server default
		{"CREATE TABLE t (id UInt64) ENGINE = ReplicatedMergeTree() ORDER BY id", true, false},
		{"CREATE TABLE t (id UInt64) ENGINE = ReplicatedMergeTree(  ) ORDER BY id", true, false},
		{"CREATE TABLE t (id UInt64) ENGINE=ReplicatedVersionedCollapsingMergeTree('/p','{r}',sign,ver)", true, true},
		// not replicated at all
		{"CREATE TABLE t (id UInt64) ENGINE = MergeTree() ORDER BY id", false, false},
		{"CREATE TABLE t (id UInt64) ENGINE = ReplacingMergeTree() ORDER BY id", false, false},
		{"CREATE TABLE t (id UInt64) ENGINE = Log", false, false},
		// the Replicated *database* engine takes no per-table path
		{"CREATE DATABASE db ENGINE = Replicated('/ch/db', '{shard}', '{replica}')", false, false},
		// comments and literals must not trigger detection
		{"CREATE TABLE t (id UInt64) ENGINE = MergeTree() -- was ReplicatedMergeTree('/p','{r}')", false, false},
		{"CREATE TABLE t (note String DEFAULT 'ENGINE = ReplicatedMergeTree') ENGINE = MergeTree()", false, false},
	}
	for _, tt := range tests {
		got := AnalyzeEngine(tt.sql)
		if got.IsReplicated != tt.replicated || got.ExplicitPath != tt.explicit {
			t.Errorf("AnalyzeEngine(%.70s)\n got replicated=%v explicit=%v\nwant replicated=%v explicit=%v",
				tt.sql, got.IsReplicated, got.ExplicitPath, tt.replicated, tt.explicit)
		}
	}
}

// The single-node answer, end to end at the planner level: with a
// non-distributed topology, nothing the planner generates carries
// ON CLUSTER — including the database DDL it synthesizes itself.
func TestPlan_SingleNodeSynthesizesWithoutOnCluster(t *testing.T) {
	t.Parallel()
	objs := []*DDLObject{replObj(replPathDDL)}
	p := NewPlanner(objs)
	refs, _ := p.ExpandTargets(nil)
	plan, err := p.Plan(context.Background(), "prod", "", refs,
		fakeLive{"!events": true}, // database missing, must be synthesized
		PlanOpts{DeferInvalid: true, OnCluster: false})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Synthesized) != 1 {
		t.Fatalf("expected a synthesized database, got %+v", plan.Synthesized)
	}
	if strings.Contains(plan.Synthesized[0].SQL, "ON CLUSTER") {
		t.Errorf("single node must not get ON CLUSTER: %s", plan.Synthesized[0].SQL)
	}

	// ...and with a distributed topology it does.
	plan, err = p.Plan(context.Background(), "prod", "", refs,
		fakeLive{"!events": true}, PlanOpts{DeferInvalid: true, OnCluster: true})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan.Synthesized[0].SQL, "ON CLUSTER '{cluster}'") {
		t.Errorf("distributed cluster should get ON CLUSTER: %s", plan.Synthesized[0].SQL)
	}
}

// Plan must not mutate the Planner or the caller's objects: an earlier
// version registered synthesized databases into p.index and appended
// implicit edges onto DDLObject.DependsOn, so a second Plan on the same
// Planner silently produced a different result.
func TestPlan_IsRepeatableAndDoesNotMutateInput(t *testing.T) {
	t.Parallel()
	objs := []*DDLObject{replObj(replPathDDL)}
	depsBefore := len(objs[0].DependsOn)
	p := NewPlanner(objs)
	refs, _ := p.ExpandTargets(nil)

	run := func() *Plan {
		t.Helper()
		plan, err := p.Plan(context.Background(), "prod", "", refs,
			fakeLive{"!events": true}, PlanOpts{DeferInvalid: true})
		if err != nil {
			t.Fatal(err)
		}
		return plan
	}

	first, second := run(), run()
	if len(first.Synthesized) != len(second.Synthesized) {
		t.Errorf("Plan is not repeatable: synthesized %d then %d",
			len(first.Synthesized), len(second.Synthesized))
	}
	if first.Render() != second.Render() {
		t.Errorf("two identical Plan calls rendered differently:\n--- first ---\n%s\n--- second ---\n%s",
			first.Render(), second.Render())
	}
	if len(objs[0].DependsOn) != depsBefore {
		t.Errorf("Plan mutated the caller's DependsOn: %d -> %d", depsBefore, len(objs[0].DependsOn))
	}
	if _, leaked := p.index[ObjectRef{Database: "events"}]; leaked {
		t.Error("Plan leaked a synthesized database into the planner index")
	}
	// The implicit database edge must still reach the executor via the plan.
	if got := first.Edges[ObjectRef{Database: "events", Name: "clicks"}]; len(got) != 1 || !got[0].IsDatabase() {
		t.Errorf("plan edges missing the implicit database prerequisite: %v", got)
	}
}

func FuzzAnalyzeEngine(f *testing.F) {
	for _, seed := range []string{
		replPathDDL, bareReplDDL, "", "ENGINE", "ENGINE = ReplicatedMergeTree(",
		"ENGINE = ReplicatedMergeTree('", "ENGINE=ReplicatedMergeTree()",
		"ENGINE = Replicated", "'ENGINE = ReplicatedMergeTree",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, sql string) {
		info := AnalyzeEngine(sql) // must never panic
		if !info.IsReplicated && info.ExplicitPath {
			t.Fatalf("ExplicitPath without IsReplicated: %q", sql)
		}
	})
}

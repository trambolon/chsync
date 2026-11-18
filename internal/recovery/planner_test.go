package recovery

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"gitlab.example.com/infra/ddl-saver/internal/model"
)

// fakeLive is an ExistsChecker backed by a set of "db.name" strings.
// Databases exist by default; mark one missing with "!<db>".
type fakeLive map[string]bool

func (f fakeLive) ObjectExists(_ context.Context, ref ObjectRef) (bool, error) {
	if ref.IsDatabase() {
		return !f["!"+ref.Database], nil
	}
	return f[ref.String()], nil
}

// obj builds a repo object with DDL that is realistic enough to pass the
// execution guard (the planner validates every statement it plans).
func obj(db, name string, kind ObjectKind, deps ...string) *DDLObject {
	var sql string
	switch kind {
	case KindMaterializedView:
		sql = fmt.Sprintf("CREATE MATERIALIZED VIEW %s.%s TO %s.target AS SELECT 1", db, name, db)
	case KindView:
		sql = fmt.Sprintf("CREATE VIEW %s.%s AS SELECT 1", db, name)
	case KindDictionary:
		sql = fmt.Sprintf("CREATE DICTIONARY %s.%s (id UInt64) PRIMARY KEY id", db, name)
	default:
		sql = fmt.Sprintf("CREATE TABLE %s.%s (id UInt64) ENGINE = MergeTree() ORDER BY id", db, name)
	}
	o := &DDLObject{
		Ref:  ObjectRef{Database: db, Name: name},
		Kind: kind,
		SQL:  sql,
	}
	for _, d := range deps {
		ref, _ := model.ParseRef(d, "")
		o.DependsOn = append(o.DependsOn, ref)
	}
	return o
}

// Standard test repo:
//
//	clicks_raw (table)
//	clicks_agg (table)
//	clicks_mv  (mv)  -> clicks_raw, clicks_agg
//	rollup     (table)
//	rollup_mv  (mv)  -> clicks_agg, rollup
//	geo_dict   (dict)-> refdata.geo  (refdata.geo NOT in repo)
func testRepo() []*DDLObject {
	return []*DDLObject{
		obj("events", "clicks_raw", KindTable),
		obj("events", "clicks_agg", KindTable),
		obj("events", "clicks_mv", KindMaterializedView, "events.clicks_raw", "events.clicks_agg"),
		obj("events", "rollup", KindTable),
		obj("events", "rollup_mv", KindMaterializedView, "events.clicks_agg", "events.rollup"),
		obj("events", "geo_dict", KindDictionary, "refdata.geo"),
	}
}

func mustPlan(t *testing.T, p *Planner, targets []string, live fakeLive, opts PlanOpts) *Plan {
	t.Helper()
	refs, err := p.ExpandTargets(targets)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := p.Plan(context.Background(), "prod", "abc123", refs, live, opts)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func createOrder(plan *Plan) []string {
	var out []string
	for _, it := range plan.Items {
		if it.Action == ActionCreate {
			out = append(out, it.Ref.String())
		}
	}
	return out
}

func itemByRef(plan *Plan, ref string) *PlanItem {
	for i := range plan.Items {
		if plan.Items[i].Ref.String() == ref {
			return &plan.Items[i]
		}
	}
	return nil
}

func TestPlan_UpstreamClosureAndOrder(t *testing.T) {
	p := NewPlanner(testRepo())
	plan := mustPlan(t, p, []string{"events.clicks_mv"}, fakeLive{}, PlanOpts{DeferInvalid: true})

	order := createOrder(plan)
	// Both tables must precede the MV; ties break alphabetically.
	want := []string{"events.clicks_agg", "events.clicks_raw", "events.clicks_mv"}
	if strings.Join(order, ",") != strings.Join(want, ",") {
		t.Errorf("order = %v, want %v", order, want)
	}
	if it := itemByRef(plan, "events.clicks_mv"); it.Reason != "requested" {
		t.Errorf("target reason = %q", it.Reason)
	}
	if it := itemByRef(plan, "events.clicks_raw"); !strings.Contains(it.Reason, "required by events.clicks_mv") {
		t.Errorf("dep reason = %q", it.Reason)
	}
}

func TestPlan_LiveDependencyStopsRecursion(t *testing.T) {
	p := NewPlanner(testRepo())
	live := fakeLive{"events.clicks_raw": true}
	plan := mustPlan(t, p, []string{"events.clicks_mv"}, live, PlanOpts{DeferInvalid: true})

	order := createOrder(plan)
	if strings.Join(order, ",") != "events.clicks_agg,events.clicks_mv" {
		t.Errorf("order = %v", order)
	}
	it := itemByRef(plan, "events.clicks_raw")
	if it == nil || it.Action != ActionSkip || !strings.Contains(it.Reason, "dependency satisfied") {
		t.Errorf("live dep should be a skip row: %+v", it)
	}
}

func TestPlan_TargetAlreadyExists(t *testing.T) {
	p := NewPlanner(testRepo())
	live := fakeLive{"events.clicks_mv": true, "events.clicks_raw": true, "events.clicks_agg": true}
	plan := mustPlan(t, p, []string{"events.clicks_mv"}, live, PlanOpts{DeferInvalid: true})

	if got := createOrder(plan); len(got) != 0 {
		t.Errorf("nothing should be created, got %v", got)
	}
	it := itemByRef(plan, "events.clicks_mv")
	if it.Action != ActionSkip || !strings.Contains(it.Reason, "already exists") {
		t.Errorf("existing target: %+v", it)
	}
}

func TestPlan_WithDependents(t *testing.T) {
	p := NewPlanner(testRepo())
	// Restoring clicks_agg should pull both MVs when asked; their other
	// deps (clicks_raw, rollup) come along via upstream closure.
	plan := mustPlan(t, p, []string{"events.clicks_agg"}, fakeLive{}, PlanOpts{WithDependents: true, DeferInvalid: true})

	order := createOrder(plan)
	pos := map[string]int{}
	for i, r := range order {
		pos[r] = i
	}
	for _, r := range []string{"events.clicks_mv", "events.rollup_mv", "events.clicks_raw", "events.rollup"} {
		if _, ok := pos[r]; !ok {
			t.Fatalf("%s missing from plan: %v", r, order)
		}
	}
	if pos["events.clicks_agg"] > pos["events.clicks_mv"] || pos["events.rollup"] > pos["events.rollup_mv"] {
		t.Errorf("dependencies must precede dependents: %v", order)
	}
	if it := itemByRef(plan, "events.clicks_mv"); !strings.Contains(it.Reason, "dependent of events.clicks_agg") {
		t.Errorf("dependent reason = %q", it.Reason)
	}
}

func TestPlan_WithDependents_StopsAtLive(t *testing.T) {
	p := NewPlanner(testRepo())
	live := fakeLive{"events.rollup_mv": true}
	plan := mustPlan(t, p, []string{"events.clicks_agg"}, live, PlanOpts{WithDependents: true, DeferInvalid: true})

	for _, r := range createOrder(plan) {
		if r == "events.rollup_mv" || r == "events.rollup" {
			t.Errorf("live dependent (and its private deps) must not be created: %v", createOrder(plan))
		}
	}
}

func TestPlan_MissingDepDefersAndPropagates(t *testing.T) {
	repo := testRepo()
	// chained: report_mv depends on geo_dict whose dep refdata.geo is
	// missing everywhere -> geo_dict deferred -> report_mv deferred too.
	repo = append(repo, obj("events", "report_mv", KindMaterializedView, "events.geo_dict"))
	p := NewPlanner(repo)
	plan := mustPlan(t, p, []string{"events.report_mv"}, fakeLive{}, PlanOpts{DeferInvalid: true})

	if got := createOrder(plan); len(got) != 0 {
		t.Errorf("nothing creatable, got %v", got)
	}
	d := itemByRef(plan, "events.geo_dict")
	if d.Action != ActionDefer || !strings.Contains(d.Reason, "refdata.geo") {
		t.Errorf("geo_dict: %+v", d)
	}
	r := itemByRef(plan, "events.report_mv")
	if r.Action != ActionDefer || !strings.Contains(r.Reason, "upstream deferred") {
		t.Errorf("report_mv: %+v", r)
	}
}

func TestPlan_MissingDepSatisfiedByLiveCluster(t *testing.T) {
	p := NewPlanner(testRepo())
	live := fakeLive{"refdata.geo": true}
	plan := mustPlan(t, p, []string{"events.geo_dict"}, live, PlanOpts{DeferInvalid: true})

	if got := createOrder(plan); strings.Join(got, ",") != "events.geo_dict" {
		t.Errorf("order = %v", got)
	}
	it := itemByRef(plan, "refdata.geo")
	if it == nil || it.Action != ActionSkip {
		t.Errorf("live external dep should appear as skip: %+v", it)
	}
}

func TestPlan_StrictModeFailsOnMissingDep(t *testing.T) {
	p := NewPlanner(testRepo())
	refs, _ := p.ExpandTargets([]string{"events.geo_dict"})
	_, err := p.Plan(context.Background(), "prod", "", refs, fakeLive{}, PlanOpts{DeferInvalid: false})
	if err == nil || !strings.Contains(err.Error(), "refdata.geo") {
		t.Fatalf("want strict-mode error naming refdata.geo, got %v", err)
	}
}

func TestPlan_CycleDetection(t *testing.T) {
	p := NewPlanner([]*DDLObject{
		obj("db", "a", KindView, "db.b"),
		obj("db", "b", KindView, "db.a"),
	})
	refs, _ := p.ExpandTargets(nil)
	_, err := p.Plan(context.Background(), "prod", "", refs, fakeLive{}, PlanOpts{DeferInvalid: true})
	if err == nil || !strings.Contains(err.Error(), "cycle") ||
		!strings.Contains(err.Error(), "db.a") || !strings.Contains(err.Error(), "db.b") {
		t.Fatalf("want cycle error naming members, got %v", err)
	}
}

func TestExpandTargets(t *testing.T) {
	p := NewPlanner(testRepo())

	refs, err := p.ExpandTargets([]string{"events.clicks_*"})
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 3 { // clicks_raw, clicks_agg, clicks_mv
		t.Errorf("glob matched %v", refs)
	}

	if _, err := p.ExpandTargets([]string{"events.nope"}); err == nil {
		t.Error("unknown object must error")
	}
	if _, err := p.ExpandTargets([]string{"events.zzz_*"}); err == nil {
		t.Error("zero-match glob must error")
	}
	if _, err := p.ExpandTargets([]string{"unqualified"}); err == nil {
		t.Error("unqualified ref must error")
	}

	all, err := p.ExpandTargets(nil)
	if err != nil || len(all) != len(testRepo()) {
		t.Errorf("nil targets should mean the whole repo, got %v (%v)", all, err)
	}
}

func TestPlan_NoticesForLiveDependents(t *testing.T) {
	p := NewPlanner(testRepo())
	// clicks_mv exists live and reads clicks_agg, which we recreate.
	live := fakeLive{"events.clicks_mv": true}
	plan := mustPlan(t, p, []string{"events.clicks_agg"}, live, PlanOpts{DeferInvalid: true})

	found := false
	for _, n := range plan.Notices {
		if strings.Contains(n, "events.clicks_mv") && strings.Contains(n, "events.clicks_agg") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected live-dependent notice, got %v", plan.Notices)
	}
}

func TestPlan_RenderContainsEssentials(t *testing.T) {
	p := NewPlanner(testRepo())
	plan := mustPlan(t, p, []string{"events.clicks_mv"}, fakeLive{"events.clicks_raw": true}, PlanOpts{DeferInvalid: true})
	out := plan.Render()
	for _, want := range []string{"ORDER", "OBJECT", "ACTION", "events.clicks_mv", "create", "skip", "prod"} {
		if !strings.Contains(out, want) {
			t.Errorf("render missing %q:\n%s", want, out)
		}
	}
}

// --- database prerequisites ---------------------------------------------

func TestPlan_MissingDatabaseIsCreatedFirst_FromSnapshot(t *testing.T) {
	t.Parallel()
	repo := append(testRepo(), &DDLObject{
		Ref: ObjectRef{Database: "events"}, Kind: KindDatabase,
		SQL: "CREATE DATABASE events ON CLUSTER '{cluster}' ENGINE = Replicated('/ch/events', '{shard}', '{replica}')",
	})
	p := NewPlanner(repo)
	plan := mustPlan(t, p, []string{"events.clicks_raw"}, fakeLive{"!events": true}, PlanOpts{DeferInvalid: true})

	order := createOrder(plan)
	if strings.Join(order, ",") != "events,events.clicks_raw" {
		t.Fatalf("database must be created first: %v", order)
	}
	it := itemByRef(plan, "events")
	if it.Kind != KindDatabase || it.Reason != "database missing" {
		t.Errorf("db item: %+v", it)
	}
	if len(plan.Synthesized) != 0 {
		t.Error("snapshotted database DDL must be used, not synthesized")
	}
}

func TestPlan_MissingDatabaseIsSynthesizedWhenNotSnapshotted(t *testing.T) {
	t.Parallel()
	p := NewPlanner(testRepo()) // no _database.sql for events
	plan := mustPlan(t, p, []string{"events.clicks_raw"}, fakeLive{"!events": true}, PlanOpts{DeferInvalid: true, OnCluster: true})

	if got := createOrder(plan); strings.Join(got, ",") != "events,events.clicks_raw" {
		t.Fatalf("order %v", got)
	}
	if len(plan.Synthesized) != 1 || !plan.Synthesized[0].Synthesized {
		t.Fatalf("expected one synthesized database, got %+v", plan.Synthesized)
	}
	sql := plan.Synthesized[0].SQL
	if !strings.Contains(sql, "CREATE DATABASE `events`") || !strings.Contains(sql, "ON CLUSTER '{cluster}'") {
		t.Errorf("synthesized DDL: %s", sql)
	}
	if !strings.Contains(itemByRef(plan, "events").Reason, "minimal CREATE DATABASE") {
		t.Error("the plan must tell the operator the DDL was synthesized")
	}
}

func TestPlan_LiveDatabaseIsSkipRow(t *testing.T) {
	t.Parallel()
	p := NewPlanner(testRepo())
	plan := mustPlan(t, p, []string{"events.clicks_raw"}, fakeLive{}, PlanOpts{DeferInvalid: true})
	it := itemByRef(plan, "events")
	if it == nil || it.Action != ActionSkip {
		t.Errorf("live database should be visible as a skip row: %+v", it)
	}
}

func TestPlan_DatabaseFailurePropagates(t *testing.T) {
	t.Parallel()
	objs := testRepo()
	plan := planFor(t, objs, []string{"events.clicks_raw"}, fakeLive{"!events": true}, PlanOpts{DeferInvalid: true})
	fe := &fakeExec{existing: map[string]bool{"!events": true}, failSQL: map[string]int{"CREATE DATABASE": 100}}
	res, err := NewExecutor(fe, nil, ExecOpts{ContinueOnError: true}).Apply(context.Background(), plan, objs)
	if err != nil {
		t.Fatal(err)
	}
	if res.Failed != 1 || res.Deferred != 1 || res.Created != 0 {
		t.Errorf("table must be deferred when its database fails: %s %+v", res.Summary(), res.Objects)
	}
}

func TestPlan_CycleErrorIsTyped(t *testing.T) {
	t.Parallel()
	p := NewPlanner([]*DDLObject{obj("db", "a", KindView, "db.b"), obj("db", "b", KindView, "db.a")})
	refs, _ := p.ExpandTargets(nil)
	_, err := p.Plan(context.Background(), "prod", "", refs, fakeLive{}, PlanOpts{DeferInvalid: true})
	var ce *CycleError
	if !errors.As(err, &ce) || len(ce.Members) != 2 {
		t.Fatalf("want *CycleError with 2 members, got %v", err)
	}
}

// A glob that matches both an MV and its source table: the table is an
// explicit target, so the plan must say "requested", not "required by".
func TestPlan_ExplicitTargetReasonWinsOverDependency(t *testing.T) {
	t.Parallel()
	p := NewPlanner(testRepo())
	refs, err := p.ExpandTargets([]string{"events.clicks_*"})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := p.Plan(context.Background(), "prod", "", refs, fakeLive{}, PlanOpts{DeferInvalid: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"events.clicks_raw", "events.clicks_agg", "events.clicks_mv"} {
		if it := itemByRef(plan, name); it == nil || it.Reason != "requested" {
			t.Errorf("%s: reason = %+v, want requested", name, it)
		}
	}
}

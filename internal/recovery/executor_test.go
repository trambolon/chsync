package recovery

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// fakeExec records DDL executions and can fail specific objects.
type fakeExec struct {
	existing map[string]bool
	failSQL  map[string]int // substring -> remaining failures
	executed []string
}

func (f *fakeExec) ObjectExists(_ context.Context, ref ObjectRef) (bool, error) {
	if ref.IsDatabase() {
		return !f.existing["!"+ref.Database], nil
	}
	return f.existing[ref.String()], nil
}

func (f *fakeExec) Exec(_ context.Context, sql string) error {
	for sub, n := range f.failSQL {
		if n > 0 && strings.Contains(sql, sub) {
			f.failSQL[sub]--
			return errors.New("simulated ClickHouse error")
		}
	}
	f.executed = append(f.executed, sql)
	return nil
}

func planFor(t *testing.T, objs []*DDLObject, targets []string, live fakeLive, opts PlanOpts) *Plan {
	t.Helper()
	p := NewPlanner(objs)
	refs, err := p.ExpandTargets(targets)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := p.Plan(context.Background(), "prod", "", refs, live, opts)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func TestApply_CreatesInOrder(t *testing.T) {
	objs := testRepo()
	plan := planFor(t, objs, []string{"events.clicks_mv"}, fakeLive{}, PlanOpts{DeferInvalid: true})
	fe := &fakeExec{existing: map[string]bool{}}

	res, err := NewExecutor(fe, nil, ExecOpts{ContinueOnError: true}).Apply(context.Background(), plan, objs)
	if err != nil {
		t.Fatal(err)
	}
	if res.Created != 3 || res.ExitCode() != 0 {
		t.Errorf("summary %s exit %d", res.Summary(), res.ExitCode())
	}
	if len(fe.executed) != 3 || !strings.Contains(fe.executed[2], "clicks_mv") {
		t.Errorf("execution order wrong: %v", fe.executed)
	}
}

func TestApply_RecheckSkipsRaces(t *testing.T) {
	objs := testRepo()
	plan := planFor(t, objs, []string{"events.clicks_raw"}, fakeLive{}, PlanOpts{DeferInvalid: true})
	// Object appears between plan and apply.
	fe := &fakeExec{existing: map[string]bool{"events.clicks_raw": true}}

	res, err := NewExecutor(fe, nil, ExecOpts{}).Apply(context.Background(), plan, objs)
	if err != nil {
		t.Fatal(err)
	}
	// Two skips: the live database row and the table that appeared late.
	if res.Created != 0 || res.Skipped != 2 || res.ExitCode() != 0 {
		t.Errorf("summary %s", res.Summary())
	}
	for _, o := range res.Objects {
		if o.Ref.Name == "clicks_raw" && !strings.Contains(o.Detail, "appeared in cluster after planning") {
			t.Errorf("table should be skipped by the re-check: %+v", o)
		}
	}
	if len(fe.executed) != 0 {
		t.Errorf("nothing should execute, got %v", fe.executed)
	}
}

func TestApply_RetrySucceeds(t *testing.T) {
	objs := testRepo()
	plan := planFor(t, objs, []string{"events.clicks_raw"}, fakeLive{}, PlanOpts{DeferInvalid: true})
	fe := &fakeExec{existing: map[string]bool{}, failSQL: map[string]int{"clicks_raw": 2}}

	res, err := NewExecutor(fe, nil, ExecOpts{Retries: 3}).Apply(context.Background(), plan, objs)
	if err != nil {
		t.Fatal(err)
	}
	if res.Created != 1 || res.Failed != 0 {
		t.Errorf("retry should recover: %s", res.Summary())
	}
}

func TestApply_FailurePropagatesDownstream(t *testing.T) {
	objs := testRepo()
	plan := planFor(t, objs, []string{"events.clicks_mv"}, fakeLive{}, PlanOpts{DeferInvalid: true})
	// clicks_agg permanently fails; clicks_mv depends on it, clicks_raw does not.
	fe := &fakeExec{existing: map[string]bool{}, failSQL: map[string]int{"clicks_agg": 100}}

	res, err := NewExecutor(fe, nil, ExecOpts{Retries: 1, ContinueOnError: true}).Apply(context.Background(), plan, objs)
	if err != nil {
		t.Fatal(err)
	}
	if res.Failed != 1 || res.Deferred != 1 || res.Created != 1 {
		t.Errorf("summary %s objects %+v", res.Summary(), res.Objects)
	}
	if res.ExitCode() != 2 {
		t.Errorf("partial result must exit 2, got %d", res.ExitCode())
	}
	for _, o := range res.Objects {
		if o.Ref.Name == "clicks_mv" && (o.Outcome != OutcomeDeferred || !strings.Contains(o.Detail, "clicks_agg")) {
			t.Errorf("clicks_mv should be deferred naming root cause: %+v", o)
		}
	}
}

func TestApply_AbortWithoutContinueOnError(t *testing.T) {
	objs := testRepo()
	plan := planFor(t, objs, []string{"events.clicks_mv"}, fakeLive{}, PlanOpts{DeferInvalid: true})
	fe := &fakeExec{existing: map[string]bool{}, failSQL: map[string]int{"clicks_agg": 100}}

	res, err := NewExecutor(fe, nil, ExecOpts{ContinueOnError: false}).Apply(context.Background(), plan, objs)
	if err != nil {
		t.Fatal(err)
	}
	// First create (clicks_agg alphabetically) fails -> everything after aborts.
	if res.Failed != 1 || res.Created != 0 || res.Deferred != 2 {
		t.Errorf("summary %s objects %+v", res.Summary(), res.Objects)
	}
}

func TestApply_CarriesPlanSkipsAndDefers(t *testing.T) {
	repo := append(testRepo(), obj("events", "report_mv", KindMaterializedView, "events.geo_dict"))
	plan := planFor(t, repo, []string{"events.report_mv"}, fakeLive{}, PlanOpts{DeferInvalid: true})
	fe := &fakeExec{existing: map[string]bool{}}

	res, err := NewExecutor(fe, nil, ExecOpts{}).Apply(context.Background(), plan, repo)
	if err != nil {
		t.Fatal(err)
	}
	if res.Deferred != 2 || res.ExitCode() != 2 { // geo_dict + report_mv from the plan
		t.Errorf("summary %s", res.Summary())
	}
}

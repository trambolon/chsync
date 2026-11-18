package recovery

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"
)

// ExecClient is the slice of the ClickHouse client the executor needs.
// clickhouse.Client satisfies it directly.
type ExecClient interface {
	ObjectExists(ctx context.Context, ref ObjectRef) (bool, error)
	Exec(ctx context.Context, sql string) error
}

type ExecOpts struct {
	Retries         int           // attempts beyond the first
	Backoff         time.Duration // base backoff, multiplied by attempt number
	ContinueOnError bool          // keep creating independent objects after a failure
}

type ObjectOutcome string

const (
	OutcomeCreated  ObjectOutcome = "created"
	OutcomeSkipped  ObjectOutcome = "skipped"
	OutcomeDeferred ObjectOutcome = "deferred"
	OutcomeFailed   ObjectOutcome = "failed"
)

type ObjectResult struct {
	Ref     ObjectRef
	Outcome ObjectOutcome
	Detail  string
}

type Result struct {
	Objects                            []ObjectResult
	Created, Skipped, Deferred, Failed int
}

// ExitCode maps a result to the CLI contract: 0 = fully applied,
// 2 = partial (something deferred or failed). Fatal errors are exit 1 and
// happen before a Result exists.
func (r *Result) ExitCode() int {
	if r.Failed > 0 || r.Deferred > 0 {
		return 2
	}
	return 0
}

func (r *Result) add(ref ObjectRef, o ObjectOutcome, detail string) {
	r.Objects = append(r.Objects, ObjectResult{Ref: ref, Outcome: o, Detail: detail})
	switch o {
	case OutcomeCreated:
		r.Created++
	case OutcomeSkipped:
		r.Skipped++
	case OutcomeDeferred:
		r.Deferred++
	case OutcomeFailed:
		r.Failed++
	}
}

func (r *Result) Summary() string {
	return fmt.Sprintf("created=%d skipped=%d deferred=%d failed=%d",
		r.Created, r.Skipped, r.Deferred, r.Failed)
}

type Executor struct {
	client ExecClient
	log    *slog.Logger
	opts   ExecOpts
}

func NewExecutor(client ExecClient, log *slog.Logger, opts ExecOpts) *Executor {
	if log == nil {
		log = slog.Default()
	}
	return &Executor{client: client, log: log, opts: opts}
}

// Apply executes the plan's create items in order; objs supplies SQL and
// dependency edges (typically the LoadObjects output the plan was built
// from). Skip and defer items are carried into the result untouched.
//
// Failure policy: when a create fails after retries, every remaining create
// that transitively depends on it is deferred — its DDL would fail anyway.
// With ContinueOnError=false the remaining creates are deferred wholesale so
// the operator gets a clean stop instead of a half-applied set.
func (e *Executor) Apply(ctx context.Context, plan *Plan, objs []*DDLObject) (*Result, error) {
	sqlByRef := make(map[ObjectRef]string, len(objs))
	kindByRef := make(map[ObjectRef]ObjectKind, len(objs))
	depsOf := make(map[ObjectRef][]ObjectRef, len(objs))
	for _, o := range append(slices.Clone(objs), plan.Synthesized...) {
		sqlByRef[o.Ref] = o.SQL
		kindByRef[o.Ref] = o.Kind
		depsOf[o.Ref] = o.DependsOn
	}
	// The plan's edges include the implicit database prerequisites, which
	// the raw objects do not carry; prefer them when present.
	if plan.Edges != nil {
		for ref, deps := range plan.Edges {
			depsOf[ref] = deps
		}
	}

	res := &Result{}
	var creates []PlanItem
	for _, it := range plan.Items {
		switch it.Action {
		case ActionCreate:
			creates = append(creates, it)
		case ActionSkip:
			res.add(it.Ref, OutcomeSkipped, it.Reason)
		case ActionDefer:
			res.add(it.Ref, OutcomeDeferred, it.Reason)
		}
	}

	bad := map[ObjectRef]string{} // failed/deferred refs -> root cause
	abort := false

	for _, it := range creates {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		if abort {
			res.add(it.Ref, OutcomeDeferred, "aborted after earlier failure")
			continue
		}
		if root := blockedBy(it.Ref, depsOf, bad); root != "" {
			bad[it.Ref] = root
			res.add(it.Ref, OutcomeDeferred, "upstream failed: "+root)
			continue
		}

		sql, ok := sqlByRef[it.Ref]
		if !ok {
			bad[it.Ref] = it.Ref.String()
			res.add(it.Ref, OutcomeFailed, "no SQL content for object (internal error)")
			if !e.opts.ContinueOnError {
				abort = true
			}
			continue
		}

		// Defense in depth: the planner already guarded this statement,
		// but the executor never trusts SQL it has not validated itself —
		// this is the only place that talks to a live cluster.
		safeSQL, err := GuardDDL(it.Ref, kindByRef[it.Ref], sql)
		if err != nil {
			e.log.Error("blocked unsafe DDL", "object", it.Ref.String(), "error", err)
			bad[it.Ref] = it.Ref.String()
			res.add(it.Ref, OutcomeFailed, err.Error())
			abort = true // never continue past a safety violation
			continue
		}
		sql = safeSQL

		// Plans can be minutes old by the time an operator confirms;
		// re-check right before creating (create strategy: IF NOT EXISTS).
		exists, err := e.client.ObjectExists(ctx, it.Ref)
		if err != nil {
			return res, fmt.Errorf("existence re-check %s: %w", it.Ref, err)
		}
		if exists {
			res.add(it.Ref, OutcomeSkipped, "appeared in cluster after planning")
			continue
		}

		if err, remedy := e.execWithRetry(ctx, it.Ref, sql); err != nil {
			e.log.Error("create failed", "object", it.Ref.String(), "error", err)
			bad[it.Ref] = it.Ref.String()
			res.add(it.Ref, OutcomeFailed, withRemedy(err.Error(), remedy))
			if !e.opts.ContinueOnError {
				abort = true
			}
			continue
		}
		e.log.Info("created", "object", it.Ref.String(), "order", it.Order)
		res.add(it.Ref, OutcomeCreated, "")
	}
	return res, nil
}

// blockedBy returns the root cause if any dependency of ref is in bad.
func blockedBy(ref ObjectRef, depsOf map[ObjectRef][]ObjectRef, bad map[ObjectRef]string) string {
	for _, dep := range depsOf[ref] {
		if root, ok := bad[dep]; ok {
			return root
		}
	}
	return ""
}

// execWithRetry runs the statement, retrying only errors that could
// plausibly succeed on another attempt (see Classify). It returns the
// failure and any operator-facing remedy.
func (e *Executor) execWithRetry(ctx context.Context, ref ObjectRef, sql string) (err error, remedy string) {
	attempts := e.opts.Retries + 1
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr, ""
		}
		execErr := e.client.Exec(ctx, sql)
		if execErr == nil {
			return nil, ""
		}
		lastErr = execErr

		retryable, r := Classify(execErr)
		remedy = r
		if !retryable {
			return fmt.Errorf("permanent failure (ClickHouse code %d): %w", ErrorCode(execErr), execErr), remedy
		}
		if attempt < attempts {
			delay := e.opts.Backoff * time.Duration(attempt)
			e.log.Warn("create attempt failed, retrying",
				"object", ref.String(), "attempt", attempt, "delay", delay.String(), "error", execErr)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ctx.Err(), remedy
			}
		}
	}
	return fmt.Errorf("after %d attempts: %w", attempts, lastErr), remedy
}

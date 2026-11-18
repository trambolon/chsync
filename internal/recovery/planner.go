package recovery

import (
	"context"
	"fmt"
	"path"
	"slices"
	"sort"
	"strings"
)

type PlanAction string

const (
	ActionCreate PlanAction = "create"
	ActionSkip   PlanAction = "skip"
	ActionDefer  PlanAction = "defer"
)

type PlanItem struct {
	Ref    ObjectRef
	Kind   ObjectKind
	Action PlanAction
	Reason string
	Order  int // 1-based creation order; 0 for skip/defer
}

type Plan struct {
	Cluster   string
	GitRef    string
	Requested []ObjectRef
	Items     []PlanItem
	Notices   []string
	// Synthesized holds DDL the planner generated (missing databases with
	// no snapshot file). The executor needs them alongside the repo objects.
	Synthesized []*DDLObject
	// Edges are the dependency edges this plan was ordered by: the
	// extracted ones plus implicit "object needs its database" edges. The
	// executor uses them to defer the dependents of a failed create.
	Edges map[ObjectRef][]ObjectRef
}

func (p *Plan) Counts() (creates, skips, defers int) {
	for _, it := range p.Items {
		switch it.Action {
		case ActionCreate:
			creates++
		case ActionSkip:
			skips++
		case ActionDefer:
			defers++
		}
	}
	return
}

type PlanOpts struct {
	// WithDependents also restores repo objects downstream of the targets
	// (e.g. MVs reading from a recreated table), stopping at any dependent
	// that still exists live.
	WithDependents bool
	// DeferInvalid maps recovery.defer_invalid_objects: when true, objects
	// with unresolvable dependencies are deferred (exit code 2); when false,
	// planning fails.
	DeferInvalid bool
	// OnCluster makes synthesized database DDL (used when the snapshot
	// predates _database.sql files) carry ON CLUSTER '{cluster}'.
	OnCluster bool
	// ReplicaDefaults describes the target server's default_replica_path /
	// default_replica_name, which decide whether argument-less Replicated
	// engines are legal there and whether their resolved path is
	// UUID-based. Zero value means "unknown", and the plan says so rather
	// than guessing.
	ReplicaDefaults ReplicaDefaults
}

// ReplicaDefaults mirrors the server settings that make
// `ENGINE = ReplicatedMergeTree` (no arguments) valid.
type ReplicaDefaults struct {
	Known bool
	Path  string
	Name  string
}

// UUIDBased reports whether the default path is regenerated per table.
func (r ReplicaDefaults) UUIDBased() bool {
	return r.Known && strings.Contains(r.Path, "{uuid}")
}

// ExistsChecker answers "is this present in the live cluster right now".
// Recovery treats a live object as a satisfied dependency and never touches
// it (create strategy is locked to IF NOT EXISTS). Database refs (Name=="")
// are answered against system.databases.
type ExistsChecker interface {
	ObjectExists(ctx context.Context, ref ObjectRef) (bool, error)
}

// CycleError is returned when the create set cannot be ordered.
type CycleError struct {
	Members []ObjectRef
}

func (e *CycleError) Error() string {
	names := make([]string, len(e.Members))
	for i, m := range e.Members {
		names[i] = m.String()
	}
	return "dependency cycle detected among: " + strings.Join(names, ", ")
}

type Planner struct {
	objs  []*DDLObject
	index map[ObjectRef]*DDLObject
	rev   map[ObjectRef][]ObjectRef
}

func NewPlanner(objs []*DDLObject) *Planner {
	p := &Planner{objs: objs, index: make(map[ObjectRef]*DDLObject, len(objs))}
	for _, o := range objs {
		p.index[o.Ref] = o
	}
	p.rev = ReverseIndex(objs)
	return p
}

// ExpandTargets resolves --object arguments against the repo. Patterns must
// be qualified ("db.name"); '*', '?' and '[...]' glob over the combined
// "db.name" string. A pattern matching nothing is an error — recovering the
// wrong nothing silently is worse than failing loudly.
func (p *Planner) ExpandTargets(patterns []string) ([]ObjectRef, error) {
	if len(patterns) == 0 {
		// Full-repo recovery for the cluster. Databases are not targets in
		// their own right; they come in as prerequisites of their objects.
		refs := make([]ObjectRef, 0, len(p.objs))
		for _, o := range p.objs {
			if o.Kind != KindDatabase {
				refs = append(refs, o.Ref)
			}
		}
		sortRefs(refs)
		return refs, nil
	}
	seen := map[ObjectRef]bool{}
	var out []ObjectRef
	for _, pat := range patterns {
		if !strings.Contains(pat, ".") {
			return nil, fmt.Errorf("object %q must be qualified as db.name", pat)
		}
		if strings.ContainsAny(pat, "*?[") {
			matched := false
			for _, o := range p.objs {
				if o.Kind == KindDatabase {
					continue
				}
				ok, err := path.Match(pat, o.Ref.String())
				if err != nil {
					return nil, fmt.Errorf("bad pattern %q: %w", pat, err)
				}
				if ok && !seen[o.Ref] {
					seen[o.Ref] = true
					out = append(out, o.Ref)
					matched = true
				} else if ok {
					matched = true
				}
			}
			if !matched {
				return nil, fmt.Errorf("pattern %q matches no object in the repository", pat)
			}
			continue
		}
		ref, err := ParseRefStrict(pat)
		if err != nil {
			return nil, err
		}
		if _, ok := p.index[ref]; !ok {
			return nil, fmt.Errorf("object %s not found in the repository snapshot", ref)
		}
		if !seen[ref] {
			seen[ref] = true
			out = append(out, ref)
		}
	}
	sortRefs(out)
	return out, nil
}

// ParseRefStrict parses "db.name" with no default database.
func ParseRefStrict(s string) (ObjectRef, error) {
	parts := strings.SplitN(s, ".", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return ObjectRef{}, fmt.Errorf("invalid object reference %q: want db.name", s)
	}
	return ObjectRef{Database: parts[0], Name: parts[1]}, nil
}

// Plan builds the ordered recovery plan for targets.
func (p *Planner) Plan(ctx context.Context, cluster, gitRef string, targets []ObjectRef, live ExistsChecker, opts PlanOpts) (*Plan, error) {
	plan := &Plan{Cluster: cluster, GitRef: gitRef, Requested: targets}

	include := map[ObjectRef]bool{}     // repo objects to create
	skip := map[ObjectRef]string{}      // ref -> reason
	cause := map[ObjectRef]string{}     // why an object entered the plan
	missing := map[ObjectRef][]string{} // include obj -> unresolvable deps

	exists := func(ref ObjectRef) (bool, error) { return live.ObjectExists(ctx, ref) }

	// Upstream closure: everything a repo object needs before it can be
	// created. A live dependency satisfies the edge and stops recursion.
	var closeUp func(obj *DDLObject) error
	closeUp = func(obj *DDLObject) error {
		for _, dep := range obj.DependsOn {
			if include[dep] || skip[dep] != "" {
				continue
			}
			ok, err := exists(dep)
			if err != nil {
				return fmt.Errorf("existence check %s: %w", dep, err)
			}
			if ok {
				skip[dep] = "exists in cluster (dependency satisfied)"
				continue
			}
			depObj, inRepo := p.index[dep]
			if !inRepo {
				missing[obj.Ref] = append(missing[obj.Ref],
					fmt.Sprintf("%s (not in repo, not in cluster)", dep))
				continue
			}
			include[dep] = true
			cause[dep] = "required by " + obj.Ref.String()
			if err := closeUp(depObj); err != nil {
				return err
			}
		}
		return nil
	}

	addTarget := func(ref ObjectRef, why string) error {
		if include[ref] {
			// Reached earlier as another target's dependency; an explicit
			// request is the more useful reason to show the operator.
			if why == "requested" {
				cause[ref] = why
			}
			return nil
		}
		if skip[ref] != "" {
			return nil
		}
		ok, err := exists(ref)
		if err != nil {
			return fmt.Errorf("existence check %s: %w", ref, err)
		}
		if ok {
			skip[ref] = "already exists in cluster"
			return nil
		}
		include[ref] = true
		cause[ref] = why
		return closeUp(p.index[ref])
	}

	for _, t := range targets {
		if err := addTarget(t, "requested"); err != nil {
			return nil, err
		}
	}

	// Downstream expansion: repo dependents of anything we are creating,
	// stopping at dependents that still exist live.
	if opts.WithDependents {
		queue := make([]ObjectRef, 0, len(include))
		for r := range include {
			queue = append(queue, r)
		}
		sortRefs(queue)
		visited := map[ObjectRef]bool{}
		for len(queue) > 0 {
			cur := queue[0]
			queue = queue[1:]
			if visited[cur] {
				continue
			}
			visited[cur] = true
			for _, dep := range p.rev[cur] {
				if include[dep] || skip[dep] != "" {
					continue
				}
				if err := addTarget(dep, "dependent of "+cur.String()); err != nil {
					return nil, err
				}
				if include[dep] {
					queue = append(queue, dep)
				}
			}
		}
	}

	// Database prerequisites: every object to be created needs its
	// database. A live database is a skip row; a missing one is created
	// first — from the snapshotted _database.sql when present, otherwise
	// from a minimal synthesized statement that the plan shows verbatim.
	synthesized := map[ObjectRef]*DDLObject{}
	// lookup resolves a ref against the repo objects first, then anything
	// this plan synthesized. Plan never writes to p.index.
	lookup := func(ref ObjectRef) *DDLObject {
		if o, ok := p.index[ref]; ok {
			return o
		}
		return synthesized[ref]
	}
	for ref := range include {
		if ref.IsDatabase() {
			continue
		}
		dbRef := ref.DatabaseRef()
		if include[dbRef] || skip[dbRef] != "" {
			continue
		}
		ok, err := exists(dbRef)
		if err != nil {
			return nil, fmt.Errorf("existence check database %s: %w", dbRef, err)
		}
		if ok {
			skip[dbRef] = "database exists in cluster"
			continue
		}
		if _, inRepo := p.index[dbRef]; !inRepo {
			synthesized[dbRef] = synthesizeDatabase(dbRef, opts.OnCluster)
			cause[dbRef] = "database missing (no _database.sql in snapshot; using minimal CREATE DATABASE)"
		} else {
			cause[dbRef] = "database missing"
		}
		include[dbRef] = true
	}

	// Effective dependency edges for THIS plan: the extracted ones plus the
	// implicit "object needs its database" edges. Held here rather than
	// appended to DDLObject.DependsOn, so Plan never mutates the objects
	// its caller owns and stays callable more than once.
	edges := make(map[ObjectRef][]ObjectRef, len(include))
	for ref := range include {
		deps := slices.Clone(lookup(ref).DependsOn)
		if !ref.IsDatabase() && include[ref.DatabaseRef()] && !slices.Contains(deps, ref.DatabaseRef()) {
			deps = append(deps, ref.DatabaseRef())
		}
		edges[ref] = deps
	}

	// Defer propagation: an object with a missing dependency is deferred,
	// and so is everything in the plan that depends on it — to a fixpoint.
	deferred := map[ObjectRef]string{}
	for ref, deps := range missing {
		deferred[ref] = "missing dependency: " + strings.Join(deps, ", ")
	}
	for changed := true; changed; {
		changed = false
		for ref := range include {
			if deferred[ref] != "" {
				continue
			}
			for _, dep := range edges[ref] {
				if reason := deferred[dep]; reason != "" {
					deferred[ref] = "upstream deferred: " + dep.String()
					changed = true
					break
				}
			}
		}
	}
	if len(deferred) > 0 && !opts.DeferInvalid {
		var lines []string
		for ref, reason := range deferred {
			lines = append(lines, fmt.Sprintf("%s: %s", ref, reason))
		}
		sort.Strings(lines)
		return nil, fmt.Errorf(
			"cannot build a complete plan (defer_invalid_objects=false):\n  %s",
			strings.Join(lines, "\n  "))
	}

	// Safety gate: every statement that would be executed is validated
	// before the operator ever sees a confirmation prompt. A repository
	// holding anything but a matching CREATE is a signal that something is
	// wrong (tampering, a bad merge, hand-editing), so planning refuses
	// wholesale rather than executing "the safe subset".
	var unsafe []string
	for ref := range include {
		if deferred[ref] != "" {
			continue
		}
		o := lookup(ref)
		if _, err := GuardDDL(o.Ref, o.Kind, o.SQL); err != nil {
			unsafe = append(unsafe, "  "+err.Error())
		}
	}
	if len(unsafe) > 0 {
		sort.Strings(unsafe)
		return nil, fmt.Errorf(
			"refusing to build a recovery plan: %d file(s) in the schema repository "+
				"are not safe to execute:\n%s\n\nInspect these files at ref %s before retrying.",
			len(unsafe), strings.Join(unsafe, "\n"), gitRef)
	}

	// Topological order over the create set.
	creates := make([]ObjectRef, 0, len(include))
	for ref := range include {
		if deferred[ref] == "" {
			creates = append(creates, ref)
		}
	}
	ordered, err := p.toposort(creates, edges)
	if err != nil {
		return nil, err
	}

	plan.Edges = edges
	for _, o := range synthesized {
		plan.Synthesized = append(plan.Synthesized, o)
	}
	slices.SortFunc(plan.Synthesized, func(a, b *DDLObject) int { return a.Ref.Compare(b.Ref) })

	// Assemble items: creates in order, then skips, then defers.
	for i, ref := range ordered {
		plan.Items = append(plan.Items, PlanItem{
			Ref: ref, Kind: lookup(ref).Kind,
			Action: ActionCreate, Reason: cause[ref], Order: i + 1,
		})
	}
	skipRefs := make([]ObjectRef, 0, len(skip))
	for ref := range skip {
		skipRefs = append(skipRefs, ref)
	}
	sortRefs(skipRefs)
	for _, ref := range skipRefs {
		kind := KindUnknown
		if o, ok := p.index[ref]; ok {
			kind = o.Kind
		}
		plan.Items = append(plan.Items, PlanItem{Ref: ref, Kind: kind, Action: ActionSkip, Reason: skip[ref]})
	}
	deferRefs := make([]ObjectRef, 0, len(deferred))
	for ref := range deferred {
		deferRefs = append(deferRefs, ref)
	}
	sortRefs(deferRefs)
	for _, ref := range deferRefs {
		plan.Items = append(plan.Items, PlanItem{Ref: ref, Kind: lookup(ref).Kind, Action: ActionDefer, Reason: deferred[ref]})
	}

	// Notices: repo objects OUTSIDE the plan that read from something we
	// are about to recreate and still exist live — after recovery the
	// operator should verify those pipelines.
	noticed := map[string]bool{}
	for _, ref := range ordered {
		for _, dependent := range p.rev[ref] {
			if include[dependent] {
				continue
			}
			ok, err := exists(dependent)
			if err != nil {
				return nil, err
			}
			if ok {
				n := fmt.Sprintf("live object %s reads from recreated %s — verify it resumes correctly", dependent, ref)
				if !noticed[n] {
					noticed[n] = true
					plan.Notices = append(plan.Notices, n)
				}
			}
		}
	}
	for _, ref := range ordered {
		for _, ext := range lookup(ref).External {
			n := fmt.Sprintf("%s has external dependency (%s) that cannot be verified", ref, ext)
			if !noticed[n] {
				noticed[n] = true
				plan.Notices = append(plan.Notices, n)
			}
		}
	}
	// Replicated engines bind to a ZooKeeper path, and what can go wrong
	// depends on where that path comes from — see AnalyzeEngine. The
	// operator is told before confirming rather than by a failure report.
	var pinned, deferredPath []ObjectRef
	for _, ref := range ordered {
		switch info := AnalyzeEngine(lookup(ref).SQL); {
		case !info.IsReplicated:
		case info.ExplicitPath:
			pinned = append(pinned, ref)
		default:
			deferredPath = append(deferredPath, ref)
		}
	}
	if len(pinned) > 0 {
		plan.Notices = append(plan.Notices, fmt.Sprintf(
			"%d object(s) pin an explicit ZooKeeper path (%s): those paths must be free. "+
				"If these were dropped without SYNC, the stale registration makes CREATE fail "+
				"with \"Replica ... already exists\"",
			len(pinned), summarizeRefs(pinned, 5)))
	}
	if len(deferredPath) > 0 {
		rd := opts.ReplicaDefaults
		switch {
		case !rd.Known:
			plan.Notices = append(plan.Notices, fmt.Sprintf(
				"%d object(s) omit the ZooKeeper path (%s) and rely on the server's "+
					"default_replica_path, which could not be read from this cluster "+
					"(needs ClickHouse 22.10+ and access to system.server_settings). "+
					"If the target does not define it, these CREATEs fail",
				len(deferredPath), summarizeRefs(deferredPath, 5)))
		case rd.UUIDBased():
			plan.Notices = append(plan.Notices, fmt.Sprintf(
				"%d object(s) omit the ZooKeeper path (%s) and take the server default %q, "+
					"which is UUID-based: each recreated table gets a fresh path, so no stale-path "+
					"conflict — but it is a new replica set and starts empty rather than "+
					"reattaching to existing data",
				len(deferredPath), summarizeRefs(deferredPath, 5), rd.Path))
		default:
			plan.Notices = append(plan.Notices, fmt.Sprintf(
				"%d object(s) omit the ZooKeeper path (%s) and take the server default %q, "+
					"which is not UUID-based: the resolved path is deterministic, so a stale "+
					"registration from a previous incarnation will block CREATE",
				len(deferredPath), summarizeRefs(deferredPath, 5), rd.Path))
		}
	}

	sort.Strings(plan.Notices)
	return plan, nil
}

// summarizeRefs renders at most max refs, then "and N more".
func summarizeRefs(refs []ObjectRef, maxNamed int) string {
	names := make([]string, 0, maxNamed)
	for i, r := range refs {
		if i == maxNamed {
			return strings.Join(names, ", ") + fmt.Sprintf(" and %d more", len(refs)-maxNamed)
		}
		names = append(names, r.String())
	}
	return strings.Join(names, ", ")
}

// toposort runs Kahn's algorithm over the create set with deterministic
// (sorted) tie-breaking. Only edges between two created objects count;
// skipped/live dependencies are already satisfied.
func (p *Planner) toposort(creates []ObjectRef, edges map[ObjectRef][]ObjectRef) ([]ObjectRef, error) {
	inSet := map[ObjectRef]bool{}
	for _, r := range creates {
		inSet[r] = true
	}
	indeg := map[ObjectRef]int{}
	out := map[ObjectRef][]ObjectRef{}
	for _, r := range creates {
		indeg[r] += 0
		for _, dep := range edges[r] {
			if inSet[dep] {
				out[dep] = append(out[dep], r)
				indeg[r]++
			}
		}
	}
	var ready []ObjectRef
	for r, d := range indeg {
		if d == 0 {
			ready = append(ready, r)
		}
	}
	sortRefs(ready)

	var ordered []ObjectRef
	for len(ready) > 0 {
		cur := ready[0]
		ready = ready[1:]
		ordered = append(ordered, cur)
		next := out[cur]
		sortRefs(next)
		for _, dependent := range next {
			indeg[dependent]--
			if indeg[dependent] == 0 {
				ready = append(ready, dependent)
				sortRefs(ready)
			}
		}
	}
	if len(ordered) != len(creates) {
		var cycle []ObjectRef
		for r, d := range indeg {
			if d > 0 {
				cycle = append(cycle, r)
			}
		}
		sortRefs(cycle)
		return nil, &CycleError{Members: cycle}
	}
	return ordered, nil
}

func sortRefs(refs []ObjectRef) { slices.SortFunc(refs, ObjectRef.Compare) }

// synthesizeDatabase builds the fallback DDL for a database the snapshot
// never captured. Deliberately minimal: engine defaults to the server's
// default (Atomic on modern servers). Shown in the plan so the operator can
// object before it runs.
func synthesizeDatabase(ref ObjectRef, onCluster bool) *DDLObject {
	sql := "CREATE DATABASE `" + strings.ReplaceAll(ref.Database, "`", "``") + "`"
	if onCluster {
		sql += " ON CLUSTER '{cluster}'"
	}
	return &DDLObject{Ref: ref, Kind: KindDatabase, SQL: sql, Synthesized: true}
}

// Render formats the plan as the table shown to the operator before
// confirmation.
func (p *Plan) Render() string {
	var b strings.Builder
	creates, skips, defers := p.Counts()
	fmt.Fprintf(&b, "Recovery plan for cluster %q", p.Cluster)
	if p.GitRef != "" {
		fmt.Fprintf(&b, " at ref %s", p.GitRef)
	}
	fmt.Fprintf(&b, "\n%d to create, %d skipped, %d deferred\n\n", creates, skips, defers)

	wRef, wKind := len("OBJECT"), len("KIND")
	for _, it := range p.Items {
		if n := len(it.Ref.String()); n > wRef {
			wRef = n
		}
		if n := len(string(it.Kind)); n > wKind {
			wKind = n
		}
	}
	fmt.Fprintf(&b, "%-5s  %-*s  %-*s  %-6s  %s\n", "ORDER", wRef, "OBJECT", wKind, "KIND", "ACTION", "REASON")
	for _, it := range p.Items {
		order := ""
		if it.Order > 0 {
			order = fmt.Sprintf("%d", it.Order)
		}
		fmt.Fprintf(&b, "%-5s  %-*s  %-*s  %-6s  %s\n",
			order, wRef, it.Ref.String(), wKind, string(it.Kind), string(it.Action), it.Reason)
	}
	if len(p.Notices) > 0 {
		b.WriteString("\nNotices:\n")
		for _, n := range p.Notices {
			fmt.Fprintf(&b, "  ! %s\n", n)
		}
	}
	return b.String()
}

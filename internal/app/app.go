// Package app holds the orchestration that used to live in main: the
// snapshot loop body and the recover flow. Every side effect (ClickHouse,
// GitLab, stdout, the confirmation prompt) enters through Deps, so both
// flows are unit-testable with fakes and main only parses flags.
package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path"
	"strings"
	"time"

	"gitlab.example.com/infra/ddl-saver/internal/clickhouse"
	"gitlab.example.com/infra/ddl-saver/internal/config"
	"gitlab.example.com/infra/ddl-saver/internal/gitlab"
	"gitlab.example.com/infra/ddl-saver/internal/recovery"
	"gitlab.example.com/infra/ddl-saver/internal/snapshot"
)

// Connector opens a ClickHouse client for a configured cluster.
type Connector func(ctx context.Context, cl *config.Cluster) (clickhouse.Client, error)

// Confirmer asks the operator a yes/no question.
type Confirmer func(prompt string) (bool, error)

// Deps is everything the flows need from the outside world.
type Deps struct {
	Config  *config.Config
	Log     *slog.Logger
	Connect Connector
	Repo    gitlab.Repo
	Out     io.Writer // plan and report output (stdout in the CLI)
	Confirm Confirmer // nil = never confirms
	Status  *Status   // optional; updated by snapshot runs
	Now     func() time.Time
}

func (d *Deps) log() *slog.Logger {
	if d.Log == nil {
		return slog.Default()
	}
	return d.Log
}

func (d *Deps) now() time.Time {
	if d.Now == nil {
		return time.Now()
	}
	return d.Now()
}

// ErrSnapshotPartial is returned by SnapshotAll when at least one cluster
// failed; the per-cluster errors are joined into it.
var ErrSnapshotPartial = errors.New("snapshot finished with errors")

// SnapshotOpts mirrors the snapshot subcommand flags.
type SnapshotOpts struct {
	Cluster string // empty = every configured cluster
	DryRun  bool
	// AllowEmpty lets a cluster that reports no objects archive what the
	// repository holds for it. Only ever set from the command line: the
	// daemon must not do that on its own (see snapshot.ErrEmptyCluster).
	AllowEmpty bool
}

// SnapshotCluster connects, snapshots, disconnects.
func (d *Deps) SnapshotCluster(ctx context.Context, cl *config.Cluster, o SnapshotOpts) (snapshot.Stats, error) {
	filter, err := clickhouse.NewFilter(cl.Filter.SkipDatabases, cl.Filter.SkipTablesRegex, cl.Filter.IncludeEngines)
	if err != nil {
		return snapshot.Stats{}, fmt.Errorf("cluster %s filter: %w", cl.Name, err)
	}
	client, err := d.Connect(ctx, cl)
	if err != nil {
		return snapshot.Stats{}, fmt.Errorf("cluster %s: %w", cl.Name, err)
	}
	defer client.Close()

	g := d.Config.GitLab
	return snapshot.Run(ctx, client, d.Repo, snapshot.Options{
		Cluster:    cl.Name,
		BasePath:   g.BasePath,
		Branch:     g.Branch,
		Filter:     filter,
		DryRun:     o.DryRun || g.DryRun,
		AllowEmpty: o.AllowEmpty,
		OnDeleted:  snapshot.Policy(g.OnDeleted),
		BatchSize:  g.CommitBatchSize,
		Now:        d.now,
	}, d.log())
}

// SnapshotAll runs every configured cluster (or just o.Cluster)
// sequentially. Sequential on purpose: all clusters commit to one branch
// of one repo, and concurrent commits to the same branch race on its tip.
func (d *Deps) SnapshotAll(ctx context.Context, o SnapshotOpts) error {
	var errs []error
	for i := range d.Config.Clusters {
		cl := &d.Config.Clusters[i]
		if o.Cluster != "" && cl.Name != o.Cluster {
			continue
		}
		started := d.now()
		st, err := d.SnapshotCluster(ctx, cl, o)
		d.Status.record(cl.Name, started, d.now(), st, err)
		if err != nil {
			d.log().Error("snapshot failed", "cluster", cl.Name, "error", err)
			errs = append(errs, fmt.Errorf("%s: %w", cl.Name, err))
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("%w: %w", ErrSnapshotPartial, errors.Join(errs...))
	}
	return nil
}

// RecoverOpts mirrors the recover subcommand flags.
type RecoverOpts struct {
	Cluster        string
	Ref            string   // git ref; empty = configured branch
	Objects        []string // db.name patterns; empty = whole cluster
	WithDependents bool
	DryRun         bool
	Yes            bool
}

// Exit codes for the recover flow.
const (
	ExitOK      = 0
	ExitFatal   = 1
	ExitPartial = 2
)

// Recover implements plan → confirm → apply and returns the process exit
// code alongside any fatal error.
func (d *Deps) Recover(ctx context.Context, o RecoverOpts) (int, error) {
	if o.Cluster == "" {
		return ExitFatal, errors.New("-cluster is required: recovery never guesses which cluster you mean")
	}
	cl, err := d.Config.FindCluster(o.Cluster)
	if err != nil {
		return ExitFatal, err
	}
	gitRef := o.Ref
	if gitRef == "" {
		gitRef = d.Config.GitLab.Branch
	}
	log := d.log().With("cluster", o.Cluster, "ref", gitRef)

	objs, err := d.loadSnapshot(ctx, o.Cluster, gitRef, log)
	if err != nil {
		return ExitFatal, err
	}
	client, err := d.Connect(ctx, cl)
	if err != nil {
		return ExitFatal, err
	}
	defer client.Close()

	plan, err := d.buildPlan(ctx, client, objs, o, gitRef)
	if err != nil {
		return ExitFatal, err
	}
	fmt.Fprint(d.Out, plan.Render())

	if creates, _, _ := plan.Counts(); creates == 0 {
		fmt.Fprintln(d.Out, "\nNothing to create.")
		return ExitOK, nil
	}
	if o.DryRun {
		return ExitOK, nil
	}
	if ok, err := d.confirm(o); err != nil {
		return ExitFatal, err
	} else if !ok {
		fmt.Fprintln(d.Out, "Aborted.")
		return ExitFatal, nil
	}

	res, err := recovery.NewExecutor(client, log, recovery.ExecOpts{
		Retries:         d.Config.Recovery.Retries,
		Backoff:         d.Config.Recovery.RetryBackoff.Std(),
		ContinueOnError: d.Config.Recovery.DeferInvalidObjects,
	}).Apply(ctx, plan, objs)
	if err != nil {
		return ExitFatal, err
	}
	fmt.Fprintf(d.Out, "\nRecovery finished: %s\n", res.Summary())
	for _, r := range res.Objects {
		if r.Outcome == recovery.OutcomeFailed || r.Outcome == recovery.OutcomeDeferred {
			fmt.Fprintf(d.Out, "  %-8s %s: %s\n", r.Outcome, r.Ref, r.Detail)
		}
	}
	return res.ExitCode(), nil
}

// loadSnapshot reads the cluster's schema files at gitRef.
func (d *Deps) loadSnapshot(ctx context.Context, cluster, gitRef string, log *slog.Logger) ([]*recovery.DDLObject, error) {
	prefix := path.Join(d.Config.GitLab.BasePath, cluster)
	log.Info("reading schema repository", "path", prefix)
	tree, err := d.Repo.ListTree(ctx, gitRef, prefix)
	if err != nil {
		return nil, err
	}
	files, err := d.Repo.GetFiles(ctx, gitRef, tree)
	if err != nil {
		return nil, err
	}
	objs, warns := recovery.LoadObjects(files, d.Config.GitLab.BasePath, cluster)
	for _, w := range warns {
		log.Warn("repo loader", "warning", w)
	}
	if len(objs) == 0 {
		return nil, fmt.Errorf("no objects found under %s at ref %s", prefix, gitRef)
	}
	return objs, nil
}

// buildPlan compares the snapshot with the live cluster.
func (d *Deps) buildPlan(ctx context.Context, client clickhouse.Client, objs []*recovery.DDLObject, o RecoverOpts, gitRef string) (*recovery.Plan, error) {
	topo, err := client.Topology(ctx)
	if err != nil {
		return nil, fmt.Errorf("topology: %w", err)
	}
	existing, err := clickhouse.LoadExistingSet(ctx, client)
	if err != nil {
		return nil, err
	}
	planner := recovery.NewPlanner(objs)
	targets, err := planner.ExpandTargets(o.Objects)
	if err != nil {
		return nil, err
	}
	plan, err := planner.Plan(ctx, o.Cluster, gitRef, targets, existing, recovery.PlanOpts{
		WithDependents: o.WithDependents,
		DeferInvalid:   d.Config.Recovery.DeferInvalidObjects,
		OnCluster:      topo.IsDistributed,
		ReplicaDefaults: recovery.ReplicaDefaults{
			Known: topo.ReplicaDefaultsKnown,
			Path:  topo.DefaultReplicaPath,
			Name:  topo.DefaultReplicaName,
		},
	})
	if err != nil {
		return nil, err
	}
	if topo.IsDistributed {
		// "Exists" is answered by one node. Saying so beats letting the
		// operator assume the whole cluster was inspected.
		plan.Notices = append(plan.Notices, fmt.Sprintf(
			"existence was checked on %s only: an object missing on another replica shows as existing here. "+
				"To restore it there, list that replica first in connection.endpoints and run recovery again",
			client.Endpoint()))
	}
	return plan, nil
}

// confirm asks the operator unless the config or -yes waives it.
func (d *Deps) confirm(o RecoverOpts) (bool, error) {
	if !d.Config.Recovery.ConfirmationRequired() || o.Yes {
		return true, nil
	}
	if d.Confirm == nil {
		return false, errors.New("confirmation required but no interactive terminal; pass -yes to apply")
	}
	return d.Confirm(fmt.Sprintf(
		"Apply this plan to cluster %q? Only CREATE IF NOT EXISTS will be executed. [y/N]: ", o.Cluster))
}

// Validate reports on a loaded (hence already valid) config. With connect
// it also checks every cluster and the schema repository, and it checks
// all of them rather than stopping at the first failure, so one run shows
// everything that has to be fixed.
func (d *Deps) Validate(ctx context.Context, connect bool) (string, error) {
	var b strings.Builder
	g := d.Config.GitLab
	fmt.Fprintf(&b, "configuration valid: %d cluster(s), gitlab project %s\n", len(d.Config.Clusters), g.ProjectPath)
	if !connect {
		return b.String(), nil
	}
	var errs []error
	fail := func(what string, err error) {
		fmt.Fprintf(&b, "%s FAILED: %v\n", what, err)
		errs = append(errs, fmt.Errorf("%s: %w", what, err))
	}
	for i := range d.Config.Clusters {
		cl := &d.Config.Clusters[i]
		what := "cluster " + cl.Name
		topo, err := d.probeCluster(ctx, cl)
		if err != nil {
			fail(what, err)
			continue
		}
		fmt.Fprintf(&b, "%s reachable: distributed=%v shards=%d replicas=%d\n",
			what, topo.IsDistributed, topo.ShardCount, topo.ReplicaCount)
	}
	// Read access only: validate never writes. A wrong URL, token, project
	// or branch all surface here instead of at the first scheduled run.
	what := fmt.Sprintf("gitlab project %s (branch %s)", g.ProjectPath, g.Branch)
	if files, err := d.Repo.ListTree(ctx, g.Branch, g.BasePath); err != nil {
		fail(what, err)
	} else {
		fmt.Fprintf(&b, "%s reachable: %d file(s) under %s/\n", what, len(files), g.BasePath)
	}
	return b.String(), errors.Join(errs...)
}

func (d *Deps) probeCluster(ctx context.Context, cl *config.Cluster) (*clickhouse.Topology, error) {
	c, err := d.Connect(ctx, cl)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	topo, err := c.Topology(ctx)
	if err != nil {
		return nil, fmt.Errorf("topology: %w", err)
	}
	return topo, nil
}

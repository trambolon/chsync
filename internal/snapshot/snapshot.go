// Package snapshot: one cluster in, zero or more commits out.
//
// The flow is deliberately stateless: desired state is recomputed from
// ClickHouse on every run, actual state is read back from the repository,
// and the diff between them becomes the commit. No local caches to corrupt.
//
// Deletion lifecycle (gitlab.on_deleted):
//
//	keep      — the file stays in the live layout; a warning is logged
//	deprecate — the file MOVES to <base>/<cluster>/_deprecated/<db>/<kind>/,
//	            and the manifest records since when; if the object later
//	            reappears in ClickHouse, the archived copy is removed again
//	prune     — the file is deleted (git history still has it)
//
// The _deprecated area is excluded from the desired/actual diff, so once a
// file is archived the next runs are no-ops until something really changes.
package snapshot

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"slices"
	"strings"
	"time"

	"gitlab.example.com/infra/ddl-saver/internal/clickhouse"
	"gitlab.example.com/infra/ddl-saver/internal/gitlab"
	"gitlab.example.com/infra/ddl-saver/internal/model"
	"gitlab.example.com/infra/ddl-saver/internal/state"
)

// Source is the slice of the ClickHouse client the snapshot needs.
type Source interface {
	Topology(ctx context.Context) (*clickhouse.Topology, error)
	ListDatabases(ctx context.Context) ([]clickhouse.ListedDatabase, error)
	ListObjects(ctx context.Context) ([]clickhouse.ListedObject, error)
}

type Policy string

const (
	PolicyKeep      Policy = "keep"
	PolicyDeprecate Policy = "deprecate"
	PolicyPrune     Policy = "prune"
)

// DeprecatedDir is the archive directory under <base>/<cluster>/. The
// leading underscore keeps it outside the <database>/<kind>/ namespace, and
// the recovery loader skips it by prefix.
const DeprecatedDir = model.DeprecatedDir

type Options struct {
	Cluster  string
	BasePath string // repo prefix, e.g. "schemas"
	Branch   string
	Filter   *clickhouse.Filter
	DryRun   bool
	// AllowEmpty disables guardEmpty for this run: the operator has
	// confirmed that the cluster really holds nothing now.
	AllowEmpty bool
	OnDeleted  Policy // default: PolicyDeprecate
	BatchSize  int    // max file actions per commit
	Now        func() time.Time
}

type Stats struct {
	Objects      int  `json:"objects"`      // objects included after filtering
	Creates      int  `json:"creates"`      // .sql files created in the live layout
	Updates      int  `json:"updates"`      // .sql files updated in the live layout
	Deletes      int  `json:"deletes"`      // hard deletions (prune only)
	Deprecated   int  `json:"deprecated"`   // files moved to the _deprecated area this run
	Undeprecated int  `json:"undeprecated"` // archived copies removed because the object came back
	Skipped      int  `json:"skipped"`      // deletions withheld (keep policy, unmanaged files)
	Commits      int  `json:"commits"`
	Unchanged    bool `json:"unchanged"`
}

// ErrEmptyCluster is returned when ClickHouse reports no objects at all
// while the repository still holds some. See guardEmpty.
var ErrEmptyCluster = errors.New("cluster returned no objects")

// layout holds the repository paths of one cluster's snapshot.
type layout struct {
	prefix   string // <base>/<cluster>
	archive  string // <base>/<cluster>/_deprecated
	manifest string // <base>/<cluster>/manifest.json
}

func newLayout(basePath, cluster string) layout {
	prefix := path.Join(basePath, cluster)
	return layout{prefix: prefix, archive: path.Join(prefix, DeprecatedDir), manifest: path.Join(prefix, "manifest.json")}
}

// rel strips the cluster prefix; archived reports whether p is in the archive
// and strips that prefix too, giving the path the file had while live.
func (l layout) rel(p string) string { return strings.TrimPrefix(p, l.prefix+"/") }
func (l layout) archived(p string) (rel string, ok bool) {
	return strings.CutPrefix(p, l.archive+"/")
}

// clusterState is what ClickHouse says should be in the repository.
type clusterState struct {
	files    map[string]string // path -> normalized DDL
	manifest *state.Manifest
	objects  int
}

// repoState is what the repository holds now.
type repoState struct {
	live        map[string]string // files in the live layout
	archive     map[string]string // files under _deprecated/
	prev        *state.Manifest   // nil when absent or unreadable
	hasManifest bool
}

// Run snapshots one cluster into the repository.
func Run(ctx context.Context, src Source, repo gitlab.Repo, o Options, log *slog.Logger) (Stats, error) {
	if log == nil {
		log = slog.Default()
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.OnDeleted == "" {
		o.OnDeleted = PolicyDeprecate
	}
	switch o.OnDeleted {
	case PolicyKeep, PolicyDeprecate, PolicyPrune:
	default:
		return Stats{}, fmt.Errorf("unknown on_deleted policy %q", o.OnDeleted)
	}
	log = log.With("cluster", o.Cluster)
	lay := newLayout(o.BasePath, o.Cluster)

	want, err := readCluster(ctx, src, o, lay)
	if err != nil {
		return Stats{}, err
	}
	st := Stats{Objects: want.objects}
	have, err := readRepo(ctx, repo, o.Branch, lay, log)
	if err != nil {
		return st, err
	}
	if !o.AllowEmpty {
		if err := guardEmpty(want, have, lay); err != nil {
			return st, err
		}
	}

	actions := planActions(want, have, lay, o, &st, log)
	if len(actions) == 0 {
		st.Unchanged = true
		log.Info("snapshot unchanged", "objects", st.Objects)
		return st, nil
	}
	if o.DryRun {
		log.Info("dry run: would commit", "actions", len(actions),
			"creates", st.Creates, "updates", st.Updates, "deletes", st.Deletes,
			"deprecated", st.Deprecated, "undeprecated", st.Undeprecated)
		return st, nil
	}

	message := commitMessage(o.Cluster, st)
	batches := gitlab.Batch(actions, o.BatchSize)
	for i, batch := range batches {
		msg := message
		if len(batches) > 1 {
			msg = fmt.Sprintf("%s [%d/%d]", message, i+1, len(batches))
		}
		if err := repo.Commit(ctx, o.Branch, msg, batch); err != nil {
			return st, fmt.Errorf("commit batch %d/%d: %w", i+1, len(batches), err)
		}
		st.Commits++
	}
	log.Info("snapshot committed",
		"objects", st.Objects, "commits", st.Commits,
		"creates", st.Creates, "updates", st.Updates, "deletes", st.Deletes,
		"deprecated", st.Deprecated, "undeprecated", st.Undeprecated)
	return st, nil
}

// readCluster builds the desired repository content from ClickHouse.
func readCluster(ctx context.Context, src Source, o Options, lay layout) (clusterState, error) {
	want := clusterState{files: map[string]string{}, manifest: state.New(o.Cluster, o.Now())}

	topo, err := src.Topology(ctx)
	if err != nil {
		return want, fmt.Errorf("topology: %w", err)
	}
	databases, err := src.ListDatabases(ctx)
	if err != nil {
		return want, fmt.Errorf("list databases: %w", err)
	}
	objects, err := src.ListObjects(ctx)
	if err != nil {
		return want, fmt.Errorf("list objects: %w", err)
	}

	// A Replicated *database* replicates its own DDL and rejects ON CLUSTER
	// for the objects inside it, so those are stored without the clause.
	replicatedDB := map[string]bool{}
	for _, db := range databases {
		replicatedDB[db.Name] = db.Engine == "Replicated"
	}

	usedDBs := map[string]bool{}
	for _, obj := range objects {
		if clickhouse.IsImplicitObject(obj.Ref.Name) {
			continue // owned by a materialized view; recreated with it
		}
		if o.Filter != nil && !o.Filter.Match(obj.Ref.Database, obj.Ref.Name, obj.Engine) {
			continue
		}
		onCluster := topo.IsDistributed && !replicatedDB[obj.Ref.Database]
		ddl := clickhouse.NormalizeDDL(obj.DDL, onCluster)
		want.files[path.Join(lay.prefix, model.ObjectPath(obj.Ref, obj.Kind))] = ddl
		want.manifest.Add(obj.Ref, obj.Kind, ddl)
		usedDBs[obj.Ref.Database] = true
		want.objects++
	}
	// A database file is written only for databases that contribute at
	// least one snapshotted object, so a filter that excludes a whole
	// database excludes its _database.sql too.
	for _, db := range databases {
		if !usedDBs[db.Name] || db.DDL == "" {
			continue
		}
		ref := model.ObjectRef{Database: db.Name}
		ddl := clickhouse.NormalizeDDL(db.DDL, topo.IsDistributed)
		want.files[path.Join(lay.prefix, model.ObjectPath(ref, model.KindDatabase))] = ddl
		want.manifest.Add(ref, model.KindDatabase, ddl)
	}
	return want, nil
}

// readRepo loads the cluster's directory and splits it into live layout,
// archive and manifest.
func readRepo(ctx context.Context, repo gitlab.Repo, branch string, lay layout, log *slog.Logger) (repoState, error) {
	have := repoState{live: map[string]string{}, archive: map[string]string{}}
	tree, err := repo.ListTree(ctx, branch, lay.prefix)
	if err != nil {
		return have, fmt.Errorf("list repo tree: %w", err)
	}
	files, err := repo.GetFiles(ctx, branch, tree)
	if err != nil {
		return have, fmt.Errorf("read repo files: %w", err)
	}
	for p, content := range files {
		if p == lay.manifest {
			have.hasManifest = true
			if m, err := state.Unmarshal([]byte(content)); err == nil {
				have.prev = m
			} else {
				log.Warn("existing manifest unreadable, will rewrite", "error", err)
			}
		} else if _, ok := lay.archived(p); ok {
			have.archive[p] = content
		} else {
			have.live[p] = content
		}
	}
	return have, nil
}

// guardEmpty refuses to act on an empty answer from ClickHouse when the
// repository is not empty. A user that lost its grants, or an endpoint that
// now points at a blank node, sees zero tables without any error — and
// taking that at face value would archive (or with on_deleted=prune,
// delete) every file in one commit.
func guardEmpty(want clusterState, have repoState, lay layout) error {
	if want.objects > 0 {
		return nil
	}
	managed := 0
	for p := range have.live {
		if _, _, ok := model.ParseObjectPath(lay.rel(p)); ok {
			managed++
		}
	}
	if managed == 0 {
		return nil
	}
	return fmt.Errorf("%w, but the repository holds %d object file(s) under %s; nothing was changed. "+
		"Check that the ClickHouse user can still see the tables (SHOW grants), that the endpoint is the "+
		"right server, and the filters. If the cluster really is empty now, run "+
		"`ddl-saver snapshot -cluster <name> -allow-empty` once",
		ErrEmptyCluster, managed, lay.prefix)
}

// planActions turns the difference between cluster and repository into
// commit actions, applying the deletion policy, and fills st.
func planActions(want clusterState, have repoState, lay layout, o Options, st *Stats, log *slog.Logger) []gitlab.Action {
	writes, deletes := gitlab.SplitDeletes(gitlab.Diff(want.files, have.live))
	actions := slices.Clone(writes)
	for _, a := range writes {
		if a.Op == gitlab.OpCreate {
			st.Creates++
		} else {
			st.Updates++
		}
	}

	// archive tracks the archive's content after this run, so deprecation
	// records can be rebuilt from it.
	archive := map[string]bool{}
	for p := range have.archive {
		archive[p] = true
	}

	for _, d := range deletes {
		rel := lay.rel(d.Path)
		if _, _, ok := model.ParseObjectPath(rel); !ok {
			log.Warn("unmanaged file not present in cluster state; left untouched", "path", d.Path)
			st.Skipped++
			continue
		}
		switch o.OnDeleted {
		case PolicyKeep:
			log.Warn("object missing from cluster; file kept (on_deleted=keep)", "path", d.Path)
			st.Skipped++
		case PolicyPrune:
			actions = append(actions, d)
			st.Deletes++
		case PolicyDeprecate:
			// A move: write the archive copy, delete the live one.
			to := path.Join(lay.archive, rel)
			content := have.live[d.Path]
			if old, ok := have.archive[to]; !ok {
				actions = append(actions, gitlab.Action{Op: gitlab.OpCreate, Path: to, Content: content})
			} else if old != content {
				actions = append(actions, gitlab.Action{Op: gitlab.OpUpdate, Path: to, Content: content})
			}
			actions = append(actions, d)
			archive[to] = true
			st.Deprecated++
			log.Info("object missing from cluster; file deprecated", "from", d.Path, "to", to)
		}
	}

	if o.OnDeleted == PolicyDeprecate {
		// An archived object that is alive again supersedes its archive copy.
		for p := range have.archive {
			rel, _ := lay.archived(p)
			ref, kind, ok := model.ParseObjectPath(rel)
			if !ok {
				continue
			}
			if _, alive := want.files[path.Join(lay.prefix, model.ObjectPath(ref, kind))]; alive {
				actions = append(actions, gitlab.Action{Op: gitlab.OpDelete, Path: p})
				delete(archive, p)
				st.Undeprecated++
				log.Info("object is back in the cluster; deprecated copy removed", "path", p)
			}
		}
		// Deprecation records mirror the archive. `since` is carried over
		// from the previous manifest, so an unchanged archive produces an
		// unchanged manifest and therefore no commit.
		now := o.Now().UTC().Truncate(time.Second)
		for p := range archive {
			rel, _ := lay.archived(p)
			ref, kind, ok := model.ParseObjectPath(rel)
			if !ok {
				continue
			}
			since := now
			if have.prev != nil {
				if rec, ok := have.prev.Deprecated[ref.String()]; ok {
					since = rec.Since
				}
			}
			want.manifest.Deprecated[ref.String()] = state.DeprecatedState{Kind: kind, Since: since}
		}
	}

	// The manifest is committed only when its content (objects or
	// deprecation records, not the timestamp) changed.
	if !state.SameObjects(have.prev, want.manifest) {
		if b, err := want.manifest.Marshal(); err != nil {
			log.Error("manifest not written", "error", err) // plain structs: cannot happen
		} else {
			op := gitlab.OpCreate
			if have.hasManifest {
				op = gitlab.OpUpdate
			}
			actions = append(actions, gitlab.Action{Op: op, Path: lay.manifest, Content: string(b)})
		}
	}

	// Writes before deletes, then by path. Batches are separate commits, so
	// a failure between two of them must never leave a deprecated file
	// deleted from the live layout but not yet written to the archive.
	slices.SortFunc(actions, func(a, b gitlab.Action) int {
		if da, db := a.Op == gitlab.OpDelete, b.Op == gitlab.OpDelete; da != db {
			if da {
				return 1
			}
			return -1
		}
		return cmp.Compare(a.Path, b.Path)
	})
	return actions
}

// commitMessage describes what THIS commit does to the live .sql layout,
// with the archive movements called out separately.
func commitMessage(cluster string, st Stats) string {
	base := fmt.Sprintf("ddlSaver: snapshot %s (+%d ~%d -%d", cluster, st.Creates, st.Updates, st.Deletes)
	if st.Deprecated > 0 {
		base += fmt.Sprintf(", deprecated %d", st.Deprecated)
	}
	if st.Undeprecated > 0 {
		base += fmt.Sprintf(", undeprecated %d", st.Undeprecated)
	}
	base += ")"
	if st.Creates+st.Updates+st.Deletes+st.Deprecated+st.Undeprecated == 0 {
		return fmt.Sprintf("ddlSaver: snapshot %s (manifest update)", cluster)
	}
	return base
}

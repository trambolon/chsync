// Package clickhouse: access to ClickHouse for both flows.
//
// The concrete driver (chconn) is isolated in chconn_client.go; everything
// else in this package is pure logic, and consumers depend on the Client
// interface so tests can use fakes.
package clickhouse

import (
	"context"
	"regexp"
	"strings"

	"gitlab.example.com/infra/ddl-saver/internal/model"
)

// ListedObject is one row of system.tables that passed the filter.
type ListedObject struct {
	Ref    model.ObjectRef
	Kind   model.ObjectKind
	Engine string
	DDL    string // create_table_query as stored by ClickHouse
}

type Topology struct {
	ClusterMacro  string // value of the 'cluster' macro, "" on single nodes
	ShardCount    int
	ReplicaCount  int
	IsDistributed bool

	// DefaultReplicaPath / DefaultReplicaName are the server settings that
	// make argument-less `ENGINE = ReplicatedMergeTree` legal. They are
	// read best-effort: system.server_settings only exists on ClickHouse
	// 22.10+, and the user may lack access to it.
	DefaultReplicaPath string
	DefaultReplicaName string
	// ReplicaDefaultsKnown distinguishes "the server reports no default"
	// from "we could not find out", which change the advice given.
	ReplicaDefaultsKnown bool
}

// UUIDBasedReplicaPath reports whether the server's default replica path is
// keyed by table UUID (the stock '/clickhouse/tables/{uuid}/{shard}'). Such
// a path is regenerated for every new table, so a recreated table never
// collides with a stale registration and never joins another cluster's
// replica set.
func (t *Topology) UUIDBasedReplicaPath() bool {
	return t.ReplicaDefaultsKnown && strings.Contains(t.DefaultReplicaPath, "{uuid}")
}

// ListedDatabase is one user database with its own DDL.
type ListedDatabase struct {
	Name   string
	Engine string
	DDL    string // SHOW CREATE DATABASE output
}

type Client interface {
	Ping(ctx context.Context) error
	Topology(ctx context.Context) (*Topology, error)
	ListDatabases(ctx context.Context) ([]ListedDatabase, error)
	ListObjects(ctx context.Context) ([]ListedObject, error)
	// ObjectExists answers for objects and, when ref.IsDatabase(), for
	// databases. It satisfies recovery.ExistsChecker and recovery.ExecClient
	// directly.
	ObjectExists(ctx context.Context, ref model.ObjectRef) (bool, error)
	// ListRefs returns every database and every (database, name) pair
	// visible to the connection; used to pre-warm existence checks so
	// recovery planning is O(1) per dependency.
	ListRefs(ctx context.Context) ([]model.ObjectRef, error)
	Exec(ctx context.Context, sql string) error
	// Endpoint is the "host:port" this client is connected to.
	Endpoint() string
	Close() error
}

// IsImplicitObject reports whether name is the hidden storage table of a
// materialized view created without TO: `.inner.<mv>` in Ordinary databases,
// `.inner_id.<uuid>` in Atomic ones. ClickHouse creates it together with
// the view, so snapshotting it separately would, on recovery, leave an
// orphan table next to the one the recreated view makes for itself.
func IsImplicitObject(name string) bool {
	return strings.HasPrefix(name, ".inner.") || strings.HasPrefix(name, ".inner_id.")
}

// Filter decides which objects a snapshot includes. Compiled once from config.
type Filter struct {
	skipDatabases  map[string]bool
	skipTables     []*regexp.Regexp
	includeEngines []string // empty = all
}

func NewFilter(skipDatabases, skipTablesRegex, includeEngines []string) (*Filter, error) {
	f := &Filter{skipDatabases: map[string]bool{}, includeEngines: includeEngines}
	for _, db := range skipDatabases {
		f.skipDatabases[strings.ToLower(db)] = true
	}
	for _, expr := range skipTablesRegex {
		re, err := regexp.Compile(expr)
		if err != nil {
			return nil, err
		}
		f.skipTables = append(f.skipTables, re)
	}
	return f, nil
}

// Match reports whether an object should be snapshotted. Engine matching is
// by exact name or prefix so that "MergeTree" also admits
// "ReplicatedMergeTree" style families only when listed explicitly — i.e.
// config entry is the prefix, engine is the candidate.
func (f *Filter) Match(database, name, engine string) bool {
	if f.skipDatabases[strings.ToLower(database)] {
		return false
	}
	for _, re := range f.skipTables {
		if re.MatchString(name) {
			return false
		}
	}
	if len(f.includeEngines) == 0 {
		return true
	}
	for _, e := range f.includeEngines {
		if engine == e || strings.HasSuffix(engine, e) {
			// suffix match admits Replicated* variants of a listed engine:
			// include "MergeTree" -> ReplicatedMergeTree also matches.
			return true
		}
	}
	return false
}

// ExistingSet is a pre-loaded snapshot of every object currently visible in
// the cluster, satisfying recovery.ExistsChecker without a query per object.
type ExistingSet struct {
	set map[model.ObjectRef]bool
}

func LoadExistingSet(ctx context.Context, c Client) (*ExistingSet, error) {
	refs, err := c.ListRefs(ctx)
	if err != nil {
		return nil, err
	}
	s := &ExistingSet{set: make(map[model.ObjectRef]bool, len(refs))}
	for _, r := range refs {
		s.set[r] = true
	}
	return s, nil
}

func (s *ExistingSet) ObjectExists(_ context.Context, ref model.ObjectRef) (bool, error) {
	return s.set[ref], nil
}

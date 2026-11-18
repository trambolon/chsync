package clickhouse

import (
	"strings"
	"testing"
)

func TestEnsureOnCluster(t *testing.T) {
	tests := []struct {
		name        string
		ddl         string
		want        string
		wantChanged bool
	}{
		{
			name:        "plain table",
			ddl:         "CREATE TABLE db.t (id UInt64) ENGINE = MergeTree()",
			want:        "CREATE TABLE db.t ON CLUSTER '{cluster}' (id UInt64) ENGINE = MergeTree()",
			wantChanged: true,
		},
		{
			name:        "already has ON CLUSTER",
			ddl:         "CREATE TABLE db.t ON CLUSTER 'old' (id UInt64)",
			want:        "CREATE TABLE db.t ON CLUSTER 'old' (id UInt64)",
			wantChanged: false,
		},
		{
			name:        "if not exists",
			ddl:         "CREATE TABLE IF NOT EXISTS db.t (id UInt64)",
			want:        "CREATE TABLE IF NOT EXISTS db.t ON CLUSTER '{cluster}' (id UInt64)",
			wantChanged: true,
		},
		{
			name:        "materialized view multiline",
			ddl:         "CREATE MATERIALIZED VIEW db.mv\nTO db.agg\nAS SELECT 1",
			want:        "CREATE MATERIALIZED VIEW db.mv ON CLUSTER '{cluster}'\nTO db.agg\nAS SELECT 1",
			wantChanged: true,
		},
		{
			name:        "backticked show create",
			ddl:         "CREATE TABLE `db`.`weird name` (`id` UInt64) ENGINE = MergeTree",
			want:        "CREATE TABLE `db`.`weird name` ON CLUSTER '{cluster}' (`id` UInt64) ENGINE = MergeTree",
			wantChanged: true,
		},
		{
			name:        "database",
			ddl:         "CREATE DATABASE events ENGINE = Atomic",
			want:        "CREATE DATABASE events ON CLUSTER '{cluster}' ENGINE = Atomic",
			wantChanged: true,
		},
		{
			name:        "dictionary",
			ddl:         "CREATE DICTIONARY db.d (id UInt64) PRIMARY KEY id",
			want:        "CREATE DICTIONARY db.d ON CLUSTER '{cluster}' (id UInt64) PRIMARY KEY id",
			wantChanged: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, changed := EnsureOnCluster(tt.ddl)
			if got != tt.want {
				t.Errorf("got:\n%s\nwant:\n%s", got, tt.want)
			}
			if changed != tt.wantChanged {
				t.Errorf("changed = %v, want %v", changed, tt.wantChanged)
			}
		})
	}
}

func TestNormalizeDDL(t *testing.T) {
	out := NormalizeDDL("CREATE TABLE db.t (id UInt64) ENGINE = MergeTree()\n\n", true)
	if !strings.HasSuffix(out, "\n") || strings.HasSuffix(out, "\n\n") {
		t.Errorf("want exactly one trailing newline, got %q", out)
	}
	if !strings.Contains(out, "ON CLUSTER '{cluster}'") {
		t.Error("distributed DDL should get ON CLUSTER")
	}
	out = NormalizeDDL("CREATE TABLE db.t (id UInt64)", false)
	if strings.Contains(out, "ON CLUSTER") {
		t.Error("single-node DDL must not get ON CLUSTER")
	}
}

func TestFilter_Match(t *testing.T) {
	f, err := NewFilter(
		[]string{"system", "INFORMATION_SCHEMA"},
		[]string{`^\.`},
		[]string{"MergeTree", "MaterializedView", "Dictionary"},
	)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		db, name, engine string
		want             bool
	}{
		{"events", "clicks", "MergeTree", true},
		{"events", "clicks", "ReplicatedMergeTree", true}, // suffix match
		{"events", "clicks", "ReplacingMergeTree", true},
		{"events", "mv", "MaterializedView", true},
		{"events", "d", "Dictionary", true},
		{"events", "log", "Log", false},                 // engine not listed
		{"system", "tables", "MergeTree", false},        // skipped db
		{"information_schema", "x", "MergeTree", false}, // case-insensitive db skip
		{"events", ".inner.mv1", "MergeTree", false},    // hidden table regex
	}
	for _, tt := range tests {
		if got := f.Match(tt.db, tt.name, tt.engine); got != tt.want {
			t.Errorf("Match(%s.%s, %s) = %v, want %v", tt.db, tt.name, tt.engine, got, tt.want)
		}
	}
	all, _ := NewFilter(nil, nil, nil)
	if !all.Match("any", "thing", "AnyEngine") {
		t.Error("empty filter must match everything")
	}
}

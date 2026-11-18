package recovery

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

const fixtureDir = "../../test/fixtures/recovery"

func loadFixture(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(fixtureDir, name))
	if err != nil {
		t.Fatalf("fixture %s: %v", name, err)
	}
	return string(b)
}

func refs(cands []DepCandidate) []string {
	if len(cands) == 0 {
		return nil
	}
	out := make([]string, 0, len(cands))
	for _, c := range cands {
		out = append(out, c.Ref.String())
	}
	sort.Strings(out)
	return out
}

func TestExtractDeps_Fixtures(t *testing.T) {
	tests := []struct {
		fixture      string
		defaultDB    string
		wantKind     ObjectKind
		wantDeps     []string // sorted "db.name" of ALL candidates
		wantExternal []string // substring match, one per entry
	}{
		{
			fixture:  "mv_with_to.sql",
			wantKind: KindMaterializedView,
			wantDeps: []string{"events.clicks_agg", "events.clicks_raw"},
		},
		{
			fixture:  "mv_inner_table.sql",
			wantKind: KindMaterializedView,
			wantDeps: []string{"events.raw_sessions"}, // no TO target
		},
		{
			fixture:  "mv_cascade.sql",
			wantKind: KindMaterializedView,
			wantDeps: []string{"events.clicks_agg", "events.daily_rollup"},
		},
		{
			fixture:  "view_join.sql",
			wantKind: KindView,
			// ARRAY JOIN tags must NOT appear; both FROM and JOIN refs must.
			wantDeps: []string{"dicts_db.users", "events.clicks_agg"},
		},
		{
			fixture:  "dict_clickhouse_source.sql",
			wantKind: KindDictionary,
			wantDeps: []string{"refdata.geo"}, // survives PASSWORD 'p(ss)word'
		},
		{
			fixture:      "dict_http_source.sql",
			wantKind:     KindDictionary,
			wantDeps:     nil,
			wantExternal: []string{"HTTP"},
		},
		{
			fixture:  "table_distributed.sql",
			wantKind: KindTable,
			wantDeps: []string{"events.clicks_local"},
		},
		{
			fixture:  "table_dictget_default.sql",
			wantKind: KindTable,
			// dep from dictGet; nothing from the comment or the string literal
			wantDeps: []string{"events.geo_dict"},
		},
		{
			fixture:  "table_plain.sql",
			wantKind: KindTable,
			wantDeps: nil,
		},
		{
			fixture:      "mv_from_remote.sql",
			wantKind:     KindMaterializedView,
			wantDeps:     []string{"events.imported"}, // TO target only
			wantExternal: []string{"remote"},
		},
		{
			fixture:  "view_unqualified.sql",
			wantKind: KindView,
			wantDeps: []string{"events.clicks_raw"}, // qualified by defaultDB
		},
	}

	for _, tt := range tests {
		t.Run(tt.fixture, func(t *testing.T) {
			db := tt.defaultDB
			if db == "" {
				db = "events"
			}
			kind, cands, external, _ := ExtractDeps(db, loadFixture(t, tt.fixture))

			if kind != tt.wantKind {
				t.Errorf("kind = %s, want %s", kind, tt.wantKind)
			}
			if got := refs(cands); !reflect.DeepEqual(got, tt.wantDeps) {
				t.Errorf("deps = %v, want %v", got, tt.wantDeps)
			}
			if len(external) != len(tt.wantExternal) {
				t.Fatalf("external = %v, want %d entries matching %v",
					external, len(tt.wantExternal), tt.wantExternal)
			}
			for i, want := range tt.wantExternal {
				if !strings.Contains(external[i], want) {
					t.Errorf("external[%d] = %q, want substring %q", i, external[i], want)
				}
			}
		})
	}
}

func TestExtractDeps_QualifiedFlag(t *testing.T) {
	_, cands, _, _ := ExtractDeps("events",
		"CREATE VIEW events.v AS SELECT * FROM clicks_raw JOIN refdata.geo g ON g.id = clicks_raw.id")

	byRef := map[string]DepCandidate{}
	for _, c := range cands {
		byRef[c.Ref.String()] = c
	}
	if c := byRef["events.clicks_raw"]; c.Qualified {
		t.Error("clicks_raw should be unqualified")
	}
	if c := byRef["refdata.geo"]; !c.Qualified {
		t.Error("refdata.geo should be qualified")
	}
}

func TestExtractDeps_SystemDBSkipped(t *testing.T) {
	_, cands, _, _ := ExtractDeps("events",
		"CREATE VIEW events.v AS SELECT * FROM system.tables JOIN events.real r ON 1=1")
	if got := refs(cands); !reflect.DeepEqual(got, []string{"events.real"}) {
		t.Errorf("deps = %v, want only events.real", got)
	}
}

func TestExtractDeps_SelfAndDedup(t *testing.T) {
	kind, cands, _, _ := ExtractDeps("events",
		`CREATE MATERIALIZED VIEW events.mv TO events.t AS
		 SELECT * FROM events.src UNION ALL SELECT * FROM events.src`)
	if kind != KindMaterializedView {
		t.Fatalf("kind = %s", kind)
	}
	if got := refs(cands); !reflect.DeepEqual(got, []string{"events.src", "events.t"}) {
		t.Errorf("deps = %v, want deduped [events.src events.t]", got)
	}
}

// --- loader ---

func repoFiles(t *testing.T) map[string]string {
	t.Helper()
	layout := map[string]string{
		"schemas/prod/events/tables/clicks_raw.sql":                  "table_plain.sql",
		"schemas/prod/events/tables/clicks_dist.sql":                 "table_distributed.sql",
		"schemas/prod/events/tables/orders.sql":                      "table_dictget_default.sql",
		"schemas/prod/events/materialized_views/clicks_mv.sql":       "mv_with_to.sql",
		"schemas/prod/events/materialized_views/daily_rollup_mv.sql": "mv_cascade.sql",
		"schemas/prod/events/views/recent.sql":                       "view_unqualified.sql",
		"schemas/prod/events/dictionaries/geo_dict.sql":              "dict_clickhouse_source.sql",
		// noise that must be ignored:
		"schemas/prod/README.md":                               "table_plain.sql",
		"schemas/prod/_deprecated/events/tables/old_table.sql": "table_plain.sql",
		"schemas/staging/events/tables/other.sql":              "table_plain.sql",
		"schemas/prod/events/unknown_dir/x.sql":                "table_plain.sql",
	}
	files := map[string]string{}
	for p, f := range layout {
		files[p] = loadFixture(t, f)
	}
	return files
}

func TestLoadObjects(t *testing.T) {
	objs, warns := LoadObjects(repoFiles(t), "schemas", "prod")

	byRef := map[string]*DDLObject{}
	for _, o := range objs {
		byRef[o.Ref.String()] = o
	}
	if len(objs) != 7 {
		t.Fatalf("loaded %d objects, want 7 (got %v)", len(objs), keysOf(byRef))
	}

	// identity comes from path, deps from content
	mv := byRef["events.clicks_mv"]
	if mv == nil || mv.Kind != KindMaterializedView {
		t.Fatalf("clicks_mv missing or wrong kind: %+v", mv)
	}
	wantDeps := []string{"events.clicks_agg", "events.clicks_raw"}
	if got := refStrings(mv.DependsOn); !reflect.DeepEqual(got, wantDeps) {
		t.Errorf("clicks_mv deps = %v, want %v", got, wantDeps)
	}

	// unqualified ref resolved against repo index
	view := byRef["events.recent"]
	if got := refStrings(view.DependsOn); !reflect.DeepEqual(got, []string{"events.clicks_raw"}) {
		t.Errorf("recent deps = %v", got)
	}

	// qualified ref kept even though refdata.geo is NOT in the repo
	dict := byRef["events.geo_dict"]
	if got := refStrings(dict.DependsOn); !reflect.DeepEqual(got, []string{"refdata.geo"}) {
		t.Errorf("geo_dict deps = %v, want [refdata.geo] kept for planner to check live cluster", got)
	}

	// exactly one warning about the unknown kind directory
	found := false
	for _, w := range warns {
		if strings.Contains(w, "unknown_dir") {
			found = true
		}
		if strings.Contains(w, "_deprecated") {
			t.Errorf("the deprecated area must be skipped silently, got warning %q", w)
		}
	}
	if !found {
		t.Errorf("expected a warning about unknown_dir, got %v", warns)
	}
	if _, ok := byRef["events.old_table"]; ok {
		t.Error("deprecated objects must not be loadable for recovery at this ref")
	}
}

func TestReverseIndex(t *testing.T) {
	objs, _ := LoadObjects(repoFiles(t), "schemas", "prod")
	rev := ReverseIndex(objs)

	// clicks_raw is read by clicks_mv (FROM) and by recent (resolved unqualified)
	got := refStrings(rev[ObjectRef{Database: "events", Name: "clicks_raw"}])
	want := []string{"events.clicks_mv", "events.recent"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("dependents of clicks_raw = %v, want %v", got, want)
	}

	// clicks_agg (not even present in repo) is depended on by both MVs
	got = refStrings(rev[ObjectRef{Database: "events", Name: "clicks_agg"}])
	want = []string{"events.clicks_mv", "events.daily_rollup_mv"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("dependents of clicks_agg = %v, want %v", got, want)
	}
}

func refStrings(rs []ObjectRef) []string {
	if len(rs) == 0 {
		return nil
	}
	out := make([]string, 0, len(rs))
	for _, r := range rs {
		out = append(out, r.String())
	}
	sort.Strings(out)
	return out
}

func keysOf(m map[string]*DDLObject) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// SHOW CREATE output style: backticks everywhere, multiline formatting.
func TestExtractDeps_BacktickedShowCreate(t *testing.T) {
	sql := "CREATE MATERIALIZED VIEW `events`.`clicks_mv` ON CLUSTER '{cluster}'\n" +
		"TO `events`.`clicks_agg`\n" +
		"(\n    `d` Date,\n    `c` UInt64\n)\n" +
		"AS SELECT\n    toDate(ts) AS d,\n    count() AS c\n" +
		"FROM `events`.`clicks_raw`\nGROUP BY d"
	kind, cands, _, _ := ExtractDeps("events", sql)
	if kind != KindMaterializedView {
		t.Fatalf("kind = %s", kind)
	}
	if got := refs(cands); !reflect.DeepEqual(got, []string{"events.clicks_agg", "events.clicks_raw"}) {
		t.Errorf("deps = %v", got)
	}
}

// An MV reading FROM another MV directly (not its target table).
func TestExtractDeps_MVReadsMV(t *testing.T) {
	_, cands, _, _ := ExtractDeps("events",
		"CREATE MATERIALIZED VIEW events.second_mv TO events.out AS SELECT * FROM events.first_mv")
	if got := refs(cands); !reflect.DeepEqual(got, []string{"events.first_mv", "events.out"}) {
		t.Errorf("deps = %v", got)
	}
}

func TestLoadObjects_EncodedNames(t *testing.T) {
	files := map[string]string{
		"schemas/prod/events/tables/2024%2Fq1.sql":   "CREATE TABLE events.`2024/q1` (id UInt64) ENGINE = Memory\n",
		"schemas/prod/a%2Fb/_database.sql":           "CREATE DATABASE `a/b`\n",
		"schemas/prod/a%2Fb/tables/..%2Fx.sql":       "CREATE TABLE `a/b`.`../x` (id UInt64) ENGINE = Memory\n",
		"schemas/prod/%5Fdeprecated/tables/live.sql": "CREATE TABLE _deprecated.live (id UInt64) ENGINE = Memory\n",
	}
	objs, warns := LoadObjects(files, "schemas", "prod")
	if len(warns) != 0 {
		t.Fatalf("warnings: %v", warns)
	}
	want := map[ObjectRef]ObjectKind{
		{Database: "events", Name: "2024/q1"}:   KindTable,
		{Database: "a/b"}:                       KindDatabase,
		{Database: "a/b", Name: "../x"}:         KindTable,
		{Database: "_deprecated", Name: "live"}: KindTable,
	}
	if len(objs) != len(want) {
		t.Fatalf("got %d objects, want %d", len(objs), len(want))
	}
	for _, o := range objs {
		if want[o.Ref] != o.Kind {
			t.Errorf("unexpected object %v (%s)", o.Ref, o.Kind)
		}
		// path identity and statement identity must still agree
		if _, err := GuardDDL(o.Ref, o.Kind, o.SQL); err != nil {
			t.Errorf("guard: %v", err)
		}
	}
}

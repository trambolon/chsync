package recovery

import (
	"context"
	"regexp"
	"strings"
	"testing"
)

func TestGuardDDL_AcceptsAndInjectsIfNotExists(t *testing.T) {
	ref := ObjectRef{Database: "events", Name: "clicks"}
	got, err := GuardDDL(ref, KindTable, "CREATE TABLE events.clicks (id UInt64) ENGINE = MergeTree()")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "IF NOT EXISTS events.clicks") {
		t.Errorf("IF NOT EXISTS not injected: %s", got)
	}
}

func TestGuardDDL_PreservesExistingIfNotExists(t *testing.T) {
	ref := ObjectRef{Database: "events", Name: "clicks"}
	in := "CREATE TABLE IF NOT EXISTS events.clicks (id UInt64)"
	got, err := GuardDDL(ref, KindTable, in)
	if err != nil {
		t.Fatal(err)
	}
	if got != in {
		t.Errorf("statement should be unchanged, got %s", got)
	}
}

func TestGuardDDL_InjectsAfterOnClusterForms(t *testing.T) {
	ref := ObjectRef{Database: "events", Name: "clicks_mv"}
	in := "CREATE MATERIALIZED VIEW `events`.`clicks_mv` ON CLUSTER '{cluster}'\nTO events.agg\nAS SELECT 1"
	got, err := GuardDDL(ref, KindMaterializedView, in)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "IF NOT EXISTS `events`.`clicks_mv`") {
		t.Errorf("bad injection: %s", got)
	}
	if !strings.Contains(got, "ON CLUSTER '{cluster}'") {
		t.Errorf("ON CLUSTER clause damaged: %s", got)
	}
}

func TestGuardDDL_RejectsDestructiveStatements(t *testing.T) {
	ref := ObjectRef{Database: "events", Name: "clicks"}
	tests := []struct {
		name, sql, wantReason string
	}{
		{"drop", "DROP TABLE events.clicks", "only executes CREATE/ATTACH"},
		{"truncate", "TRUNCATE TABLE events.clicks", "only executes CREATE/ATTACH"},
		{"alter", "ALTER TABLE events.clicks DELETE WHERE 1", "only executes CREATE/ATTACH"},
		{"rename", "RENAME TABLE events.clicks TO events.gone", "only executes CREATE/ATTACH"},
		{"insert", "INSERT INTO events.clicks VALUES (1)", "only executes CREATE/ATTACH"},
		{"detach", "DETACH TABLE events.clicks", "only executes CREATE/ATTACH"},
		{"system", "SYSTEM SHUTDOWN", "only executes CREATE/ATTACH"},
		{"or replace", "CREATE OR REPLACE TABLE events.clicks (id UInt64)", "never replaces"},
		{"temporary", "CREATE TEMPORARY TABLE events.clicks (id UInt64)", "TEMPORARY"},
		{"empty", "   \n  ", "empty"},
		{"not a create", "SELECT 1", "only executes CREATE/ATTACH"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := GuardDDL(ref, KindTable, tt.sql)
			if err == nil {
				t.Fatalf("statement should have been rejected: %s", tt.sql)
			}
			if !strings.Contains(err.Error(), tt.wantReason) {
				t.Errorf("reason = %q, want substring %q", err.Error(), tt.wantReason)
			}
		})
	}
}

func TestGuardDDL_RejectsChainedStatements(t *testing.T) {
	ref := ObjectRef{Database: "events", Name: "clicks"}
	_, err := GuardDDL(ref, KindTable,
		"CREATE TABLE events.clicks (id UInt64) ENGINE = MergeTree(); DROP TABLE events.orders")
	if err == nil || !strings.Contains(err.Error(), "multiple statements") {
		t.Fatalf("chained payload must be rejected, got %v", err)
	}
}

func TestGuardDDL_AllowsSemicolonsInsideLiteralsAndComments(t *testing.T) {
	ref := ObjectRef{Database: "events", Name: "clicks"}
	sql := "CREATE TABLE events.clicks (\n" +
		"  id UInt64,\n" +
		"  note String DEFAULT 'a;b;c',  -- trailing ; comment\n" +
		"  /* block ; comment */ ts DateTime\n" +
		") ENGINE = MergeTree() ORDER BY id;\n"
	if _, err := GuardDDL(ref, KindTable, sql); err != nil {
		t.Fatalf("semicolons inside literals/comments must be tolerated: %v", err)
	}
}

func TestGuardDDL_RejectsIdentityMismatch(t *testing.T) {
	ref := ObjectRef{Database: "events", Name: "orders"}
	// File claims to be events/tables/orders.sql but creates something else.
	_, err := GuardDDL(ref, KindTable, "CREATE TABLE events.customers (id UInt64)")
	if err == nil || !strings.Contains(err.Error(), "path and content must agree") {
		t.Fatalf("identity mismatch must be rejected, got %v", err)
	}

	// Cross-database smuggling.
	_, err = GuardDDL(ref, KindTable, "CREATE TABLE other_db.orders (id UInt64)")
	if err == nil || !strings.Contains(err.Error(), "path and content must agree") {
		t.Fatalf("cross-database mismatch must be rejected, got %v", err)
	}
}

func TestGuardDDL_UnqualifiedNameUsesFileDatabase(t *testing.T) {
	ref := ObjectRef{Database: "events", Name: "clicks"}
	got, err := GuardDDL(ref, KindTable, "CREATE TABLE clicks (id UInt64)")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "IF NOT EXISTS clicks") {
		t.Errorf("injection failed: %s", got)
	}
}

func TestGuardDDL_RejectsKindMismatch(t *testing.T) {
	ref := ObjectRef{Database: "events", Name: "thing"}
	// A file in materialized_views/ that actually creates a plain table.
	_, err := GuardDDL(ref, KindMaterializedView, "CREATE TABLE events.thing (id UInt64)")
	if err == nil || !strings.Contains(err.Error(), "directory") {
		t.Fatalf("kind mismatch must be rejected, got %v", err)
	}
}

func TestGuardDDL_BacktickedIdentityMatch(t *testing.T) {
	ref := ObjectRef{Database: "events", Name: "weird name"}
	if _, err := GuardDDL(ref, KindTable, "CREATE TABLE `events`.`weird name` (`id` UInt64)"); err != nil {
		t.Fatalf("backticked identifiers should match: %v", err)
	}
}

// --- end-to-end: tampered repository ------------------------------------

func TestPlan_RefusesWhenRepoContainsUnsafeDDL(t *testing.T) {
	objs := testRepo()
	// Someone merged an MR editing a snapshotted file.
	for _, o := range objs {
		if o.Ref.Name == "clicks_raw" {
			o.SQL = "DROP TABLE events.clicks_raw"
		}
	}
	p := NewPlanner(objs)
	refs, err := p.ExpandTargets([]string{"events.clicks_mv"}) // pulls clicks_raw in as a dep
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.Plan(context.Background(), "prod", "abc123", refs, fakeLive{}, PlanOpts{DeferInvalid: true})
	if err == nil {
		t.Fatal("planning must refuse when a repo file is not a safe CREATE")
	}
	for _, want := range []string{"refusing to build a recovery plan", "events.clicks_raw", "CREATE/ATTACH", "abc123"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should mention %q:\n%v", want, err)
		}
	}
}

func TestApply_BlocksUnsafeDDLEvenIfPlanWasBuiltElsewhere(t *testing.T) {
	objs := testRepo()
	plan := planFor(t, objs, []string{"events.clicks_raw"}, fakeLive{}, PlanOpts{DeferInvalid: true})
	// Content is swapped AFTER planning (simulates a stale plan, a bug, or
	// a caller that skipped the planner's gate).
	for _, o := range objs {
		if o.Ref.Name == "clicks_raw" {
			o.SQL = "DROP TABLE events.clicks_raw"
		}
	}
	fe := &fakeExec{existing: map[string]bool{}}
	res, err := NewExecutor(fe, nil, ExecOpts{ContinueOnError: true}).Apply(context.Background(), plan, objs)
	if err != nil {
		t.Fatal(err)
	}
	if len(fe.executed) != 0 {
		t.Fatalf("nothing may reach ClickHouse, but executed: %v", fe.executed)
	}
	if res.Failed != 1 || res.ExitCode() == 0 {
		t.Errorf("unsafe DDL must fail loudly: %s", res.Summary())
	}
}

func TestApply_SendsIfNotExistsToClickHouse(t *testing.T) {
	objs := testRepo()
	plan := planFor(t, objs, []string{"events.clicks_raw"}, fakeLive{}, PlanOpts{DeferInvalid: true})
	fe := &fakeExec{existing: map[string]bool{}}
	if _, err := NewExecutor(fe, nil, ExecOpts{}).Apply(context.Background(), plan, objs); err != nil {
		t.Fatal(err)
	}
	if len(fe.executed) != 1 || !strings.Contains(fe.executed[0], "IF NOT EXISTS") {
		t.Errorf("executed SQL must carry IF NOT EXISTS: %v", fe.executed)
	}
}

// Regression: the first shipped guard computed offsets on comment-stripped
// text and spliced IF NOT EXISTS into the wrong place when a file began
// with a comment. maskSQL is length-preserving precisely to prevent this.
func TestGuardDDL_InjectsCorrectlyAfterLeadingComments(t *testing.T) {
	t.Parallel()
	ref := ObjectRef{Database: "events", Name: "e"}
	cases := []string{
		"-- snapshot header\nCREATE TABLE events.e (id UInt64)",
		"/* multi\n line */ CREATE TABLE events.e (id UInt64)",
		"-- a\n-- b\n\nCREATE TABLE `events`.`e` (id UInt64) -- trailing",
		"CREATE TABLE events.e (note String DEFAULT 'has -- fake comment') ENGINE = Log",
	}
	for _, in := range cases {
		got, err := GuardDDL(ref, KindTable, in)
		if err != nil {
			t.Fatalf("%q: %v", in, err)
		}
		if !strings.Contains(got, "CREATE TABLE IF NOT EXISTS") {
			t.Errorf("keyword landed in the wrong place:\n%s", got)
		}
		if strings.Count(got, "IF NOT EXISTS") != 1 {
			t.Errorf("expected exactly one injection:\n%s", got)
		}
	}
}

func TestGuardDDL_Database(t *testing.T) {
	t.Parallel()
	db := ObjectRef{Database: "events"}
	got, err := GuardDDL(db, KindDatabase, "CREATE DATABASE events ENGINE = Atomic")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(got, "CREATE DATABASE IF NOT EXISTS events") {
		t.Errorf("got %s", got)
	}
	if _, err := GuardDDL(db, KindDatabase, "CREATE DATABASE other"); err == nil {
		t.Error("database name mismatch must be rejected")
	}
	if _, err := GuardDDL(ObjectRef{Database: "events", Name: "t"}, KindTable, "CREATE DATABASE events"); err == nil {
		t.Error("a table file may not create a database")
	}
}

func FuzzMaskSQL(f *testing.F) {
	for _, seed := range []string{
		"", "'", "''", "--", "/*", "*/", "/* x */ CREATE TABLE t (a String DEFAULT 'x')",
		"CREATE TABLE t (a String DEFAULT 'it''s') -- c\n", "'\\'", "/* unterminated",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, sql string) {
		masked := maskSQL(sql)
		if len(masked) != len(sql) {
			t.Fatalf("length not preserved: %d -> %d for %q", len(sql), len(masked), sql)
		}
		for i := range sql {
			if sql[i] == '\n' && masked[i] != '\n' {
				t.Fatalf("newline at %d destroyed", i)
			}
		}
	})
}

func FuzzGuardDDL(f *testing.F) {
	for _, seed := range []string{
		"CREATE TABLE events.e (id UInt64)",
		"CREATE TABLE IF NOT EXISTS events.e (id UInt64)",
		"DROP TABLE events.e", "CREATE OR REPLACE TABLE events.e (x Int8)",
		"-- c\nCREATE TABLE `events`.`e` (x Int8); DROP TABLE events.e",
		"CREATE DATABASE events", "",
	} {
		f.Add(seed)
	}
	ref := ObjectRef{Database: "events", Name: "e"}
	f.Fuzz(func(t *testing.T, sql string) {
		got, err := GuardDDL(ref, KindTable, sql) // must never panic
		if err != nil {
			return
		}
		// Invariants for anything the guard lets through.
		if !regexp.MustCompile(`(?i)IF\s+NOT\s+EXISTS`).MatchString(maskSQL(got)) {
			t.Fatalf("accepted statement lacks IF NOT EXISTS: %q", got)
		}
		if strings.Contains(maskSQL(strings.TrimRight(strings.TrimSpace(got), ";")), ";") {
			t.Fatalf("accepted statement contains a second statement: %q", got)
		}
		if v := strings.ToUpper(strings.Fields(maskSQL(got))[0]); v != "CREATE" && v != "ATTACH" {
			t.Fatalf("accepted statement starts with %s: %q", v, got)
		}
	})
}

func FuzzExtractDeps(f *testing.F) {
	for _, seed := range []string{
		"CREATE MATERIALIZED VIEW e.mv TO e.t AS SELECT * FROM e.src",
		"CREATE DICTIONARY e.d (id UInt64) PRIMARY KEY id SOURCE(CLICKHOUSE(DB 'x' TABLE 'y' PASSWORD 'p(('))",
		"CREATE TABLE e.t ENGINE = Distributed(c, e, ", "FROM", "JOIN (",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, sql string) {
		_, _, _, _ = ExtractDeps("e", sql) // must never panic on arbitrary input
	})
}

// Comment and literal markers inside a quoted identifier are part of the
// name. Treating them as markers used to blank the rest of the line, which
// both broke legitimate names and hid whatever followed from the guard.
func TestGuardDDL_MarkersInsideQuotedIdentifiers(t *testing.T) {
	ok := []struct {
		ref ObjectRef
		sql string
	}{
		{ObjectRef{Database: "db", Name: "a--b"}, "CREATE TABLE db.`a--b` (id UInt64) ENGINE = Memory"},
		{ObjectRef{Database: "db", Name: "it's"}, "CREATE TABLE db.`it's` (id UInt64) ENGINE = Memory"},
		{ObjectRef{Database: "db", Name: "a/*b"}, "CREATE TABLE db.`a/*b` (`x--y` UInt64, `q'` String DEFAULT 'v;') ENGINE = Memory"},
		{ObjectRef{Database: "db", Name: "t"}, "CREATE TABLE db.t (\"c--d\" UInt64) ENGINE = Memory"},
	}
	for _, tt := range ok {
		got, err := GuardDDL(tt.ref, KindTable, tt.sql)
		if err != nil {
			t.Errorf("%s: rejected: %v", tt.sql, err)
			continue
		}
		if !strings.Contains(got, "IF NOT EXISTS") || strings.Replace(got, "IF NOT EXISTS ", "", 1) != tt.sql {
			t.Errorf("only IF NOT EXISTS may be added: %q", got)
		}
	}
	hidden := []string{
		"CREATE TABLE db.`a--b` (id UInt64) ENGINE = Memory; DROP TABLE db.other",
		"CREATE TABLE db.`a'b` (id UInt64) ENGINE = Memory; DROP TABLE db.other --'",
		"CREATE TABLE db.`a/*b` (id UInt64) ENGINE = Memory; DROP TABLE db.other /* */",
	}
	for i, sql := range hidden {
		ref := ObjectRef{Database: "db", Name: []string{"a--b", "a'b", "a/*b"}[i]}
		if _, err := GuardDDL(ref, KindTable, sql); err == nil || !strings.Contains(err.Error(), "multiple statements") {
			t.Errorf("second statement not caught in %q: %v", sql, err)
		}
	}
}

package recovery

// Execution guard.
//
// The schema repository is a trust boundary: anyone who can merge an MR can
// change a .sql file, and `recover` executes those files against a live
// cluster. This file is the last check before anything reaches ClickHouse.
//
// Every statement must pass ALL of:
//
//  1. exactly one statement (no `;`-chained payloads)
//  2. verb is CREATE or ATTACH — never DROP/ALTER/TRUNCATE/RENAME/INSERT/...
//  3. no OR REPLACE (that is a destructive overwrite of a live object)
//  4. the object it creates matches the identity of the file it came from
//  5. IF NOT EXISTS is present — injected here if the snapshot lacked it
//
// Rule 4 matters more than it looks: without it, a file at
// tables/orders.sql containing `CREATE TABLE events.customers ...` would let
// the planner believe it created `orders` while it actually touched a
// different, possibly live, object.

import (
	"fmt"
	"regexp"
	"strings"
)

// ErrUnsafeDDL is returned when a repo statement fails a guard rule.
type ErrUnsafeDDL struct {
	Ref    ObjectRef
	Reason string
}

func (e *ErrUnsafeDDL) Error() string {
	return fmt.Sprintf("refusing to execute DDL for %s: %s", e.Ref, e.Reason)
}

var (
	// Leading verb of the statement.
	reLeadingVerb = regexp.MustCompile(`(?is)^\s*([A-Za-z_]+)`)

	// CREATE|ATTACH [OR REPLACE] [TEMPORARY] <kind> [IF NOT EXISTS] <name>
	reCreateHead = regexp.MustCompile(
		`(?is)^\s*(CREATE|ATTACH)\s+` +
			`(OR\s+REPLACE\s+)?` +
			`(TEMPORARY\s+)?` +
			`(MATERIALIZED\s+VIEW|LIVE\s+VIEW|WINDOW\s+VIEW|VIEW|DICTIONARY|TABLE|DATABASE)\s+` +
			`(IF\s+NOT\s+EXISTS\s+)?` +
			`(` + identPat + `(?:\s*\.\s*` + identPat + `)?)`)
)

// GuardDDL validates sql for the object it claims to define and returns the
// statement that is safe to execute (with IF NOT EXISTS injected).
func GuardDDL(ref ObjectRef, kind ObjectKind, sql string) (string, error) {
	fail := func(format string, a ...any) (string, error) {
		return "", &ErrUnsafeDDL{Ref: ref, Reason: fmt.Sprintf(format, a...)}
	}

	if strings.TrimSpace(sql) == "" {
		return fail("file is empty")
	}

	// (1) exactly one statement. Comments and string literals are blanked
	// first so a `;` inside them does not trip the check.
	stripped := stripSQL(sql)
	body := strings.TrimRight(strings.TrimSpace(stripped), ";")
	if strings.Contains(body, ";") {
		return fail("file contains multiple statements; only one CREATE per file is allowed")
	}

	// (2) verb allow-list.
	m := reLeadingVerb.FindStringSubmatch(stripped)
	if m == nil {
		return fail("cannot determine the leading SQL keyword")
	}
	switch verb := strings.ToUpper(m[1]); verb {
	case "CREATE", "ATTACH":
	default:
		return fail("statement starts with %s; recovery only executes CREATE/ATTACH", verb)
	}

	idx := reCreateHead.FindStringSubmatchIndex(stripped)
	if idx == nil {
		return fail("not a recognizable CREATE statement")
	}
	group := func(n int) string {
		if idx[2*n] < 0 {
			return ""
		}
		return stripped[idx[2*n]:idx[2*n+1]]
	}
	head := make([]string, 7)
	for n := range head {
		head[n] = group(n)
	}

	// (3) no destructive overwrite.
	if strings.TrimSpace(head[2]) != "" {
		return fail("CREATE OR REPLACE would overwrite a live object; recovery never replaces")
	}
	if strings.TrimSpace(head[3]) != "" {
		return fail("TEMPORARY objects are not recoverable state")
	}

	// (4) identity match between file path and statement.
	declared := kindFromKeyword(head[4])
	if ref.IsDatabase() || declared == KindDatabase {
		// Database DDL: `CREATE DATABASE <name>`; the ref carries only the
		// database part, and the kinds must agree both ways.
		if !ref.IsDatabase() || declared != KindDatabase {
			return fail("statement creates a %s but the file identifies a %s", declared, kind)
		}
		_, gotName := splitQualified(head[6])
		if gotName != ref.Database {
			return fail("statement creates database %s but the file path says %s", gotName, ref.Database)
		}
	} else {
		gotDB, gotName := splitQualified(head[6])
		if gotDB == "" {
			gotDB = ref.Database // unqualified: the file's database applies
		}
		if gotDB != ref.Database || gotName != ref.Name {
			return fail("statement creates %s.%s but the file path says %s (path and content must agree)",
				gotDB, gotName, ref)
		}
		if declared != KindUnknown && declared != kind {
			return fail("statement creates a %s but the file lives in the %s directory", declared, kind.Dir())
		}
	}

	// (5) IF NOT EXISTS, injected if absent. ClickHouse's create_table_query
	// does not include it, so most snapshotted files land here.
	if strings.TrimSpace(head[5]) != "" {
		return sql, nil
	}
	// maskSQL is length-preserving, so the group offset on the masked text
	// is the same offset in the original.
	nameStart := idx[2*6]
	return sql[:nameStart] + "IF NOT EXISTS " + sql[nameStart:], nil
}

// kindFromKeyword maps the CREATE keyword to a kind. Window views return
// KindUnknown: they are not part of the object model, and rule 4's name
// check still constrains them.
func kindFromKeyword(kw string) ObjectKind {
	switch strings.ToUpper(strings.Join(strings.Fields(kw), " ")) {
	case "DATABASE":
		return KindDatabase
	case "TABLE":
		return KindTable
	case "VIEW", "LIVE VIEW":
		return KindView
	case "MATERIALIZED VIEW":
		return KindMaterializedView
	case "DICTIONARY":
		return KindDictionary
	}
	return KindUnknown
}

// splitQualified splits `db`.`name` / db.name / name, unquoting each part.
func splitQualified(s string) (db, name string) {
	s = strings.TrimSpace(s)
	depth := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '`', '"':
			q := s[i]
			for i++; i < len(s) && s[i] != q; i++ {
			}
		case '.':
			if depth == 0 {
				return unquoteIdent(s[:i]), unquoteIdent(s[i+1:])
			}
		}
	}
	return "", unquoteIdent(s)
}

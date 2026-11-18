// Package recovery: read-only extraction of object references from DDL text.
//
// The schema repo stores plain .sql files; this file does NOT parse SQL into
// an AST and never rewrites DDL. It scans the text for the handful of clauses
// that create ordering constraints between objects:
//
//   - MV:         TO db.t (write target), FROM/JOIN in the AS SELECT
//   - View:       FROM/JOIN refs
//   - Dictionary: SOURCE(CLICKHOUSE(... DB 'x' TABLE 'y'))
//   - Table:      Distributed(cluster, db, t) engine args,
//     dictGet('db.dict', ...) in column expressions
//
// Everything it cannot resolve (table functions, non-ClickHouse dictionary
// sources) is reported as External so the planner can surface it in the plan
// instead of guessing.
package recovery

import (
	"fmt"
	"regexp"
	"strings"

	"gitlab.example.com/infra/ddl-saver/internal/model"
)

// Identity types live in internal/model so clickhouse / gitlab / snapshot
// can share them without importing recovery. Aliased here for brevity.
type (
	ObjectKind = model.ObjectKind
	ObjectRef  = model.ObjectRef
)

const (
	KindTable            = model.KindTable
	KindView             = model.KindView
	KindMaterializedView = model.KindMaterializedView
	KindDictionary       = model.KindDictionary
	KindDatabase         = model.KindDatabase
	KindUnknown          = model.KindUnknown
)

// DepCandidate is a reference found in DDL text. Qualified reports whether
// the source text named the database explicitly. The loader/planner keeps
// qualified candidates unconditionally; unqualified ones are kept only when
// they resolve against the repo index (this drops false positives such as
// `trim(BOTH ' ' FROM col)` matching a column name).
type DepCandidate struct {
	Ref       ObjectRef
	Qualified bool
	Origin    string // "from" | "join" | "to" | "dict_get" | "distributed" | "dict_source"
}

// DDLObject is one .sql file from the schema repo enriched with extracted
// references. SQL is stored verbatim.
type DDLObject struct {
	Ref  ObjectRef
	Kind ObjectKind
	SQL  string
	// Synthesized marks DDL the planner generated itself (a database with
	// no _database.sql in the snapshot). Never true for repo content.
	Synthesized bool
	DependsOn   []ObjectRef // resolved upstream deps: must exist before this object
	External    []string    // refs that cannot be satisfied from the repo
	Warnings    []string
}

// systemDBs are never dependencies.
var systemDBs = map[string]bool{"system": true, "information_schema": true}

// Words that can follow FROM/JOIN without being a table reference.
var notATable = map[string]bool{"SELECT": true, "FINAL": true, "VALUES": true}

var (
	reKind = regexp.MustCompile(
		`(?is)^\s*(?:CREATE|ATTACH)\s+(?:OR\s+REPLACE\s+)?(MATERIALIZED\s+VIEW|LIVE\s+VIEW|VIEW|DICTIONARY|TABLE)\b`)

	reFromJoin = regexp.MustCompile(`(?i)\b(FROM|JOIN)\b`)

	// ENGINE = Distributed(cluster, db, table[, sharding_key])
	reDistributed = regexp.MustCompile(
		`(?is)\bENGINE\s*=\s*Distributed\s*\(\s*([^,()]+?)\s*,\s*([^,()]+?)\s*,\s*([^,()]+?)\s*[,)]`)

	// dictGet, dictGetOrDefault, dictGetUInt64, dictHas, ... with a literal first arg
	reDictGet = regexp.MustCompile(`(?i)\bdict(?:Get[A-Za-z0-9]*|Has|IsIn)\s*\(\s*'([^']+)'`)

	// SOURCE(<TYPE>(  — type captured, args scanned separately
	reDictSource = regexp.MustCompile(`(?is)\bSOURCE\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\(`)

	reSrcDB    = regexp.MustCompile(`(?i)\bDB\s+'([^']*)'`)
	reSrcTable = regexp.MustCompile(`(?i)\bTABLE\s+'([^']*)'`)
	reSrcQuery = regexp.MustCompile(`(?i)\bQUERY\b`)

	// MATERIALIZED VIEW <name> [ON CLUSTER x] TO <target>
	reMVTo = regexp.MustCompile(
		`(?is)\bMATERIALIZED\s+VIEW\s+` + qualIdentPat +
			`(?:\s+ON\s+CLUSTER\s+(?:'[^']*'|\S+))?\s+TO\s+(` + identPat + `)(?:\.(` + identPat + `))?`)
)

const identPat = "(?:`[^`]+`|\"[^\"]+\"|[A-Za-z_][A-Za-z0-9_]*)"
const qualIdentPat = identPat + `(?:\.` + identPat + `)?`

// ExtractDeps scans one DDL statement. defaultDB qualifies unqualified refs
// (for repo files this is the database the file lives under).
func ExtractDeps(defaultDB, sql string) (kind ObjectKind, cands []DepCandidate, external []string, warnings []string) {
	kind = detectKind(sql)
	stripped := stripSQL(sql)

	add := func(db, name string, qualified bool, origin string) {
		if name == "" {
			return
		}
		if db == "" {
			db = defaultDB
		}
		if systemDBs[strings.ToLower(db)] {
			return
		}
		cands = append(cands, DepCandidate{
			Ref:       ObjectRef{Database: unquoteIdent(db), Name: unquoteIdent(name)},
			Qualified: qualified,
			Origin:    origin,
		})
	}

	// --- FROM / JOIN (views, MVs; harmless no-op for plain tables) ---
	if kind == KindView || kind == KindMaterializedView {
		for _, m := range reFromJoin.FindAllStringSubmatchIndex(stripped, -1) {
			kw := strings.ToUpper(stripped[m[2]:m[3]])
			if kw == "JOIN" && precededByWord(stripped, m[2], "ARRAY") {
				continue // ARRAY JOIN operates on columns, not tables
			}
			i := skipSpaces(stripped, m[3])
			if i >= len(stripped) || stripped[i] == '(' {
				continue // subquery
			}
			first, j, ok := scanIdent(stripped, i)
			if !ok || isDigit(first[0]) {
				continue // e.g. substring(x FROM 2)
			}
			db, name, qualified := "", first, false
			if j < len(stripped) && stripped[j] == '.' {
				if second, j2, ok2 := scanIdent(stripped, j+1); ok2 {
					db, name, qualified, j = first, second, true, j2
				}
			}
			if k := skipSpaces(stripped, j); k < len(stripped) && stripped[k] == '(' {
				external = append(external, fmt.Sprintf("table function %s(...)", first))
				continue
			}
			if !qualified && notATable[strings.ToUpper(name)] {
				continue
			}
			add(db, name, qualified, strings.ToLower(kw))
		}
	}

	// --- MV write target: TO db.t ---
	if kind == KindMaterializedView {
		if m := reMVTo.FindStringSubmatch(stripped); m != nil {
			if m[2] != "" {
				add(m[1], m[2], true, "to")
			} else {
				add("", m[1], false, "to")
			}
		}
	}

	// --- Dictionary source (needs original SQL: args are string literals) ---
	if kind == KindDictionary {
		if m := reDictSource.FindStringSubmatchIndex(sql); m != nil {
			srcType := strings.ToUpper(sql[m[2]:m[3]])
			args, ok := balancedParen(sql, m[1]-1) // m[1]-1 = index of the inner '('
			if !ok {
				warnings = append(warnings, "dictionary SOURCE: unbalanced parentheses, deps unknown")
			} else if srcType != "CLICKHOUSE" {
				external = append(external, "dictionary source "+srcType)
			} else if reSrcQuery.MatchString(args) {
				warnings = append(warnings, "dictionary SOURCE uses QUERY, dependencies not extracted")
			} else {
				db, table := "", ""
				if dm := reSrcDB.FindStringSubmatch(args); dm != nil {
					db = dm[1]
				}
				if tm := reSrcTable.FindStringSubmatch(args); tm != nil {
					table = tm[1]
				}
				if table != "" {
					add(db, table, db != "", "dict_source")
				}
			}
		}
	}

	// --- Table-level refs (original SQL: args may be string literals) ---
	if kind == KindTable {
		if m := reDistributed.FindStringSubmatch(sql); m != nil {
			db, table := unquoteArg(m[2]), unquoteArg(m[3])
			if strings.EqualFold(strings.TrimSpace(m[2]), "currentDatabase()") {
				db = ""
			}
			add(db, table, db != "", "distributed")
		}
	}
	for _, m := range reDictGet.FindAllStringSubmatch(sql, -1) {
		db, name := "", m[1]
		if i := strings.IndexByte(m[1], '.'); i >= 0 {
			db, name = m[1][:i], m[1][i+1:]
		}
		add(db, name, db != "", "dict_get")
	}

	return kind, dedupCandidates(cands), external, warnings
}

func detectKind(sql string) ObjectKind {
	m := reKind.FindStringSubmatch(sql)
	if m == nil {
		return KindUnknown
	}
	switch strings.ToUpper(regexp.MustCompile(`\s+`).ReplaceAllString(m[1], " ")) {
	case "MATERIALIZED VIEW":
		return KindMaterializedView
	case "VIEW", "LIVE VIEW":
		return KindView
	case "DICTIONARY":
		return KindDictionary
	case "TABLE":
		return KindTable
	}
	return KindUnknown
}

// maskSQL returns a copy of sql with the same length in which every comment
// and every single-quoted string literal's interior is replaced by spaces.
// Newlines are kept so line structure survives.
//
// Length preservation is a hard invariant (see FuzzMaskSQL): it lets the
// guard find a token on the masked text and splice at that exact offset in
// the original. Quoting characters of literals are kept so regexes that
// expect '...' still match; backtick / double-quoted identifiers are kept
// whole because they carry names we need.
func maskSQL(sql string) string {
	out := []byte(sql)
	blank := func(i int) {
		if out[i] != '\n' && out[i] != '\r' {
			out[i] = ' '
		}
	}
	for i := 0; i < len(sql); {
		c := sql[i]
		switch {
		case c == '-' && i+1 < len(sql) && sql[i+1] == '-':
			for ; i < len(sql) && sql[i] != '\n'; i++ {
				blank(i)
			}
		case c == '/' && i+1 < len(sql) && sql[i+1] == '*':
			blank(i)
			blank(i + 1)
			i += 2
			for i < len(sql) {
				if sql[i] == '*' && i+1 < len(sql) && sql[i+1] == '/' {
					blank(i)
					blank(i + 1)
					i += 2
					break
				}
				blank(i)
				i++
			}
		case c == '\'':
			i++ // keep the opening quote
			for i < len(sql) {
				if sql[i] == '\\' && i+1 < len(sql) {
					blank(i)
					blank(i + 1)
					i += 2
					continue
				}
				if sql[i] == '\'' {
					if i+1 < len(sql) && sql[i+1] == '\'' { // '' escape
						blank(i)
						blank(i + 1)
						i += 2
						continue
					}
					break
				}
				blank(i)
				i++
			}
			i++ // keep the closing quote (or run off the end: unterminated)
		case c == '`' || c == '"':
			// Quoted identifier: kept verbatim, but walked over, so that
			// "--", "/*" or a quote inside a name is not taken for the
			// start of a comment or literal.
			for i++; i < len(sql) && sql[i] != c; i++ {
				if sql[i] == '\\' && i+1 < len(sql) {
					i++
				}
			}
			i++
		default:
			i++
		}
	}
	return string(out)
}

// stripSQL is kept as the name used by the extraction code; it is the same
// masking transform.
func stripSQL(sql string) string { return maskSQL(sql) }

// balancedParen returns the content between the '(' at s[open] and its
// matching ')'.
func balancedParen(s string, open int) (string, bool) {
	if open < 0 || open >= len(s) || s[open] != '(' {
		return "", false
	}
	depth := 0
	inStr := false
	for i := open; i < len(s); i++ {
		c := s[i]
		if inStr {
			if c == '\\' {
				i++
			} else if c == '\'' {
				inStr = false
			}
			continue
		}
		switch c {
		case '\'':
			inStr = true
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return s[open+1 : i], true
			}
		}
	}
	return "", false
}

func scanIdent(s string, i int) (string, int, bool) {
	if i >= len(s) {
		return "", i, false
	}
	switch s[i] {
	case '`', '"':
		q := s[i]
		j := strings.IndexByte(s[i+1:], q)
		if j < 0 {
			return "", i, false
		}
		return s[i+1 : i+1+j], i + j + 2, true
	default:
		j := i
		for j < len(s) && isWordByte(s[j]) {
			j++
		}
		if j == i {
			return "", i, false
		}
		return s[i:j], j, true
	}
}

func unquoteIdent(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 {
		if (s[0] == '`' && s[len(s)-1] == '`') || (s[0] == '"' && s[len(s)-1] == '"') {
			return s[1 : len(s)-1]
		}
	}
	return s
}

func unquoteArg(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		return s[1 : len(s)-1]
	}
	return unquoteIdent(s)
}

func skipSpaces(s string, i int) int {
	for i < len(s) && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r') {
		i++
	}
	return i
}

func precededByWord(s string, pos int, word string) bool {
	i := pos
	for i > 0 && (s[i-1] == ' ' || s[i-1] == '\t' || s[i-1] == '\n' || s[i-1] == '\r') {
		i--
	}
	start := i - len(word)
	return start >= 0 && strings.EqualFold(s[start:i], word)
}

func isWordByte(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func dedupCandidates(in []DepCandidate) []DepCandidate {
	seen := map[string]int{} // ref string -> index in out
	var out []DepCandidate
	for _, c := range in {
		key := c.Ref.String()
		if i, ok := seen[key]; ok {
			if c.Qualified && !out[i].Qualified {
				out[i].Qualified = true // any qualified sighting wins
			}
			continue
		}
		seen[key] = len(out)
		out = append(out, c)
	}
	return out
}

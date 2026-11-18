package clickhouse

import (
	"regexp"
	"strings"
)

// The snapshot stores DDL exactly as recovery will need it. On distributed
// clusters that means ON CLUSTER '{cluster}' must be present so a restore
// propagates to every replica; the macro form (not the literal cluster name)
// keeps the files portable between environments that define the macro.
const clusterMacro = "'{cluster}'"

var reDDLHead = regexp.MustCompile(
	`(?is)^(\s*(?:CREATE|ATTACH)\s+(?:OR\s+REPLACE\s+)?` +
		`(?:MATERIALIZED\s+VIEW|LIVE\s+VIEW|VIEW|DICTIONARY|TABLE|DATABASE)\s+` +
		`(?:IF\s+NOT\s+EXISTS\s+)?` +
		"(?:`[^`]+`|\"[^\"]+\"|[A-Za-z_][A-Za-z0-9_]*)" +
		"(?:\\.(?:`[^`]+`|\"[^\"]+\"|[A-Za-z_][A-Za-z0-9_]*))?" +
		`)`)

var reOnCluster = regexp.MustCompile(`(?is)^\s+ON\s+CLUSTER\b`)

// EnsureOnCluster injects ON CLUSTER '{cluster}' right after the object name
// unless the DDL already carries an ON CLUSTER clause (any cluster —
// operator intent is preserved, matching the snapshot contract from the
// spec). Returns the DDL and whether it was modified.
func EnsureOnCluster(ddl string) (string, bool) {
	loc := reDDLHead.FindStringIndex(ddl)
	if loc == nil {
		return ddl, false // not a recognizable CREATE; leave untouched
	}
	rest := ddl[loc[1]:]
	if reOnCluster.MatchString(rest) {
		return ddl, false
	}
	return ddl[:loc[1]] + " ON CLUSTER " + clusterMacro + rest, true
}

// NormalizeDDL is what the snapshot writes to the repository: consistent
// trailing newline, and ON CLUSTER injection when the source cluster is
// distributed.
func NormalizeDDL(ddl string, distributed bool) string {
	out := strings.TrimRight(ddl, " \t\r\n")
	if distributed {
		out, _ = EnsureOnCluster(out)
	}
	return out + "\n"
}

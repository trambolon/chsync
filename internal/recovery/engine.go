package recovery

// Replicated engine analysis.
//
// A ReplicatedMergeTree-family table binds itself to a ZooKeeper path, and
// how it does so changes what can go wrong on restore:
//
//	ENGINE = ReplicatedMergeTree('/ch/tables/{shard}/db/t', '{replica}')
//	    The path is pinned in the DDL. Recreating the table reuses it, so a
//	    stale registration from a previous incarnation blocks CREATE (code
//	    253), and restoring onto a second cluster that shares a ZooKeeper
//	    ensemble silently joins the original table's replica set.
//
//	ENGINE = ReplicatedMergeTree
//	    The path comes from the server's default_replica_path /
//	    default_replica_name settings. Portable only to servers that define
//	    them — and when the default is UUID-based (the stock
//	    '/clickhouse/tables/{uuid}/{shard}'), each recreated table gets a
//	    fresh path, so neither hazard above applies.
//
// The two therefore deserve different warnings, which means detecting the
// difference rather than just "is this replicated".

import "regexp"

// reReplicatedEngine matches the Replicated*MergeTree family in both forms.
// Group 1 is the engine name; the optional "(" is checked separately so the
// argument-less form still matches. It deliberately does not match the
// Replicated *database* engine (no MergeTree suffix), which takes no
// per-table path.
var reReplicatedEngine = regexp.MustCompile(`(?i)\bENGINE\s*=\s*(Replicated[A-Za-z]*MergeTree)\b`)

// ReplicatedInfo describes how an object binds to ZooKeeper.
type ReplicatedInfo struct {
	IsReplicated bool
	Engine       string
	// ExplicitPath is true when the DDL passes a path/replica pair;
	// false means the server's default_replica_path applies.
	ExplicitPath bool
}

// AnalyzeEngine inspects a CREATE statement's engine clause.
func AnalyzeEngine(sql string) ReplicatedInfo {
	// maskSQL is length-preserving, so an offset found on the masked text
	// is the same offset in the original — comments and literals cannot
	// produce a false match, and the original is still usable for args.
	masked := maskSQL(sql)
	loc := reReplicatedEngine.FindStringSubmatchIndex(masked)
	if loc == nil {
		return ReplicatedInfo{}
	}
	info := ReplicatedInfo{IsReplicated: true, Engine: masked[loc[2]:loc[3]]}

	// Arguments, if any, start at the first non-space byte after the name.
	i := skipSpaces(masked, loc[1])
	if i < len(masked) && masked[i] == '(' {
		if args, ok := balancedParen(sql, i); ok {
			// "ReplicatedMergeTree()" also falls back to the server default.
			info.ExplicitPath = len(trimSpace(args)) > 0
		} else {
			info.ExplicitPath = true // unbalanced: assume the path is there
		}
	}
	return info
}

func trimSpace(s string) string {
	start, end := 0, len(s)
	for start < end && isSpaceByte(s[start]) {
		start++
	}
	for end > start && isSpaceByte(s[end-1]) {
		end--
	}
	return s[start:end]
}

func isSpaceByte(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

// isReplicatedEngine reports whether sql creates a Replicated* table.
func isReplicatedEngine(sql string) bool { return AnalyzeEngine(sql).IsReplicated }

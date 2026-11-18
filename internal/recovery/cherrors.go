package recovery

// ClickHouse error classification.
//
// Recovery retries failed CREATEs, which is right for a ZooKeeper blip or a
// DDL-queue timeout and pointless for a syntax error or an occupied replica
// path: those fail identically every time, and on a large restore the wasted
// backoff adds up to minutes of nothing.
//
// So errors are split into retryable and permanent, and the handful an
// operator actually hits during a restore carry a remedy in the report.
// Unknown codes default to RETRYABLE: retrying a permanent error costs a few
// seconds, while giving up on a transient one costs the restore.

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// reErrCode finds the code in an error's text, for errors that do not
// carry it in a typed form: "Code: 253." as the server prints it, or
// "DB::Exception (253)" as the driver does.
var reErrCode = regexp.MustCompile(`(?i)\bcode:\s*(\d{1,4})\b|\bException \((\d{1,4})\)`)

// ErrorCode extracts the ClickHouse error code from err, or -1. A typed
// server error (anything with a ClickHouseCode method in its chain) wins
// over parsing the message.
func ErrorCode(err error) int {
	if err == nil {
		return -1
	}
	var coded interface{ ClickHouseCode() int }
	if errors.As(err, &coded) {
		return coded.ClickHouseCode()
	}
	m := reErrCode.FindStringSubmatch(err.Error())
	if m == nil {
		return -1
	}
	digits := m[1] + m[2]
	n := 0
	for _, c := range digits {
		n = n*10 + int(c-'0')
	}
	return n
}

// ClickHouse error codes this tool reasons about (from
// src/Common/ErrorCodes.cpp).
const (
	codeBadArguments           = 36
	codeUnknownIdentifier      = 47
	codeUnknownType            = 50
	codeTableAlreadyExists     = 57
	codeUnknownTable           = 60
	codeSyntaxError            = 62
	codeUnknownDatabase        = 81
	codeDatabaseAlreadyExists  = 82
	codeNoZooKeeper            = 225
	codeReplicaAlreadyExists   = 253
	codeInconsistentClusterDef = 371
	codeAccessDenied           = 497
	codeAuthenticationFailed   = 516
)

// permanent lists codes where another identical attempt cannot succeed.
var permanent = map[int]bool{
	codeBadArguments:           true,
	codeUnknownIdentifier:      true,
	codeUnknownType:            true,
	codeTableAlreadyExists:     true,
	codeUnknownTable:           true,
	codeSyntaxError:            true,
	codeUnknownDatabase:        true,
	codeDatabaseAlreadyExists:  true,
	codeNoZooKeeper:            true,
	codeReplicaAlreadyExists:   true,
	codeInconsistentClusterDef: true,
	codeAccessDenied:           true,
	codeAuthenticationFailed:   true,
}

// remedies maps a code to operator-facing advice. Deliberately short and
// specific; anything vaguer than this belongs in the runbook, not a report.
var remedies = map[int]string{
	codeReplicaAlreadyExists: "the ZooKeeper path in this DDL is still registered by a previous incarnation of the table. " +
		"Confirm no live replica is using it, then release it with " +
		"`SYSTEM DROP REPLICA '<replica>' FROM ZKPATH '<path>'` (or `DROP TABLE ... SYNC` on the surviving replica). " +
		"Never delete the ZooKeeper node by hand while a replica is live",
	codeNoZooKeeper: "this DDL uses a Replicated* engine but the server has no ZooKeeper/Keeper configured. " +
		"Either point the server at Keeper, or restore to a cluster that has it",
	codeAccessDenied: "the recovery user lacks CREATE on this database. " +
		"Recovery needs a user with CREATE; the snapshot daemon should stay read-only",
	codeAuthenticationFailed: "authentication failed for the recovery user; check clusters[].auth in the config",
	codeSyntaxError: "the snapshotted DDL is not valid for this server version. " +
		"Check whether the target runs an older ClickHouse than the cluster the snapshot came from",
	codeInconsistentClusterDef: "the ON CLUSTER target is defined differently across nodes; reconcile remote_servers first",
	codeTableAlreadyExists: "the object exists but was not visible in system.tables at plan time " +
		"(an in-flight DROP, or a Replicated database still converging)",
}

// reMissingMacro catches the single-node case: DDL carrying ON CLUSTER
// '{cluster}' (or a Replicated path with {shard}/{replica}) executed on a
// server that does not define that macro. The server reports this as
// BAD_ARGUMENTS with a distinctive message rather than a dedicated code.
var reMissingMacro = regexp.MustCompile(`(?i)no macro '?([a-z_][a-z0-9_]*)'?|macro '?([a-z_][a-z0-9_]*)'? (?:is )?not found`)

// Classify reports whether another attempt could plausibly succeed, and any
// operator-facing remedy for this failure.
func Classify(err error) (retryable bool, remedy string) {
	if err == nil {
		return false, ""
	}
	if m := reMissingMacro.FindStringSubmatch(err.Error()); m != nil {
		name := m[1]
		if name == "" {
			name = m[2]
		}
		return false, fmt.Sprintf(
			"this server does not define the %q macro used by the snapshotted DDL. "+
				"That usually means DDL captured from a distributed cluster is being restored to a "+
				"single node (or a differently macro'd one): define the macro in the server config, "+
				"or restore from a snapshot of this cluster", name)
	}
	code := ErrorCode(err)
	if code == codeReplicaAlreadyExists {
		return false, replicaRemedy(err.Error())
	}
	if code < 0 {
		// No server code: transport/driver level. Worth retrying.
		return true, ""
	}
	return !permanent[code], remedies[code]
}

// reStaleReplica pulls the ZooKeeper path and replica name out of
// "Replica /ch/tables/01/db/t/replicas/r1 already exists".
var reStaleReplica = regexp.MustCompile(`Replica (/\S+)/replicas/(\S+) already exists`)

// replicaRemedy names the exact statement that releases the path when the
// server told us which one it is.
func replicaRemedy(msg string) string {
	m := reStaleReplica.FindStringSubmatch(msg)
	if m == nil {
		return remedies[codeReplicaAlreadyExists]
	}
	return fmt.Sprintf("the ZooKeeper path in this DDL is still registered by a previous incarnation of the table. "+
		"If the table was dropped without SYNC, the server releases the path by itself after "+
		"database_atomic_delay_before_drop_table_sec (8 minutes by default) — waiting is enough. "+
		"To release it now, confirm no live table uses it and run, on a DIFFERENT replica than %[2]s: "+
		"SYSTEM DROP REPLICA '%[2]s' FROM ZKPATH '%[1]s' (and likewise for any other replica that reports this). "+
		"Never delete the ZooKeeper node by hand while a replica is live", m[1], m[2])
}

// withRemedy appends the remedy to a failure detail, if there is one.
func withRemedy(detail, remedy string) string {
	if remedy == "" {
		return detail
	}
	return detail + "\n           hint: " + strings.TrimSpace(remedy)
}

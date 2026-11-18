# Changelog

## Unreleased — review, real-dependency build, end-to-end test

Until now the project had only ever been compiled against stand-ins for its
four libraries. This round built it with Go 1.26 against the real chconn,
go-toml and gocron (and the GitLab SDK's predecessor, same API), and ran the
binary against a real two-replica ClickHouse 25.8 cluster with Keeper and
TLS. Everything below was found by review or by that run.

### Fixed — found by running against a real cluster
- **One failed `CREATE` aborted the whole recovery.** The driver closes the
  connection when the server returns an exception, so every later statement
  failed with `conn closed` and the run ended with a fatal error instead of
  a report. The client now reconnects.
- **Error codes were read from the message text**, which only contains
  `Code: N` when the failure passed through `ON CLUSTER`. On a single node a
  permanent error would have been retried and shown without its hint. The
  code now comes from the driver's typed error.
- **The hint for code 253 was a template.** It now names the statement to
  run, with the real ZooKeeper path and replica, says which node to run it
  from, and says that waiting out the delayed drop is enough.
- **chconn pinned to v3.0.3**: v3.0.4+ require Go 1.27.

### Fixed — found by review
- **A cluster that returns zero objects no longer archives the repository.**
  A user that lost `SHOW` sees an empty `system.tables` and no error; that
  used to move (or, with `on_deleted = "prune"`, delete) every file in one
  commit. The snapshot now refuses, and `snapshot -cluster X -allow-empty`
  is the explicit way through. The daemon never passes it.
- **`recover -cluster prod events.clicks` planned the whole cluster**: words
  after the flags were ignored. They are now an error.
- **A mistyped flag exited with status 2**, the code reserved for "recovery
  applied partially". Usage errors now exit 1, before the config is read.
- **Secrets containing `"` or `\` broke the config**, or could spill into the
  next key: `${VAR}` was substituted as text. Values are now escaped for the
  TOML string they land in.
- **Batched commits could lose a deprecated file**: with a database name
  sorting before `_deprecated`, the live copy was deleted in an earlier
  commit than the archive copy was written. Writes now precede deletes.
- **First snapshot of a new cluster failed on GitLab's 404** for a directory
  that does not exist yet. A missing directory or an empty repository is now
  "no files"; a wrong project, ref or token is still an error that says so.
- **GitLab calls ignored cancellation**: shutdown and `job_timeout` could
  not interrupt a request in flight.
- **Hidden tables of materialized views** (`.inner.*`, `.inner_id.*`) were
  snapshotted; recovering one leaves an orphan next to the table the
  recreated view makes for itself. They are skipped unconditionally.
- **Objects in `ENGINE = Replicated` databases got `ON CLUSTER`**, which the
  server rejects there.
- **Names with `/`, `..` or control characters** produced paths outside
  their directory or commits GitLab rejects, taking the whole cluster's
  snapshot down. Such names are percent-encoded in the file name.
- **`--`, `/*` or a quote inside a quoted identifier** was taken for a
  comment or literal by the statement guard, which rejected valid DDL.
- `validate -connect` stopped at the first unreachable cluster and never
  checked GitLab. It now checks every cluster and the project, branch and
  token, and reports all failures at once.

### Changed
- `snapshot.Run` and `app.Recover` are split into named steps; behaviour is
  pinned by the existing tests. Environment expansion moved to
  `internal/config/env.go`; path encoding lives in `internal/model`.
- Reading the repository downloads eight files in parallel.
- Recovery plans on a distributed cluster name the node that answered the
  existence checks.
- `test/integration` (testcontainers, never run) is replaced by `test/e2e`.

### Added
- `test/e2e`: the built binary against a real cluster — see its README.
- `internal/gitlab/gitlabtest`: an in-process GitLab REST server for tests.
- Tests for the scheduler, config loading through the real TOML parser, the
  GitLab adapter over HTTP, and the command line.

## Unreleased — TLS with password auth (port 9440)

### Added
- **`connection.secure = true`** enables TLS on the native protocol for any auth
  type, so password auth works on the secure port 9440. With TLS on, an endpoint
  without a port defaults to 9440. `auth.ca_file` (a company root CA) is optional;
  without it the system trust store is used.

### Fixed
- **TLS connections set no `ServerName`.** The TLS config was handed to the driver
  without a host name, so Go either refused the handshake or the host name was not
  checked. Each endpoint now gets its own copy with `ServerName` set to the host it
  dials, and the certificate must match it. Tested with a real handshake against
  a private CA, a wrong host name and an untrusted CA.
- **`auth.ca_file` was silently ignored with password auth**, leaving a plaintext
  connection that looked configured for TLS. It is now a validation error unless
  TLS is on.
- A failed plaintext connection to port 9440 now says to set `connection.secure`.
- **`${VAR}` inside a TOML comment broke loading.** Expansion ran over the raw file,
  so the shipped `examples/config.toml` (whose header comment mentions `${VAR}`)
  failed the Quickstart with `undefined environment variables: VAR`, as would any
  commented-out line like `# password = "${OLD_PASS}"`. Comments are now skipped;
  `#` inside strings is still treated as text.

## Unreleased — fixes found while recording the video manual

### Fixed
- **`/status` reported `"last_success": "0001-01-01T00:00:00Z"`** for a cluster that
  had never succeeded. The field is now a pointer and is omitted until the first
  successful run, so alerting can test for its absence. Regression test:
  `TestStatusJSON_Shape`.
- **`/status` statistics used Go field names** (`Objects`, `Creates`, …) while the rest
  of the document is snake_case. `snapshot.Stats` now has explicit JSON tags.
- **An explicitly requested object was labelled "required by …"** in the recovery plan
  when another requested object also depended on it. The explicit request now wins.
  Regression test: `TestPlan_ExplicitTargetReasonWinsOverDependency`.

### Added
- `cmd/demogen`: drives the real validate / snapshot / recover / health code against
  in-memory ClickHouse and GitLab fakes and prints JSON transcripts. Every terminal
  frame in the video manual comes from it.

## Unreleased — replica-path awareness

### Fixed
- **Argument-less `ENGINE = ReplicatedMergeTree` was not detected as replicated.**
  The engine matcher shipped in the previous pass required a `(`, so tables relying on
  the server's `default_replica_path` got no ZooKeeper notice at all — silently
  unwarned, which is the worst kind of warning bug. Detection now handles both forms
  (including `ReplicatedMergeTree()` with empty parentheses) via `AnalyzeEngine`, and
  reports *which* form it found.

### Added
- **`default_replica_path` / `default_replica_name` are read** from
  `system.server_settings` into `Topology`, best-effort: the table needs ClickHouse
  22.10+ and read access, and a failure only leaves `ReplicaDefaultsKnown` false rather
  than failing the run. The single-node code path (no `cluster` macro) reads them too.
- **Four distinct plan notices** instead of one blanket warning, because the hazard
  genuinely differs: an explicit path can go stale, a UUID-based server default cannot
  (but yields an empty new replica set), a non-UUID default can, and unreadable defaults
  mean the CREATE may simply fail on a server that lacks them.
- `FuzzAnalyzeEngine`, plus a table covering all `Replicated*MergeTree` variants,
  empty parens, the `Replicated` *database* engine, and engine names hidden in comments
  and string literals.

## Unreleased — topology & replicated-engine pass

### Fixed
- **`Plan` mutated the planner and its input.** It registered synthesized databases
  into `Planner.index` and appended implicit database edges onto the caller's
  `DDLObject.DependsOn`, so calling `Plan` twice on one `Planner` produced different
  results (the second call synthesized nothing). Plan is now pure: it resolves
  synthesized objects through a local lookup and carries its own `Plan.Edges`, which
  the executor prefers over the raw objects. Regression test:
  `TestPlan_IsRepeatableAndDoesNotMutateInput`.
- **Permanent errors burned the whole retry budget.** A stale ZooKeeper path
  (`Replica ... already exists`, code 253) fails identically every attempt, but the
  executor retried it with backoff — on a large restore, minutes of waiting for a
  guaranteed failure. Errors are now classified: permanent codes fail on the first
  attempt, transient ones (Keeper blips, timeouts, network) still retry, and unknown
  codes default to retryable.
- Removed `DDLObject.SafeSQL`: written at plan time, never read (the executor
  re-guards independently).

### Added
- **Operator-facing remedies** on failure, for the errors that actually occur during a
  restore: stale replica path, no Keeper configured, missing `CREATE` grant, server
  version skew, inconsistent cluster definition, and a missing macro — the last of
  which names the "distributed snapshot restored onto a single node" case explicitly.
- **Plan-time warning for `Replicated*` engines**, listing the affected objects and the
  ZooKeeper-path precondition, shown before the confirmation prompt rather than after
  a failure.
- Tests pinning both topology answers: single node produces no `ON CLUSTER` anywhere
  (objects, `_database.sql`, synthesized databases) and causes no snapshot churn; and
  ZooKeeper paths with `{shard}`/`{replica}`/`{uuid}` survive normalization, the
  `IF NOT EXISTS` injection, and dependency extraction byte-for-byte.
- `README` / `README.ru`: a "Topologies and engines" section covering single-node
  behaviour, the Replicated ZooKeeper-path contract, the cross-cluster shared-ensemble
  hazard, and the schema-vs-data boundary.

## Unreleased — quality pass

### Fixed
- **Guard offset bug.** `GuardDDL` computed the `IF NOT EXISTS` insertion point on
  comment-stripped text and spliced it into the original at that index. Any file
  beginning with a comment got the keyword inside the comment (silently losing the
  guarantee) or mid-keyword (syntax error). The sanitizer is now length-preserving
  (`maskSQL`), the guard uses regex submatch indices, and `FuzzMaskSQL` enforces the
  invariant. Regression test: `TestGuardDDL_InjectsCorrectlyAfterLeadingComments`.
- Readiness probe opened a fresh ClickHouse connection to every cluster on every poll.
  It now answers from in-memory status.

### Added
- **Database-level recovery.** Snapshots write `<db>/_database.sql` (`SHOW CREATE
  DATABASE`); the planner makes each object's database an implicit prerequisite,
  creating it first when missing — from the snapshot, or from a minimal synthesized
  statement that the plan labels as such. The guard accepts `CREATE DATABASE` with
  name matching. A dropped database now archives its `_database.sql` with its tables.
- `GET /status` JSON endpoint; `app.Status` with per-cluster last run/success/error.
- `scheduler.job_timeout` (default 30m) replaces a hard-coded value.
- `app.graceful_timeout` is now honoured: shutdown waits that long for an in-flight
  snapshot, then returns `scheduler.ErrShutdownTimeout`.
- Typed `recovery.CycleError` (use `errors.As`); `app.ErrSnapshotPartial` sentinel.
- `ddl-saver version`.
- Fuzz targets (`FuzzMaskSQL`, `FuzzGuardDDL`, `FuzzExtractDeps`) with a CI job;
  `-shuffle=on` in the unit job; `make cover` / `make fuzz`.

### Changed
- Orchestration moved from `main.go` into `internal/app` with injected dependencies
  (`Connector`, `Repo`, `Confirmer`, `io.Writer`); `main` only parses flags. The
  recover flow now has end-to-end unit tests (dry-run, declined, headless, tampered
  repo, partial apply).
- `recovery.ExecClient.ExecDDL` renamed to `Exec`; `clickhouse.Client` and
  `clickhouse.ExistingSet` satisfy the recovery interfaces directly (adapters removed).
- Headless `recover` (no terminal) without `-yes` now fails instead of hanging on stdin.
- Sorting uses `slices`/`cmp`; `model.ObjectRef.Compare` gives one canonical order.
- Stricter golangci set (`errorlint`, `copyloopvar`, `nilerr`, `unconvert`, `prealloc`,
  `gofumpt`).

### Removed
- `recovery.dependency_check` (was never read). Dependency analysis is not optional.

//go:build e2e

// End-to-end test: the real ddl-saver binary against a real ClickHouse
// cluster (two replicas, Keeper, TLS on the secure native port) and an
// in-process GitLab speaking the real REST API.
//
// Needs a running cluster; see test/e2e/README.md. Environment:
//
//	E2E_CLICKHOUSE_CLIENT  path to the `clickhouse` binary (used as client)
//	E2E_CA_FILE            root CA that signed the servers' certificates
//	E2E_PASSWORD           password of user ddl_saver
//	E2E_HOST               host name in the certificates (default localhost)
//	E2E_PORTS              plain,secure of replica 1 and 2 (default 9000,9440,9001,9441)
package e2e

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"gitlab.example.com/infra/ddl-saver/internal/gitlab/gitlabtest"
)

type env struct {
	t                              *testing.T
	bin, cfg, cfgRev, client, host string
	plain1, tls1, plain2           string
	password                       string
	git                            *gitlabtest.Server
}

func setup(t *testing.T) *env {
	t.Helper()
	e := &env{t: t, client: os.Getenv("E2E_CLICKHOUSE_CLIENT"), password: os.Getenv("E2E_PASSWORD"), host: "localhost"}
	if e.client == "" {
		t.Skip("E2E_CLICKHOUSE_CLIENT not set")
	}
	if h := os.Getenv("E2E_HOST"); h != "" {
		e.host = h
	}
	ports := strings.Split("9000,9440,9001,9441", ",")
	if p := os.Getenv("E2E_PORTS"); p != "" {
		ports = strings.Split(p, ",")
	}
	e.plain1, e.tls1, e.plain2 = ports[0], ports[1], ports[2]

	dir := t.TempDir()
	e.bin = filepath.Join(dir, "ddl-saver")
	if out, err := exec.Command("go", "build", "-o", e.bin, "../../cmd/ddl-saver").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	e.git = &gitlabtest.Server{Files: map[string]string{}, EmptyRepo: true}
	srv := httptest.NewServer(e.git)
	t.Cleanup(srv.Close)

	// Two configs that differ only in which replica is tried first.
	write := func(name string, first, second string) string {
		p := filepath.Join(dir, name)
		cfg := fmt.Sprintf(`
[app]
log_level = "warn"

[[clusters]]
name = "e2e"
  [clusters.connection]
  secure    = true
  endpoints = [{ host = %q, port = %s }, { host = %q, port = %s }]
  [clusters.auth]
  type     = "password"
  user     = "ddl_saver"
  password = "${E2E_PASSWORD}"
  ca_file  = %q
  [clusters.filter]
  skip_tables_regex = ["bms", "john111"]

[gitlab]
url           = %q
token         = "${GITLAB_TOKEN}"
project_path  = %q
branch        = %q
commit_author = "DDL Saver Bot <bot@example.com>"

[recovery]
defer_invalid_objects = true
retries       = 1
retry_backoff = "100ms"
`, e.host, first, e.host, second, os.Getenv("E2E_CA_FILE"), srv.URL, gitlabtest.Project, gitlabtest.Branch)
		if err := os.WriteFile(p, []byte(cfg), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	e.cfg = write("config.toml", ports[1], ports[3])
	e.cfgRev = write("replica2-first.toml", ports[3], ports[1])
	e.sql(e.plain1, "DROP DATABASE IF EXISTS events ON CLUSTER '{cluster}' SYNC")
	e.sql(e.plain1, "DROP DATABASE IF EXISTS solo ON CLUSTER '{cluster}' SYNC")
	t.Cleanup(func() {
		e.sql(e.plain1, "DROP DATABASE IF EXISTS events ON CLUSTER '{cluster}' SYNC")
		e.sql(e.plain1, "DROP DATABASE IF EXISTS solo ON CLUSTER '{cluster}' SYNC")
	})
	return e
}

// sql runs a statement as the admin user on the plain port and returns stdout.
func (e *env) sql(port, query string) string {
	e.t.Helper()
	out, err := exec.Command(e.client, "client", "--host", "127.0.0.1", "--port", port, "-q", query).CombinedOutput()
	if err != nil {
		e.t.Fatalf("clickhouse client: %v\n%s\nquery: %s", err, out, query)
	}
	return strings.TrimSpace(string(out))
}

// run executes the binary and returns exit code, stdout, stderr.
func (e *env) run(args ...string) (int, string, string) {
	e.t.Helper()
	return e.runWith(e.cfg, args...)
}

func (e *env) runWith(cfg string, args ...string) (int, string, string) {
	e.t.Helper()
	cmd := exec.Command(e.bin, append(args[:1:1], append([]string{"-config", cfg}, args[1:]...)...)...)
	cmd.Env = append(os.Environ(), "E2E_PASSWORD="+e.password, "GITLAB_TOKEN="+gitlabtest.Token)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	err := cmd.Run()
	code := 0
	if ee, ok := err.(*exec.ExitError); ok {
		code = ee.ExitCode()
	} else if err != nil {
		e.t.Fatal(err)
	}
	e.t.Logf("$ ddl-saver %s -> exit %d\n%s%s", strings.Join(args, " "), code, out.String(), errb.String())
	return code, out.String(), errb.String()
}

func (e *env) mustRun(args ...string) string {
	e.t.Helper()
	code, out, errb := e.run(args...)
	if code != 0 {
		e.t.Fatalf("ddl-saver %v: exit %d\n%s%s", args, code, out, errb)
	}
	return out + errb
}

const tablesOn = "SELECT count() FROM system.tables WHERE database = 'events' AND name = '%s'"

func (e *env) existsOnBoth(name string) bool {
	return e.sql(e.plain1, fmt.Sprintf(tablesOn, name)) == "1" && e.sql(e.plain2, fmt.Sprintf(tablesOn, name)) == "1"
}

func TestEndToEnd(t *testing.T) {
	e := setup(t)
	// A ZooKeeper path of its own per run, so a previous failed run cannot
	// leave anything in the way. Like production paths, it has no {uuid}.
	run := fmt.Sprintf("e2e_%d", time.Now().UnixNano())
	for _, q := range []string{
		"CREATE DATABASE events ON CLUSTER '{cluster}' ENGINE = Atomic",
		// path without {uuid}: only {shard} and {replica} vary
		"CREATE TABLE events.clicks_raw ON CLUSTER '{cluster}' (ts DateTime, user_id UInt64, url String) " +
			"ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/" + run + "/clicks_raw', '{replica}') ORDER BY ts",
		"CREATE TABLE events.clicks_agg ON CLUSTER '{cluster}' (d Date, c UInt64) " +
			"ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{shard}/" + run + "/clicks_agg', '{replica}') ORDER BY d",
		"CREATE MATERIALIZED VIEW events.clicks_mv ON CLUSTER '{cluster}' TO events.clicks_agg " +
			"AS SELECT toDate(ts) AS d, count() AS c FROM events.clicks_raw GROUP BY d",
		"CREATE TABLE events.clicks_all ON CLUSTER '{cluster}' AS events.clicks_raw " +
			"ENGINE = Distributed('{cluster}', events, clicks_raw, rand())",
		"CREATE VIEW events.daily ON CLUSTER '{cluster}' AS SELECT d, sum(c) AS c FROM events.clicks_agg GROUP BY d",
		"CREATE MATERIALIZED VIEW events.per_user ON CLUSTER '{cluster}' ENGINE = MergeTree ORDER BY user_id " +
			"AS SELECT user_id, count() AS c FROM events.clicks_raw GROUP BY user_id",
		"CREATE DICTIONARY events.days ON CLUSTER '{cluster}' (d Date, c UInt64) PRIMARY KEY d " +
			"SOURCE(CLICKHOUSE(DB 'events' TABLE 'clicks_agg')) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(300)",
		"CREATE TABLE events.`2024/q1` ON CLUSTER '{cluster}' (id UInt64) ENGINE = MergeTree ORDER BY id",
		"CREATE TABLE events.bms_audit ON CLUSTER '{cluster}' (id UInt64) ENGINE = MergeTree ORDER BY id",
		"CREATE TABLE events.tmp_john111 ON CLUSTER '{cluster}' (id UInt64) ENGINE = MergeTree ORDER BY id",
	} {
		e.sql(e.plain1, q)
	}

	// --- validate: TLS on the secure port, password auth, company CA ---
	out := e.mustRun("validate", "-connect")
	for _, want := range []string{"cluster e2e reachable: distributed=true shards=1 replicas=2", "gitlab project infra/schemas (branch main) reachable: 0 file(s)"} {
		if !strings.Contains(out, want) {
			t.Errorf("validate output lacks %q", want)
		}
	}

	// --- first snapshot ---
	e.mustRun("snapshot")
	files, msgs := e.git.Snapshot()
	if len(msgs) != 1 {
		t.Fatalf("commits: %v", msgs)
	}
	wantFiles := []string{
		"schemas/e2e/manifest.json", "schemas/e2e/events/_database.sql",
		"schemas/e2e/events/tables/clicks_raw.sql", "schemas/e2e/events/tables/clicks_agg.sql",
		"schemas/e2e/events/tables/clicks_all.sql", "schemas/e2e/events/tables/2024%2Fq1.sql",
		"schemas/e2e/events/materialized_views/clicks_mv.sql", "schemas/e2e/events/materialized_views/per_user.sql",
		"schemas/e2e/events/views/daily.sql", "schemas/e2e/events/dictionaries/days.sql",
	}
	for _, p := range wantFiles {
		if _, ok := files[p]; !ok {
			t.Errorf("missing %s", p)
		}
	}
	if len(files) != len(wantFiles) {
		for p := range files {
			t.Logf("file: %s", p)
		}
		t.Errorf("got %d files, want %d (filtered and implicit objects must be absent)", len(files), len(wantFiles))
	}
	raw := files["schemas/e2e/events/tables/clicks_raw.sql"]
	t.Logf("clicks_raw.sql:\n%s", raw)
	for _, want := range []string{"ON CLUSTER '{cluster}'", "'/clickhouse/tables/{shard}/" + run + "/clicks_raw'", "'{replica}'"} {
		if !strings.Contains(raw, want) {
			t.Errorf("clicks_raw.sql lacks %s", want)
		}
	}

	// --- second snapshot: nothing changed, nothing committed ---
	e.mustRun("snapshot")
	if _, msgs := e.git.Snapshot(); len(msgs) != 1 {
		t.Fatalf("an unchanged cluster produced a commit: %v", msgs)
	}

	// --- schema change is picked up ---
	e.sql(e.plain1, "ALTER TABLE events.clicks_raw ON CLUSTER '{cluster}' ADD COLUMN referer String")
	e.mustRun("snapshot")
	if files, _ := e.git.Snapshot(); !strings.Contains(files["schemas/e2e/events/tables/clicks_raw.sql"], "referer") {
		t.Error("ALTER not reflected in the snapshot")
	}

	// --- recover two dropped objects; replication must work afterwards ---
	e.sql(e.plain1, "DROP TABLE events.clicks_mv ON CLUSTER '{cluster}' SYNC")
	e.sql(e.plain1, "DROP TABLE events.clicks_raw ON CLUSTER '{cluster}' SYNC")
	code, plan, _ := e.run("recover", "-cluster", "e2e", "-object", "events.clicks_mv", "-dry-run")
	if code != 0 || !strings.Contains(plan, "2 to create") || e.existsOnBoth("clicks_raw") {
		t.Fatalf("dry run: exit %d, must plan clicks_raw then clicks_mv and create nothing", code)
	}
	out = e.mustRun("recover", "-cluster", "e2e", "-object", "events.clicks_mv", "-yes")
	if !strings.Contains(out, "created=2") || !e.existsOnBoth("clicks_raw") || !e.existsOnBoth("clicks_mv") {
		t.Fatalf("recover did not restore both objects on both replicas")
	}
	e.sql(e.plain1, "INSERT INTO events.clicks_raw (ts, user_id, url) VALUES (now(), 1, 'a'), (now(), 2, 'b')")
	e.sql(e.plain2, "SYSTEM SYNC REPLICA events.clicks_raw")
	e.sql(e.plain2, "SYSTEM SYNC REPLICA events.clicks_agg")
	if got := e.sql(e.plain2, "SELECT count() FROM events.clicks_raw"); got != "2" {
		t.Errorf("replica 2 has %s rows in the recovered table, want 2", got)
	}
	if got := e.sql(e.plain2, "SELECT sum(c) FROM events.clicks_agg"); got != "2" {
		t.Errorf("recovered materialized view did not feed its target: sum = %s", got)
	}
	// running it again touches nothing
	out = e.mustRun("recover", "-cluster", "e2e", "-yes")
	if !strings.Contains(out, "Nothing to create") {
		t.Error("second recover should be a no-op")
	}

	// --- a table lost on ONE replica ---
	e.sql(e.plain2, "DROP TABLE events.clicks_raw SYNC") // no ON CLUSTER: replica 2 only
	// Seen from replica 1 it exists, so there is nothing to do — and the
	// plan says which node it asked.
	code, out, _ = e.run("recover", "-cluster", "e2e", "-yes")
	if code != 0 || !strings.Contains(out, "Nothing to create") || !strings.Contains(out, "existence was checked on "+e.host+":"+e.tls1+" only") {
		t.Errorf("replica 1 first: exit %d", code)
	}
	// Seen from replica 2 it is missing; CREATE ... ON CLUSTER IF NOT
	// EXISTS is a no-op on replica 1, and replica 2 rejoins the same
	// ZooKeeper path and fetches the data back.
	code, out, _ = e.runWith(e.cfgRev, "recover", "-cluster", "e2e", "-object", "events.clicks_raw", "-yes")
	if code != 0 || !strings.Contains(out, "created=1") {
		t.Fatalf("replica 2 first: exit %d", code)
	}
	e.sql(e.plain2, "SYSTEM SYNC REPLICA events.clicks_raw")
	if got := e.sql(e.plain2, "SELECT count() FROM events.clicks_raw"); got != "2" {
		t.Errorf("replica 2 fetched %s rows back, want 2", got)
	}

	// --- DROP without SYNC: the ZooKeeper path is still registered ---
	e.sql(e.plain1, "DROP TABLE events.clicks_all ON CLUSTER '{cluster}' SYNC") // reads from clicks_raw
	e.sql(e.plain1, "DROP TABLE events.clicks_mv ON CLUSTER '{cluster}' SYNC")
	e.sql(e.plain1, "DROP TABLE events.per_user ON CLUSTER '{cluster}' SYNC")
	e.sql(e.plain1, "DROP TABLE events.clicks_raw ON CLUSTER '{cluster}'")
	code, out, _ = e.run("recover", "-cluster", "e2e", "-yes")
	if code != 2 {
		t.Errorf("stale replica path: exit %d, want 2 (partial)", code)
	}
	for _, want := range []string{
		"pin an explicit ZooKeeper path", "code 253", "upstream failed: events.clicks_raw",
		"FROM ZKPATH '/clickhouse/tables/01/" + run + "/clicks_raw'", // the exact statement, not a template
	} {
		if !strings.Contains(out, want) {
			t.Errorf("report lacks %q", want)
		}
	}
	// Do what the hint says (each replica is released from the other
	// node), and the same recovery goes through.
	e.sql(e.plain2, "SYSTEM DROP REPLICA 'r1' FROM ZKPATH '/clickhouse/tables/01/"+run+"/clicks_raw'")
	e.sql(e.plain1, "SYSTEM DROP REPLICA 'r2' FROM ZKPATH '/clickhouse/tables/01/"+run+"/clicks_raw'")
	if out = e.mustRun("recover", "-cluster", "e2e", "-yes"); !strings.Contains(out, "created=4") {
		t.Errorf("recovery after releasing the path: %s", out)
	}

	// --- whole database dropped: the guard, then full recovery ---
	e.sql(e.plain1, "DROP DATABASE events ON CLUSTER '{cluster}' SYNC")
	before, _ := e.git.Snapshot()
	code, _, errOut := e.run("snapshot")
	if code != 1 || !strings.Contains(errOut, "cluster returned no objects") {
		t.Errorf("empty cluster: exit %d, want the guard to refuse", code)
	}
	if after, _ := e.git.Snapshot(); len(after) != len(before) {
		t.Fatal("the guard let the repository change")
	}
	out = e.mustRun("recover", "-cluster", "e2e", "-yes")
	if !strings.Contains(out, "failed=0") || !strings.Contains(out, "deferred=0") {
		t.Errorf("full recovery was not clean")
	}
	for _, name := range []string{"clicks_raw", "clicks_agg", "clicks_mv", "clicks_all", "daily", "per_user", "days", "2024/q1"} {
		if !e.existsOnBoth(name) {
			t.Errorf("%s not restored on both replicas", name)
		}
	}
	if e.existsOnBoth("bms_audit") {
		t.Error("a filtered table came back")
	}
	if got := e.sql(e.plain2, "SELECT count() FROM system.tables WHERE database = 'events' AND name LIKE '.inner%'"); got != "1" {
		t.Errorf("per_user should have exactly one inner table after recovery, got %s", got)
	}
	// the restored schema is what the snapshot holds: no diff, no commit
	_, msgsBefore := e.git.Snapshot()
	e.mustRun("snapshot")
	if _, msgs := e.git.Snapshot(); len(msgs) != len(msgsBefore) {
		t.Errorf("snapshot after full recovery produced a commit: %q", msgs[len(msgs)-1])
	}

	// --- a dropped table is archived, and -allow-empty is the way out ---
	e.sql(e.plain1, "DROP TABLE events.daily ON CLUSTER '{cluster}' SYNC")
	e.mustRun("snapshot")
	files, _ = e.git.Snapshot()
	if _, ok := files["schemas/e2e/_deprecated/events/views/daily.sql"]; !ok {
		t.Error("dropped view not archived")
	}
	if _, ok := files["schemas/e2e/events/views/daily.sql"]; ok {
		t.Error("dropped view still in the live layout")
	}
}

// The daemon: snapshot on start, health endpoints, clean stop on SIGTERM.
func TestDaemon(t *testing.T) {
	e := setup(t)
	e.sql(e.plain1, "CREATE DATABASE solo ON CLUSTER '{cluster}'")
	e.sql(e.plain1, "CREATE TABLE solo.t ON CLUSTER '{cluster}' (id UInt64) ENGINE = MergeTree ORDER BY id")

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()

	raw, _ := os.ReadFile(e.cfg)
	cfg := strings.Replace(string(raw), "[app]\n", fmt.Sprintf(
		"[scheduler]\nspec = \"0 3 * * *\"\ntimezone = \"Europe/Moscow\"\nrun_on_start = true\n\n[app]\nhealth_addr = %q\ngraceful_timeout = \"10s\"\n", addr), 1)
	path := filepath.Join(t.TempDir(), "daemon.toml")
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(e.bin, "run", "-config", path)
	cmd.Env = append(os.Environ(), "E2E_PASSWORD="+e.password, "GITLAB_TOKEN="+gitlabtest.Token)
	var logs bytes.Buffer
	cmd.Stdout, cmd.Stderr = &logs, &logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill() }()

	get := func(p string) (int, string) {
		resp, err := http.Get("http://" + addr + p)
		if err != nil {
			return 0, ""
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	deadline := time.Now().Add(30 * time.Second)
	var status string
	for time.Now().Before(deadline) {
		if _, status = get("/status"); strings.Contains(status, `"last_success"`) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !strings.Contains(status, `"e2e"`) || !strings.Contains(status, `"last_success"`) || !strings.Contains(status, `"objects": 1`) {
		t.Fatalf("/status after the first run:\n%s\nlogs:\n%s", status, logs.String())
	}
	if code, _ := get("/health/live"); code != 200 {
		t.Errorf("live = %d", code)
	}
	if code, _ := get("/health/ready"); code != 200 {
		t.Errorf("ready = %d", code)
	}
	if files, _ := e.git.Snapshot(); files["schemas/e2e/solo/tables/t.sql"] == "" {
		t.Error("run_on_start did not snapshot")
	}

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("daemon exit after SIGTERM: %v\n%s", err, logs.String())
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("daemon did not stop on SIGTERM\n%s", logs.String())
	}
}

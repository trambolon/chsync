package recovery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestErrorCode(t *testing.T) {
	t.Parallel()
	tests := []struct {
		err  error
		want int
	}{
		{errors.New("code: 253, message: Replica ... already exists"), 253},
		{errors.New("Code: 62. DB::Exception: Syntax error"), 62},
		{errors.New("DB::Exception: something (CODE: 497)"), 497},
		{errors.New("connection reset by peer"), -1},
		{nil, -1},
	}
	for _, tt := range tests {
		if got := ErrorCode(tt.err); got != tt.want {
			t.Errorf("ErrorCode(%v) = %d, want %d", tt.err, got, tt.want)
		}
	}
}

func TestClassify(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		err           error
		wantRetryable bool
		wantRemedy    string // substring; "" = no remedy expected
	}{
		{
			name:       "replica already exists is permanent and actionable",
			err:        errors.New("code: 253, message: Replica /clickhouse/tables/01/t/replicas/r1 already exists"),
			wantRemedy: "SYSTEM DROP REPLICA",
		},
		{
			name:       "no zookeeper is permanent",
			err:        errors.New("Code: 225. DB::Exception: There is no Zookeeper configuration in server config"),
			wantRemedy: "no ZooKeeper/Keeper configured",
		},
		{
			name:       "access denied names the grant",
			err:        errors.New("Code: 497. DB::Exception: ddl_bot: Not enough privileges"),
			wantRemedy: "lacks CREATE",
		},
		{
			name:       "syntax error points at the version gap",
			err:        errors.New("Code: 62. DB::Exception: Syntax error: failed at position 42"),
			wantRemedy: "server version",
		},
		{
			name:       "missing macro names the single-node case",
			err:        errors.New("Code: 36. DB::Exception: No macro 'cluster' in config"),
			wantRemedy: `"cluster" macro`,
		},
		{
			name:       "missing shard macro also caught",
			err:        errors.New("DB::Exception: Macro 'shard' is not found in config"),
			wantRemedy: `"shard" macro`,
		},
		{
			name:          "keeper blip is retryable",
			err:           errors.New("code: 999, message: Coordination::Exception: Connection loss"),
			wantRetryable: true,
		},
		{
			name:          "timeout is retryable",
			err:           errors.New("code: 159, message: Timeout exceeded"),
			wantRetryable: true,
		},
		{
			name:          "unknown server code defaults to retryable",
			err:           errors.New("code: 1234, message: something new"),
			wantRetryable: true,
		},
		{
			name:          "transport error with no code is retryable",
			err:           errors.New("connection reset by peer"),
			wantRetryable: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			retryable, remedy := Classify(tt.err)
			if retryable != tt.wantRetryable {
				t.Errorf("retryable = %v, want %v", retryable, tt.wantRetryable)
			}
			if tt.wantRemedy == "" {
				return
			}
			if !strings.Contains(remedy, tt.wantRemedy) {
				t.Errorf("remedy = %q, want substring %q", remedy, tt.wantRemedy)
			}
		})
	}
}

// countingExec fails every statement with a fixed error.
type countingExec struct {
	n   int
	err error
}

func (c *countingExec) ObjectExists(_ context.Context, ref ObjectRef) (bool, error) {
	return ref.IsDatabase(), nil // database exists, object does not
}
func (c *countingExec) Exec(context.Context, string) error { c.n++; return c.err }

func TestApply_PermanentErrorIsNotRetried(t *testing.T) {
	t.Parallel()
	objs := []*DDLObject{obj("events", "clicks", KindTable)}
	plan := planFor(t, objs, nil, fakeLive{}, PlanOpts{DeferInvalid: true})

	ce := &countingExec{err: errors.New("code: 253, message: Replica ... already exists")}
	start := time.Now()
	res, err := NewExecutor(ce, slog.New(slog.NewTextHandler(io.Discard, nil)),
		ExecOpts{Retries: 3, Backoff: 200 * time.Millisecond, ContinueOnError: true}).
		Apply(context.Background(), plan, objs)
	if err != nil {
		t.Fatal(err)
	}
	if ce.n != 1 {
		t.Errorf("permanent error attempted %d times, want exactly 1", ce.n)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Errorf("burned %s of backoff on a permanent error", elapsed)
	}
	if res.Failed != 1 {
		t.Fatalf("summary %s", res.Summary())
	}
	detail := res.Objects[len(res.Objects)-1].Detail
	if !strings.Contains(detail, "code 253") || !strings.Contains(detail, "hint:") {
		t.Errorf("report should carry the code and a hint:\n%s", detail)
	}
}

func TestApply_TransientErrorStillRetries(t *testing.T) {
	t.Parallel()
	objs := []*DDLObject{obj("events", "clicks", KindTable)}
	plan := planFor(t, objs, nil, fakeLive{}, PlanOpts{DeferInvalid: true})

	ce := &countingExec{err: errors.New("code: 999, message: Coordination::Exception: Connection loss")}
	res, err := NewExecutor(ce, slog.New(slog.NewTextHandler(io.Discard, nil)),
		ExecOpts{Retries: 2, Backoff: time.Millisecond, ContinueOnError: true}).
		Apply(context.Background(), plan, objs)
	if err != nil {
		t.Fatal(err)
	}
	if ce.n != 3 { // 1 + 2 retries
		t.Errorf("transient error attempted %d times, want 3", ce.n)
	}
	if res.Failed != 1 {
		t.Errorf("summary %s", res.Summary())
	}
}

type codedErr struct{ code int }

func (e codedErr) Error() string       { return "server said no" }
func (e codedErr) ClickHouseCode() int { return e.code }

// The code must be found however the error reaches us. On a single node
// the driver's text has no "Code: N" in it at all, and missing it would
// make a permanent failure look retryable and drop its hint.
func TestErrorCode_Sources(t *testing.T) {
	for _, tt := range []struct {
		err  error
		want int
	}{
		{codedErr{253}, 253},
		{fmt.Errorf("after 2 attempts: %w", codedErr{62}), 62},
		{errors.New(" DB::Exception (253): Replica /p/replicas/r1 already exists"), 253},
		{errors.New("Code: 497. DB::Exception: Not enough privileges"), 497},
		{errors.New("read tcp 10.0.0.1:9440: connection reset by peer"), -1},
		{nil, -1},
	} {
		if got := ErrorCode(tt.err); got != tt.want {
			t.Errorf("ErrorCode(%v) = %d, want %d", tt.err, got, tt.want)
		}
	}
	// a typed code beats whatever the text happens to contain
	if got := ErrorCode(fmt.Errorf("Code: 999 in text: %w", codedErr{57})); got != 57 {
		t.Errorf("typed code should win, got %d", got)
	}
}

func TestClassify_StaleReplicaNamesTheExactStatement(t *testing.T) {
	err := codedErr{253}
	retry, hint := Classify(fmt.Errorf("Replica /clickhouse/tables/01/events/clicks_raw/replicas/r2 already exists. (REPLICA_ALREADY_EXISTS): %w", err))
	if retry {
		t.Error("253 must not be retried")
	}
	for _, want := range []string{
		"SYSTEM DROP REPLICA 'r2' FROM ZKPATH '/clickhouse/tables/01/events/clicks_raw'",
		"DIFFERENT replica than r2", "8 minutes",
	} {
		if !strings.Contains(hint, want) {
			t.Errorf("hint lacks %q:\n%s", want, hint)
		}
	}
	// without the path in the message, fall back to the general advice
	if _, hint := Classify(err); !strings.Contains(hint, "SYSTEM DROP REPLICA '<replica>'") {
		t.Errorf("generic hint: %s", hint)
	}
}

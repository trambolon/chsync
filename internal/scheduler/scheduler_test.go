package scheduler

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestRun_RunOnStartThenStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var runs atomic.Int32
	started := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Options{Spec: "0 3 1 1 *", Timezone: "Europe/Moscow", RunOnStart: true, GracefulTimeout: 5 * time.Second},
			func(context.Context) { runs.Add(1); started <- struct{}{} }, quiet)
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("run_on_start did not run the job")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("clean shutdown returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	if runs.Load() != 1 {
		t.Errorf("job ran %d times", runs.Load())
	}
}

func TestRun_WithoutRunOnStartWaitsForTheSchedule(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	var runs atomic.Int32
	if err := Run(ctx, Options{Spec: "0 3 1 1 *", Timezone: "UTC", GracefulTimeout: time.Second},
		func(context.Context) { runs.Add(1) }, quiet); err != nil {
		t.Fatal(err)
	}
	if runs.Load() != 0 {
		t.Error("job ran although its schedule is months away")
	}
}

// A job in flight gets its context cancelled and is waited for; one that
// ignores the context makes shutdown give up after GracefulTimeout.
func TestRun_Shutdown(t *testing.T) {
	run := func(job func(context.Context), graceful time.Duration) (error, time.Duration) {
		ctx, cancel := context.WithCancel(context.Background())
		started := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			done <- Run(ctx, Options{Spec: "0 3 1 1 *", Timezone: "UTC", RunOnStart: true, GracefulTimeout: graceful},
				func(c context.Context) { close(started); job(c) }, quiet)
		}()
		<-started
		t0 := time.Now()
		cancel()
		return <-done, time.Since(t0)
	}

	finished := make(chan struct{})
	err, took := run(func(c context.Context) { <-c.Done(); time.Sleep(50 * time.Millisecond); close(finished) }, 5*time.Second)
	if err != nil {
		t.Errorf("cooperative job: %v", err)
	}
	select {
	case <-finished:
	default:
		t.Error("Run returned before the in-flight job finished")
	}
	if took > 3*time.Second {
		t.Errorf("shutdown took %v", took)
	}

	release := make(chan struct{})
	defer close(release)
	err, took = run(func(context.Context) { <-release }, 200*time.Millisecond)
	if !errors.Is(err, ErrShutdownTimeout) {
		t.Errorf("stuck job: want ErrShutdownTimeout, got %v", err)
	}
	if took > 3*time.Second {
		t.Errorf("gave up after %v, want ~200ms", took)
	}
}

func TestRun_RejectsBadInput(t *testing.T) {
	noop := func(context.Context) {}
	if err := Run(context.Background(), Options{Spec: "0 * * * *", Timezone: "Mars/Olympus"}, noop, quiet); err == nil {
		t.Error("bad timezone accepted")
	}
	if err := Run(context.Background(), Options{Spec: "every now and then", Timezone: "UTC"}, noop, quiet); err == nil {
		t.Error("bad cron spec accepted")
	}
}

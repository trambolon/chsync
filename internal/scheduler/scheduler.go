// Package scheduler wraps gocron v2 behind one blocking function. Singleton
// mode with LimitModeReschedule guarantees overlapping snapshot runs never
// happen — a slow cluster walk simply pushes the next tick.
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-co-op/gocron/v2"
)

type Options struct {
	Spec            string // cron expression, e.g. "0 */6 * * *"
	Timezone        string // IANA name
	RunOnStart      bool
	GracefulTimeout time.Duration // how long to wait for an in-flight job on shutdown
}

// ErrShutdownTimeout is returned when an in-flight job did not finish
// within GracefulTimeout. The job's context was cancelled at shutdown, so
// this indicates a job that ignores its context.
var ErrShutdownTimeout = errors.New("scheduler: in-flight job did not stop within graceful timeout")

// Run blocks until ctx is done, executing job on the cron schedule. The
// job receives a context that is cancelled when Run's ctx is.
func Run(ctx context.Context, o Options, job func(context.Context), log *slog.Logger) error {
	loc, err := time.LoadLocation(o.Timezone)
	if err != nil {
		return fmt.Errorf("timezone %q: %w", o.Timezone, err)
	}
	s, err := gocron.NewScheduler(gocron.WithLocation(loc))
	if err != nil {
		return fmt.Errorf("scheduler: %w", err)
	}

	jobOpts := []gocron.JobOption{gocron.WithSingletonMode(gocron.LimitModeReschedule)}
	if o.RunOnStart {
		jobOpts = append(jobOpts, gocron.WithStartAt(gocron.WithStartImmediately()))
	}
	if _, err := s.NewJob(gocron.CronJob(o.Spec, false), gocron.NewTask(func() { job(ctx) }), jobOpts...); err != nil {
		return fmt.Errorf("cron spec %q: %w", o.Spec, err)
	}

	log.Info("scheduler started", "spec", o.Spec, "timezone", o.Timezone, "run_on_start", o.RunOnStart)
	s.Start()
	<-ctx.Done()
	log.Info("scheduler stopping", "graceful_timeout", o.GracefulTimeout.String())

	done := make(chan error, 1)
	go func() { done <- s.Shutdown() }()
	if o.GracefulTimeout <= 0 {
		return <-done
	}
	select {
	case err := <-done:
		return err
	case <-time.After(o.GracefulTimeout):
		return ErrShutdownTimeout
	}
}

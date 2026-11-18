// ddl-saver: ClickHouse DDL snapshots into GitLab, and manual recovery back.
//
// Subcommands:
//
//	run       — daemon: scheduled snapshots (the only long-running mode)
//	snapshot  — one-shot snapshot of all clusters (or -cluster)
//	recover   — manual recovery: plan, confirm, apply (never scheduled)
//	validate  — parse + validate config, optionally check connectivity
//
// Exit codes: 0 success, 1 fatal error (including bad command-line usage),
// 2 partial recovery (objects deferred or failed — see the printed report).
//
// This file only parses flags and wires real dependencies; the flows live
// in internal/app where they are tested with fakes.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"gitlab.example.com/infra/ddl-saver/internal/app"
	"gitlab.example.com/infra/ddl-saver/internal/clickhouse"
	"gitlab.example.com/infra/ddl-saver/internal/config"
	"gitlab.example.com/infra/ddl-saver/internal/gitlab"
	"gitlab.example.com/infra/ddl-saver/internal/observability"
	"gitlab.example.com/infra/ddl-saver/internal/scheduler"
)

// version is stamped by the build (-ldflags "-X main.version=...").
var version = "dev"

const usage = `ddl-saver — ClickHouse DDL snapshots to GitLab, with manual recovery

Usage:
  ddl-saver run       -config <file>                 start the snapshot daemon
  ddl-saver snapshot  -config <file> [-cluster name] [-dry-run] [-allow-empty]
  ddl-saver recover   -config <file> -cluster name [flags]
  ddl-saver validate  -config <file> [-connect]      -connect also checks ClickHouse and GitLab
  ddl-saver version

Recover flags:
  -object db.name     object(s) to restore; repeatable; supports globs
                      (db.prefix_*); omit to restore the whole cluster
  -ref <sha|branch>   git ref to restore from (default: configured branch)
  -with-dependents    also restore repo objects that depend on the targets
  -dry-run            print the plan and exit
  -yes                skip the confirmation prompt
`

func main() { os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr)) }

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return app.ExitFatal
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	code, err := dispatch(ctx, args, stdin, stdout, stderr)
	switch {
	case errors.Is(err, flag.ErrHelp):
		return app.ExitOK
	case errors.As(err, new(usageError)):
		fmt.Fprintf(stderr, "ddl-saver %s: %v\n", args[0], err)
		return app.ExitFatal
	case err != nil:
		slog.Error("fatal", "error", err)
		return app.ExitFatal
	}
	return code
}

func dispatch(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) (int, error) {
	switch args[0] {
	case "run":
		return app.ExitOK, cmdRun(ctx, args[1:], stdout, stderr)
	case "snapshot":
		return app.ExitOK, cmdSnapshot(ctx, args[1:], stdout, stderr)
	case "recover":
		return cmdRecover(ctx, args[1:], stdin, stdout, stderr)
	case "validate":
		return app.ExitOK, cmdValidate(ctx, args[1:], stdout, stderr)
	case "version":
		fmt.Fprintln(stdout, "ddl-saver", version)
		return app.ExitOK, nil
	case "-h", "--help", "help":
		fmt.Fprint(stdout, usage)
		return app.ExitOK, nil
	}
	fmt.Fprintf(stderr, "unknown command %q\n\n%s", args[0], usage)
	return app.ExitFatal, nil
}

// usageError is a mistake on the command line, as opposed to a failure of
// the command itself.
type usageError struct{ msg string }

func (e usageError) Error() string { return e.msg }

func newFlagSet(name string, stderr io.Writer) (*flag.FlagSet, *string) {
	// ContinueOnError, not ExitOnError: the flag package exits with status
	// 2, which this tool reserves for "recovery applied partially".
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	return fs, fs.String("config", "config.toml", "path to config file")
}

// parseFlags rejects anything left over after the flags. A stray word is
// almost always a flag that lost its dash or its name, and silently
// ignoring it changes what the command does: `recover -cluster prod
// events.clicks` would otherwise plan the WHOLE cluster.
func parseFlags(fs *flag.FlagSet, args []string) error {
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return err
		}
		return usageError{err.Error()}
	}
	if fs.NArg() > 0 {
		return usageError{fmt.Sprintf("unexpected argument %q (every option is a -flag; see `ddl-saver help`)", fs.Arg(0))}
	}
	return nil
}

// --- wiring -------------------------------------------------------------

func buildDeps(cfgPath string, stdin io.Reader, stdout io.Writer) (*app.Deps, error) {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		observability.NewLogger("info", "text")
		return nil, err
	}
	log := observability.NewLogger(cfg.App.LogLevel, cfg.App.LogFormat)
	name, email := cfg.GitLab.CommitAuthorParts()
	repo, err := gitlab.NewRepo(gitlab.ClientOptions{
		BaseURL: cfg.GitLab.URL, Token: cfg.GitLab.Token, ProjectPath: cfg.GitLab.ProjectPath,
		AuthorName: name, AuthorEmail: email,
	})
	if err != nil {
		return nil, err
	}
	return &app.Deps{
		Config:  cfg,
		Log:     log,
		Connect: connectClickHouse,
		Repo:    repo,
		Out:     stdout,
		Confirm: terminalConfirm(stdin, stdout),
		Status:  app.NewStatus(),
	}, nil
}

func connectClickHouse(ctx context.Context, cl *config.Cluster) (clickhouse.Client, error) {
	o := clickhouse.Options{
		User:           cl.Auth.User,
		Password:       cl.Auth.Password,
		CompressionLZ4: cl.Connection.Compression == "lz4",
		ConnectTimeout: 10 * time.Second,
	}
	for _, ep := range cl.Connection.Endpoints {
		o.Endpoints = append(o.Endpoints, clickhouse.HostPort{Host: ep.Host, Port: ep.Port})
	}
	if cl.UsesTLS() {
		o.TLS = &clickhouse.TLSFiles{CAFile: cl.Auth.CAFile}
		if cl.Auth.Type == "tls_certificate" {
			o.TLS.CertFile, o.TLS.KeyFile = cl.Auth.CertFile, cl.Auth.KeyFile
		}
	}
	return clickhouse.Connect(ctx, o)
}

func terminalConfirm(stdin io.Reader, stdout io.Writer) app.Confirmer {
	reader := bufio.NewReader(stdin)
	return func(prompt string) (bool, error) {
		fmt.Fprint(stdout, "\n"+prompt)
		line, err := reader.ReadString('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return false, err
		}
		return strings.EqualFold(strings.TrimSpace(line), "y"), nil
	}
}

// --- commands -----------------------------------------------------------

func cmdRun(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, cfgPath := newFlagSet("run", stderr)
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	d, err := buildDeps(*cfgPath, strings.NewReader(""), stdout)
	if err != nil {
		return err
	}
	cfg := d.Config
	if cfg.Scheduler.Spec == "" {
		return errors.New("scheduler.spec is required for run mode")
	}
	d.Confirm = nil // the daemon never asks questions

	observability.ServeHealth(ctx, observability.HealthOptions{
		Addr:   cfg.App.HealthAddr,
		Ready:  d.Status.Ready,
		Status: func() any { return d.Status.Snapshot() },
	}, d.Log)

	job := func(jctx context.Context) {
		runCtx, cancel := context.WithTimeout(jctx, cfg.Scheduler.JobTimeout.Std())
		defer cancel()
		if err := d.SnapshotAll(runCtx, app.SnapshotOpts{}); err != nil {
			d.Log.Error("scheduled snapshot finished with errors", "error", err)
		}
	}
	return scheduler.Run(ctx, scheduler.Options{
		Spec:            cfg.Scheduler.Spec,
		Timezone:        cfg.Scheduler.Timezone,
		RunOnStart:      cfg.Scheduler.RunOnStart,
		GracefulTimeout: cfg.App.GracefulTimeout.Std(),
	}, job, d.Log)
}

func cmdSnapshot(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, cfgPath := newFlagSet("snapshot", stderr)
	var o app.SnapshotOpts
	fs.StringVar(&o.Cluster, "cluster", "", "snapshot only this cluster")
	fs.BoolVar(&o.DryRun, "dry-run", false, "compute the diff but do not commit")
	fs.BoolVar(&o.AllowEmpty, "allow-empty", false,
		"accept that the cluster holds no objects and archive its files (requires -cluster)")
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if o.AllowEmpty && o.Cluster == "" {
		return usageError{"-allow-empty requires -cluster: name the cluster that is really empty"}
	}

	d, err := buildDeps(*cfgPath, strings.NewReader(""), stdout)
	if err != nil {
		return err
	}
	if o.Cluster != "" {
		if _, err := d.Config.FindCluster(o.Cluster); err != nil {
			return err
		}
	}
	return d.SnapshotAll(ctx, o)
}

type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }

func cmdRecover(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) (int, error) {
	fs, cfgPath := newFlagSet("recover", stderr)
	var o app.RecoverOpts
	var objects multiFlag
	fs.StringVar(&o.Cluster, "cluster", "", "cluster to recover (required)")
	fs.StringVar(&o.Ref, "ref", "", "git ref to restore from (default: configured branch)")
	fs.Var(&objects, "object", "object to restore (db.name, repeatable, globs allowed)")
	fs.BoolVar(&o.WithDependents, "with-dependents", false, "also restore dependents of the targets")
	fs.BoolVar(&o.DryRun, "dry-run", false, "print the plan and exit")
	fs.BoolVar(&o.Yes, "yes", false, "skip the confirmation prompt")
	if err := parseFlags(fs, args); err != nil {
		return app.ExitFatal, err
	}
	if o.Cluster == "" {
		return app.ExitFatal, usageError{"-cluster is required: recovery never guesses which cluster you mean"}
	}
	o.Objects = objects

	d, err := buildDeps(*cfgPath, stdin, stdout)
	if err != nil {
		return app.ExitFatal, err
	}
	return d.Recover(ctx, o)
}

func cmdValidate(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, cfgPath := newFlagSet("validate", stderr)
	connect := fs.Bool("connect", false, "also check ClickHouse and GitLab connectivity")
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	d, err := buildDeps(*cfgPath, strings.NewReader(""), stdout)
	if err != nil {
		return err
	}
	report, err := d.Validate(ctx, *connect)
	fmt.Fprint(stdout, report)
	return err
}

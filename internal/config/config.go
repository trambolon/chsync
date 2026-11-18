// Package config: TOML configuration with strict ${ENV} interpolation.
//
// Design decisions:
//   - No app.mode: whether the process is a daemon or a one-shot tool is
//     decided by the CLI subcommand (run / snapshot / recover / validate),
//     never by configuration.
//   - Missing environment variables are a hard load error, not an empty
//     string: a snapshot job running with an empty GitLab token should fail
//     at boot, not at the first commit.
//   - recovery.create_strategy is locked to "if_not_exists"; any other value
//     fails validation. The tool never drops or replaces objects.
package config

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

type Config struct {
	App       App       `toml:"app"`
	Scheduler Scheduler `toml:"scheduler"`
	Clusters  []Cluster `toml:"clusters"`
	GitLab    GitLab    `toml:"gitlab"`
	Recovery  Recovery  `toml:"recovery"`
}

type App struct {
	Name            string   `toml:"name"`
	LogLevel        string   `toml:"log_level"`  // debug | info | warn | error
	LogFormat       string   `toml:"log_format"` // text | json
	GracefulTimeout Duration `toml:"graceful_timeout"`
	HealthAddr      string   `toml:"health_addr"` // e.g. ":8080"; empty disables
}

type Scheduler struct {
	Spec       string   `toml:"spec"`     // cron expression
	Timezone   string   `toml:"timezone"` // IANA name
	RunOnStart bool     `toml:"run_on_start"`
	JobTimeout Duration `toml:"job_timeout"` // upper bound for one snapshot pass over all clusters
}

type Cluster struct {
	Name       string     `toml:"name"`
	Connection Connection `toml:"connection"`
	Auth       Auth       `toml:"auth"`
	Filter     Filter     `toml:"filter"`
}

type Connection struct {
	Scheme      string `toml:"scheme"`      // "native" (http reserved)
	Compression string `toml:"compression"` // "lz4" | "none"
	// Secure turns on TLS for the native protocol (ClickHouse port 9440)
	// with any auth type. The server certificate is checked against
	// auth.ca_file, or the system trust store when ca_file is empty.
	// auth.type = "tls_certificate" implies TLS regardless of this flag.
	Secure    bool       `toml:"secure"`
	Endpoints []Endpoint `toml:"endpoints"`
}

// UsesTLS reports whether connections to this cluster are encrypted.
func (cl *Cluster) UsesTLS() bool {
	return cl.Connection.Secure || cl.Auth.Type == "tls_certificate"
}

type Endpoint struct {
	Host string `toml:"host"`
	Port int    `toml:"port"`
}

type Auth struct {
	Type     string `toml:"type"` // "password" | "tls_certificate"
	User     string `toml:"user"`
	Password string `toml:"password"`
	CertFile string `toml:"cert_file"`
	KeyFile  string `toml:"key_file"`
	CAFile   string `toml:"ca_file"`
}

type Filter struct {
	SkipDatabases   []string `toml:"skip_databases"`
	SkipTablesRegex []string `toml:"skip_tables_regex"`
	IncludeEngines  []string `toml:"include_engines"`
}

type GitLab struct {
	URL          string `toml:"url"`
	Token        string `toml:"token"`
	ProjectPath  string `toml:"project_path"` // owner/repo
	Branch       string `toml:"branch"`
	CommitAuthor string `toml:"commit_author"` // "Name <email>"
	BasePath     string `toml:"base_path"`
	DryRun       bool   `toml:"dry_run"`
	// OnDeleted controls what happens to repo files whose objects have
	// disappeared from ClickHouse (a dropped table, or every table of a
	// dropped database):
	//   keep      — leave the file in place, log a warning
	//   deprecate — move it to <base>/<cluster>/_deprecated/<db>/<kind>/ (default)
	//   prune     — delete it
	OnDeleted       string `toml:"on_deleted"`
	CommitBatchSize int    `toml:"commit_batch_size"` // max file actions per commit
}

type Recovery struct {
	CreateStrategy      string `toml:"create_strategy"` // locked: "if_not_exists"
	DependencyCheck     bool   `toml:"dependency_check"`
	DeferInvalidObjects bool   `toml:"defer_invalid_objects"`
	// RequireConfirmation is a pointer so that an ABSENT key defaults to
	// true. With a plain bool, omitting the key from config.toml would
	// silently disable the interactive gate — the single most dangerous
	// default this tool could have.
	RequireConfirmation *bool    `toml:"require_confirmation"`
	Retries             int      `toml:"retries"`
	RetryBackoff        Duration `toml:"retry_backoff"`
}

// Duration parses TOML strings like "30s".
type Duration time.Duration

func (d *Duration) UnmarshalText(b []byte) error {
	v, err := time.ParseDuration(string(b))
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

func (d Duration) Std() time.Duration { return time.Duration(d) }

// ApplyDefaults fills unset optional fields.
func (c *Config) ApplyDefaults() {
	if c.App.Name == "" {
		c.App.Name = "ddlSaver"
	}
	if c.App.LogLevel == "" {
		c.App.LogLevel = "info"
	}
	if c.App.LogFormat == "" {
		c.App.LogFormat = "text"
	}
	if c.App.GracefulTimeout == 0 {
		c.App.GracefulTimeout = Duration(30 * time.Second)
	}
	if c.Scheduler.Timezone == "" {
		c.Scheduler.Timezone = "UTC"
	}
	if c.Scheduler.JobTimeout == 0 {
		c.Scheduler.JobTimeout = Duration(30 * time.Minute)
	}
	if c.GitLab.Branch == "" {
		c.GitLab.Branch = "main"
	}
	if c.GitLab.BasePath == "" {
		c.GitLab.BasePath = "schemas"
	}
	if c.GitLab.OnDeleted == "" {
		c.GitLab.OnDeleted = "deprecate"
	}
	if c.GitLab.CommitBatchSize <= 0 {
		c.GitLab.CommitBatchSize = 100 // GitLab commit API practical limit
	}
	if c.Recovery.CreateStrategy == "" {
		c.Recovery.CreateStrategy = "if_not_exists"
	}
	if c.Recovery.RequireConfirmation == nil {
		yes := true
		c.Recovery.RequireConfirmation = &yes
	}
	if c.Recovery.Retries == 0 {
		c.Recovery.Retries = 3
	}
	if c.Recovery.RetryBackoff == 0 {
		c.Recovery.RetryBackoff = Duration(2 * time.Second)
	}
	for i := range c.Clusters {
		cl := &c.Clusters[i]
		if cl.Connection.Scheme == "" {
			cl.Connection.Scheme = "native"
		}
		if cl.Connection.Compression == "" {
			cl.Connection.Compression = "lz4"
		}
		if cl.Auth.Type == "" {
			cl.Auth.Type = "password"
		}
		for j := range cl.Connection.Endpoints {
			if cl.Connection.Endpoints[j].Port == 0 {
				cl.Connection.Endpoints[j].Port = 9000
				if cl.UsesTLS() {
					cl.Connection.Endpoints[j].Port = 9440
				}
			}
		}
	}
}

// Validate returns every problem found, joined, so operators fix the config
// in one pass instead of one error per run.
func (c *Config) Validate() error {
	var errs []string
	add := func(format string, a ...any) { errs = append(errs, fmt.Sprintf(format, a...)) }

	switch c.App.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		add("app.log_level: unknown level %q", c.App.LogLevel)
	}
	switch c.App.LogFormat {
	case "text", "json":
	default:
		add("app.log_format: must be text or json, got %q", c.App.LogFormat)
	}

	if c.Scheduler.Spec != "" {
		if _, err := time.LoadLocation(c.Scheduler.Timezone); err != nil {
			add("scheduler.timezone: %v", err)
		}
	}
	if c.App.GracefulTimeout <= 0 || c.Scheduler.JobTimeout <= 0 {
		add("app.graceful_timeout and scheduler.job_timeout must be positive")
	}

	if len(c.Clusters) == 0 {
		add("clusters: at least one cluster is required")
	}
	seen := map[string]bool{}
	for i, cl := range c.Clusters {
		where := fmt.Sprintf("clusters[%d] (%s)", i, cl.Name)
		if cl.Name == "" {
			add("%s: name is required", where)
		} else if seen[cl.Name] {
			add("%s: duplicate cluster name", where)
		}
		seen[cl.Name] = true

		if cl.Connection.Scheme != "native" {
			add("%s: connection.scheme %q not supported (only \"native\")", where, cl.Connection.Scheme)
		}
		if cl.Connection.Compression != "lz4" && cl.Connection.Compression != "none" {
			add("%s: connection.compression must be lz4 or none", where)
		}
		if len(cl.Connection.Endpoints) == 0 {
			add("%s: at least one connection endpoint is required", where)
		}
		for j, ep := range cl.Connection.Endpoints {
			if ep.Host == "" {
				add("%s: endpoints[%d].host is required", where, j)
			}
			if ep.Port <= 0 || ep.Port > 65535 {
				add("%s: endpoints[%d].port %d out of range", where, j, ep.Port)
			}
		}

		switch cl.Auth.Type {
		case "password":
			if cl.Auth.User == "" {
				add("%s: auth.user is required for password auth", where)
			}
		case "tls_certificate":
			if cl.Auth.CertFile == "" || cl.Auth.KeyFile == "" {
				add("%s: auth.cert_file and auth.key_file are required for tls_certificate auth", where)
			}
		default:
			add("%s: auth.type %q not supported", where, cl.Auth.Type)
		}
		if cl.Auth.CAFile != "" && !cl.UsesTLS() {
			// Silently ignoring a CA file would mean a plaintext connection
			// the operator believes is verified TLS.
			add("%s: auth.ca_file is set but TLS is off: set connection.secure = true", where)
		}

		for j, expr := range cl.Filter.SkipTablesRegex {
			if _, err := regexp.Compile(expr); err != nil {
				add("%s: filter.skip_tables_regex[%d]: %v", where, j, err)
			}
		}
	}

	if c.GitLab.URL == "" {
		add("gitlab.url is required")
	}
	if c.GitLab.Token == "" {
		add("gitlab.token is required")
	}
	if c.GitLab.ProjectPath == "" || !strings.Contains(c.GitLab.ProjectPath, "/") {
		add("gitlab.project_path must be in owner/repo form, got %q", c.GitLab.ProjectPath)
	}
	if a := c.GitLab.CommitAuthor; a != "" && (!strings.Contains(a, "<") || !strings.HasSuffix(a, ">")) {
		add("gitlab.commit_author must look like \"Name <email>\", got %q", a)
	}
	switch c.GitLab.OnDeleted {
	case "keep", "deprecate", "prune":
	default:
		add("gitlab.on_deleted must be keep, deprecate or prune, got %q", c.GitLab.OnDeleted)
	}

	if c.Recovery.CreateStrategy != "if_not_exists" {
		add("recovery.create_strategy is locked to \"if_not_exists\" (got %q): this tool never drops or replaces objects", c.Recovery.CreateStrategy)
	}

	if len(errs) > 0 {
		return errors.New("invalid configuration:\n  - " + strings.Join(errs, "\n  - "))
	}
	return nil
}

// FindCluster returns the cluster config by name.
func (c *Config) FindCluster(name string) (*Cluster, error) {
	for i := range c.Clusters {
		if c.Clusters[i].Name == name {
			return &c.Clusters[i], nil
		}
	}
	known := make([]string, 0, len(c.Clusters))
	for _, cl := range c.Clusters {
		known = append(known, cl.Name)
	}
	return nil, fmt.Errorf("cluster %q not found in config (known: %s)", name, strings.Join(known, ", "))
}

// CommitAuthorParts splits "Name <email>".
func (g GitLab) CommitAuthorParts() (name, email string) {
	a := g.CommitAuthor
	i := strings.LastIndex(a, "<")
	if i < 0 {
		return strings.TrimSpace(a), ""
	}
	return strings.TrimSpace(a[:i]), strings.TrimSuffix(strings.TrimSpace(a[i+1:]), ">")
}

// ConfirmationRequired reports whether recovery must prompt before applying.
// Safe by default: only an explicit `require_confirmation = false` disables it.
func (r Recovery) ConfirmationRequired() bool {
	return r.RequireConfirmation == nil || *r.RequireConfirmation
}

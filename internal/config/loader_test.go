package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The shipped example must load exactly as the Quickstart says, and a
// secret must arrive byte for byte whatever it contains.
func TestLoad_ExampleConfigWithHostileSecret(t *testing.T) {
	const password = `p"a\ss'w#rd ${NOT_A_VAR} ` + "\t€"
	t.Setenv("CH_PROD_USER", "ddl_reader")
	t.Setenv("CH_PROD_PASSWORD", password)
	t.Setenv("GITLAB_TOKEN", `glpat-"x"\y`)

	cfg, err := Load("../../examples/config.toml")
	if err != nil {
		t.Fatalf("example config does not load: %v", err)
	}
	cl := cfg.Clusters[0]
	if cl.Auth.Password != password {
		t.Errorf("password changed in transit:\n got %q\nwant %q", cl.Auth.Password, password)
	}
	if cfg.GitLab.Token != `glpat-"x"\y` {
		t.Errorf("token changed in transit: %q", cfg.GitLab.Token)
	}
	if cl.Name != "prod-analytics" || len(cl.Connection.Endpoints) != 2 || cl.Connection.Endpoints[0].Port != 9000 {
		t.Errorf("cluster parsed wrong: %+v", cl)
	}
	if cfg.Scheduler.JobTimeout.Std() != 30*time.Minute || !cfg.Recovery.ConfirmationRequired() {
		t.Errorf("durations/defaults: %+v %+v", cfg.Scheduler, cfg.Recovery)
	}
	if cfg.GitLab.OnDeleted != "deprecate" || cfg.Scheduler.Timezone != "Europe/Moscow" {
		t.Errorf("gitlab/scheduler: %+v", cfg.GitLab)
	}
}

func TestLoad_SecureCluster(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.toml")
	body := `
[[clusters]]
name = "prod"
  [clusters.connection]
  secure = true
  endpoints = [{ host = "ch-1.prod.internal" }, { host = "ch-2.prod.internal", port = ${PORT} }]
  [clusters.auth]
  user = "u"
  password = '${LITERAL}'
  ca_file = "/etc/ssl/company-root.crt"
[gitlab]
url = "https://gitlab.example.com"
token = "t"
project_path = "infra/schemas"
commit_author = "Bot <bot@example.com>"
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PORT", "9441")
	t.Setenv("LITERAL", `back\slash "quoted"`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	cl := cfg.Clusters[0]
	if !cl.UsesTLS() || cl.Connection.Endpoints[0].Port != 9440 || cl.Connection.Endpoints[1].Port != 9441 {
		t.Errorf("secure cluster: %+v", cl.Connection)
	}
	if cl.Auth.Password != `back\slash "quoted"` {
		t.Errorf("literal string: %q", cl.Auth.Password)
	}
}

func TestLoad_Errors(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	for name, tt := range map[string]struct{ path, want string }{
		"missing file": {filepath.Join(dir, "nope.toml"), "read config"},
		"bad toml":     {write("bad.toml", "x = = 1\n"), "parse config"},
		"missing env":  {write("env.toml", `a = "${DDLSAVER_TEST_UNSET}"`), "undefined environment variables: DDLSAVER_TEST_UNSET"},
		"invalid":      {write("empty.toml", "[app]\nlog_level = \"loud\"\n"), "log_level"},
	} {
		if _, err := Load(tt.path); err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("%s: want %q, got %v", name, tt.want, err)
		}
	}
}

package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

func baseConfig() *Config {
	c := &Config{
		Clusters: []Cluster{{
			Name:       "prod",
			Connection: Connection{Endpoints: []Endpoint{{Host: "ch1"}}},
			Auth:       Auth{User: "bot"},
		}},
		GitLab: GitLab{
			URL: "https://gitlab.example.com", Token: "t",
			ProjectPath: "infra/schemas", CommitAuthor: "Bot <bot@example.com>",
		},
	}
	c.ApplyDefaults()
	return c
}

func TestDefaultsAndValidate_OK(t *testing.T) {
	c := baseConfig()
	if err := c.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	if c.Clusters[0].Connection.Endpoints[0].Port != 9000 {
		t.Error("default port not applied")
	}
	if c.Recovery.CreateStrategy != "if_not_exists" {
		t.Error("default create_strategy not applied")
	}
	if c.App.GracefulTimeout.Std() != 30*time.Second {
		t.Error("default graceful_timeout not applied")
	}
	if c.GitLab.CommitBatchSize != 100 {
		t.Error("default commit_batch_size not applied")
	}
	if c.GitLab.OnDeleted != "deprecate" {
		t.Error("default on_deleted not applied")
	}
	if c.Scheduler.JobTimeout.Std() != 30*time.Minute {
		t.Error("default job_timeout not applied")
	}
}

func TestValidate_CollectsAllErrors(t *testing.T) {
	c := baseConfig()
	c.Clusters = append(c.Clusters, Cluster{Name: "prod"}) // dup + missing endpoint
	c.Clusters[1].Connection.Scheme = "http"
	c.Clusters[1].Auth.Type = "ssh_key"
	c.GitLab.ProjectPath = "no-slash"
	c.Recovery.CreateStrategy = "drop_and_create"
	c.GitLab.OnDeleted = "archive"
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{
		"duplicate cluster name", "scheme", "endpoint", "auth.type",
		"owner/repo", "locked to \"if_not_exists\"", "on_deleted",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should mention %q, got:\n%v", want, err)
		}
	}
}

func TestValidate_TLSAuthRequiresFiles(t *testing.T) {
	c := baseConfig()
	c.Clusters[0].Auth = Auth{Type: "tls_certificate", User: "bot"}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "cert_file") {
		t.Fatalf("want cert_file error, got %v", err)
	}
	c.Clusters[0].Auth.CertFile = "/c.crt"
	c.Clusters[0].Auth.KeyFile = "/c.key"
	if err := c.Validate(); err != nil {
		t.Fatalf("tls config rejected: %v", err)
	}
}

func TestSecurePassword_DefaultsAndValidation(t *testing.T) {
	c := &Config{
		Clusters: []Cluster{{
			Name: "prod",
			Connection: Connection{Secure: true, Endpoints: []Endpoint{
				{Host: "ch1"}, {Host: "ch2", Port: 9441},
			}},
			Auth: Auth{User: "bot", Password: "p", CAFile: "/etc/ssl/company-root.crt"},
		}},
		GitLab: baseConfig().GitLab,
	}
	c.ApplyDefaults()
	if err := c.Validate(); err != nil {
		t.Fatalf("password over TLS with a CA file rejected: %v", err)
	}
	if got := c.Clusters[0].Connection.Endpoints[0].Port; got != 9440 {
		t.Errorf("secure default port = %d, want 9440", got)
	}
	if got := c.Clusters[0].Connection.Endpoints[1].Port; got != 9441 {
		t.Errorf("explicit port overwritten: %d", got)
	}
	if !c.Clusters[0].UsesTLS() {
		t.Error("UsesTLS() = false for secure = true")
	}
}

func TestValidate_CAFileWithoutTLSRejected(t *testing.T) {
	c := baseConfig()
	c.Clusters[0].Auth.CAFile = "/etc/ssl/company-root.crt"
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "connection.secure = true") {
		t.Fatalf("want ca_file/secure error, got %v", err)
	}
}

func TestTLSCertificateImpliesTLS(t *testing.T) {
	c := baseConfig()
	c.Clusters[0].Auth = Auth{Type: "tls_certificate", User: "bot", CertFile: "/c", KeyFile: "/k", CAFile: "/ca"}
	if !c.Clusters[0].UsesTLS() {
		t.Fatal("tls_certificate must imply TLS")
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("tls_certificate with ca_file rejected: %v", err)
	}
}

func TestValidate_BadRegex(t *testing.T) {
	c := baseConfig()
	c.Clusters[0].Filter.SkipTablesRegex = []string{"^\\.", "]["}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "skip_tables_regex[1]") {
		t.Fatalf("want regex error, got %v", err)
	}
}

func TestExpandEnv(t *testing.T) {
	env := map[string]string{"CH_USER": "bot", "CH_PASS": "s3cr3t"}
	lookup := func(k string) (string, bool) { v, ok := env[k]; return v, ok }

	out, err := ExpandEnv(`user = "${CH_USER}" pass = "${CH_PASS}"`, lookup)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, `"bot"`) || !strings.Contains(out, `"s3cr3t"`) {
		t.Errorf("expansion failed: %s", out)
	}

	_, err = ExpandEnv(`a = "${MISSING_ONE}" b = "${MISSING_TWO}" c = "${MISSING_ONE}"`, lookup)
	if err == nil {
		t.Fatal("expected error for missing vars")
	}
	if !strings.Contains(err.Error(), "MISSING_ONE, MISSING_TWO") {
		t.Errorf("error should list all missing vars once, sorted: %v", err)
	}
}

func TestExpandEnv_IgnoresComments(t *testing.T) {
	lookup := func(k string) (string, bool) { return map[string]string{"PASS": "s3cr3t"}[k], k == "PASS" }
	in := "# Every ${VAR} is interpolated\n" +
		"password = \"${PASS}\"  # was ${OLD_PASS}\n" +
		"# password = \"${OLD_PASS}\"\n" +
		"url = \"https://h/#${PASS}\" # hash inside a string is not a comment\n" +
		"lit = 'a#${PASS}'\n" +
		"esc = \"q\\\"#${PASS}\"\n" +
		"multi = \"\"\"\n# not a comment ${PASS}\n\"\"\"\n"
	out, err := ExpandEnv(in, lookup)
	if err != nil {
		t.Fatalf("comments must not be expanded: %v", err)
	}
	for _, want := range []string{
		"# Every ${VAR} is interpolated", `password = "s3cr3t"  # was ${OLD_PASS}`,
		`# password = "${OLD_PASS}"`, `url = "https://h/#s3cr3t"`, `lit = 'a#s3cr3t'`,
		`esc = "q\"#s3cr3t"`, "# not a comment s3cr3t",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestExpandEnv_SecretsStayOneValue(t *testing.T) {
	env := map[string]string{
		"QUOTE":   `pa"ss`,
		"SLASH":   `C:\new\tab`,
		"INJECT":  "x\"\nurl = \"https://evil.example",
		"APOS":    "it's",
		"PORT":    "9440",
		"CONTROL": "a\x01b",
	}
	lookup := func(k string) (string, bool) { v, ok := env[k]; return v, ok }

	out, err := ExpandEnv("a = \"${QUOTE}\"\nb = \"${SLASH}\"\nc = \"${INJECT}\"\nd = \"${APOS}\"\nport = ${PORT}\ne = \"${CONTROL}\"\n", lookup)
	if err != nil {
		t.Fatal(err)
	}
	want := "a = \"pa\\\"ss\"\n" +
		"b = \"C:\\\\new\\\\tab\"\n" +
		"c = \"x\\\"\\nurl = \\\"https://evil.example\"\n" +
		"d = \"it's\"\n" +
		"port = 9440\n" +
		"e = \"a\\u0001b\"\n"
	if out != want {
		t.Errorf("got:\n%s\nwant:\n%s", out, want)
	}
	if n := strings.Count(out, "\n"); n != 6 {
		t.Errorf("a secret added a line to the config: %d lines", n)
	}

	// literal strings cannot hold a quote: name the variable, do not guess
	_, err = ExpandEnv("p = '${APOS}'\n", lookup)
	if err == nil || !strings.Contains(err.Error(), "APOS") || !strings.Contains(err.Error(), "double-quoted") {
		t.Errorf("want an error naming APOS, got %v", err)
	}
	// ...but hold backslashes and double quotes verbatim
	if out, err := ExpandEnv("p = '${SLASH}${QUOTE}'", lookup); err != nil || out != `p = 'C:\new\tabpa"ss'` {
		t.Errorf("literal string: %q %v", out, err)
	}
	// an escaped quote does not end the string
	if out, _ := ExpandEnv(`p = "a\"${QUOTE}"`, lookup); out != `p = "a\"pa\"ss"` {
		t.Errorf("escaped quote: %q", out)
	}
	// multi-line basic strings escape too
	if out, _ := ExpandEnv("p = \"\"\"\n${QUOTE}\"\"\"", lookup); out != "p = \"\"\"\npa\\\"ss\"\"\"" {
		t.Errorf("multi-line: %q", out)
	}
}

// The Quickstart copies examples/config.toml and exports exactly these
// three variables; the env step must succeed with nothing else set.
func TestExampleConfig_EnvExpansion(t *testing.T) {
	raw, err := os.ReadFile("../../examples/config.toml")
	if err != nil {
		t.Fatal(err)
	}
	quickstart := map[string]bool{"CH_PROD_USER": true, "CH_PROD_PASSWORD": true, "GITLAB_TOKEN": true}
	if _, err := ExpandEnv(string(raw), func(k string) (string, bool) { return "x", quickstart[k] }); err != nil {
		t.Fatalf("example config needs more than the Quickstart variables: %v", err)
	}
}

func TestCommitAuthorParts(t *testing.T) {
	g := GitLab{CommitAuthor: "DDL Bot <ddlbot@example.com>"}
	name, email := g.CommitAuthorParts()
	if name != "DDL Bot" || email != "ddlbot@example.com" {
		t.Errorf("got %q / %q", name, email)
	}
}

func TestFindCluster(t *testing.T) {
	c := baseConfig()
	if _, err := c.FindCluster("prod"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.FindCluster("nope"); err == nil || !strings.Contains(err.Error(), "known: prod") {
		t.Fatalf("want helpful not-found error, got %v", err)
	}
}

func TestConfirmationDefaultsToRequired(t *testing.T) {
	// The dangerous case: the key is simply absent from config.toml.
	c := baseConfig()
	if !c.Recovery.ConfirmationRequired() {
		t.Fatal("omitting require_confirmation must NOT disable the confirmation prompt")
	}
	// Only an explicit false turns it off.
	no := false
	c.Recovery.RequireConfirmation = &no
	if c.Recovery.ConfirmationRequired() {
		t.Error("explicit false should disable the prompt")
	}
	yes := true
	c.Recovery.RequireConfirmation = &yes
	if !c.Recovery.ConfirmationRequired() {
		t.Error("explicit true should require the prompt")
	}
}

func TestValidate_TimezoneChecked(t *testing.T) {
	t.Parallel()
	c := baseConfig()
	c.Scheduler.Spec = "0 * * * *"
	c.Scheduler.Timezone = "Mars/Olympus_Mons"
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "scheduler.timezone") {
		t.Fatalf("want timezone error, got %v", err)
	}
}

func TestDuration_UnmarshalText(t *testing.T) {
	t.Parallel()
	var d Duration
	if err := d.UnmarshalText([]byte("90s")); err != nil || d.Std() != 90*time.Second {
		t.Errorf("parse: %v %v", d, err)
	}
	if err := d.UnmarshalText([]byte("soon")); err == nil {
		t.Error("garbage duration must error")
	}
}

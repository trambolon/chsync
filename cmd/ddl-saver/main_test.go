package main

import (
	"bytes"
	"strings"
	"testing"
)

func exec(args ...string) (code int, stdout, stderr string) {
	var out, errb bytes.Buffer
	code = run(args, strings.NewReader(""), &out, &errb)
	return code, out.String(), errb.String()
}

// Exit status 2 means "recovery applied partially". A typo on the command
// line must never be reported as that — and must never reach the config,
// let alone a cluster.
func TestUsageErrorsExitOneBeforeTouchingAnything(t *testing.T) {
	const missing = "/nonexistent/config.toml" // loading it would fail differently
	for name, tt := range map[string]struct {
		args []string
		want string
	}{
		"unknown flag":             {[]string{"recover", "-config", missing, "-cluster", "prod", "-objekt", "a.b"}, "flag provided but not defined: -objekt"},
		"object without its flag":  {[]string{"recover", "-config", missing, "-cluster", "prod", "events.clicks"}, `unexpected argument "events.clicks"`},
		"flag after a stray word":  {[]string{"recover", "-config", missing, "prod", "-dry-run"}, `unexpected argument "prod"`},
		"recover without cluster":  {[]string{"recover", "-config", missing}, "-cluster is required"},
		"snapshot stray word":      {[]string{"snapshot", "-config", missing, "prod"}, `unexpected argument "prod"`},
		"allow-empty needs a name": {[]string{"snapshot", "-config", missing, "-allow-empty"}, "-allow-empty requires -cluster"},
		"bad bool":                 {[]string{"validate", "-config", missing, "-connect=maybe"}, "invalid boolean value"},
		"run stray word":           {[]string{"run", "now"}, `unexpected argument "now"`},
		"unknown command":          {[]string{"restore"}, `unknown command "restore"`},
		"no command":               {nil, "Usage:"},
	} {
		code, _, stderr := exec(tt.args...)
		if code != 1 {
			t.Errorf("%s: exit %d, want 1", name, code)
		}
		if !strings.Contains(stderr, tt.want) {
			t.Errorf("%s: stderr lacks %q:\n%s", name, tt.want, stderr)
		}
		if strings.Contains(stderr, "read config") {
			t.Errorf("%s: went on to load the config", name)
		}
	}
}

func TestHelpAndVersion(t *testing.T) {
	if code, out, _ := exec("version"); code != 0 || !strings.HasPrefix(out, "ddl-saver ") {
		t.Errorf("version: %d %q", code, out)
	}
	if code, out, _ := exec("help"); code != 0 || !strings.Contains(out, "-allow-empty") {
		t.Errorf("help: %d %q", code, out)
	}
	// -h on a subcommand prints its flags and is not an error
	if code, _, stderr := exec("recover", "-h"); code != 0 || !strings.Contains(stderr, "-with-dependents") {
		t.Errorf("recover -h: %d %q", code, stderr)
	}
}

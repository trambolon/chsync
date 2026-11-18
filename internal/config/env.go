package config

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

var envRef = regexp.MustCompile(`^\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// tomlCtx is where in a TOML document the scanner currently is. It decides
// how a substituted value has to be written so that it stays ONE value.
type tomlCtx int

const (
	ctxBare      tomlCtx = iota // outside any string
	ctxComment                  // after '#', until end of line
	ctxBasic                    // "..."
	ctxLiteral                  // '...'
	ctxMLBasic                  // """..."""
	ctxMLLiteral                // '''...'''
)

// ExpandEnv replaces ${VAR} using lookup. Unknown variables are collected
// and returned as a single error naming every missing variable.
//
// Substitution is context-aware rather than textual, because secrets are
// arbitrary bytes:
//
//   - in a comment nothing is expanded, so `# password = "${OLD}"` cannot
//     make loading fail;
//   - in a double-quoted string the value is escaped, so a password
//     containing `"` or `\` neither breaks the file nor spills into the
//     next key;
//   - in a single-quoted (literal) string TOML has no escapes at all, so a
//     value that could not be represented is an error naming the variable.
func ExpandEnv(raw string, lookup func(string) (string, bool)) (string, error) {
	var out strings.Builder
	out.Grow(len(raw))
	missing := map[string]bool{}
	var unrepresentable []string

	ctx := ctxBare
	for i := 0; i < len(raw); {
		c := raw[i]

		if c == '$' && ctx != ctxComment {
			if m := envRef.FindStringSubmatch(raw[i:]); m != nil {
				name := m[1]
				if v, ok := lookup(name); !ok {
					missing[name] = true
					out.WriteString(m[0])
				} else if enc, ok := encodeFor(ctx, v); ok {
					out.WriteString(enc)
				} else {
					unrepresentable = append(unrepresentable, name)
				}
				i += len(m[0])
				continue
			}
		}

		switch ctx {
		case ctxBare:
			switch {
			case c == '#':
				ctx = ctxComment
			case strings.HasPrefix(raw[i:], `"""`):
				ctx = ctxMLBasic
				out.WriteString(`""`)
				i += 2
			case strings.HasPrefix(raw[i:], `'''`):
				ctx = ctxMLLiteral
				out.WriteString(`''`)
				i += 2
			case c == '"':
				ctx = ctxBasic
			case c == '\'':
				ctx = ctxLiteral
			}
		case ctxComment:
			if c == '\n' {
				ctx = ctxBare
			}
		case ctxBasic:
			switch {
			case c == '\\' && i+1 < len(raw):
				out.WriteByte(c)
				i++
				c = raw[i]
			case c == '"' || c == '\n': // newline: unterminated, let TOML report it
				ctx = ctxBare
			}
		case ctxLiteral:
			if c == '\'' || c == '\n' {
				ctx = ctxBare
			}
		case ctxMLBasic:
			switch {
			case c == '\\' && i+1 < len(raw):
				out.WriteByte(c)
				i++
				c = raw[i]
			case strings.HasPrefix(raw[i:], `"""`):
				ctx = ctxBare
				out.WriteString(`""`)
				i += 2
			}
		case ctxMLLiteral:
			if strings.HasPrefix(raw[i:], `'''`) {
				ctx = ctxBare
				out.WriteString(`''`)
				i += 2
			}
		}
		out.WriteByte(c)
		i++
	}

	var problems []string
	if len(missing) > 0 {
		names := make([]string, 0, len(missing))
		for n := range missing {
			names = append(names, n)
		}
		slices.Sort(names)
		problems = append(problems, "undefined environment variables: "+strings.Join(names, ", "))
	}
	if len(unrepresentable) > 0 {
		slices.Sort(unrepresentable)
		problems = append(problems, fmt.Sprintf(
			"environment variables %s contain characters a single-quoted TOML string cannot hold; "+
				`reference them from a double-quoted string ("${VAR}") instead`,
			strings.Join(slices.Compact(unrepresentable), ", ")))
	}
	if len(problems) > 0 {
		return "", fmt.Errorf("%s", strings.Join(problems, "; "))
	}
	return out.String(), nil
}

// encodeFor writes v so that it is taken literally in the given context.
// Bare context is passed through: that is how `port = ${CH_PORT}` works.
func encodeFor(ctx tomlCtx, v string) (string, bool) {
	switch ctx {
	case ctxBasic, ctxMLBasic:
		var b strings.Builder
		for _, r := range v {
			switch {
			case r == '"' || r == '\\':
				b.WriteByte('\\')
				b.WriteRune(r)
			case r == '\n':
				b.WriteString(`\n`)
			case r == '\r':
				b.WriteString(`\r`)
			case r == '\t':
				b.WriteString(`\t`)
			case r < 0x20 || r == 0x7f:
				fmt.Fprintf(&b, `\u%04X`, r)
			default:
				b.WriteRune(r)
			}
		}
		return b.String(), true
	case ctxLiteral:
		return v, !strings.ContainsAny(v, "'\n\r")
	case ctxMLLiteral:
		return v, !strings.Contains(v, "'''")
	}
	return v, true
}

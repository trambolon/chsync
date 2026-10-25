package schema

import (
	"regexp"
	"slices"
	"strings"

	"chsync/internal/xstrings"
)

var uniq = xstrings.Uniq

// ============================== Аргументы движка ==============================

var engineRe = regexp.MustCompile(`ENGINE\s*=\s*(\w+)`)

// EngineArgs находит "ENGINE = <движок, начинающийся с prefix>(...)" и возвращает имя движка и
// сырые аргументы верхнего уровня. found=false — такого движка нет; args=nil — движок без скобок.
func EngineArgs(create, prefix string) (engine string, args []string, found bool) {
	for _, loc := range engineRe.FindAllStringSubmatchIndex(create, -1) {
		engine = create[loc[2]:loc[3]]
		if !strings.HasPrefix(engine, prefix) {
			continue
		}
		i := loc[1]
		for i < len(create) && strings.ContainsRune(" \n\t", rune(create[i])) {
			i++
		}
		if i >= len(create) || create[i] != '(' {
			return engine, nil, true
		}
		args, _ = SplitArgs(create, i)
		return engine, args, true
	}
	return "", nil, false
}

// SplitArgs разбирает список аргументов, начиная со скобки в позиции open,
// с учётом вложенных скобок и строк. Возвращает аргументы и позицию закрывающей скобки.
func SplitArgs(s string, open int) ([]string, int) {
	var args []string
	depth, start := 0, open+1
	for i := open; i < len(s); i++ {
		switch s[i] {
		case '\'', '"', '`':
			i = skipQuoted(s, i)
		case '(', '[':
			depth++
		case ')', ']':
			if depth--; depth == 0 {
				if a := strings.TrimSpace(s[start:i]); a != "" || len(args) > 0 {
					args = append(args, a)
				}
				return args, i
			}
		case ',':
			if depth == 1 {
				args = append(args, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	return args, len(s)
}

// skipQuoted возвращает индекс закрывающей кавычки строки, начатой в позиции i (учитывает \' и ”).
func skipQuoted(s string, i int) int {
	q := s[i]
	for j := i + 1; j < len(s); j++ {
		switch {
		case s[j] == '\\':
			j++
		case s[j] == q && j+1 < len(s) && s[j+1] == q:
			j++
		case s[j] == q:
			return j
		}
	}
	return len(s) - 1
}

// Unquote снимает кавычки: 'str' -> str (isString=true), `id` / "id" -> id, ident -> ident.
func Unquote(a string) (val string, isString bool) {
	a = strings.TrimSpace(a)
	if len(a) < 2 || !strings.ContainsRune("'\"`", rune(a[0])) || a[len(a)-1] != a[0] {
		return a, false
	}
	q, inner := a[0], a[1:len(a)-1]
	var b strings.Builder
	for i := 0; i < len(inner); i++ {
		switch {
		case inner[i] == '\\' && i+1 < len(inner):
			i++
			switch inner[i] {
			case 'n':
				b.WriteByte('\n')
			case 't':
				b.WriteByte('\t')
			default: // \' \\ \` и прочее — сам символ
				b.WriteByte(inner[i])
			}
		case inner[i] == q && i+1 < len(inner) && inner[i+1] == q:
			i++
			b.WriteByte(q)
		default:
			b.WriteByte(inner[i])
		}
	}
	return b.String(), q == '\''
}

// ============================== Replicated: ZK-путь ==============================

// ReplInfo — что известно о реплицируемом движке объекта.
type ReplInfo struct {
	Engine      string
	PathTmpl    string // шаблон ZK-пути, как в DDL, например /clickhouse/tables/{shard}/db/t
	ReplicaTmpl string // шаблон имени реплики, обычно {replica}
	Defaults    bool   // аргументы не указаны — путь из default_replica_path сервера
	UUID        string // значение для {uuid}: UUID таблицы, а у MV — UUID её inner-таблицы
}

// ParseReplicated разбирает Replicated*MergeTree в DDL таблицы или MV с inner-движком (nil — не реплицируемая).
func ParseReplicated(create string) *ReplInfo {
	eng, args, found := EngineArgs(create, "Replicated")
	if !found || !strings.HasSuffix(eng, "MergeTree") {
		return nil
	}
	ri := &ReplInfo{Engine: eng, UUID: HeaderUUID(create)}
	if strings.Contains(create, "MATERIALIZED VIEW") {
		ri.UUID = InnerUUID(create)
	}
	if len(args) >= 2 {
		p, ps := Unquote(args[0])
		r, rs := Unquote(args[1])
		if ps && rs {
			ri.PathTmpl, ri.ReplicaTmpl = p, r
			return ri
		}
	}
	ri.Defaults = true // ReplicatedMergeTree / ReplicatedReplacingMergeTree(ver) и т.п.
	return ri
}

var macroRe = regexp.MustCompile(`\{([^{}]+)\}`)

// ExpandMacros раскрывает {macro} в шаблоне: сначала special ({database},{table},{uuid}), потом макросы
// хоста. Макрос может ссылаться на другой — до трёх проходов. Возвращает и список нераскрытых макросов.
func ExpandMacros(tmpl string, macros, special map[string]string) (string, []string) {
	var missing []string
	out := tmpl
	for pass := 0; pass < 3 && macroRe.MatchString(out); pass++ {
		out = macroRe.ReplaceAllStringFunc(out, func(m string) string {
			name := m[1 : len(m)-1]
			if v := special[name]; v != "" {
				return v
			}
			if v, ok := macros[name]; ok {
				return v
			}
			if pass == 0 {
				missing = append(missing, name)
			}
			return m
		})
	}
	return out, uniq(missing)
}

// TemplateHasMacro — есть ли в шаблоне хотя бы один из макросов names (например {shard}).
func TemplateHasMacro(tmpl string, names []string) bool {
	for _, m := range macroRe.FindAllStringSubmatch(tmpl, -1) {
		if slices.Contains(names, m[1]) {
			return true
		}
	}
	return false
}

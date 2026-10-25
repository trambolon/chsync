package main

import (
	"regexp"
	"strings"
)

// ============================== Виды объектов ==============================

// Kind — вид объекта схемы; определяет порядок создания и проверки.
type Kind int

const (
	KDatabase    Kind = iota
	KFunction         // SQL UDF
	KDictionary       // CREATE DICTIONARY
	KTable            // нереплицируемое хранилище: MergeTree, Log, Memory, Null, Join, Set, S3, URL...
	KReplicated       // Replicated*MergeTree
	KStreaming        // Kafka, RabbitMQ, NATS, S3Queue... — начинают потреблять сразу (если есть MV)
	KDistributed      // Distributed
	KProxy            // Buffer, Merge, Dictionary-engine — обёртки над другими таблицами
	KView             // VIEW
	KMatView          // MATERIALIZED VIEW
	KOtherView        // LiveView, WindowView
	KInner            // .inner/.inner_id таблицы MV: создаются вместе с MV, отдельно не переносятся
)

var kindNames = [...]string{"database", "function", "dictionary", "table", "replicated", "streaming",
	"distributed", "proxy", "view", "matview", "view*", "inner"}

func (k Kind) String() string { return kindNames[k] }

// priority — порядок создания при равных зависимостях (меньше — раньше):
// БД → функции → словари → хранилища → Distributed/прокси → VIEW → MV.
func (k Kind) priority() int {
	return [...]int{0, 1, 2, 3, 3, 3, 4, 4, 5, 6, 7, 7}[k]
}

var streamingEngines = map[string]bool{"Kafka": true, "RabbitMQ": true, "NATS": true, "S3Queue": true, "AzureQueue": true, "FileLog": true}

// classify определяет вид объекта по имени, движку и DDL.
func classify(name, engine, create string) Kind {
	switch {
	case strings.HasPrefix(name, ".inner.") || strings.HasPrefix(name, ".inner_id."):
		return KInner
	case strings.HasPrefix(create, "CREATE DICTIONARY"):
		return KDictionary
	case engine == "MaterializedView":
		return KMatView
	case engine == "View":
		return KView
	case engine == "LiveView" || engine == "WindowView":
		return KOtherView
	case engine == "Distributed":
		return KDistributed
	case engine == "Buffer" || engine == "Merge" || engine == "Dictionary":
		return KProxy
	case strings.HasPrefix(engine, "Replicated"):
		return KReplicated
	case streamingEngines[engine]:
		return KStreaming
	}
	return KTable
}

// ============================== Заголовок CREATE ==============================

// identPat — идентификатор: `quoted`, "quoted" или bare.
const identPat = "(?:`(?:[^`\\\\]|\\\\.)*`|\"(?:[^\"\\\\]|\\\\.)*\"|[^\\s.(`\"]+)"

// headerRe разбирает "CREATE <TYPE> [IF NOT EXISTS] name [UUID '...'] [TO INNER UUID '...']".
// Группы: 1 — CREATE <TYPE>, 2 — IF NOT EXISTS, 3 — имя, 4 — UUID, 5 — TO INNER UUID.
var headerRe = regexp.MustCompile(
	`^(CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMPORARY\s+)?(?:TABLE|VIEW|MATERIALIZED\s+VIEW|LIVE\s+VIEW|WINDOW\s+VIEW|DICTIONARY|DATABASE|FUNCTION)\s+)` +
		`(IF\s+NOT\s+EXISTS\s+)?` +
		`(` + identPat + `(?:\.` + identPat + `)?)` +
		`(\s+UUID\s+'[0-9a-fA-F-]{36}')?` +
		`(\s+TO\s+INNER\s+UUID\s+'[0-9a-fA-F-]{36}')?`)

var (
	wsRe           = regexp.MustCompile(`\s+`)
	uuidInHeaderRe = regexp.MustCompile(`^[^(]*?\sUUID\s+'([0-9a-fA-F-]{36})'`)
	innerUUIDRe    = regexp.MustCompile(`TO\s+INNER\s+UUID\s+'([0-9a-fA-F-]{36})'`)
)

// normalizeDDL — форма DDL для сравнения между хостами: без UUID, без IF NOT EXISTS, пробелы схлопнуты.
// SHOW CREATE (многострочный) и create_table_query (однострочный) дают одинаковый результат.
func normalizeDDL(s string) string {
	s = strings.TrimSpace(s)
	if m := headerRe.FindStringSubmatchIndex(s); m != nil {
		s = s[m[2]:m[3]] + s[m[6]:m[7]] + s[m[1]:]
	}
	s = wsRe.ReplaceAllString(s, " ")
	return strings.NewReplacer("( ", "(", " )", ")").Replace(s)
}

// stripUUID убирает UUID-клаузы из заголовка (режим preserve_uuid=false).
func stripUUID(s string) string {
	m := headerRe.FindStringSubmatchIndex(s)
	if m == nil {
		return s
	}
	head := s[m[2]:m[3]]
	if m[4] >= 0 {
		head += s[m[4]:m[5]]
	}
	return head + s[m[6]:m[7]] + s[m[1]:]
}

// addIfNotExists вставляет IF NOT EXISTS после "CREATE <TYPE>", если его ещё нет.
func addIfNotExists(s string) string {
	m := headerRe.FindStringSubmatchIndex(s)
	if m == nil || m[4] >= 0 {
		return s
	}
	return s[:m[3]] + "IF NOT EXISTS " + s[m[3]:]
}

// headerUUID — UUID объекта из заголовка ("" — не указан).
func headerUUID(s string) string {
	if m := uuidInHeaderRe.FindStringSubmatch(s); m != nil {
		return m[1]
	}
	return ""
}

// innerUUID — UUID inner-таблицы MV из "TO INNER UUID '...'".
func innerUUID(s string) string {
	if m := innerUUIDRe.FindStringSubmatch(s); m != nil {
		return m[1]
	}
	return ""
}

// ddlBody — DDL без заголовка "CREATE ... db.name": в нём ищем ссылки на другие объекты.
func ddlBody(create string) string {
	if m := headerRe.FindStringIndex(create); m != nil {
		return create[m[1]:]
	}
	return create
}

// ============================== Аргументы движка ==============================

var engineRe = regexp.MustCompile(`ENGINE\s*=\s*(\w+)`)

// engineArgs находит "ENGINE = <движок, начинающийся с engPrefix>(...)" и возвращает имя движка и
// сырые аргументы верхнего уровня. found=false — такого движка нет; args=nil — движок без скобок.
func engineArgs(create, engPrefix string) (engine string, args []string, found bool) {
	for _, loc := range engineRe.FindAllStringSubmatchIndex(create, -1) {
		engine = create[loc[2]:loc[3]]
		if !strings.HasPrefix(engine, engPrefix) {
			continue
		}
		i := loc[1]
		for i < len(create) && strings.ContainsRune(" \n\t", rune(create[i])) {
			i++
		}
		if i >= len(create) || create[i] != '(' {
			return engine, nil, true
		}
		args, _ = splitArgs(create, i)
		return engine, args, true
	}
	return "", nil, false
}

// splitArgs разбирает список аргументов, начиная со скобки в позиции open,
// с учётом вложенных скобок и строк. Возвращает аргументы и позицию закрывающей скобки.
func splitArgs(s string, open int) ([]string, int) {
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

// unquote снимает кавычки: 'str' -> str (isString=true), `id` / "id" -> id, ident -> ident.
func unquote(a string) (val string, isString bool) {
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

// parseReplicated разбирает Replicated*MergeTree в DDL таблицы или MV с inner-движком (nil — не реплицируемая).
func parseReplicated(create string) *ReplInfo {
	eng, args, found := engineArgs(create, "Replicated")
	if !found || !strings.HasSuffix(eng, "MergeTree") {
		return nil
	}
	ri := &ReplInfo{Engine: eng, UUID: headerUUID(create)}
	if strings.Contains(create, "MATERIALIZED VIEW") {
		ri.UUID = innerUUID(create)
	}
	if len(args) >= 2 {
		p, ps := unquote(args[0])
		r, rs := unquote(args[1])
		if ps && rs {
			ri.PathTmpl, ri.ReplicaTmpl = p, r
			return ri
		}
	}
	ri.Defaults = true // ReplicatedMergeTree / ReplicatedReplacingMergeTree(ver) и т.п.
	return ri
}

var macroRe = regexp.MustCompile(`\{([^{}]+)\}`)

// expandMacros раскрывает {macro} в шаблоне: сначала special ({database},{table},{uuid}), потом макросы
// хоста. Макрос может ссылаться на другой — до трёх проходов. Возвращает и список нераскрытых макросов.
func expandMacros(tmpl string, macros, special map[string]string) (string, []string) {
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

// templateHasMacro — есть ли в шаблоне хотя бы один из макросов names (например {shard}).
func templateHasMacro(tmpl string, names []string) bool {
	for _, m := range macroRe.FindAllStringSubmatch(tmpl, -1) {
		if containsStr(names, m[1]) {
			return true
		}
	}
	return false
}

// ============================== Ссылки между объектами ==============================

// ObjKey — полное имя объекта.
type ObjKey struct{ DB, Name string }

func (k ObjKey) String() string { return k.DB + "." + k.Name }

var wordRe = regexp.MustCompile("[A-Za-z0-9_$]+|`[^`]*`|\"[^\"]*\"")

// tokens — все идентификаторы и строковые литералы тела DDL (включая части 'db.dict' из dictGet).
// Это дешёвый предфильтр: точную проверку ссылки делает refersTo.
func tokens(body string) map[string]bool {
	t := map[string]bool{}
	for _, w := range wordRe.FindAllString(body, -1) {
		if w[0] == '`' || w[0] == '"' {
			w = w[1 : len(w)-1]
		}
		t[w] = true
	}
	for i := 0; i < len(body); i++ {
		if body[i] != '\'' {
			continue
		}
		j := skipQuoted(body, i)
		if lit := body[i+1 : j]; len(lit) < 256 {
			t[lit] = true
			for _, p := range strings.Split(lit, ".") {
				t[p] = true
			}
		}
		i = j
	}
	return t
}

// identAlt — регэксп имени в любом виде цитирования: name, `name`, "name".
func identAlt(x string) string {
	q := regexp.QuoteMeta(x)
	return "(?:`" + q + "`|\"" + q + "\"|" + q + ")"
}

var (
	dictSourceRe = regexp.MustCompile(`SOURCE\(CLICKHOUSE\((.*?)\)\)`)
	dictDBRe     = regexp.MustCompile(`\bDB\s+'([^']*)'`)
	dictTableRe  = regexp.MustCompile(`\bTABLE\s+'([^']*)'`)
)

// explicitRefs — явные ссылки из аргументов движков Distributed/Buffer и источника словаря CLICKHOUSE.
// Там имена — строки, поэтому их не ловит обычный поиск "db.table".
func explicitRefs(o *Obj) []ObjKey {
	var out []ObjKey
	add := func(db, name string) {
		if name == "" {
			return
		}
		if db == "" || strings.Contains(db, "(") { // currentDatabase() и т.п.
			db = o.DB
		}
		out = append(out, ObjKey{db, name})
	}
	arg2 := func(engine string, iDB, iTable int) {
		if _, a, ok := engineArgs(o.Create, engine); ok && len(a) > iTable {
			db, _ := unquote(a[iDB])
			t, _ := unquote(a[iTable])
			add(db, t)
		}
	}
	switch o.Kind {
	case KDistributed:
		arg2("Distributed", 1, 2) // Distributed(cluster, db, table, ...)
	case KProxy:
		arg2("Buffer", 0, 1) // Buffer(db, table, ...)
	case KDictionary:
		if m := dictSourceRe.FindStringSubmatch(o.Create); m != nil {
			var db, t string
			if mm := dictDBRe.FindStringSubmatch(m[1]); mm != nil {
				db = mm[1]
			}
			if mm := dictTableRe.FindStringSubmatch(m[1]); mm != nil {
				t = mm[1]
			}
			add(db, t)
		}
	}
	return out
}

// refersTo — ссылается ли тело DDL объекта o на объект p: "db.name" в любом цитировании,
// или просто "name", если o и p в одной БД (сервер сохраняет такие ссылки как есть).
func refersTo(o *Obj, p ObjKey, body string) bool {
	q := regexp.MustCompile(`(?:^|[^\w])` + identAlt(p.DB) + `\s*\.\s*` + identAlt(p.Name) + `(?:[^\w]|$)`)
	if q.MatchString(body) {
		return true
	}
	return o.DB == p.DB && regexp.MustCompile(`(?:^|[^\w.])`+identAlt(p.Name)+`(?:[^\w]|$)`).MatchString(body)
}

// ============================== Прочие проверки DDL ==============================

var (
	storagePolicyRe = regexp.MustCompile(`\bstorage_policy\s*=\s*'([^']+)'`)
	diskSettingRe   = regexp.MustCompile(`\bdisk\s*=\s*'([^']+)'`)
)

// settingValues — все значения настройки вида name = '...' из DDL (storage_policy, disk).
func settingValues(re *regexp.Regexp, create string) []string {
	var out []string
	for _, m := range re.FindAllStringSubmatch(create, -1) {
		out = append(out, m[1])
	}
	return uniq(out)
}

// hasHiddenSecrets — сервер замаскировал пароли в DDL (без display_secrets такой DDL выполнять нельзя).
func hasHiddenSecrets(create string) bool { return strings.Contains(create, "[HIDDEN]") }

// ============================== Фильтр ==============================

// builtinSystemDBs — системные БД, которые пропускаются всегда, независимо от конфига.
var builtinSystemDBs = []string{"system", "information_schema", "_temporary_and_external_tables"}

// Filter решает, какие БД, объекты и функции не переносить.
type Filter struct {
	include   map[string]bool // lower-case имена БД; пусто — все
	excludeDB map[string]bool // lower-case
	nameRe    []*regexp.Regexp
	objects   map[string]bool // "db.name"
	engines   map[string]bool // lower-case
}

// newFilter собирает фильтр из конфига; onlyDBs (флаг --databases) заменяет include_databases.
func newFilter(fc FilterConfig, onlyDBs []string) *Filter {
	f := &Filter{include: map[string]bool{}, excludeDB: map[string]bool{}, objects: map[string]bool{}, engines: map[string]bool{}}
	inc := fc.IncludeDatabases
	if len(onlyDBs) > 0 {
		inc = onlyDBs
	}
	for _, d := range inc {
		f.include[strings.ToLower(d)] = true
	}
	for _, d := range append(append([]string{}, fc.ExcludeDatabases...), builtinSystemDBs...) {
		f.excludeDB[strings.ToLower(d)] = true
	}
	for _, r := range fc.ExcludeNameRegex {
		f.nameRe = append(f.nameRe, regexp.MustCompile(r)) // уже проверено в validate
	}
	for _, o := range fc.ExcludeObjects {
		f.objects[o] = true
	}
	for _, e := range fc.ExcludeEngines {
		f.engines[strings.ToLower(e)] = true
	}
	return f
}

// matchName — причина исключения имени по exclude_name_regex или "".
func (f *Filter) matchName(name string) string {
	for _, r := range f.nameRe {
		if r.MatchString(name) {
			return "exclude_name_regex " + r.String()
		}
	}
	return ""
}

// SkipDB возвращает причину пропуска БД или "".
func (f *Filter) SkipDB(db string) string {
	l := strings.ToLower(db)
	switch {
	case f.excludeDB[l]:
		return "exclude_databases"
	case len(f.include) > 0 && !f.include[l]:
		return "not in include_databases"
	}
	return f.matchName(db)
}

// SkipObject возвращает причину пропуска объекта или "": БД, точное имя, регэксп имени, движок.
func (f *Filter) SkipObject(o *Obj) string {
	if r := f.SkipDB(o.DB); r != "" {
		return "db: " + r
	}
	if f.objects[o.DB+"."+o.Name] {
		return "exclude_objects"
	}
	if r := f.matchName(o.Name); r != "" {
		return r
	}
	if f.engines[strings.ToLower(o.Engine)] {
		return "exclude_engines " + o.Engine
	}
	return ""
}

// SkipFunction возвращает причину пропуска SQL-функции или "".
func (f *Filter) SkipFunction(name string) string { return f.matchName(name) }

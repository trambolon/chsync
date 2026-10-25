// Package schema — разбор DDL ClickHouse без обращения к серверу: вид объекта, заголовок CREATE и UUID,
// аргументы движка, ZK-путь Replicated-таблиц, ссылки между объектами.
package schema

import (
	"regexp"
	"strings"
)

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

// Priority — порядок создания при равных зависимостях (меньше — раньше):
// БД → функции → словари → хранилища → Distributed/прокси → VIEW → MV.
func (k Kind) Priority() int {
	return [...]int{0, 1, 2, 3, 3, 3, 4, 4, 5, 6, 7, 7}[k]
}

var streamingEngines = map[string]bool{"Kafka": true, "RabbitMQ": true, "NATS": true, "S3Queue": true, "AzureQueue": true, "FileLog": true}

// Classify определяет вид объекта по имени, движку и DDL.
func Classify(name, engine, create string) Kind {
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
	parenSpaces    = strings.NewReplacer("( ", "(", " )", ")")
)

// Normalize — форма DDL для сравнения между хостами: без UUID, без IF NOT EXISTS, пробелы схлопнуты.
// SHOW CREATE (многострочный) и create_table_query (однострочный) дают одинаковый результат.
func Normalize(s string) string {
	s = strings.TrimSpace(s)
	if m := headerRe.FindStringSubmatchIndex(s); m != nil {
		s = s[m[2]:m[3]] + s[m[6]:m[7]] + s[m[1]:]
	}
	return parenSpaces.Replace(wsRe.ReplaceAllString(s, " "))
}

// StripUUID убирает UUID-клаузы из заголовка (режим preserve_uuid=false).
func StripUUID(s string) string {
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

// AddIfNotExists вставляет IF NOT EXISTS после "CREATE <TYPE>", если его ещё нет.
func AddIfNotExists(s string) string {
	m := headerRe.FindStringSubmatchIndex(s)
	if m == nil || m[4] >= 0 {
		return s
	}
	return s[:m[3]] + "IF NOT EXISTS " + s[m[3]:]
}

// HeaderUUID — UUID объекта из заголовка ("" — не указан).
func HeaderUUID(s string) string {
	if m := uuidInHeaderRe.FindStringSubmatch(s); m != nil {
		return m[1]
	}
	return ""
}

// InnerUUID — UUID inner-таблицы MV из "TO INNER UUID '...'".
func InnerUUID(s string) string {
	if m := innerUUIDRe.FindStringSubmatch(s); m != nil {
		return m[1]
	}
	return ""
}

// Body — DDL без заголовка "CREATE ... db.name": в нём ищем ссылки на другие объекты.
func Body(create string) string {
	if m := headerRe.FindStringIndex(create); m != nil {
		return create[m[1]:]
	}
	return create
}

// ============================== Прочие проверки DDL ==============================

var (
	storagePolicyRe = regexp.MustCompile(`\bstorage_policy\s*=\s*'([^']+)'`)
	diskSettingRe   = regexp.MustCompile(`\bdisk\s*=\s*'([^']+)'`)
)

// StoragePolicies — значения storage_policy = '...' из DDL.
func StoragePolicies(create string) []string { return settingValues(storagePolicyRe, create) }

// Disks — значения disk = '...' из DDL.
func Disks(create string) []string { return settingValues(diskSettingRe, create) }

func settingValues(re *regexp.Regexp, create string) []string {
	var out []string
	for _, m := range re.FindAllStringSubmatch(create, -1) {
		out = append(out, m[1])
	}
	return uniq(out)
}

// HasHiddenSecrets — сервер замаскировал пароли в DDL (без display_secrets такой DDL выполнять нельзя).
func HasHiddenSecrets(create string) bool { return strings.Contains(create, "[HIDDEN]") }

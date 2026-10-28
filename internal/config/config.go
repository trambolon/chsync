// Package config — загрузка, умолчания и проверка chsync.toml.
package config

import (
	"cmp"
	_ "embed"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"

	"github.com/BurntSushi/toml"
)

// Example — пример конфига с комментариями (команда chsync init).
//
//go:embed chsync.example.toml
var Example string

// ConflictPolicy — что делать, если DDL объекта различается между источниками.
type ConflictPolicy string

const (
	ConflictError ConflictPolicy = "error" // объект BLOCKED
	ConflictFirst ConflictPolicy = "first" // взять DDL с первого источника
)

// PartialPolicy — что делать, если объект есть не на всех источниках.
type PartialPolicy string

const (
	PartialWarn PartialPolicy = "warn" // создать и предупредить
	PartialSkip PartialPolicy = "skip" // не создавать
)

// GlobalPolicy — что делать с Replicated-таблицей, у которой ZK-путь без макроса шарда.
type GlobalPolicy string

const (
	GlobalBlock  GlobalPolicy = "block"  // не создавать, показать в плане
	GlobalSkip   GlobalPolicy = "skip"   // пропустить
	GlobalCreate GlobalPolicy = "create" // подключить к общему набору реплик
)

// Protocol — протокол подключения к ClickHouse.
type Protocol string

const (
	ProtocolHTTP   Protocol = "http"   // HTTP-интерфейс: 8123, с TLS — 8443
	ProtocolNative Protocol = "native" // native TCP: 9000, с TLS — 9440
)

// DefaultPort — порт ClickHouse по умолчанию для протокола.
func DefaultPort(p Protocol, secure bool) int {
	switch {
	case p == ProtocolNative && secure:
		return 9440
	case p == ProtocolNative:
		return 9000
	case secure:
		return 8443
	}
	return 8123
}

// Config — содержимое chsync.toml.
type Config struct {
	Cluster   string `toml:"cluster"`    // имя кластера из <remote_servers>
	OutputDir string `toml:"output_dir"` // куда писать отчёты и SQL
	DryRun    bool   `toml:"dry_run"`    // apply только показывает план, пока нет --execute

	Connection Connection `toml:"connection"`
	Source     Source     `toml:"source"`
	Target     Target     `toml:"target"`
	Filter     Filter     `toml:"filter"`
	DDL        DDL        `toml:"ddl"`
}

// Connection — параметры подключения, общие для всех хостов.
type Connection struct {
	Protocol           Protocol `toml:"protocol"` // http (по умолчанию) | native
	Port               int      `toml:"port"`
	Secure             bool     `toml:"secure"`
	User               string   `toml:"user"`
	Password           string   `toml:"password"`
	PasswordEnv        string   `toml:"password_env"`
	TimeoutSec         int      `toml:"timeout_sec"`
	CAFile             string   `toml:"ca_file"`
	InsecureSkipVerify bool     `toml:"insecure_skip_verify"`
	UseEnvProxy        bool     `toml:"use_env_proxy"`
}

// Source — откуда берём DDL и как разрешаем расхождения между источниками.
type Source struct {
	Hosts      []string       `toml:"hosts"`
	OnConflict ConflictPolicy `toml:"on_conflict"`
	OnPartial  PartialPolicy  `toml:"on_partial"`
}

// Target — куда разворачиваем схему и какую топологию ожидаем.
type Target struct {
	Hosts            []string `toml:"hosts"`
	ExpectShards     int      `toml:"expect_shards"`      // сколько шардов должен видеть target (0 — не проверять)
	ReplicasPerShard int      `toml:"replicas_per_shard"` // ожидаемый rf новых шардов (0 — не проверять)
}

// Filter — что не переносим.
type Filter struct {
	IncludeDatabases []string `toml:"include_databases"`
	ExcludeDatabases []string `toml:"exclude_databases"`
	ExcludeNameRegex []string `toml:"exclude_name_regex"`
	ExcludeObjects   []string `toml:"exclude_objects"`
	ExcludeEngines   []string `toml:"exclude_engines"`
	CascadeExclude   bool     `toml:"cascade_exclude"`
}

// DDL — как формировать и проверять DDL.
type DDL struct {
	PreserveUUID     bool           `toml:"preserve_uuid"`
	IfNotExists      bool           `toml:"if_not_exists"`
	SyncFunctions    bool           `toml:"sync_functions"`
	DisplaySecrets   bool           `toml:"display_secrets"`
	GlobalReplicated GlobalPolicy   `toml:"global_replicated"`
	ShardMacros      []string       `toml:"shard_macros"`
	CheckZooKeeper   bool           `toml:"check_zookeeper"`
	Settings         map[string]any `toml:"settings"`
}

// Default — конфиг со значениями по умолчанию. TOML декодируется поверх него,
// поэтому незаданные в файле ключи сохраняют эти значения.
func Default() Config {
	return Config{
		OutputDir:  "./chsync-out",
		DryRun:     true,
		Connection: Connection{User: "default", TimeoutSec: 120},
		Source:     Source{OnConflict: ConflictError, OnPartial: PartialWarn},
		Filter:     Filter{CascadeExclude: true},
		DDL: DDL{
			PreserveUUID:     true,
			IfNotExists:      true,
			SyncFunctions:    true,
			GlobalReplicated: GlobalBlock,
			ShardMacros:      []string{"shard"},
			CheckZooKeeper:   true,
		},
	}
}

// Load читает TOML, отвергает неизвестные ключи (опечатки), дополняет умолчания и проверяет значения.
func Load(path string) (*Config, error) {
	c := Default()
	md, err := toml.DecodeFile(path, &c)
	if err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}
	if und := md.Undecoded(); len(und) > 0 {
		keys := make([]string, len(und))
		for i, k := range und {
			keys[i] = k.String()
		}
		return nil, fmt.Errorf("config %s: неизвестные ключи: %s", path, strings.Join(keys, ", "))
	}
	c.Finalize()
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}
	return &c, nil
}

// Finalize заполняет то, что зависит от других параметров или было задано пустым:
// порт по схеме, пароль из переменной окружения, пустые строки и списки — умолчаниями.
func (c *Config) Finalize() {
	def := Default()
	c.OutputDir = cmp.Or(c.OutputDir, def.OutputDir)
	c.Connection.User = cmp.Or(c.Connection.User, def.Connection.User)
	c.Connection.TimeoutSec = cmp.Or(c.Connection.TimeoutSec, def.Connection.TimeoutSec)
	c.Source.OnConflict = cmp.Or(c.Source.OnConflict, def.Source.OnConflict)
	c.Source.OnPartial = cmp.Or(c.Source.OnPartial, def.Source.OnPartial)
	c.DDL.GlobalReplicated = cmp.Or(c.DDL.GlobalReplicated, def.DDL.GlobalReplicated)
	c.Connection.Protocol = cmp.Or(c.Connection.Protocol, ProtocolHTTP)
	if c.Connection.Port == 0 {
		c.Connection.Port = DefaultPort(c.Connection.Protocol, c.Connection.Secure)
	}
	if c.Connection.Password == "" && c.Connection.PasswordEnv != "" {
		c.Connection.Password = os.Getenv(c.Connection.PasswordEnv)
	}
	if len(c.DDL.ShardMacros) == 0 {
		c.DDL.ShardMacros = def.DDL.ShardMacros
	}
}

// ValidationError — все ошибки конфига сразу, чтобы не чинить их по одной.
type ValidationError struct{ Problems []error }

func (e *ValidationError) Error() string {
	var b strings.Builder
	b.WriteString("ошибки конфига:")
	for _, p := range e.Problems {
		b.WriteString("\n  - ")
		b.WriteString(p.Error())
	}
	return b.String()
}

func (e *ValidationError) Unwrap() []error { return e.Problems }

// Validate проверяет значения параметров. Возвращает *ValidationError со всеми проблемами.
func (c *Config) Validate() error {
	var errs []error
	bad := func(format string, a ...any) { errs = append(errs, fmt.Errorf(format, a...)) }

	if len(c.Source.Hosts) == 0 {
		bad("source.hosts пуст")
	}
	if len(c.Target.Hosts) == 0 {
		bad("target.hosts пуст")
	}
	for _, h := range c.Target.Hosts {
		if slices.Contains(c.Source.Hosts, h) {
			bad("хост %s указан и в source, и в target", h)
		}
	}
	for name, hosts := range map[string][]string{"source.hosts": c.Source.Hosts, "target.hosts": c.Target.Hosts} {
		if d := duplicates(hosts); len(d) > 0 {
			bad("%s: хосты повторяются: %s", name, strings.Join(d, ", "))
		}
	}
	errs = appendIfErr(errs, oneOf("source.on_conflict", c.Source.OnConflict, ConflictError, ConflictFirst))
	errs = appendIfErr(errs, oneOf("source.on_partial", c.Source.OnPartial, PartialWarn, PartialSkip))
	errs = appendIfErr(errs, oneOf("ddl.global_replicated", c.DDL.GlobalReplicated, GlobalBlock, GlobalSkip, GlobalCreate))
	if c.Target.ReplicasPerShard < 0 {
		bad("target.replicas_per_shard не может быть отрицательным")
	}
	if c.Target.ExpectShards < 0 {
		bad("target.expect_shards не может быть отрицательным")
	}
	errs = appendIfErr(errs, oneOf("connection.protocol", c.Connection.Protocol, ProtocolHTTP, ProtocolNative))
	if cn := c.Connection; cn.Protocol == ProtocolNative && (cn.Port == 8123 || cn.Port == 8443) ||
		cn.Protocol == ProtocolHTTP && (cn.Port == 9000 || cn.Port == 9440) {
		bad("connection.port %d не подходит для protocol = %q (http: 8123/8443, native: 9000/9440)", cn.Port, cn.Protocol)
	}
	if p := c.Connection.Port; p < 1 || p > 65535 {
		bad("connection.port %d вне диапазона 1..65535", p)
	}
	for _, r := range c.Filter.ExcludeNameRegex {
		if _, err := regexp.Compile(r); err != nil {
			bad("filter.exclude_name_regex %q: %v", r, err)
		}
	}
	for _, o := range c.Filter.ExcludeObjects {
		if db, name, ok := strings.Cut(o, "."); !ok || db == "" || name == "" {
			bad("filter.exclude_objects %q: нужен формат db.name", o)
		}
	}
	if len(errs) > 0 {
		slices.SortFunc(errs, func(a, b error) int { return strings.Compare(a.Error(), b.Error()) })
		return &ValidationError{Problems: errs}
	}
	return nil
}

// SettingsParams — настройки из [ddl.settings] в виде параметров запроса; передаются с каждым CREATE на target.
func (d *DDL) SettingsParams() map[string]string {
	out := make(map[string]string, len(d.Settings))
	for k, v := range d.Settings {
		switch t := v.(type) {
		case bool:
			out[k] = "0"
			if t {
				out[k] = "1"
			}
		default:
			out[k] = fmt.Sprint(t)
		}
	}
	return out
}

// oneOf — ошибка, если значение перечисления не из списка допустимых.
func oneOf[T ~string](name string, v T, allowed ...T) error {
	if slices.Contains(allowed, v) {
		return nil
	}
	s := make([]string, len(allowed))
	for i, a := range allowed {
		s[i] = string(a)
	}
	return fmt.Errorf("%s = %q: допустимо %s", name, v, strings.Join(s, "|"))
}

func appendIfErr(errs []error, err error) []error {
	if err != nil {
		return append(errs, err)
	}
	return errs
}

// duplicates — значения, встречающиеся в срезе больше одного раза.
func duplicates(ss []string) []string {
	seen := make(map[string]int, len(ss))
	var out []string
	for _, s := range ss {
		if seen[s]++; seen[s] == 2 {
			out = append(out, s)
		}
	}
	return out
}

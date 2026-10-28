// Package storage — работа с ClickHouse через официальный драйвер clickhouse-go v2 и снимок схемы хоста (Inventory).
//
// Протоколы: HTTP(S) — 8123/8443 (по умолчанию) или native — 9000/9440 (connection.protocol).
// Результаты SELECT сканируются в структуры с тегами `ch:"колонка"` — без JSON и ручного разбора.
// Каждый запрос пишется в журнал на уровне Debug (хост, БД, длительность, строки, ошибка, текст):
// в файл queries.log каталога запуска — всегда, в консоль — с флагом -v.
//
// chsync выполняет только SELECT/SHOW и CREATE ... IF NOT EXISTS.
package storage

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"chsync/internal/config"
	"chsync/internal/logging"
	"chsync/internal/xstrings"
)

// ClientOptions — параметры клиента, не зависящие от хоста.
type ClientOptions struct {
	RunID   string       // метка запуска → system.query_log.log_comment
	Version string       // версия chsync → system.query_log.client_name / http_user_agent
	Log     *slog.Logger // журнал запросов (Debug); nil — не писать
}

// Client — подключение к одному хосту ClickHouse. Для каждой БД, в контексте которой выполняется DDL,
// открывается своё подключение (Auth.Database): в HTTP-протоколе нет сессии, и USE между запросами не живёт.
type Client struct {
	name     string // хост так, как он записан в конфиге
	addr     string // host:port
	protocol clickhouse.Protocol
	base     clickhouse.Options // шаблон опций; Auth.Database подставляется на каждую БД
	log      *slog.Logger

	mu    sync.Mutex
	conns map[string]driver.Conn // БД ("" — по умолчанию) -> подключение
}

// Error — ошибка ClickHouse (или сети) с кодом исключения, если сервер его вернул.
type Error struct {
	Host    string
	Code    int
	Message string
}

func (e *Error) Error() string {
	if e.Code > 0 {
		return fmt.Sprintf("%s: Code %d: %s", e.Host, e.Code, e.Message)
	}
	return e.Host + ": " + e.Message
}

// Code достаёт код исключения ClickHouse из ошибки (0 — если это не ошибка сервера).
func Code(err error) int {
	if e, ok := errors.AsType[*Error](err); ok {
		return e.Code
	}
	return 0
}

// New создаёт клиент по строке из конфига: "host", "host:port", "https://host:8443" или "clickhouse://host:9440".
// Подключение ленивое: сеть трогается при первом запросе. HTTPS_PROXY из окружения игнорируется,
// если не задано connection.use_env_proxy — до прод-хостов ходим напрямую.
func New(spec string, cc config.Connection, o ClientOptions) (*Client, error) {
	proto, secure, port := cc.Protocol, cc.Secure, cc.Port
	hostport := spec
	if scheme, rest, ok := strings.Cut(spec, "://"); ok {
		switch scheme {
		case "http", "https":
			proto, secure = config.ProtocolHTTP, scheme == "https"
		case "clickhouse", "tcp":
			proto = config.ProtocolNative
		case "clickhouses":
			proto, secure = config.ProtocolNative, true
		default:
			return nil, fmt.Errorf("хост %q: неизвестная схема %s://", spec, scheme)
		}
		hostport = strings.TrimSuffix(rest, "/")
		if proto != cc.Protocol || secure != cc.Secure { // порт из конфига относится к другому протоколу
			port = config.DefaultPort(proto, secure)
		}
	}
	host := hostport
	if h, p, err := net.SplitHostPort(hostport); err == nil {
		host = h
		if port, err = strconv.Atoi(p); err != nil {
			return nil, fmt.Errorf("хост %q: неверный порт", spec)
		}
	}

	opt := clickhouse.Options{
		Protocol: clickhouse.Native,
		Addr:     []string{net.JoinHostPort(host, strconv.Itoa(port))},
		Auth:     clickhouse.Auth{Username: cc.User, Password: cc.Password},
		ClientInfo: clickhouse.ClientInfo{Products: []struct{ Name, Version string }{
			{Name: "chsync", Version: o.Version},
		}},
		Settings:        clickhouse.Settings{"log_comment": "chsync " + o.RunID},
		DialTimeout:     10 * time.Second,
		ReadTimeout:     time.Duration(cc.TimeoutSec) * time.Second,
		MaxOpenConns:    4,
		MaxIdleConns:    2,
		ConnMaxLifetime: 10 * time.Minute,
	}
	if proto == config.ProtocolHTTP {
		opt.Protocol = clickhouse.HTTP
		useEnv := cc.UseEnvProxy
		opt.TransportFunc = func(t *http.Transport) (http.RoundTripper, error) {
			if !useEnv {
				t.Proxy = nil // драйвер по умолчанию берёт HTTPS_PROXY из окружения
			}
			return t, nil
		}
	} else {
		opt.Compression = &clickhouse.Compression{Method: clickhouse.CompressionLZ4}
	}
	if secure {
		tc := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: cc.InsecureSkipVerify, ServerName: host} //nolint:gosec // явная опция конфига
		if cc.CAFile != "" {
			pem, err := os.ReadFile(cc.CAFile)
			if err != nil {
				return nil, fmt.Errorf("ca_file: %w", err)
			}
			tc.RootCAs = x509.NewCertPool()
			if !tc.RootCAs.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("ca_file %s: не найдено сертификатов", cc.CAFile)
			}
		}
		opt.TLS = tc
	}
	log := o.Log
	if log == nil {
		log = logging.Discard()
	}
	return &Client{name: spec, addr: opt.Addr[0], protocol: opt.Protocol, base: opt, log: log, conns: map[string]driver.Conn{}}, nil
}

// Name — хост так, как он записан в конфиге.
func (c *Client) Name() string { return c.name }

// Close закрывает все подключения клиента.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var errs []error
	for db, conn := range c.conns {
		errs = append(errs, conn.Close())
		delete(c.conns, db)
	}
	return errors.Join(errs...)
}

// conn — подключение с текущей БД db ("" — БД пользователя по умолчанию).
func (c *Client) conn(db string) (driver.Conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if conn, ok := c.conns[db]; ok {
		return conn, nil
	}
	opt := c.base
	opt.Auth.Database = db
	opt.Settings = maps.Clone(c.base.Settings)
	conn, err := clickhouse.Open(&opt)
	if err != nil {
		return nil, c.wrap(err)
	}
	c.conns[db] = conn
	return conn, nil
}

// queryCtx добавляет к контексту настройки уровня запроса.
func queryCtx(ctx context.Context, settings map[string]string) context.Context {
	if len(settings) == 0 {
		return ctx
	}
	s := make(clickhouse.Settings, len(settings))
	for k, v := range settings {
		s[k] = v
	}
	return clickhouse.Context(ctx, clickhouse.WithSettings(s))
}

// trace пишет запрос в журнал: хост, БД, длительность, строки, ошибка и полный текст.
func (c *Client) trace(db, query string, settings map[string]string, start time.Time, rows int, err error) {
	attrs := []any{"host", c.name, "dur", time.Since(start).Round(time.Millisecond)}
	if db != "" {
		attrs = append(attrs, "db", db)
	}
	if rows >= 0 {
		attrs = append(attrs, "rows", rows)
	}
	if len(settings) > 0 {
		attrs = append(attrs, "settings", fmt.Sprint(settings))
	}
	if err != nil {
		attrs = append(attrs, "err", err.Error())
	}
	c.log.Debug("query", append(attrs, "query", strings.Join(strings.Fields(query), " "))...)
}

// wrap превращает ошибку драйвера в *Error с кодом исключения ClickHouse.
func (c *Client) wrap(err error) error {
	if err == nil {
		return nil
	}
	if e, ok := errors.AsType[*clickhouse.Exception](err); ok {
		return &Error{Host: c.name, Code: int(e.Code), Message: xstrings.Truncate(strings.TrimSpace(e.Message), 2000)}
	}
	return &Error{Host: c.name, Message: xstrings.Truncate(err.Error(), 2000)}
}

// Exec выполняет запрос без результата (DDL) в контексте БД пользователя по умолчанию.
func (c *Client) Exec(ctx context.Context, query string, settings map[string]string) error {
	return c.ExecIn(ctx, "", query, settings)
}

// ExecIn выполняет запрос в контексте БД db: неквалифицированные имена в DDL (VIEW ... FROM t)
// разрешаются относительно неё.
func (c *Client) ExecIn(ctx context.Context, db, query string, settings map[string]string) error {
	start := time.Now()
	conn, err := c.conn(db)
	if err == nil {
		err = c.wrap(conn.Exec(queryCtx(ctx, settings), query))
	}
	c.trace(db, query, settings, start, -1, err)
	return err
}

// Select выполняет SELECT/SHOW и сканирует строки в []T. Поля T сопоставляются с колонками по тегу `ch:"имя"`;
// типы должны соответствовать колонкам (UInt32 → uint32, String → string, Nullable(T) → *T).
func (c *Client) Select[T any](ctx context.Context, query string, settings map[string]string) ([]T, error) {
	start := time.Now()
	var out []T
	conn, err := c.conn("")
	if err == nil {
		err = c.wrap(conn.Select(queryCtx(ctx, settings), &out, query))
	}
	c.trace("", query, settings, start, len(out), err)
	if err != nil {
		return nil, err
	}
	return out, nil
}

var (
	identEscaper  = strings.NewReplacer("\\", "\\\\", "`", "\\`")
	stringEscaper = strings.NewReplacer("\\", "\\\\", "'", "\\'")
)

// QuoteIdent экранирует идентификатор обратными кавычками: my db -> `my db`.
func QuoteIdent(s string) string { return "`" + identEscaper.Replace(s) + "`" }

// QuoteString экранирует строковый литерал: it's -> 'it\'s'.
func QuoteString(s string) string { return "'" + stringEscaper.Replace(s) + "'" }

// Endpoint — «протокол://адрес» подключения (для check и отчётов): http+tls://host:8443, native+tls://host:9440.
func (c *Client) Endpoint() string {
	p := "native"
	if c.protocol == clickhouse.HTTP {
		p = "http"
	}
	if c.base.TLS != nil {
		p += "+tls"
	}
	return p + "://" + c.addr
}

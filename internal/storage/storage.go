// Package storage — работа с ClickHouse: HTTP-клиент (8123/8443) и снимок схемы хоста (Inventory).
//
// Драйверы не используются: HTTP прозрачен для аудита и работает через любые балансировщики/chproxy.
// chsync выполняет только SELECT/SHOW и CREATE ... IF NOT EXISTS.
package storage

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"chsync/internal/config"
	"chsync/internal/xstrings"
)

// maxBody — предел размера ответа сервера (DDL тысяч таблиц — десятки мегабайт).
const maxBody = 256 << 20

// Client — клиент одного хоста ClickHouse поверх HTTP-интерфейса.
type Client struct {
	name       string // хост так, как он записан в конфиге
	base       string // http(s)://host:port/
	user, pass string
	hc         *http.Client
	logComment string // попадает в system.query_log.log_comment — легко найти запросы chsync
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

// New создаёт клиент по строке из конфига: "host", "host:port" или "https://host:8443".
// По умолчанию HTTPS_PROXY из окружения игнорируется — до прод-хостов ходим напрямую.
func New(spec string, cc config.Connection, runID string) (*Client, error) {
	scheme := "http"
	if cc.Secure {
		scheme = "https"
	}
	hostport := spec
	if strings.HasPrefix(spec, "http://") || strings.HasPrefix(spec, "https://") {
		u, err := url.Parse(spec)
		if err != nil {
			return nil, fmt.Errorf("хост %q: %w", spec, err)
		}
		scheme, hostport = u.Scheme, u.Host
	}
	host, port := hostport, cc.Port
	if h, p, err := net.SplitHostPort(hostport); err == nil {
		host = h
		if port, err = strconv.Atoi(p); err != nil {
			return nil, fmt.Errorf("хост %q: неверный порт", spec)
		}
	}
	tr := &http.Transport{
		MaxIdleConnsPerHost: 4,
		IdleConnTimeout:     60 * time.Second,
		DialContext:         (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
	}
	if cc.UseEnvProxy {
		tr.Proxy = http.ProxyFromEnvironment
	}
	if scheme == "https" {
		tc := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: cc.InsecureSkipVerify} //nolint:gosec // явная опция конфига
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
		tr.TLSClientConfig = tc
	}
	return &Client{
		name:       spec,
		base:       fmt.Sprintf("%s://%s/", scheme, net.JoinHostPort(host, strconv.Itoa(port))),
		user:       cc.User,
		pass:       cc.Password,
		logComment: "chsync " + runID,
		hc:         &http.Client{Transport: tr, Timeout: time.Duration(cc.TimeoutSec) * time.Second},
	}, nil
}

// Name — хост так, как он записан в конфиге.
func (c *Client) Name() string { return c.name }

var codeRe = regexp.MustCompile(`Code: (\d+)`)

// do отправляет запрос и возвращает ответ со статусом 200 (тело закрывает вызывающий).
// wait_end_of_query=1 — чтобы ошибка посреди выполнения пришла HTTP-статусом, а не обрывком тела.
func (c *Client) do(ctx context.Context, query string, settings map[string]string) (*http.Response, error) {
	v := url.Values{}
	v.Set("wait_end_of_query", "1")
	v.Set("output_format_json_quote_64bit_integers", "0")
	v.Set("log_comment", c.logComment)
	for k, val := range settings {
		v.Set(k, val)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"?"+v.Encode(), strings.NewReader(query))
	if err != nil {
		return nil, &Error{Host: c.name, Message: err.Error()}
	}
	req.Header.Set("X-ClickHouse-User", c.user)
	if c.pass != "" {
		req.Header.Set("X-ClickHouse-Key", c.pass) // пароль в заголовке, не в URL
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, &Error{Host: c.name, Message: err.Error()}
	}
	if resp.StatusCode == http.StatusOK {
		return resp, nil
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	e := &Error{Host: c.name, Message: xstrings.Truncate(strings.TrimSpace(string(body)), 2000)}
	if h := resp.Header.Get("X-ClickHouse-Exception-Code"); h != "" {
		e.Code, _ = strconv.Atoi(h)
	} else if m := codeRe.FindStringSubmatch(e.Message); m != nil {
		e.Code, _ = strconv.Atoi(m[1])
	}
	if e.Message == "" {
		e.Message = resp.Status
	}
	return nil, e
}

// Exec выполняет один запрос (DDL) и возвращает тело ответа. settings уходят параметрами URL.
func (c *Client) Exec(ctx context.Context, query string, settings map[string]string) ([]byte, error) {
	resp, err := c.do(ctx, query, settings)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return nil, &Error{Host: c.name, Message: "read body: " + err.Error()}
	}
	return body, nil
}

// Select выполняет SELECT/SHOW в формате JSONEachRow и потоково декодирует каждую строку в T
// (encoding/json/v2: без промежуточного буфера на весь ответ).
// Невалидный UTF-8 в строках заменяется на U+FFFD — так же вёл себя encoding/json v1.
func (c *Client) Select[T any](ctx context.Context, query string, settings map[string]string) ([]T, error) {
	s := maps.Clone(settings)
	if s == nil {
		s = map[string]string{}
	}
	s["default_format"] = "JSONEachRow"
	resp, err := c.do(ctx, query, s)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	dec := jsontext.NewDecoder(io.LimitReader(resp.Body, maxBody), jsontext.AllowInvalidUTF8(true))
	var out []T
	for {
		var row T
		switch err := json.UnmarshalDecode(dec, &row); {
		case errors.Is(err, io.EOF):
			return out, nil
		case err != nil:
			return nil, &Error{Host: c.name, Message: "decode JSONEachRow: " + xstrings.Truncate(err.Error(), 300)}
		}
		out = append(out, row)
	}
}

var (
	identEscaper  = strings.NewReplacer("\\", "\\\\", "`", "\\`")
	stringEscaper = strings.NewReplacer("\\", "\\\\", "'", "\\'")
)

// QuoteIdent экранирует идентификатор обратными кавычками: my db -> `my db`.
func QuoteIdent(s string) string { return "`" + identEscaper.Replace(s) + "`" }

// QuoteString экранирует строковый литерал: it's -> 'it\'s'.
func QuoteString(s string) string { return "'" + stringEscaper.Replace(s) + "'" }

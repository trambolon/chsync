// Package logging — журналирование chsync на log/slog.
//
// Два приёмника:
//   - консоль (stderr): компактные строки "15:04:05 сообщение k=v" для человека, без длинных атрибутов (SQL);
//   - файл <host>.apply.log: slog.TextHandler — машиночитаемо, с полным SQL упавших запросов.
//
// Tee объединяет их через slog.NewMultiHandler: одна запись уходит в оба приёмника,
// а каждый сам решает по уровню, печатать ли её.
package logging

import (
	"context"
	"io"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ConsoleOptions — настройки консольного обработчика.
type ConsoleOptions struct {
	Level    slog.Leveler // минимальный уровень (по умолчанию Info)
	HideKeys []string     // атрибуты, которые в консоль не печатаются (например, "sql")
}

// NewConsole — логгер для консоли: Info, а с verbose — Debug. Атрибут sql скрыт (он есть в apply.log).
func NewConsole(w io.Writer, verbose bool) *slog.Logger {
	return slog.New(NewConsoleHandler(w, ConsoleOptions{Level: level(verbose), HideKeys: []string{"sql"}}))
}

// NewFileHandler — обработчик для файла журнала: все уровни, формат key=value.
func NewFileHandler(w io.Writer) slog.Handler {
	return slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelDebug})
}

// Tee — логгер, пишущий каждую запись во все обработчики.
func Tee(handlers ...slog.Handler) *slog.Logger {
	return slog.New(slog.NewMultiHandler(handlers...))
}

// Discard — логгер, который ничего не пишет (тесты, вызовы без журнала).
func Discard() *slog.Logger { return slog.New(slog.DiscardHandler) }

func level(verbose bool) slog.Level {
	if verbose {
		return slog.LevelDebug
	}
	return slog.LevelInfo
}

// consoleHandler — slog.Handler с коротким человекочитаемым форматом.
type consoleHandler struct {
	mu     *sync.Mutex // общий для всех производных обработчиков: строки не перемешиваются
	w      io.Writer
	opt    ConsoleOptions
	attrs  []slog.Attr // накопленные через WithAttrs (уже с префиксом групп)
	prefix string      // префикс групп "a.b."
}

// NewConsoleHandler создаёт консольный обработчик.
func NewConsoleHandler(w io.Writer, opt ConsoleOptions) slog.Handler {
	if opt.Level == nil {
		opt.Level = slog.LevelInfo
	}
	return &consoleHandler{mu: &sync.Mutex{}, w: w, opt: opt}
}

func (h *consoleHandler) Enabled(_ context.Context, l slog.Level) bool {
	return l >= h.opt.Level.Level()
}

// Handle форматирует запись: время, метка уровня (кроме INFO), сообщение, атрибуты.
func (h *consoleHandler) Handle(_ context.Context, r slog.Record) error {
	buf := make([]byte, 0, 256)
	buf = r.Time.AppendFormat(buf, time.TimeOnly)
	buf = append(buf, ' ')
	switch {
	case r.Level >= slog.LevelError:
		buf = append(buf, "ERROR "...)
	case r.Level >= slog.LevelWarn:
		buf = append(buf, "WARN "...)
	case r.Level < slog.LevelInfo:
		buf = append(buf, "DEBUG "...)
	}
	buf = append(buf, r.Message...)
	for _, a := range h.attrs {
		buf = h.appendAttr(buf, "", a)
	}
	r.Attrs(func(a slog.Attr) bool {
		buf = h.appendAttr(buf, h.prefix, a)
		return true
	})
	buf = append(buf, '\n')

	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := h.w.Write(buf)
	return err
}

// appendAttr дописывает " key=value"; группы раскрываются в "group.key=value".
func (h *consoleHandler) appendAttr(buf []byte, prefix string, a slog.Attr) []byte {
	a.Value = a.Value.Resolve()
	if a.Equal(slog.Attr{}) || slices.Contains(h.opt.HideKeys, a.Key) {
		return buf
	}
	if a.Value.Kind() == slog.KindGroup {
		p := prefix
		if a.Key != "" {
			p += a.Key + "."
		}
		for _, ga := range a.Value.Group() {
			buf = h.appendAttr(buf, p, ga)
		}
		return buf
	}
	buf = append(buf, ' ')
	buf = append(buf, prefix...)
	buf = append(buf, a.Key...)
	buf = append(buf, '=')
	s := a.Value.String()
	if s == "" || strings.ContainsAny(s, " \t\n\"=") {
		return strconv.AppendQuote(buf, s)
	}
	return append(buf, s...)
}

func (h *consoleHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	h2 := *h
	h2.attrs = slices.Clip(h.attrs)
	for _, a := range attrs {
		if h.prefix != "" {
			a.Key = h.prefix + a.Key
		}
		h2.attrs = append(h2.attrs, a)
	}
	return &h2
}

func (h *consoleHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	h2 := *h
	h2.prefix += name + "."
	return &h2
}

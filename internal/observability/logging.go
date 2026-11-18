// Package observability: logging and health endpoints.
package observability

import (
	"log/slog"
	"os"
	"strings"
)

// NewLogger builds the process-wide slog.Logger from config values and
// installs it as the default.
func NewLogger(level, format string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: lvl}
	var h slog.Handler
	if strings.ToLower(format) == "json" {
		h = slog.NewJSONHandler(os.Stderr, opts)
	} else {
		h = slog.NewTextHandler(os.Stderr, opts)
	}
	log := slog.New(h)
	slog.SetDefault(log)
	return log
}

package observability

import (
	"log/slog"
	"testing"
)

func TestNewLogger(t *testing.T) {
	for _, tt := range []struct {
		level, format string
		enabledDebug  bool
	}{
		{"debug", "text", true},
		{"info", "json", false},
		{"warn", "text", false},
		{"garbage", "garbage", false}, // falls back to info/text
	} {
		log := NewLogger(tt.level, tt.format)
		if got := log.Enabled(nil, slog.LevelDebug); got != tt.enabledDebug {
			t.Errorf("%s/%s: debug enabled=%v want %v", tt.level, tt.format, got, tt.enabledDebug)
		}
		if slog.Default() != log {
			t.Error("must install itself as the default logger")
		}
	}
}

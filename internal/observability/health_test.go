package observability

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestServeHealth(t *testing.T) {
	// Reserve a free port.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()

	ready := true
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ServeHealth(ctx, HealthOptions{
		Addr:   addr,
		Ready:  func() bool { return ready },
		Status: func() any { return map[string]int{"clusters": 2} },
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	get := func(p string) (int, string) {
		t.Helper()
		var resp *http.Response
		for i := 0; i < 50; i++ { // wait for listener
			resp, err = http.Get("http://" + addr + p)
			if err == nil {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if err != nil {
			t.Fatalf("GET %s: %v", p, err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}

	if code, _ := get("/health/live"); code != 200 {
		t.Errorf("live = %d", code)
	}
	if code, _ := get("/health/ready"); code != 200 {
		t.Errorf("ready = %d", code)
	}
	ready = false
	if code, _ := get("/health/ready"); code != 503 {
		t.Errorf("not-ready = %d, want 503", code)
	}
	code, body := get("/status")
	var got map[string]int
	if code != 200 || json.Unmarshal([]byte(body), &got) != nil || got["clusters"] != 2 {
		t.Errorf("status = %d %q", code, body)
	}
	if code, _ := get("/nope"); code != 404 {
		t.Errorf("unknown path = %d", code)
	}
}

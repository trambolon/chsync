package observability

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

// HealthOptions wires the endpoints to the daemon's state.
type HealthOptions struct {
	Addr   string
	Ready  func() bool // nil = always ready
	Status func() any  // rendered as JSON on /status; nil = disabled
}

// ServeHealth exposes /health/live, /health/ready and /status until ctx is
// done. Errors after startup are logged, not fatal — a broken health port
// must not kill the snapshot loop. Readiness is answered from in-memory
// state; it never opens database connections.
func ServeHealth(ctx context.Context, o HealthOptions, log *slog.Logger) {
	if o.Addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, _ *http.Request) {
		if o.Ready != nil && !o.Ready() {
			http.Error(w, "not ready: last snapshot failed for at least one cluster", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})
	if o.Status != nil {
		mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			enc := json.NewEncoder(w)
			enc.SetIndent("", "  ")
			_ = enc.Encode(o.Status())
		})
	}

	srv := &http.Server{Addr: o.Addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shCtx)
	}()
	go func() {
		log.Info("health endpoints listening", "addr", o.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("health server failed", "error", err)
		}
	}()
}

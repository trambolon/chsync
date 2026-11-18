package app

import (
	"sync"
	"time"

	"gitlab.example.com/infra/ddl-saver/internal/snapshot"
)

// Status is the daemon's runtime state: the outcome of the last snapshot
// per cluster. The readiness probe reads it instead of opening fresh
// ClickHouse connections on every poll.
type Status struct {
	mu       sync.RWMutex
	clusters map[string]ClusterStatus
}

type ClusterStatus struct {
	LastStarted  time.Time `json:"last_started"`
	LastFinished time.Time `json:"last_finished"`
	// LastSuccess is nil until the cluster has had one successful run.
	// A pointer rather than the `omitzero` tag (Go 1.24+), so it is
	// omitted from /status on every toolchain the module builds with.
	LastSuccess *time.Time     `json:"last_success,omitempty"`
	LastError   string         `json:"last_error,omitempty"`
	LastStats   snapshot.Stats `json:"last_stats"`
}

func NewStatus() *Status { return &Status{clusters: map[string]ClusterStatus{}} }

func (s *Status) record(cluster string, started, finished time.Time, st snapshot.Stats, err error) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cs := s.clusters[cluster]
	cs.LastStarted, cs.LastFinished, cs.LastStats = started, finished, st
	if err != nil {
		cs.LastError = err.Error()
	} else {
		cs.LastError = ""
		ok := finished
		cs.LastSuccess = &ok
	}
	s.clusters[cluster] = cs
}

// Ready reports readiness: no cluster's most recent run failed. Before any
// run has happened it is true — a daemon whose first tick is hours away
// must not be killed by an orchestrator for "never becoming ready".
func (s *Status) Ready() bool {
	if s == nil {
		return true
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, cs := range s.clusters {
		if cs.LastError != "" {
			return false
		}
	}
	return true
}

// Snapshot returns a copy for the /status endpoint.
func (s *Status) Snapshot() map[string]ClusterStatus {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]ClusterStatus, len(s.clusters))
	for k, v := range s.clusters {
		out[k] = v
	}
	return out
}

// Package gitlabtest is an in-process GitLab for tests: the handful of REST
// endpoints the adapter calls, backed by a map.
package gitlabtest

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Server is the slice of the GitLab REST API the adapter uses (project
// "infra/schemas", branch "main", token "secret"), served over real HTTP
// so the SDK, its URL escaping and its pagination are all exercised.
type Server struct {
	mu        sync.Mutex
	Files     map[string]string // path -> content, on branch "main"
	NoProject bool
	EmptyRepo bool
	Commits   []map[string]any
	inFlight  atomic.Int32
	maxSeen   atomic.Int32
	Delay     time.Duration // per file download
	FailFile  string        // this path answers 404
}

const (
	Project = "infra/schemas"
	Branch  = "main"
	Token   = "secret"

	projectAPI = "/api/v4/projects/infra%2Fschemas"
)

// PeakDownloads is the highest number of file downloads seen in flight.
func (g *Server) PeakDownloads() int { return int(g.maxSeen.Load()) }

// Snapshot returns a copy of the repository content and the commit messages.
func (g *Server) Snapshot() (files map[string]string, messages []string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	files = make(map[string]string, len(g.Files))
	for k, v := range g.Files {
		files[k] = v
	}
	for _, c := range g.Commits {
		messages = append(messages, fmt.Sprint(c["commit_message"]))
	}
	return files, messages
}

func (g *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Private-Token") != Token {
		http.Error(w, `{"message":"401 Unauthorized"}`, http.StatusUnauthorized)
		return
	}
	rest, ok := strings.CutPrefix(r.URL.EscapedPath(), projectAPI)
	if !ok || g.NoProject {
		http.Error(w, `{"message":"404 Project Not Found"}`, http.StatusNotFound)
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	switch {
	case rest == "" && r.Method == http.MethodGet:
		fmt.Fprintf(w, `{"id":1,"empty_repo":%v}`, g.EmptyRepo)

	case rest == "/repository/tree":
		q := r.URL.Query()
		if q.Get("ref") != Branch || g.EmptyRepo {
			http.Error(w, `{"message":"404 Tree Not Found"}`, http.StatusNotFound)
			return
		}
		var match []string
		for p := range g.Files {
			if q.Get("path") == "" || strings.HasPrefix(p, q.Get("path")+"/") {
				match = append(match, p)
			}
		}
		if len(match) == 0 && q.Get("path") != "" {
			http.Error(w, `{"message":"404 invalid revision or path Not Found"}`, http.StatusNotFound)
			return
		}
		slices.Sort(match)
		page, per := 1, 2 // tiny pages: pagination is always exercised
		fmt.Sscan(q.Get("page"), &page)
		lo := min((page-1)*per, len(match))
		hi := min(lo+per, len(match))
		if hi < len(match) {
			w.Header().Set("X-Next-Page", fmt.Sprint(page+1))
		}
		nodes := []map[string]string{{"path": "schemas", "type": "tree"}}
		for _, p := range match[lo:hi] {
			nodes = append(nodes, map[string]string{"path": p, "type": "blob"})
		}
		_ = json.NewEncoder(w).Encode(nodes)

	case strings.HasPrefix(rest, "/repository/files/") && strings.HasSuffix(rest, "/raw"):
		g.mu.Unlock()
		n := g.inFlight.Add(1)
		for {
			if m := g.maxSeen.Load(); n <= m || g.maxSeen.CompareAndSwap(m, n) {
				break
			}
		}
		time.Sleep(g.Delay)
		g.inFlight.Add(-1)
		g.mu.Lock()
		p, _ := url.PathUnescape(strings.TrimSuffix(strings.TrimPrefix(rest, "/repository/files/"), "/raw"))
		content, ok := g.Files[p]
		if !ok || p == g.FailFile {
			http.Error(w, `{"message":"404 File Not Found"}`, http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, content)

	case rest == "/repository/commits" && r.Method == http.MethodPost:
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		g.Commits = append(g.Commits, body)
		for _, a := range body["actions"].([]any) {
			act := a.(map[string]any)
			if act["action"] == "delete" {
				delete(g.Files, act["file_path"].(string))
			} else {
				g.Files[act["file_path"].(string)] = act["content"].(string)
			}
		}
		g.EmptyRepo = false
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"id":"abc"}`)

	default:
		http.Error(w, `{"message":"404 Not Found"}`, http.StatusNotFound)
	}
}

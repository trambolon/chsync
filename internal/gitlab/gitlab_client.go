package gitlab

// Adapter for the official GitLab Go SDK.
//
// NOTE: the historical xanzy/go-gitlab module was archived in 2025 and now
// lives at gitlab.com/gitlab-org/api/client-go with an identical API — this
// project uses the maintained path. See README "Dependencies".

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	gl "gitlab.com/gitlab-org/api/client-go"
)

type ClientOptions struct {
	BaseURL     string // e.g. https://gitlab.example.com
	Token       string
	ProjectPath string // owner/repo
	AuthorName  string
	AuthorEmail string
	CommitPause time.Duration // throttle between consecutive commits
	// ReadConcurrency bounds parallel file downloads. A snapshot reads
	// every file of the cluster on every run, one request per file.
	ReadConcurrency int
}

type apiClient struct {
	git  *gl.Client
	opts ClientOptions
}

func NewRepo(o ClientOptions) (Repo, error) {
	git, err := gl.NewClient(o.Token, gl.WithBaseURL(strings.TrimSuffix(o.BaseURL, "/")+"/api/v4"))
	if err != nil {
		return nil, fmt.Errorf("gitlab client: %w", err)
	}
	if o.CommitPause == 0 {
		o.CommitPause = 500 * time.Millisecond
	}
	if o.ReadConcurrency <= 0 {
		o.ReadConcurrency = 8
	}
	return &apiClient{git: git, opts: o}, nil
}

func notFound(resp *gl.Response) bool {
	return resp != nil && resp.Response != nil && resp.StatusCode == http.StatusNotFound
}

func (c *apiClient) ListTree(ctx context.Context, ref, basePath string) ([]string, error) {
	var paths []string
	opt := &gl.ListTreeOptions{
		Path:        gl.Ptr(basePath),
		Ref:         gl.Ptr(ref),
		Recursive:   gl.Ptr(true),
		ListOptions: gl.ListOptions{PerPage: 100},
	}
	for {
		nodes, resp, err := c.git.Repositories.ListTree(c.opts.ProjectPath, opt, gl.WithContext(ctx))
		if err != nil {
			if notFound(resp) {
				// GitLab answers 404 for a directory that does not exist
				// yet, which is the normal state before the first snapshot
				// of a cluster. It answers 404 for a wrong project or ref
				// too, and those must not pass for "empty".
				return nil, c.explainNotFound(ctx, ref, basePath)
			}
			return nil, fmt.Errorf("list tree %s@%s: %w", basePath, ref, err)
		}
		for _, n := range nodes {
			if n.Type == "blob" {
				paths = append(paths, n.Path)
			}
		}
		if resp == nil || resp.NextPage == 0 {
			break
		}
		opt.Page = resp.NextPage
	}
	return paths, nil
}

// explainNotFound turns a 404 from the tree API into nil when only the
// directory is missing, and into a specific error otherwise.
func (c *apiClient) explainNotFound(ctx context.Context, ref, basePath string) error {
	project, resp, err := c.git.Projects.GetProject(c.opts.ProjectPath, nil, gl.WithContext(ctx))
	if err != nil {
		if notFound(resp) {
			return fmt.Errorf("gitlab project %q not found: check gitlab.project_path and that the token can see the project",
				c.opts.ProjectPath)
		}
		return fmt.Errorf("get project %s: %w", c.opts.ProjectPath, err)
	}
	if project.EmptyRepo {
		return nil // no commits yet: the first snapshot creates the branch
	}
	// The root always exists on a ref that exists.
	_, resp, err = c.git.Repositories.ListTree(c.opts.ProjectPath,
		&gl.ListTreeOptions{Ref: gl.Ptr(ref), ListOptions: gl.ListOptions{PerPage: 1}}, gl.WithContext(ctx))
	if err != nil {
		if notFound(resp) {
			return fmt.Errorf("ref %q not found in gitlab project %s", ref, c.opts.ProjectPath)
		}
		return fmt.Errorf("list tree @%s: %w", ref, err)
	}
	return nil // the ref exists, basePath just has no files yet
}

// GetFiles downloads paths with bounded parallelism. The first failure
// cancels the rest.
func (c *apiClient) GetFiles(ctx context.Context, ref string, paths []string) (map[string]string, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		mu       sync.Mutex
		out      = make(map[string]string, len(paths))
		firstErr error
		wg       sync.WaitGroup
		next     = make(chan string)
	)
	fail := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if firstErr == nil {
			firstErr = err
			cancel()
		}
	}
	for range min(c.opts.ReadConcurrency, len(paths)) {
		wg.Go(func() {
			for p := range next {
				raw, _, err := c.git.RepositoryFiles.GetRawFile(c.opts.ProjectPath, p,
					&gl.GetRawFileOptions{Ref: gl.Ptr(ref)}, gl.WithContext(ctx))
				if err != nil {
					fail(fmt.Errorf("get file %s@%s: %w", p, ref, err))
					continue // keep draining so the producer never blocks
				}
				mu.Lock()
				out[p] = string(raw)
				mu.Unlock()
			}
		})
	}
feed:
	for _, p := range paths {
		select {
		case next <- p:
		case <-ctx.Done():
			break feed
		}
	}
	close(next)
	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *apiClient) Commit(ctx context.Context, branch, message string, actions []Action) error {
	if len(actions) == 0 {
		return nil
	}
	glActions := make([]*gl.CommitActionOptions, 0, len(actions))
	for _, a := range actions {
		var op gl.FileActionValue
		switch a.Op {
		case OpCreate:
			op = gl.FileCreate
		case OpUpdate:
			op = gl.FileUpdate
		case OpDelete:
			op = gl.FileDelete
		default:
			return fmt.Errorf("unknown action op %q", a.Op)
		}
		act := &gl.CommitActionOptions{Action: gl.Ptr(op), FilePath: gl.Ptr(a.Path)}
		if a.Op != OpDelete {
			act.Content = gl.Ptr(a.Content)
		}
		glActions = append(glActions, act)
	}
	opt := &gl.CreateCommitOptions{
		Branch:        gl.Ptr(branch),
		CommitMessage: gl.Ptr(message),
		Actions:       glActions,
	}
	if c.opts.AuthorName != "" {
		opt.AuthorName = gl.Ptr(c.opts.AuthorName)
	}
	if c.opts.AuthorEmail != "" {
		opt.AuthorEmail = gl.Ptr(c.opts.AuthorEmail)
	}
	if _, _, err := c.git.Commits.CreateCommit(c.opts.ProjectPath, opt, gl.WithContext(ctx)); err != nil {
		return fmt.Errorf("create commit on %s: %w", branch, err)
	}
	// Light throttle so batched snapshot commits stay well under API limits.
	select {
	case <-time.After(c.opts.CommitPause):
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

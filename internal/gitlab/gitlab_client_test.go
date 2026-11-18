package gitlab_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gitlab.example.com/infra/ddl-saver/internal/gitlab"
	"gitlab.example.com/infra/ddl-saver/internal/gitlab/gitlabtest"
)

func newTestRepo(t *testing.T, g *gitlabtest.Server, token string) gitlab.Repo {
	t.Helper()
	srv := httptest.NewServer(g)
	t.Cleanup(srv.Close)
	repo, err := gitlab.NewRepo(gitlab.ClientOptions{
		BaseURL: srv.URL + "/", Token: token, ProjectPath: "infra/schemas",
		AuthorName: "DDL Saver Bot", AuthorEmail: "bot@example.com",
		CommitPause: time.Millisecond, ReadConcurrency: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	return repo
}

func seeded() *gitlabtest.Server {
	g := &gitlabtest.Server{Files: map[string]string{"README.md": "hi"}}
	for i := range 7 {
		g.Files[fmt.Sprintf("schemas/prod/events/tables/t%d.sql", i)] = fmt.Sprintf("CREATE TABLE events.t%d\n", i)
	}
	g.Files["schemas/prod/events/tables/2024%2Fq1.sql"] = "encoded name\n"
	g.Files["schemas/staging/x/tables/other.sql"] = "other cluster\n"
	return g
}

func TestRepo_ListAndReadOverHTTP(t *testing.T) {
	g := seeded()
	g.Delay = 20 * time.Millisecond
	repo := newTestRepo(t, g, "secret")
	ctx := context.Background()

	paths, err := repo.ListTree(ctx, "main", "schemas/prod")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 8 { // 4 pages of 2; directories and other clusters excluded
		t.Fatalf("got %d paths: %v", len(paths), paths)
	}
	files, err := repo.GetFiles(ctx, "main", paths)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range paths {
		if files[p] != g.Files[p] {
			t.Errorf("%s: got %q", p, files[p])
		}
	}
	if files["schemas/prod/events/tables/2024%2Fq1.sql"] != "encoded name\n" {
		t.Error("a path containing '%' must survive URL escaping")
	}
	if peak := int32(g.PeakDownloads()); peak < 2 || peak > 4 {
		t.Errorf("peak parallel downloads = %d, want 2..4", peak)
	}
	if got, err := repo.GetFiles(ctx, "main", nil); err != nil || len(got) != 0 {
		t.Errorf("no paths: %v %v", got, err)
	}
}

func TestRepo_ListTree_NotFoundMeansDifferentThings(t *testing.T) {
	ctx := context.Background()

	// 1. the cluster has never been snapshotted: empty, not an error
	paths, err := newTestRepo(t, seeded(), "secret").ListTree(ctx, "main", "schemas/brand-new")
	if err != nil || len(paths) != 0 {
		t.Errorf("missing directory: %v %v", paths, err)
	}
	// 2. a repository with no commits at all
	paths, err = newTestRepo(t, &gitlabtest.Server{EmptyRepo: true, Files: map[string]string{}}, "secret").ListTree(ctx, "main", "schemas/prod")
	if err != nil || len(paths) != 0 {
		t.Errorf("empty repository: %v %v", paths, err)
	}
	// 3. a wrong ref is an error, or recovery would report "no objects"
	_, err = newTestRepo(t, seeded(), "secret").ListTree(ctx, "no-such-branch", "schemas/prod")
	if err == nil || !strings.Contains(err.Error(), `ref "no-such-branch" not found`) {
		t.Errorf("missing ref: %v", err)
	}
	// 4. a wrong project path
	_, err = newTestRepo(t, &gitlabtest.Server{NoProject: true}, "secret").ListTree(ctx, "main", "schemas/prod")
	if err == nil || !strings.Contains(err.Error(), "gitlab.project_path") {
		t.Errorf("missing project: %v", err)
	}
	// 5. a bad token
	_, err = newTestRepo(t, seeded(), "wrong").ListTree(ctx, "main", "schemas/prod")
	if err == nil || !strings.Contains(err.Error(), "401") {
		t.Errorf("bad token: %v", err)
	}
}

func TestRepo_GetFiles_FirstErrorWinsAndCancels(t *testing.T) {
	g := seeded()
	g.FailFile = "schemas/prod/events/tables/t3.sql"
	repo := newTestRepo(t, g, "secret")
	paths, _ := repo.ListTree(context.Background(), "main", "schemas/prod")
	_, err := repo.GetFiles(context.Background(), "main", paths)
	if err == nil || !strings.Contains(err.Error(), "t3.sql") {
		t.Fatalf("want the failing file named, got %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := repo.GetFiles(ctx, "main", paths); err == nil {
		t.Fatal("a cancelled context must stop the download")
	}
}

func TestRepo_Commit(t *testing.T) {
	g := seeded()
	repo := newTestRepo(t, g, "secret")
	err := repo.Commit(context.Background(), "main", "ddlSaver: snapshot prod (+1 ~1 -1)", []gitlab.Action{
		{Op: gitlab.OpCreate, Path: "schemas/prod/events/tables/new.sql", Content: "CREATE TABLE events.new\n"},
		{Op: gitlab.OpUpdate, Path: "schemas/prod/events/tables/t0.sql", Content: "changed\n"},
		{Op: gitlab.OpDelete, Path: "schemas/prod/events/tables/t1.sql"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(g.Commits) != 1 {
		t.Fatalf("commits: %d", len(g.Commits))
	}
	c := g.Commits[0]
	if c["branch"] != "main" || c["commit_message"] != "ddlSaver: snapshot prod (+1 ~1 -1)" ||
		c["author_name"] != "DDL Saver Bot" || c["author_email"] != "bot@example.com" {
		t.Errorf("commit metadata: %v", c)
	}
	actions := c["actions"].([]any)
	if len(actions) != 3 {
		t.Fatalf("actions: %v", actions)
	}
	if del := actions[2].(map[string]any); del["action"] != "delete" || del["content"] != nil {
		t.Errorf("a delete must not carry content: %v", del)
	}
	if g.Files["schemas/prod/events/tables/t0.sql"] != "changed\n" {
		t.Error("update not applied")
	}
	if _, ok := g.Files["schemas/prod/events/tables/t1.sql"]; ok {
		t.Error("delete not applied")
	}

	if err := repo.Commit(context.Background(), "main", "nothing", nil); err != nil || len(g.Commits) != 1 {
		t.Errorf("an empty commit must not reach the API: %v", err)
	}
	if err := repo.Commit(context.Background(), "main", "x", []gitlab.Action{{Op: "rename", Path: "a"}}); err == nil {
		t.Error("unknown op accepted")
	}
}

// Package gitlab: GitOps side of ddlSaver. The Repo interface is what the
// snapshot and recovery flows consume; the go-gitlab adapter lives in
// gitlab_client.go and is the only file importing the SDK.
package gitlab

import "context"

type Op string

const (
	OpCreate Op = "create"
	OpUpdate Op = "update"
	OpDelete Op = "delete"
)

type Action struct {
	Op      Op
	Path    string
	Content string // empty for deletes
}

type Repo interface {
	// ListTree returns file paths under basePath at ref (recursive).
	ListTree(ctx context.Context, ref, basePath string) ([]string, error)
	// GetFiles fetches raw contents for the given paths at ref.
	GetFiles(ctx context.Context, ref string, paths []string) (map[string]string, error)
	// Commit applies actions as one commit on branch.
	Commit(ctx context.Context, branch, message string, actions []Action) error
}

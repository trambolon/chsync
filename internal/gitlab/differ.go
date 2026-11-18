package gitlab

import (
	"cmp"
	"slices"
)

// Diff computes the commit actions turning actual into desired. Both maps
// are path -> content. Output is sorted by path for deterministic commits
// (and reviewable diffs in the schema repo history).
func Diff(desired, actual map[string]string) []Action {
	var out []Action
	for path, want := range desired {
		have, ok := actual[path]
		switch {
		case !ok:
			out = append(out, Action{Op: OpCreate, Path: path, Content: want})
		case have != want:
			out = append(out, Action{Op: OpUpdate, Path: path, Content: want})
		}
	}
	for path := range actual {
		if _, ok := desired[path]; !ok {
			out = append(out, Action{Op: OpDelete, Path: path})
		}
	}
	slices.SortFunc(out, func(a, b Action) int { return cmp.Compare(a.Path, b.Path) })
	return out
}

// SplitDeletes separates deletes so the prune policy can be applied.
func SplitDeletes(actions []Action) (writes, deletes []Action) {
	for _, a := range actions {
		if a.Op == OpDelete {
			deletes = append(deletes, a)
		} else {
			writes = append(writes, a)
		}
	}
	return writes, deletes
}

// Batch splits actions into chunks of at most size (GitLab's commit API
// degrades beyond ~100 file actions per request).
func Batch(actions []Action, size int) [][]Action {
	if size <= 0 {
		size = 100
	}
	var out [][]Action
	for len(actions) > 0 {
		n := size
		if n > len(actions) {
			n = len(actions)
		}
		out = append(out, actions[:n])
		actions = actions[n:]
	}
	return out
}

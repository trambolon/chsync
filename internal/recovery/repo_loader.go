package recovery

import (
	"fmt"
	"path"
	"slices"
	"strings"

	"gitlab.example.com/infra/ddl-saver/internal/model"
)

// Repo layout (mirrors what the snapshot job writes):
//
//	<base_path>/<cluster>/<database>/<kind_dir>/<object>.sql
//
// Identity (db, name, kind) comes from the PATH. Dependencies come from the
// CONTENT via ExtractDeps. The two are cross-checked: a kind mismatch between
// directory and DDL text becomes a warning, path identity wins.
// Kind directories are defined by model.KindFromDir / ObjectKind.Dir.

// LoadObjects builds DDLObjects from a path->content map (as returned by the
// GitLab reader for a given ref). Files outside basePath/cluster or not
// matching the layout are skipped with a warning.
//
// Candidate resolution policy:
//   - qualified refs are always kept (the planner checks the live cluster for
//     anything the repo cannot supply);
//   - unqualified refs are kept only if they resolve inside the repo index —
//     unresolved unqualified refs are almost always false positives (column
//     names after keyword-like constructs) and are dropped with a note.
func LoadObjects(files map[string]string, basePath, cluster string) ([]*DDLObject, []string) {
	var globalWarnings []string
	prefix := path.Join(basePath, cluster) + "/"

	type parsed struct {
		obj   *DDLObject
		cands []DepCandidate
	}
	var items []parsed
	index := map[ObjectRef]bool{}

	// Deterministic order regardless of map iteration.
	paths := make([]string, 0, len(files))
	for p := range files {
		paths = append(paths, p)
	}
	slices.Sort(paths)

	for _, p := range paths {
		if !strings.HasPrefix(p, prefix) || !strings.HasSuffix(p, ".sql") {
			continue
		}
		rel := strings.TrimPrefix(p, prefix)
		if strings.HasPrefix(rel, model.DeprecatedDir+"/") {
			// Archived objects are deliberately outside the recovery
			// surface of the current ref; restore them by pointing --ref
			// at a commit where they were still live.
			continue
		}
		ref, pathKind, ok := model.ParseObjectPath(rel)
		if !ok {
			globalWarnings = append(globalWarnings,
				fmt.Sprintf("skip %s: expected <db>/<kind>/<name>.sql or <db>/%s", p, model.DatabaseFile))
			continue
		}
		// <db>/_database.sql — the database's own DDL.
		if pathKind == KindDatabase {
			obj := &DDLObject{Ref: ref, Kind: KindDatabase, SQL: files[p]}
			index[obj.Ref] = true
			items = append(items, parsed{obj: obj})
			continue
		}
		db := ref.Database

		sql := files[p]
		ddlKind, cands, external, warns := ExtractDeps(db, sql)

		obj := &DDLObject{
			Ref:      ref,
			Kind:     pathKind,
			SQL:      sql,
			External: external,
			Warnings: warns,
		}
		if ddlKind != KindUnknown && ddlKind != pathKind {
			obj.Warnings = append(obj.Warnings,
				fmt.Sprintf("kind mismatch: path says %s, DDL says %s (path wins)", pathKind, ddlKind))
		}
		index[obj.Ref] = true
		items = append(items, parsed{obj: obj, cands: cands})
	}

	// Second pass: resolve candidates now that the full index exists.
	var objs []*DDLObject
	for _, it := range items {
		seen := map[ObjectRef]bool{it.obj.Ref: true} // never depend on self
		for _, c := range it.cands {
			if seen[c.Ref] {
				continue
			}
			if !c.Qualified && !index[c.Ref] {
				it.obj.Warnings = append(it.obj.Warnings,
					fmt.Sprintf("dropped unqualified ref %q (origin %s): not in repo, likely not a table",
						c.Ref.Name, c.Origin))
				continue
			}
			seen[c.Ref] = true
			it.obj.DependsOn = append(it.obj.DependsOn, c.Ref)
		}
		objs = append(objs, it.obj)
	}
	return objs, globalWarnings
}

// ReverseIndex maps each object to the objects that depend on it (downstream
// dependents). Used by the planner for --with-dependents and for the
// "MV reads from a table you are about to recreate" notice.
func ReverseIndex(objs []*DDLObject) map[ObjectRef][]ObjectRef {
	rev := map[ObjectRef][]ObjectRef{}
	for _, o := range objs {
		for _, dep := range o.DependsOn {
			rev[dep] = append(rev[dep], o.Ref)
		}
	}
	for _, refs := range rev {
		slices.SortFunc(refs, ObjectRef.Compare)
	}
	return rev
}

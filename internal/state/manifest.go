// Package state: the manifest tracks what a snapshot last saw, keyed by
// "db.name". It is committed to the schema repo next to the DDL files, which
// makes every snapshot's view of the world reviewable in git history and
// lets the next run compute created/modified/removed counts cheaply.
package state

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"gitlab.example.com/infra/ddl-saver/internal/model"
)

const Version = "2.0"

type Manifest struct {
	Version     string                 `json:"version"`
	Cluster     string                 `json:"cluster"`
	GeneratedAt time.Time              `json:"generated_at"`
	Objects     map[string]ObjectState `json:"objects"` // key: "db.name"
	// Deprecated records objects whose files were moved to the _deprecated
	// area (on_deleted = "deprecate"): when the object was first seen
	// missing. Keys mirror Objects ("db.name").
	Deprecated map[string]DeprecatedState `json:"deprecated,omitempty"`
}

type DeprecatedState struct {
	Kind  model.ObjectKind `json:"kind"`
	Since time.Time        `json:"since"`
}

type ObjectState struct {
	Kind     model.ObjectKind `json:"kind"`
	Checksum string           `json:"checksum"` // sha256 of normalized DDL
}

func New(cluster string, now time.Time) *Manifest {
	return &Manifest{
		Version:     Version,
		Cluster:     cluster,
		GeneratedAt: now.UTC(),
		Objects:     map[string]ObjectState{},
		Deprecated:  map[string]DeprecatedState{},
	}
}

func (m *Manifest) Add(ref model.ObjectRef, kind model.ObjectKind, ddl string) {
	m.Objects[ref.String()] = ObjectState{Kind: kind, Checksum: Checksum(ddl)}
}

func Checksum(ddl string) string {
	sum := sha256.Sum256([]byte(ddl))
	return hex.EncodeToString(sum[:])
}

func (m *Manifest) Marshal() ([]byte, error) {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

func Unmarshal(data []byte) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parse manifest: %w", err)
	}
	if m.Objects == nil {
		m.Objects = map[string]ObjectState{}
	}
	if m.Deprecated == nil {
		m.Deprecated = map[string]DeprecatedState{}
	}
	return &m, nil
}

// DiffCounts compares two manifests by object key and checksum. prev may be
// nil (first snapshot).
func DiffCounts(prev, cur *Manifest) (created, modified, removed []string) {
	prevObjs := map[string]ObjectState{}
	if prev != nil {
		prevObjs = prev.Objects
	}
	for key, state := range cur.Objects {
		old, ok := prevObjs[key]
		switch {
		case !ok:
			created = append(created, key)
		case old.Checksum != state.Checksum:
			modified = append(modified, key)
		}
	}
	for key := range prevObjs {
		if _, ok := cur.Objects[key]; !ok {
			removed = append(removed, key)
		}
	}
	slices.Sort(created)
	slices.Sort(modified)
	slices.Sort(removed)
	return created, modified, removed
}

// SameObjects reports whether two manifests describe identical object AND
// deprecation sets (ignoring GeneratedAt), so an unchanged snapshot produces
// zero commits.
func SameObjects(a, b *Manifest) bool {
	if a == nil || b == nil {
		return a == b
	}
	if len(a.Objects) != len(b.Objects) || len(a.Deprecated) != len(b.Deprecated) {
		return false
	}
	for k, v := range a.Objects {
		if b.Objects[k] != v {
			return false
		}
	}
	for k, v := range a.Deprecated {
		w, ok := b.Deprecated[k]
		if !ok || w.Kind != v.Kind || !w.Since.Equal(v.Since) {
			return false
		}
	}
	return true
}

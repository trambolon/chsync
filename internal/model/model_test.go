package model

import (
	"slices"
	"strings"
	"testing"
)

func TestObjectRef(t *testing.T) {
	t.Parallel()
	obj := ObjectRef{Database: "events", Name: "clicks"}
	db := obj.DatabaseRef()

	if obj.IsDatabase() || !db.IsDatabase() {
		t.Error("IsDatabase misclassifies")
	}
	if obj.String() != "events.clicks" || db.String() != "events" {
		t.Errorf("String: %q / %q", obj, db)
	}

	refs := []ObjectRef{obj, {Database: "a", Name: "z"}, db, {Database: "a"}}
	slices.SortFunc(refs, ObjectRef.Compare)
	want := []string{"a", "a.z", "events", "events.clicks"}
	for i, r := range refs {
		if r.String() != want[i] {
			t.Fatalf("sort order %v, want %v", refs, want)
		}
	}
}

func TestParseRef(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in, defaultDB string
		want          string
		wantErr       bool
	}{
		{"db.t", "", "db.t", false},
		{"t", "db", "db.t", false},
		{"t", "", "", true},
		{"a.b.c", "", "", true},
		{".t", "", "", true},
		{"db.", "", "", true},
	}
	for _, tt := range tests {
		got, err := ParseRef(tt.in, tt.defaultDB)
		if (err != nil) != tt.wantErr {
			t.Errorf("ParseRef(%q): err=%v wantErr=%v", tt.in, err, tt.wantErr)
			continue
		}
		if err == nil && got.String() != tt.want {
			t.Errorf("ParseRef(%q) = %s, want %s", tt.in, got, tt.want)
		}
	}
}

func TestKindRoundTrip(t *testing.T) {
	t.Parallel()
	for _, k := range []ObjectKind{KindTable, KindView, KindMaterializedView, KindDictionary} {
		back, ok := KindFromDir(k.Dir())
		if !ok || back != k {
			t.Errorf("kind %s does not round-trip through Dir(): %s/%v", k, back, ok)
		}
	}
	if _, ok := KindFromDir(KindDatabase.Dir()); ok {
		t.Error("database has no kind directory")
	}
	if KindFromEngine("ReplicatedMergeTree") != KindTable || KindFromEngine("MaterializedView") != KindMaterializedView {
		t.Error("KindFromEngine mapping wrong")
	}
}

func TestPathSegment_RoundTripAndSafety(t *testing.T) {
	plain := []string{"events", "clicks_raw", "Analytics", "таблица", "a.b", "_tmp", "x-y", "with space"}
	for _, n := range plain {
		if got := PathSegment(n); got != n {
			t.Errorf("ordinary name %q must not change, got %q", n, got)
		}
	}
	for name, want := range map[string]string{
		"a/b": "a%2Fb", "..": "%2E.", ".": "%2E", "50%": "50%25", `a\b`: "a%5Cb",
		"a\nb": "a%0Ab", "_deprecated": "%5Fdeprecated", "../../etc": "..%2F..%2Fetc",
	} {
		got := PathSegment(name)
		if got != want {
			t.Errorf("PathSegment(%q) = %q, want %q", name, got, want)
		}
		if strings.ContainsAny(got, "/\\") || got == "." || got == ".." || got == DeprecatedDir {
			t.Errorf("PathSegment(%q) = %q is not a safe single segment", name, got)
		}
		if back := NameFromSegment(got); back != name {
			t.Errorf("round trip %q -> %q -> %q", name, got, back)
		}
	}
	// files written before encoding existed keep their names
	for _, legacy := range []string{"50%", "a%b", "100%zz", "%"} {
		if got := NameFromSegment(legacy); got != legacy {
			t.Errorf("legacy segment %q decoded to %q", legacy, got)
		}
	}
}

func TestObjectPath_RoundTrip(t *testing.T) {
	for _, tt := range []struct {
		ref  ObjectRef
		kind ObjectKind
		path string
	}{
		{ObjectRef{"events", "clicks"}, KindTable, "events/tables/clicks.sql"},
		{ObjectRef{"events", "mv"}, KindMaterializedView, "events/materialized_views/mv.sql"},
		{ObjectRef{Database: "events"}, KindDatabase, "events/_database.sql"},
		{ObjectRef{"a/b", "../x"}, KindView, "a%2Fb/views/..%2Fx.sql"},
		{ObjectRef{"db", "v1.2"}, KindDictionary, "db/dictionaries/v1.2.sql"},
	} {
		if got := ObjectPath(tt.ref, tt.kind); got != tt.path {
			t.Errorf("ObjectPath(%v) = %q, want %q", tt.ref, got, tt.path)
		}
		ref, kind, ok := ParseObjectPath(tt.path)
		if !ok || ref != tt.ref || kind != tt.kind {
			t.Errorf("ParseObjectPath(%q) = %v %v %v", tt.path, ref, kind, ok)
		}
	}
	for _, bad := range []string{"", "x.sql", "db/tables/t.txt", "db/nope/t.sql", "db/tables/a/b.sql", "manifest.json", "db/tables/.sql", "/_database.sql"} {
		if _, _, ok := ParseObjectPath(bad); ok {
			t.Errorf("ParseObjectPath(%q) accepted", bad)
		}
	}
}

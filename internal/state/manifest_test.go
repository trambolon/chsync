package state

import (
	"reflect"
	"testing"
	"time"

	"gitlab.example.com/infra/ddl-saver/internal/model"
)

func ref(db, name string) model.ObjectRef { return model.ObjectRef{Database: db, Name: name} }

func TestManifestRoundTrip(t *testing.T) {
	m := New("prod", time.Date(2026, 8, 23, 9, 0, 0, 0, time.UTC))
	m.Add(ref("events", "clicks"), model.KindTable, "CREATE TABLE ...")
	b, err := m.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	got, err := Unmarshal(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != Version || got.Cluster != "prod" || len(got.Objects) != 1 {
		t.Errorf("round trip mismatch: %+v", got)
	}
	if got.Objects["events.clicks"].Checksum != Checksum("CREATE TABLE ...") {
		t.Error("checksum mismatch after round trip")
	}
}

func TestDiffCounts(t *testing.T) {
	now := time.Now()
	prev := New("prod", now)
	prev.Add(ref("db", "keep"), model.KindTable, "same")
	prev.Add(ref("db", "change"), model.KindTable, "v1")
	prev.Add(ref("db", "gone"), model.KindTable, "x")

	cur := New("prod", now)
	cur.Add(ref("db", "keep"), model.KindTable, "same")
	cur.Add(ref("db", "change"), model.KindTable, "v2")
	cur.Add(ref("db", "new"), model.KindView, "y")

	created, modified, removed := DiffCounts(prev, cur)
	if !reflect.DeepEqual(created, []string{"db.new"}) ||
		!reflect.DeepEqual(modified, []string{"db.change"}) ||
		!reflect.DeepEqual(removed, []string{"db.gone"}) {
		t.Errorf("created=%v modified=%v removed=%v", created, modified, removed)
	}

	c2, m2, r2 := DiffCounts(nil, cur)
	if len(c2) != 3 || m2 != nil || r2 != nil {
		t.Errorf("first snapshot: created=%v modified=%v removed=%v", c2, m2, r2)
	}
}

func TestSameObjects_IgnoresTimestamp(t *testing.T) {
	a := New("prod", time.Now())
	a.Add(ref("db", "t"), model.KindTable, "ddl")
	b := New("prod", time.Now().Add(time.Hour))
	b.Add(ref("db", "t"), model.KindTable, "ddl")
	if !SameObjects(a, b) {
		t.Error("manifests differing only in GeneratedAt must compare equal")
	}
	b.Add(ref("db", "u"), model.KindTable, "ddl")
	if SameObjects(a, b) {
		t.Error("different object sets must not compare equal")
	}
}

func TestSameObjects_TracksDeprecation(t *testing.T) {
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	a := New("prod", now)
	a.Add(ref("db", "t"), model.KindTable, "ddl")
	b := New("prod", now.Add(time.Hour))
	b.Add(ref("db", "t"), model.KindTable, "ddl")
	b.Deprecated["db.old"] = DeprecatedState{Kind: model.KindTable, Since: now}
	if SameObjects(a, b) {
		t.Error("differing deprecation records must not compare equal")
	}
	a.Deprecated["db.old"] = DeprecatedState{Kind: model.KindTable, Since: now}
	if !SameObjects(a, b) {
		t.Error("identical deprecation records must compare equal")
	}

	// Round trip keeps records comparable (Since survives JSON).
	raw, err := b.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := Unmarshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !SameObjects(b, parsed) {
		t.Error("manifest with deprecation records must survive a round trip")
	}
}

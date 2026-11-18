package gitlab

import (
	"reflect"
	"testing"
)

func TestDiff(t *testing.T) {
	desired := map[string]string{
		"a.sql": "one",
		"b.sql": "two-changed",
		"c.sql": "three",
	}
	actual := map[string]string{
		"b.sql": "two",
		"c.sql": "three",
		"d.sql": "gone",
	}
	got := Diff(desired, actual)
	want := []Action{
		{Op: OpCreate, Path: "a.sql", Content: "one"},
		{Op: OpUpdate, Path: "b.sql", Content: "two-changed"},
		{Op: OpDelete, Path: "d.sql"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Diff = %+v, want %+v", got, want)
	}
}

func TestDiff_NoChanges(t *testing.T) {
	m := map[string]string{"a": "x"}
	if got := Diff(m, map[string]string{"a": "x"}); len(got) != 0 {
		t.Errorf("expected empty diff, got %+v", got)
	}
}

func TestSplitDeletes(t *testing.T) {
	w, d := SplitDeletes([]Action{
		{Op: OpCreate, Path: "a"}, {Op: OpDelete, Path: "b"}, {Op: OpUpdate, Path: "c"},
	})
	if len(w) != 2 || len(d) != 1 || d[0].Path != "b" {
		t.Errorf("writes=%v deletes=%v", w, d)
	}
}

func TestBatch(t *testing.T) {
	actions := make([]Action, 250)
	batches := Batch(actions, 100)
	if len(batches) != 3 || len(batches[0]) != 100 || len(batches[2]) != 50 {
		t.Errorf("bad batching: %d batches, sizes %d/%d", len(batches), len(batches[0]), len(batches[len(batches)-1]))
	}
	if got := Batch(nil, 100); got != nil {
		t.Errorf("empty input should produce no batches, got %v", got)
	}
}

package clickhouse

import (
	"context"
	"errors"
	"testing"

	"gitlab.example.com/infra/ddl-saver/internal/model"
)

type refsOnly struct {
	Client // embed so only ListRefs needs implementing
	refs   []model.ObjectRef
	err    error
}

func (r refsOnly) ListRefs(context.Context) ([]model.ObjectRef, error) { return r.refs, r.err }

func TestLoadExistingSet(t *testing.T) {
	t.Parallel()
	c := refsOnly{refs: []model.ObjectRef{
		{Database: "events"},
		{Database: "events", Name: "clicks"},
	}}
	set, err := LoadExistingSet(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	for ref, want := range map[model.ObjectRef]bool{
		{Database: "events"}:                 true,
		{Database: "events", Name: "clicks"}: true,
		{Database: "events", Name: "nope"}:   false,
		{Database: "other"}:                  false,
	} {
		got, _ := set.ObjectExists(context.Background(), ref)
		if got != want {
			t.Errorf("exists(%s) = %v, want %v", ref, got, want)
		}
	}

	if _, err := LoadExistingSet(context.Background(), refsOnly{err: errors.New("boom")}); err == nil {
		t.Error("list error must propagate")
	}
}

func TestNewFilter_BadRegex(t *testing.T) {
	t.Parallel()
	if _, err := NewFilter(nil, []string{"]["}, nil); err == nil {
		t.Error("invalid regex must error")
	}
}

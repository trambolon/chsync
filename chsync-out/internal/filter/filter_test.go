package filter

import (
	"testing"

	"chsync/internal/config"
	"chsync/internal/schema"
)

func TestFilter(t *testing.T) {
	f := New(config.Filter{ExcludeDatabases: []string{"test"}, ExcludeNameRegex: []string{"(?i)bms"}, ExcludeEngines: []string{"kafka"}}, nil)
	for db, skip := range map[string]bool{"system": true, "INFORMATION_SCHEMA": true, "TEST": true, "BD1": false, "bms_db": true, "xBmSy": true} {
		if (f.SkipDB(db) != "") != skip {
			t.Errorf("SkipDB(%s)", db)
		}
	}
	if f.SkipObject(schema.ObjKey{DB: "BD1", Name: "dev_BMS_p"}, "") == "" || f.SkipObject(schema.ObjKey{DB: "BD1", Name: "dev_p"}, "") != "" {
		t.Error("SkipObject name")
	}
	if f.SkipObject(schema.ObjKey{DB: "BD1", Name: "q"}, "Kafka") == "" {
		t.Error("engine")
	}
	f2 := New(config.Filter{IncludeDatabases: []string{"BD1"}}, []string{"BD2"})
	if f2.SkipDB("BD1") == "" || f2.SkipDB("BD2") != "" {
		t.Error("--databases overrides include")
	}
}

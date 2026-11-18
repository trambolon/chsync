package schema

import (
	"reflect"
	"strings"
	"testing"
)

const mvInner = "CREATE MATERIALIZED VIEW BD1.mv_inner UUID '6690d772-d953-42e6-8928-e5e3945401cb' TO INNER UUID 'a836af67-5ddc-4fea-94f9-ef8e15dbc902' (`id` UInt64, `s` Float64) ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') ORDER BY id SETTINGS index_granularity = 8192 AS SELECT id, sum(v) AS s FROM BD1.dev_table_p GROUP BY id"

// SHOW CREATE (многострочный, без UUID) и create_table_query (с UUID) должны нормализоваться одинаково.
func TestNormalizeAndHeader(t *testing.T) {
	a := "CREATE TABLE BD1.t UUID 'fd8ce2e3-a28b-4333-acf1-f63fb72d4cf3' (`id` UInt64)  ENGINE = MergeTree ORDER BY id"
	b := "CREATE TABLE IF NOT EXISTS BD1.t\n(\n    `id` UInt64\n)\nENGINE = MergeTree\nORDER BY id"
	if Normalize(a) != Normalize(b) {
		t.Fatalf("norm differ:\n%s\n%s", Normalize(a), Normalize(b))
	}
	cases := map[string]string{
		a:       "CREATE TABLE IF NOT EXISTS BD1.t UUID 'fd8ce2e3",
		b:       b, // уже есть IF NOT EXISTS — без изменений
		mvInner: "CREATE MATERIALIZED VIEW IF NOT EXISTS BD1.mv_inner UUID",
		"CREATE TABLE `my db`.`we.ird` UUID 'fd8ce2e3-a28b-4333-acf1-f63fb72d4cf3' (`id` UInt64) ENGINE = Log": "CREATE TABLE IF NOT EXISTS `my db`.`we.ird` UUID",
		"CREATE FUNCTION lab_norm AS x -> lower(trimBoth(x))":                                                  "CREATE FUNCTION IF NOT EXISTS lab_norm AS x -> lower(trimBoth(x))",
		"CREATE DATABASE BD1 UUID '0d97477c-9272-4a2a-a3a0-df7d0d6edea0'\nENGINE = Atomic":                     "CREATE DATABASE IF NOT EXISTS BD1 UUID",
	}
	for in, prefix := range cases {
		if got := AddIfNotExists(in); !strings.HasPrefix(got, prefix) {
			t.Errorf("AddIfNotExists(%q) = %q", in, got)
		}
	}
	if got := StripUUID(mvInner); strings.Contains(got, "UUID") {
		t.Fatal(got)
	}
	if HeaderUUID(a) != "fd8ce2e3-a28b-4333-acf1-f63fb72d4cf3" || InnerUUID(mvInner) != "a836af67-5ddc-4fea-94f9-ef8e15dbc902" {
		t.Fatal("uuid")
	}
}

// Разбор аргументов Replicated: явный путь, путь по умолчанию, экранирование, MV с inner-движком.
func TestParseReplicated(t *testing.T) {
	ri := ParseReplicated(mvInner)
	if ri == nil || ri.PathTmpl != "/clickhouse/tables/{uuid}/{shard}" || ri.ReplicaTmpl != "{replica}" || ri.UUID != "a836af67-5ddc-4fea-94f9-ef8e15dbc902" {
		t.Fatalf("%+v", ri)
	}
	ri = ParseReplicated("CREATE TABLE BD1.u (`uid` UInt64, `ver` UInt32) ENGINE = ReplicatedReplacingMergeTree('/ch/{shard}/BD1/u', '{replica}', ver) ORDER BY uid")
	if ri.PathTmpl != "/ch/{shard}/BD1/u" || ri.Defaults {
		t.Fatalf("%+v", ri)
	}
	for _, s := range []string{
		"CREATE TABLE a.b (x UInt8) ENGINE = ReplicatedReplacingMergeTree(ver) ORDER BY x",
		"CREATE TABLE a.b (x UInt8) ENGINE = ReplicatedMergeTree ORDER BY x",
	} {
		if ri := ParseReplicated(s); ri == nil || !ri.Defaults {
			t.Fatalf("defaults: %s", s)
		}
	}
	if ParseReplicated("CREATE TABLE a.b (x UInt8) ENGINE = MergeTree ORDER BY x") != nil {
		t.Fatal("non-replicated")
	}
	ri = ParseReplicated(`CREATE TABLE a.b (x UInt8) ENGINE = ReplicatedMergeTree('/p/it''s/{shard}', 'r\'1') ORDER BY x`)
	if ri.PathTmpl != "/p/it's/{shard}" || ri.ReplicaTmpl != "r'1" {
		t.Fatalf("%+v", ri)
	}
}

func TestSplitArgsAndUnquote(t *testing.T) {
	s := "Distributed('{cluster}', 'BD1', 'dev_table_p', cityHash64(id, concat('a,b', \"x\")), 'pol')"
	args, _ := SplitArgs(s, strings.Index(s, "("))
	want := []string{"'{cluster}'", "'BD1'", "'dev_table_p'", "cityHash64(id, concat('a,b', \"x\"))", "'pol'"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("%q", args)
	}
	if args, _ = SplitArgs("f()", 1); len(args) != 0 {
		t.Fatalf("%q", args)
	}
	for in, out := range map[string]string{`'a\nb'`: "a\nb", "`x y`": "x y", `'it\'s'`: "it's", "ident": "ident"} {
		if got, _ := Unquote(in); got != out {
			t.Errorf("Unquote(%s) = %q", in, got)
		}
	}
}

func TestExpandMacros(t *testing.T) {
	out, miss := ExpandMacros("/clickhouse/tables/{uuid}/{shard}/{layer}", map[string]string{"shard": "04"}, map[string]string{"uuid": "U"})
	if out != "/clickhouse/tables/U/04/{layer}" || !reflect.DeepEqual(miss, []string{"layer"}) {
		t.Fatal(out, miss)
	}
	if !TemplateHasMacro("/x/{shard}/y", []string{"shard"}) || TemplateHasMacro("/x/all/{database}", []string{"shard"}) {
		t.Fatal("TemplateHasMacro")
	}
}

// Ссылки: квалифицированные, неквалифицированные в той же БД, явные (Distributed, словарь), dictGet.
func TestRefs(t *testing.T) {
	body := Body("CREATE VIEW BD2.v (`a` UInt8) AS SELECT a FROM BD1.`BMS_events_p` JOIN dim USING a")
	switch {
	case !RefersTo("BD2", ObjKey{"BD1", "BMS_events_p"}, body):
		t.Fatal("qualified backticked")
	case !RefersTo("BD2", ObjKey{"BD2", "dim"}, body):
		t.Fatal("unqualified same db")
	case RefersTo("BD2", ObjKey{"BD3", "dim"}, body):
		t.Fatal("unqualified other db must not match")
	case RefersTo("BD2", ObjKey{"BD1", "BMS_events"}, body):
		t.Fatal("prefix must not match")
	}
	if r := ExplicitRefs(KDistributed, "BD1", "CREATE TABLE BD1.d (`x` UInt8) ENGINE = Distributed('c', 'BD9', 'loc_p', rand())"); len(r) != 1 || r[0] != (ObjKey{"BD9", "loc_p"}) {
		t.Fatal(r)
	}
	dict := "CREATE DICTIONARY BD2.dd (`k` UInt64) PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'dim_p' DB 'BD2')) LIFETIME(MIN 0 MAX 300) LAYOUT(HASHED())"
	if r := ExplicitRefs(KDictionary, "BD2", dict); len(r) != 1 || r[0] != (ObjKey{"BD2", "dim_p"}) {
		t.Fatal(r)
	}
	tb := Body("CREATE TABLE BD2.f (`k` UInt64, `l` String DEFAULT dictGet('BD2.dim_dict', 'label', k)) ENGINE = Log")
	if !Tokens(tb)["dim_dict"] || !RefersTo("BD2", ObjKey{"BD2", "dim_dict"}, tb) {
		t.Fatal("dictGet ref")
	}
}

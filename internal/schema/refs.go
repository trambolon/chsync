package schema

import (
	"regexp"
	"strings"
)

// ObjKey — полное имя объекта db.name.
type ObjKey struct{ DB, Name string }

func (k ObjKey) String() string { return k.DB + "." + k.Name }

// Compare — порядок ключей для стабильной сортировки (slices.SortFunc).
func (k ObjKey) Compare(o ObjKey) int { return strings.Compare(k.String(), o.String()) }

var wordRe = regexp.MustCompile("[A-Za-z0-9_$]+|`[^`]*`|\"[^\"]*\"")

// Tokens — все идентификаторы и строковые литералы тела DDL (включая части 'db.dict' из dictGet).
// Это дешёвый предфильтр: точную проверку ссылки делает RefersTo.
func Tokens(body string) map[string]bool {
	t := map[string]bool{}
	for _, w := range wordRe.FindAllString(body, -1) {
		if w[0] == '`' || w[0] == '"' {
			w = w[1 : len(w)-1]
		}
		t[w] = true
	}
	for i := 0; i < len(body); i++ {
		if body[i] != '\'' {
			continue
		}
		j := skipQuoted(body, i)
		if lit := body[i+1 : j]; len(lit) < 256 {
			t[lit] = true
			for p := range strings.SplitSeq(lit, ".") {
				t[p] = true
			}
		}
		i = j
	}
	return t
}

// identAlt — регэксп имени в любом виде цитирования: name, `name`, "name".
func identAlt(x string) string {
	q := regexp.QuoteMeta(x)
	return "(?:`" + q + "`|\"" + q + "\"|" + q + ")"
}

var (
	dictSourceRe = regexp.MustCompile(`SOURCE\(CLICKHOUSE\((.*?)\)\)`)
	dictDBRe     = regexp.MustCompile(`\bDB\s+'([^']*)'`)
	dictTableRe  = regexp.MustCompile(`\bTABLE\s+'([^']*)'`)
)

// ExplicitRefs — явные ссылки из аргументов движков Distributed/Buffer и источника словаря CLICKHOUSE
// объекта db.<...> вида kind. Там имена — строки, поэтому их не ловит обычный поиск "db.table".
func ExplicitRefs(kind Kind, db, create string) []ObjKey {
	var out []ObjKey
	add := func(refDB, name string) {
		if name == "" {
			return
		}
		if refDB == "" || strings.Contains(refDB, "(") { // currentDatabase() и т.п.
			refDB = db
		}
		out = append(out, ObjKey{refDB, name})
	}
	arg2 := func(engine string, iDB, iTable int) {
		if _, a, ok := EngineArgs(create, engine); ok && len(a) > iTable {
			d, _ := Unquote(a[iDB])
			t, _ := Unquote(a[iTable])
			add(d, t)
		}
	}
	switch kind {
	case KDistributed:
		arg2("Distributed", 1, 2) // Distributed(cluster, db, table, ...)
	case KProxy:
		arg2("Buffer", 0, 1) // Buffer(db, table, ...)
	case KDictionary:
		if m := dictSourceRe.FindStringSubmatch(create); m != nil {
			var d, t string
			if mm := dictDBRe.FindStringSubmatch(m[1]); mm != nil {
				d = mm[1]
			}
			if mm := dictTableRe.FindStringSubmatch(m[1]); mm != nil {
				t = mm[1]
			}
			add(d, t)
		}
	}
	return out
}

// RefersTo — ссылается ли тело DDL объекта из БД db на объект p: "db.name" в любом цитировании,
// или просто "name", если оба в одной БД (сервер сохраняет такие ссылки как есть).
func RefersTo(db string, p ObjKey, body string) bool {
	q := regexp.MustCompile(`(?:^|[^\w])` + identAlt(p.DB) + `\s*\.\s*` + identAlt(p.Name) + `(?:[^\w]|$)`)
	if q.MatchString(body) {
		return true
	}
	return db == p.DB && regexp.MustCompile(`(?:^|[^\w.])`+identAlt(p.Name)+`(?:[^\w]|$)`).MatchString(body)
}

// Package filter решает, какие БД, объекты и SQL-функции не переносить (секция [filter] конфига).
package filter

import (
	"regexp"
	"strings"

	"chsync/internal/config"
	"chsync/internal/schema"
)

// builtinSystemDBs — системные БД, которые пропускаются всегда, независимо от конфига.
var builtinSystemDBs = []string{"system", "information_schema", "_temporary_and_external_tables"}

// Filter — скомпилированные правила исключения.
type Filter struct {
	include   map[string]bool // lower-case имена БД; пусто — все
	excludeDB map[string]bool // lower-case
	nameRe    []*regexp.Regexp
	objects   map[string]bool // "db.name"
	engines   map[string]bool // lower-case
}

// New собирает фильтр из конфига; onlyDBs (флаг --databases) заменяет include_databases.
// Регулярные выражения должны быть уже проверены config.Validate.
func New(fc config.Filter, onlyDBs []string) *Filter {
	f := &Filter{include: map[string]bool{}, excludeDB: map[string]bool{}, objects: map[string]bool{}, engines: map[string]bool{}}
	inc := fc.IncludeDatabases
	if len(onlyDBs) > 0 {
		inc = onlyDBs
	}
	for _, d := range inc {
		f.include[strings.ToLower(d)] = true
	}
	for _, list := range [][]string{fc.ExcludeDatabases, builtinSystemDBs} {
		for _, d := range list {
			f.excludeDB[strings.ToLower(d)] = true
		}
	}
	for _, r := range fc.ExcludeNameRegex {
		f.nameRe = append(f.nameRe, regexp.MustCompile(r))
	}
	for _, o := range fc.ExcludeObjects {
		f.objects[o] = true
	}
	for _, e := range fc.ExcludeEngines {
		f.engines[strings.ToLower(e)] = true
	}
	return f
}

// matchName — причина исключения имени по exclude_name_regex или "".
func (f *Filter) matchName(name string) string {
	for _, r := range f.nameRe {
		if r.MatchString(name) {
			return "exclude_name_regex " + r.String()
		}
	}
	return ""
}

// SkipDB возвращает причину пропуска БД или "".
func (f *Filter) SkipDB(db string) string {
	l := strings.ToLower(db)
	switch {
	case f.excludeDB[l]:
		return "exclude_databases"
	case len(f.include) > 0 && !f.include[l]:
		return "not in include_databases"
	}
	return f.matchName(db)
}

// SkipObject возвращает причину пропуска объекта или "": БД, точное имя, регэксп имени, движок.
func (f *Filter) SkipObject(k schema.ObjKey, engine string) string {
	if r := f.SkipDB(k.DB); r != "" {
		return "db: " + r
	}
	if f.objects[k.String()] {
		return "exclude_objects"
	}
	if r := f.matchName(k.Name); r != "" {
		return r
	}
	if f.engines[strings.ToLower(engine)] {
		return "exclude_engines " + engine
	}
	return ""
}

// SkipFunction возвращает причину пропуска SQL-функции или "".
func (f *Filter) SkipFunction(name string) string { return f.matchName(name) }

// Package model holds the identity types shared by every other package:
// what an object is (kind) and how it is addressed (ref). Keeping them here
// lets clickhouse, gitlab, snapshot and recovery agree on vocabulary without
// importing each other.
package model

import (
	"cmp"
	"fmt"
	"strings"
)

type ObjectKind string

const (
	KindTable            ObjectKind = "table"
	KindView             ObjectKind = "view"
	KindMaterializedView ObjectKind = "materialized_view"
	KindDictionary       ObjectKind = "dictionary"
	// KindDatabase is the container itself. It is snapshotted as
	// <db>/_database.sql and becomes an implicit prerequisite of every
	// object in that database during recovery.
	KindDatabase ObjectKind = "database"
	KindUnknown  ObjectKind = "unknown"
)

// DatabaseFile is the file name holding a database's own DDL. The leading
// underscore keeps it out of the <kind>/ directory namespace.
const DatabaseFile = "_database.sql"

// Dir is the repository directory an object of this kind lives under:
// <base>/<cluster>/<db>/<Dir()>/<name>.sql
func (k ObjectKind) Dir() string {
	switch k {
	case KindTable:
		return "tables"
	case KindView:
		return "views"
	case KindMaterializedView:
		return "materialized_views"
	case KindDictionary:
		return "dictionaries"
	case KindDatabase:
		return "" // lives directly under <db>/ as DatabaseFile
	}
	return "unknown"
}

func KindFromDir(dir string) (ObjectKind, bool) {
	switch dir {
	case "tables":
		return KindTable, true
	case "views":
		return KindView, true
	case "materialized_views":
		return KindMaterializedView, true
	case "dictionaries":
		return KindDictionary, true
	}
	return KindUnknown, false
}

// KindFromEngine maps system.tables.engine to a kind.
func KindFromEngine(engine string) ObjectKind {
	switch engine {
	case "View", "LiveView":
		return KindView
	case "MaterializedView":
		return KindMaterializedView
	case "Dictionary":
		return KindDictionary
	}
	return KindTable
}

type ObjectRef struct {
	Database string
	Name     string
}

// IsDatabase reports whether the ref addresses a database rather than an
// object inside one.
func (r ObjectRef) IsDatabase() bool { return r.Name == "" }

// DatabaseRef returns the ref of the database that contains r.
func (r ObjectRef) DatabaseRef() ObjectRef { return ObjectRef{Database: r.Database} }

func (r ObjectRef) String() string {
	if r.IsDatabase() {
		return r.Database
	}
	return r.Database + "." + r.Name
}

// Compare orders refs for deterministic output (database refs sort before
// the objects they contain).
func (r ObjectRef) Compare(o ObjectRef) int {
	if c := cmp.Compare(r.Database, o.Database); c != 0 {
		return c
	}
	return cmp.Compare(r.Name, o.Name)
}

// ParseRef parses "db.name" (or "name" when defaultDB is given).
func ParseRef(s, defaultDB string) (ObjectRef, error) {
	parts := strings.Split(s, ".")
	switch {
	case len(parts) == 2 && parts[0] != "" && parts[1] != "":
		return ObjectRef{Database: parts[0], Name: parts[1]}, nil
	case len(parts) == 1 && parts[0] != "" && defaultDB != "":
		return ObjectRef{Database: defaultDB, Name: parts[0]}, nil
	}
	return ObjectRef{}, fmt.Errorf("invalid object reference %q: want db.name", s)
}

// DeprecatedDir is the archive directory under <base>/<cluster>/. It sits
// next to the database directories, so a database of the same name must
// not be written under it (PathSegment escapes that one name).
const DeprecatedDir = "_deprecated"

// PathSegment encodes a database or object name as ONE repository path
// segment. ClickHouse names are arbitrary strings; a path separator, a
// control character or a ".." segment in one of them would either move the
// file somewhere else in the tree or make GitLab reject the whole commit.
// Those bytes are percent-encoded; every ordinary name is returned as is,
// so existing repositories do not change.
func PathSegment(name string) string {
	var b strings.Builder
	for i := 0; i < len(name); i++ {
		c := name[i]
		if c == '%' || c == '/' || c == '\\' || c < 0x20 || c == 0x7f {
			fmt.Fprintf(&b, "%%%02X", c)
			continue
		}
		b.WriteByte(c)
	}
	s := b.String()
	switch {
	case s == "." || s == "..":
		return "%2E" + s[1:]
	case s == DeprecatedDir:
		return "%5F" + s[1:]
	}
	return s
}

// NameFromSegment reverses PathSegment. A '%' that does not start a valid
// escape is kept literally, so files written before encoding existed still
// map to the name they were created from.
func NameFromSegment(seg string) string {
	if !strings.Contains(seg, "%") {
		return seg
	}
	var b strings.Builder
	for i := 0; i < len(seg); i++ {
		if seg[i] == '%' && i+2 < len(seg) {
			if hi, ok1 := unhex(seg[i+1]); ok1 {
				if lo, ok2 := unhex(seg[i+2]); ok2 {
					b.WriteByte(hi<<4 | lo)
					i += 2
					continue
				}
			}
		}
		b.WriteByte(seg[i])
	}
	return b.String()
}

func unhex(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	}
	return 0, false
}

// ObjectPath is the repository path of an object's file relative to
// <base>/<cluster>/: "<db>/<kind dir>/<name>.sql", or "<db>/_database.sql".
func ObjectPath(ref ObjectRef, kind ObjectKind) string {
	if kind == KindDatabase || ref.IsDatabase() {
		return PathSegment(ref.Database) + "/" + DatabaseFile
	}
	return PathSegment(ref.Database) + "/" + kind.Dir() + "/" + PathSegment(ref.Name) + ".sql"
}

// ParseObjectPath is the inverse of ObjectPath. ok is false for anything
// that is not a managed object file.
func ParseObjectPath(rel string) (ref ObjectRef, kind ObjectKind, ok bool) {
	parts := strings.Split(rel, "/")
	switch {
	case len(parts) == 2 && parts[1] == DatabaseFile && parts[0] != "":
		return ObjectRef{Database: NameFromSegment(parts[0])}, KindDatabase, true
	case len(parts) == 3 && strings.HasSuffix(parts[2], ".sql"):
		kind, valid := KindFromDir(parts[1])
		name := strings.TrimSuffix(parts[2], ".sql")
		if !valid || parts[0] == "" || name == "" {
			return ObjectRef{}, KindUnknown, false
		}
		return ObjectRef{Database: NameFromSegment(parts[0]), Name: NameFromSegment(name)}, kind, true
	}
	return ObjectRef{}, KindUnknown, false
}

// Package xstrings — мелкие строковые помощники, общие для нескольких пакетов.
package xstrings

import (
	"slices"
	"strings"
	"unicode/utf8"
)

// Truncate обрезает строку до n байт и добавляет "...". Режет по границе руны,
// чтобы не ломать кириллицу в сообщениях об ошибках.
func Truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n] + "..."
}

// Uniq возвращает отсортированные уникальные значения; исходный срез не меняется.
func Uniq(in []string) []string {
	return slices.Compact(slices.Sorted(slices.Values(in)))
}

// SplitList разбивает "a, b,c" в [a b c], пропуская пустые элементы (значения флагов --targets, --databases).
func SplitList(s string) []string {
	var out []string
	for x := range strings.SplitSeq(s, ",") {
		if x = strings.TrimSpace(x); x != "" {
			out = append(out, x)
		}
	}
	return out
}

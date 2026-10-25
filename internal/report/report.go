// Package report — каталог результатов одного запуска: <output_dir>/<YYYYMMDD-HHMMSS>-<команда>/.
//
// Файлы создаются через os.Root: имена строятся из имён хостов, и даже при ошибке санитизации
// запись не выйдет за пределы каталога запуска.
package report

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"time"
)

// Run — каталог результатов запуска.
type Run struct {
	ID   string // метка запуска (время старта), попадает в log_comment запросов
	Dir  string // путь к каталогу
	root *os.Root
}

// Open создаёт каталог запуска команды cmd внутри base.
func Open(base, cmd string, now time.Time) (*Run, error) {
	id := now.Format("20060102-150405")
	dir := filepath.Join(base, id+"-"+cmd)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("каталог отчёта: %w", err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("каталог отчёта: %w", err)
	}
	return &Run{ID: id, Dir: dir, root: root}, nil
}

// Create создаёт (или перезаписывает) файл в каталоге запуска. Имя приводится к безопасному виду.
func (r *Run) Create(name string) (*os.File, error) { return r.root.Create(FileSafe(name)) }

// Path — полный путь файла в каталоге запуска (для сообщений пользователю).
func (r *Run) Path(name string) string { return filepath.Join(r.Dir, FileSafe(name)) }

// Close закрывает каталог.
func (r *Run) Close() error { return r.root.Close() }

var unsafeFileRe = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

// FileSafe превращает имя хоста в безопасное имя файла: 10.0.0.1:8123 -> 10.0.0.1_8123.
func FileSafe(s string) string { return unsafeFileRe.ReplaceAllString(s, "_") }

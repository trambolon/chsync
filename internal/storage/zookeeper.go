package storage

import (
	"context"
	"errors"
	"slices"
	"strings"

	"chsync/internal/xstrings"
)

// zkBatch — сколько путей проверяем одним запросом к system.zookeeper.
const zkBatch = 100

// ZKChildren возвращает детей узлов ZooKeeper/Keeper: path -> []name (пустой срез — узла нет или он пуст).
// Сначала пакетно через path IN (...); если версия так не умеет — по одному пути.
// Пути, состояние которых узнать не удалось, в результат не попадают; последняя такая ошибка
// возвращается вторым значением (это не фатально: план просто предупредит).
func ZKChildren(ctx context.Context, c *Client, paths []string) (map[string][]string, error) {
	type row struct {
		Path string `json:"path"`
		Name string `json:"name"`
	}
	paths = xstrings.Uniq(paths)
	out := make(map[string][]string, len(paths))
	for _, p := range paths {
		out[p] = nil
	}
	var lastErr error
	for chunk := range slices.Chunk(paths, zkBatch) {
		q := make([]string, len(chunk))
		for i, p := range chunk {
			q[i] = QuoteString(p)
		}
		rows, err := c.Select[row](ctx, "SELECT path, name FROM system.zookeeper WHERE path IN ("+strings.Join(q, ",")+")", nil)
		if err == nil {
			for _, r := range rows {
				out[r.Path] = append(out[r.Path], r.Name)
			}
			continue
		}
		for _, p := range chunk { // старые версии падают на несуществующем пути в IN
			rows, err := c.Select[row](ctx, "SELECT path, name FROM system.zookeeper WHERE path = "+QuoteString(p), nil)
			switch {
			case err != nil && strings.Contains(err.Error(), "No node"):
			case err != nil:
				lastErr = err
				delete(out, p) // состояние узла неизвестно
			default:
				for _, r := range rows {
					out[r.Path] = append(out[r.Path], r.Name)
				}
			}
		}
	}
	if lastErr != nil {
		return out, errors.New(xstrings.Truncate(lastErr.Error(), 300))
	}
	return out, nil
}

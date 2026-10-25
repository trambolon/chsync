package plan

import (
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"time"

	"chsync/internal/storage"
)

// PrintUnion печатает предупреждения по источникам: недоступные, расхождения DDL баз.
func PrintUnion(w io.Writer, u *Union) {
	for _, s := range u.Warnings {
		fmt.Fprintln(w, "  ! "+s)
	}
	for _, inv := range u.Sources {
		if inv.Err != nil {
			fmt.Fprintf(w, "  ✗ источник %s недоступен: %v\n", inv.Name(), inv.Err)
		}
		for _, s := range inv.Warnings {
			fmt.Fprintf(w, "  ! %s: %s\n", inv.Name(), s)
		}
	}
}

// ReplicationLine — строка о репликации шарда target: сколько реплик и с каким internal_replication.
func ReplicationLine(t *storage.Inventory, cluster string) string {
	rows := t.OwnShard(cluster)
	if cluster == "" || len(rows) == 0 {
		return "шард target в кластере не найден (cluster не задан или remote_servers без этого хоста)"
	}
	hosts := make([]string, len(rows))
	for i, r := range rows {
		hosts[i] = fmt.Sprintf("%s:%d", r.HostName, r.Port)
	}
	ir := "?"
	if rows[0].InternalReplication != nil {
		ir = fmt.Sprint(*rows[0].InternalReplication)
	}
	s := fmt.Sprintf("шард %d в кластере %s: %d реплик(и) (%s), internal_replication=%s, weight=%d",
		rows[0].ShardNum, cluster, len(rows), strings.Join(hosts, ", "), ir, rows[0].ShardWeight)
	if len(rows) == 1 {
		s += " — без реплик: Replicated-таблицы работают с одной репликой, вторую можно добавить позже без пересоздания"
	}
	return s
}

// PrintPlan печатает план одного target: CREATE/BLOCKED/DRIFT/EXTRA подробно, EXISTS/SKIP — сводкой (или всё при verbose).
func PrintPlan(w io.Writer, tp *TargetPlan, cluster string, verbose bool) {
	inv := tp.Inv
	fmt.Fprintf(w, "\n══ TARGET %s  (version %s, host %s)\n", inv.Label(), inv.Version, inv.Hostname)
	fmt.Fprintf(w, "   %s\n", ReplicationLine(inv, cluster))
	for _, a := range []Action{ACreate, ABlocked, ADrift, AExtra, AExists, ASkip} {
		items := slices.Collect(tp.ByAction(a))
		switch {
		case len(items) == 0:
			continue
		case a == AExists && !verbose:
			fmt.Fprintf(w, "  %-8s %d объектов уже есть и совпадают (подробно: -v)\n", a, len(items))
			continue
		case a == ASkip && !verbose:
			reasons := map[string]int{}
			for _, it := range items {
				reasons[shortReason(it.Reason)]++
			}
			fmt.Fprintf(w, "  %-8s %d объектов (подробно: -v)\n", a, len(items))
			for _, r := range slices.Sorted(maps.Keys(reasons)) {
				fmt.Fprintf(w, "           · %d: %s\n", reasons[r], r)
			}
			continue
		}
		for _, it := range items {
			fmt.Fprintf(w, "  %-8s %-11s %-50s %s\n", a, it.Kind, it.Name, it.Engine)
			for _, s := range []string{it.Info, it.Reason} {
				if s != "" {
					fmt.Fprintf(w, "           ↳ %s\n", s)
				}
			}
			for _, wr := range it.Warn {
				fmt.Fprintf(w, "           ! %s\n", wr)
			}
		}
	}
	c := tp.Counts
	fmt.Fprintf(w, "  ИТОГО: create=%d blocked=%d drift=%d extra=%d exists=%d skip=%d\n",
		c[ACreate], c[ABlocked], c[ADrift], c[AExtra], c[AExists], c[ASkip])
}

// shortReason — укрупнённая причина пропуска для сводки.
func shortReason(r string) string {
	switch {
	case strings.HasPrefix(r, "cascade:"):
		return "cascade (ссылается на исключённый объект)"
	case strings.HasPrefix(r, "есть только на"):
		return "есть не на всех источниках (on_partial=skip)"
	}
	return r
}

// WriteSQL пишет план target как SQL-скрипт — для ревью/CAB или ручного прогона через clickhouse-client --multiquery.
func WriteSQL(w io.Writer, tp *TargetPlan, sources []string, now time.Time) error {
	var b strings.Builder
	fmt.Fprintf(&b, "-- chsync plan for %s\n-- generated %s\n-- sources: %s\n", tp.Inv.Label(), now.Format(time.RFC3339), strings.Join(sources, ", "))
	fmt.Fprintf(&b, "-- create=%d blocked=%d\n-- Выполняется напрямую на хосте (без ON CLUSTER).\n\n", tp.Counts[ACreate], tp.Counts[ABlocked])
	curDB := ""
	for _, it := range tp.Items {
		switch it.Action {
		case ACreate:
			if it.DB != "" && it.DB != curDB {
				fmt.Fprintf(&b, "USE %s;\n\n", storage.QuoteIdent(it.DB))
				curDB = it.DB
			}
			fmt.Fprintf(&b, "-- [%s] %s  (source: %s)\n", it.Kind, it.Name, it.Source)
			if it.Info != "" {
				fmt.Fprintf(&b, "-- %s\n", it.Info)
			}
			for _, wr := range it.Warn {
				fmt.Fprintf(&b, "-- WARN: %s\n", wr)
			}
			fmt.Fprintf(&b, "%s;\n\n", it.SQL)
		case ABlocked:
			fmt.Fprintf(&b, "-- BLOCKED [%s] %s: %s\n\n", it.Kind, it.Name, it.Reason)
		}
	}
	_, err := io.WriteString(w, b.String())
	return err
}

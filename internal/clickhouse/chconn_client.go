package clickhouse

// chconn adapter. This file is the ONLY place that imports the driver;
// everything above it talks to the Client interface.
//
// NOTE: verify the chconn major version against your go.sum — the v2 and v3
// APIs are nearly identical (both pgconn-derived), but the import path
// differs. See README "Dependencies".

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/vahid-sohrabloo/chconn/v3"
	"github.com/vahid-sohrabloo/chconn/v3/column"

	"gitlab.example.com/infra/ddl-saver/internal/model"
)

// Options carries only what the adapter needs; cmd converts config.Cluster
// into this so the package stays decoupled from the config layer.
type Options struct {
	Endpoints      []HostPort
	User           string
	Password       string
	TLS            *TLSFiles // nil = plaintext (port 9000); non-nil = TLS (port 9440)
	CompressionLZ4 bool
	ConnectTimeout time.Duration
}

type HostPort struct {
	Host string
	Port int
}

// TLSFiles configures an encrypted connection. Every field is optional:
// CertFile/KeyFile only for client-certificate auth, CAFile only when the
// server certificate is signed by a CA outside the system trust store.
type TLSFiles struct {
	CertFile string
	KeyFile  string
	CAFile   string
}

type nativeClient struct {
	opts     Options
	tls      *tls.Config
	conn     chconn.Conn
	endpoint HostPort
}

// Connect tries endpoints in order and returns the first that answers Ping.
// Endpoint order in config is therefore the failover priority.
func Connect(ctx context.Context, o Options) (Client, error) {
	if len(o.Endpoints) == 0 {
		return nil, errors.New("no endpoints configured")
	}
	tlsCfg, err := buildTLS(o.TLS)
	if err != nil {
		return nil, err
	}
	c := &nativeClient{opts: o, tls: tlsCfg}
	if err := c.dial(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *nativeClient) dial(ctx context.Context) error {
	var errs []string
	for _, ep := range c.opts.Endpoints {
		cfg, err := chconn.ParseConfig(fmt.Sprintf(
			"host=%s port=%d user=%s password=%s dbname=default",
			ep.Host, ep.Port, dsnEscape(c.opts.User), dsnEscape(c.opts.Password)))
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s:%d: parse config: %v", ep.Host, ep.Port, err))
			continue
		}
		cfg.TLSConfig = tlsForHost(c.tls, ep.Host)
		if c.opts.CompressionLZ4 {
			cfg.Compress = chconn.CompressLZ4
		}
		if c.opts.ConnectTimeout > 0 {
			cfg.ConnectTimeout = c.opts.ConnectTimeout
		}
		conn, err := chconn.ConnectConfig(ctx, cfg)
		if err != nil {
			msg := fmt.Sprintf("%s:%d: %v", ep.Host, ep.Port, err)
			if c.tls == nil && ep.Port == 9440 {
				msg += " (hint: 9440 is ClickHouse's TLS port; set connection.secure = true)"
			}
			errs = append(errs, msg)
			continue
		}
		if err := conn.Ping(ctx); err != nil {
			_ = conn.Close()
			errs = append(errs, fmt.Sprintf("%s:%d: ping: %v", ep.Host, ep.Port, err))
			continue
		}
		c.conn, c.endpoint = conn, ep
		return nil
	}
	return fmt.Errorf("all endpoints failed:\n  %s", strings.Join(errs, "\n  "))
}

// ready makes sure there is a usable connection. The driver closes the
// connection when the server answers with an exception, and a recovery run
// has to survive exactly that: one CREATE failing must not turn every
// later statement into "conn closed".
func (c *nativeClient) ready(ctx context.Context) error {
	if c.conn != nil && !c.conn.IsClosed() {
		return nil
	}
	return c.dial(ctx)
}

func (c *nativeClient) query(ctx context.Context, sql string, cols ...column.ColumnCore) (chconn.SelectStmt, error) {
	if err := c.ready(ctx); err != nil {
		return nil, err
	}
	stmt, err := c.conn.Select(ctx, sql, cols...)
	return stmt, wrapErr(err)
}

// ServerError is an exception returned by ClickHouse, as opposed to a
// transport failure. Code is the server's numeric error code.
type ServerError struct {
	Code    int
	Name    string // e.g. "DB::Exception"
	Message string
}

func (e *ServerError) Error() string {
	return fmt.Sprintf("ClickHouse error %d: %s", e.Code, e.Message)
}

// ClickHouseCode lets other packages read the code without importing the
// driver (or this package).
func (e *ServerError) ClickHouseCode() int { return e.Code }

func wrapErr(err error) error {
	var chErr *chconn.ChError
	if errors.As(err, &chErr) {
		return &ServerError{Code: int(chErr.Code), Name: chErr.Name, Message: strings.TrimSpace(chErr.Message)}
	}
	return err
}

func buildTLS(t *TLSFiles) (*tls.Config, error) {
	if t == nil {
		return nil, nil
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if t.CertFile != "" || t.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	// RootCAs stays nil without a CA file, which means the system trust
	// store (SSL_CERT_FILE / SSL_CERT_DIR are honoured on Linux).
	if t.CAFile != "" {
		ca, err := os.ReadFile(t.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(ca) {
			return nil, errors.New("CA file contains no valid certificates")
		}
		cfg.RootCAs = pool
	}
	return cfg, nil
}

// tlsForHost returns a per-endpoint copy with ServerName set, so the server
// certificate is verified against the host we dialled. Without it Go's TLS
// client refuses to handshake (or, worse, a driver could skip the check).
func tlsForHost(base *tls.Config, host string) *tls.Config {
	if base == nil {
		return nil
	}
	c := base.Clone()
	c.ServerName = host
	return c
}

// dsnEscape quotes a value for a pgconn-style keyword/value string.
func dsnEscape(v string) string {
	if v == "" {
		return "''"
	}
	if !strings.ContainsAny(v, " '\\") {
		return v
	}
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, `'`, `\'`)
	return "'" + v + "'"
}

// sqlQuote quotes a string literal for interpolation into a query. Inputs
// are internal refs (database/table names read back from system tables),
// but quote correctly anyway.
func sqlQuote(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return "'" + s + "'"
}

// quoteIdent backtick-quotes an identifier.
func quoteIdent(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }

func (c *nativeClient) Ping(ctx context.Context) error {
	if err := c.ready(ctx); err != nil {
		return err
	}
	return c.conn.Ping(ctx)
}

func (c *nativeClient) Endpoint() string {
	return fmt.Sprintf("%s:%d", c.endpoint.Host, c.endpoint.Port)
}

func (c *nativeClient) Close() error {
	if c.conn == nil || c.conn.IsClosed() {
		return nil
	}
	return c.conn.Close()
}

func (c *nativeClient) Exec(ctx context.Context, sql string) error {
	if err := c.ready(ctx); err != nil {
		return err
	}
	return wrapErr(c.conn.Exec(ctx, sql))
}

func (c *nativeClient) Topology(ctx context.Context) (*Topology, error) {
	macroCol := column.NewString()
	stmt, err := c.query(ctx,
		"SELECT substitution FROM system.macros WHERE macro = 'cluster'", macroCol)
	if err != nil {
		return nil, fmt.Errorf("query system.macros: %w", err)
	}
	macro := ""
	for stmt.Next() {
		for _, v := range macroCol.Data() {
			macro = v
		}
	}
	if err := wrapErr(stmt.Err()); err != nil {
		return nil, err
	}
	if macro == "" {
		// Single node, no cluster macro. It may still run Replicated
		// engines, so the replica defaults are still worth knowing.
		topo := &Topology{}
		c.loadReplicaDefaults(ctx, topo)
		return topo, nil
	}

	shards := column.New[uint64]()
	replicas := column.New[uint64]()
	stmt, err = c.query(ctx, fmt.Sprintf(
		"SELECT uniqExact(shard_num), uniqExact(replica_num) FROM system.clusters WHERE cluster = %s",
		sqlQuote(macro)), shards, replicas)
	if err != nil {
		return nil, fmt.Errorf("query system.clusters: %w", err)
	}
	topo := &Topology{ClusterMacro: macro}
	for stmt.Next() {
		for i, s := range shards.Data() {
			topo.ShardCount = int(s)
			topo.ReplicaCount = int(replicas.Data()[i])
		}
	}
	if err := wrapErr(stmt.Err()); err != nil {
		return nil, err
	}
	topo.IsDistributed = topo.ShardCount > 1 || topo.ReplicaCount > 1
	c.loadReplicaDefaults(ctx, topo)
	return topo, nil
}

// loadReplicaDefaults fills the default_replica_path / _name settings that
// permit argument-less Replicated engines. Best-effort by design: on
// servers older than 22.10 system.server_settings does not exist, and a
// restricted user may not read it. Neither is a reason to fail a snapshot,
// so the failure only leaves ReplicaDefaultsKnown false.
func (c *nativeClient) loadReplicaDefaults(ctx context.Context, topo *Topology) {
	name, value := column.NewString(), column.NewString()
	stmt, err := c.query(ctx,
		"SELECT name, value FROM system.server_settings "+
			"WHERE name IN ('default_replica_path', 'default_replica_name')", name, value)
	if err != nil {
		return
	}
	found := false
	for stmt.Next() {
		names, values := name.Data(), value.Data()
		for i := range names {
			switch names[i] {
			case "default_replica_path":
				topo.DefaultReplicaPath = values[i]
				found = true
			case "default_replica_name":
				topo.DefaultReplicaName = values[i]
				found = true
			}
		}
	}
	if wrapErr(stmt.Err()) != nil {
		return
	}
	topo.ReplicaDefaultsKnown = found
}

// listQuery returns every user object with its DDL. system.tables covers
// tables, views, materialized views and DDL-created dictionaries.
const listQuery = `
SELECT database, name, engine, create_table_query
FROM system.tables
WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
  AND create_table_query != ''
  AND NOT is_temporary
ORDER BY database, name`

func (c *nativeClient) ListObjects(ctx context.Context) ([]ListedObject, error) {
	db, name := column.NewString(), column.NewString()
	engine, ddl := column.NewString(), column.NewString()
	stmt, err := c.query(ctx, listQuery, db, name, engine, ddl)
	if err != nil {
		return nil, fmt.Errorf("query system.tables: %w", err)
	}
	var out []ListedObject
	for stmt.Next() {
		dbs, names, engines, ddls := db.Data(), name.Data(), engine.Data(), ddl.Data()
		for i := range dbs {
			out = append(out, ListedObject{
				Ref:    model.ObjectRef{Database: dbs[i], Name: names[i]},
				Kind:   model.KindFromEngine(engines[i]),
				Engine: engines[i],
				DDL:    ddls[i],
			})
		}
	}
	if err := wrapErr(stmt.Err()); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *nativeClient) ListDatabases(ctx context.Context) ([]ListedDatabase, error) {
	name, engine := column.NewString(), column.NewString()
	stmt, err := c.query(ctx,
		"SELECT name, engine FROM system.databases "+
			"WHERE name NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA') ORDER BY name",
		name, engine)
	if err != nil {
		return nil, fmt.Errorf("query system.databases: %w", err)
	}
	var out []ListedDatabase
	for stmt.Next() {
		names, engines := name.Data(), engine.Data()
		for i := range names {
			out = append(out, ListedDatabase{Name: names[i], Engine: engines[i]})
		}
	}
	if err := wrapErr(stmt.Err()); err != nil {
		return nil, err
	}
	// SHOW CREATE DATABASE per database: a handful of round trips, and the
	// only portable way to get the statement (system.databases has no
	// create query column across versions).
	for i := range out {
		ddlCol := column.NewString()
		stmt, err := c.query(ctx, "SHOW CREATE DATABASE "+quoteIdent(out[i].Name), ddlCol)
		if err != nil {
			return nil, fmt.Errorf("show create database %s: %w", out[i].Name, err)
		}
		for stmt.Next() {
			for _, v := range ddlCol.Data() {
				out[i].DDL = v
			}
		}
		if err := wrapErr(stmt.Err()); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (c *nativeClient) ListRefs(ctx context.Context) ([]model.ObjectRef, error) {
	var out []model.ObjectRef

	dbCol := column.NewString()
	stmt, err := c.query(ctx, "SELECT name FROM system.databases", dbCol)
	if err != nil {
		return nil, err
	}
	for stmt.Next() {
		for _, d := range dbCol.Data() {
			out = append(out, model.ObjectRef{Database: d})
		}
	}
	if err := wrapErr(stmt.Err()); err != nil {
		return nil, err
	}

	db, name := column.NewString(), column.NewString()
	stmt, err = c.query(ctx, "SELECT database, name FROM system.tables", db, name)
	if err != nil {
		return nil, err
	}
	for stmt.Next() {
		dbs, names := db.Data(), name.Data()
		for i := range dbs {
			out = append(out, model.ObjectRef{Database: dbs[i], Name: names[i]})
		}
	}
	return out, wrapErr(stmt.Err())
}

func (c *nativeClient) ObjectExists(ctx context.Context, ref model.ObjectRef) (bool, error) {
	cnt := column.New[uint64]()
	query := fmt.Sprintf("SELECT count() FROM system.tables WHERE database = %s AND name = %s",
		sqlQuote(ref.Database), sqlQuote(ref.Name))
	if ref.IsDatabase() {
		query = fmt.Sprintf("SELECT count() FROM system.databases WHERE name = %s", sqlQuote(ref.Database))
	}
	stmt, err := c.query(ctx, query, cnt)
	if err != nil {
		return false, err
	}
	exists := false
	for stmt.Next() {
		for _, v := range cnt.Data() {
			exists = v > 0
		}
	}
	return exists, wrapErr(stmt.Err())
}

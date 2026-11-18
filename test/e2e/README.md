# End-to-end test

Runs the real `ddl-saver` binary against a real ClickHouse cluster and an
in-process GitLab that speaks the real REST API (`internal/gitlab/gitlabtest`).
It is excluded from `go test ./...` by the `e2e` build tag.

The cluster it expects:

* one shard, two replicas, cluster macro `{cluster}`, plus `{shard}` and `{replica}`;
* ClickHouse Keeper (or ZooKeeper) and `distributed_ddl` configured;
* the secure native port open on both replicas, certificate issued for `E2E_HOST`;
* user `ddl_saver` with a password, allowed to read `system.*` and to `CREATE`;
* the default user without a password on the plain port (fixtures).

```bash
E2E_CLICKHOUSE_CLIENT=/usr/bin/clickhouse \
E2E_CA_FILE=/path/to/ca.crt \
E2E_PASSWORD='...' \
go test -tags e2e -count=1 -v ./test/e2e/
```

It creates and drops the databases `events` and `solo` on that cluster. Do not
point it at anything you care about.

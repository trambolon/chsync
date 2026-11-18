#!/usr/bin/env bash
# Локальный стенд для репетиции: 1 clickhouse-keeper + 6 инстансов ClickHouse на одной машине.
#   n1,n2 = shard 01 ; n3,n4 = shard 02   ("старый" кластер, 2 шарда x 2 реплики)
#   n5,n6 = shard 03                       ("новый" шард, добавляем)
# Порты: http 812N, tcp 910N, interserver 920N, keeper 9181.
#
#   ./lab.sh up            # поднять всё (старые ноды видят 2 шарда, новые - 3)
#   ./lab.sh seed          # создать тестовую схему ON CLUSTER на старом кластере
#   ./lab.sh expand [W]    # раскатить remote_servers c 3 шардами на все ноды, W - вес нового шарда (0|1)
#   ./lab.sh down          # остановить и удалить всё
#   ./lab.sh q N "SQL"     # выполнить запрос на ноде N
#
# CH_BIN - путь к бинарю clickhouse (по умолчанию: `clickhouse` из PATH)
# REPLICA_PATH - default_replica_path серверов, например 'clickhouse/tables/{shard}/{database}/{table}'
#   ./lab.sh seed_prod   # схема "как на проде": все Replicated* без аргументов движка
set -euo pipefail
LAB="${LAB_DIR:-$(cd "$(dirname "$0")" && pwd)/.lab}"
CH="${CH_BIN:-clickhouse}"
CLUSTER=main

shard_of() { case $1 in 1|2) echo 01;; 3|4) echo 02;; 5|6) echo 03;; esac; }

remote_servers() { # $1 = число шардов (2|3), $2 = вес нового (3-го) шарда
  local n=$1 nw=${2:-1}
  echo "<clickhouse><remote_servers><${CLUSTER}>"
  echo "  <secret>lab_interserver_secret</secret>"
  for s in $(seq 1 "$n"); do
    local a=$((s*2-1)) b=$((s*2)) w=1
    [ "$s" = 3 ] && w=$nw
    cat <<EOF
  <shard><weight>$w</weight><internal_replication>true</internal_replication>
    <replica><host>127.0.0.1</host><port>910$a</port></replica>
    <replica><host>127.0.0.1</host><port>910$b</port></replica>
  </shard>
EOF
  done
  echo "</${CLUSTER}></remote_servers></clickhouse>"
}

write_node() {
  local i=$1 shards=$2 d="$LAB/n$1" rp_xml=""
  local rname='{replica}'
  [ -n "${REPLICA_PATH:-}" ] && rp_xml="<default_replica_path>${REPLICA_PATH}</default_replica_path><default_replica_name>${rname}</default_replica_name>"
  mkdir -p "$d"/{data,tmp,user_files,access,log,config.d}
  cat >"$d/config.xml" <<EOF
<clickhouse>
  <logger><level>warning</level><log>$d/log/server.log</log><errorlog>$d/log/err.log</errorlog><console>0</console></logger>
  <display_name>n$i</display_name>
  <listen_host>127.0.0.1</listen_host>
  <http_port>812$i</http_port>
  <tcp_port>910$i</tcp_port>
  <interserver_http_port>920$i</interserver_http_port>
  <interserver_http_host>127.0.0.1</interserver_http_host>
  <path>$d/data/</path>
  <tmp_path>$d/tmp/</tmp_path>
  <user_files_path>$d/user_files/</user_files_path>
  <user_directories>
    <users_xml><path>$LAB/users.xml</path></users_xml>
    <local_directory><path>$d/access/</path></local_directory>
  </user_directories>
  <mark_cache_size>67108864</mark_cache_size>
  <uncompressed_cache_size>0</uncompressed_cache_size>
  <max_server_memory_usage>900000000</max_server_memory_usage>
  <background_schedule_pool_size>16</background_schedule_pool_size>
  <mlock_executable>false</mlock_executable>
  <zookeeper><node><host>127.0.0.1</host><port>9181</port></node></zookeeper>
  <distributed_ddl><path>/clickhouse/task_queue/ddl</path></distributed_ddl>
  $rp_xml
  <macros><shard>$(shard_of "$i")</shard><replica>n$i</replica><cluster>${CLUSTER}</cluster></macros>
  <storage_configuration><disks><cold><path>$d/data/cold/</path></cold></disks>
    <policies><tiered><volumes><hot><disk>default</disk></hot><cold><disk>cold</disk></cold></volumes></tiered></policies>
  </storage_configuration>
</clickhouse>
EOF
  remote_servers "$shards" >"$d/config.d/remote_servers.xml"
}

up() {
  mkdir -p "$LAB/keeper"
  cat >"$LAB/users.xml" <<'EOF'
<clickhouse>
  <profiles><default><max_memory_usage>500000000</max_memory_usage></default></profiles>
  <users><default><password></password><networks><ip>127.0.0.1</ip><ip>::1</ip></networks>
    <profile>default</profile><quota>default</quota><access_management>1</access_management>
    <named_collection_control>1</named_collection_control><show_named_collections>1</show_named_collections>
    <show_named_collections_secrets>1</show_named_collections_secrets></default></users>
  <quotas><default/></quotas>
</clickhouse>
EOF
  cat >"$LAB/keeper/keeper.xml" <<EOF
<clickhouse>
  <logger><level>warning</level><log>$LAB/keeper/keeper.log</log><console>0</console></logger>
  <listen_host>127.0.0.1</listen_host>
  <keeper_server>
    <tcp_port>9181</tcp_port><server_id>1</server_id><enable_ipv6>false</enable_ipv6>
    <log_storage_path>$LAB/keeper/log</log_storage_path>
    <snapshot_storage_path>$LAB/keeper/snap</snapshot_storage_path>
    <raft_configuration><server><id>1</id><hostname>127.0.0.1</hostname><port>9234</port></server></raft_configuration>
  </keeper_server>
</clickhouse>
EOF
  "$CH" keeper --config-file="$LAB/keeper/keeper.xml" --pid-file="$LAB/keeper/pid" --daemon
  for i in 1 2 3 4; do write_node $i 2; done
  for i in 5 6; do write_node $i 3; done
  for i in 1 2 3 4 5 6; do
    "$CH" server --config-file="$LAB/n$i/config.xml" --pid-file="$LAB/n$i/pid" --daemon
  done
  for i in 1 2 3 4 5 6; do
    for _ in $(seq 1 60); do curl -sf "http://127.0.0.1:812$i/ping" >/dev/null && break; sleep 0.5; done
    echo "n$i: $(curl -s "http://127.0.0.1:812$i/?query=SELECT+version()")"
  done
}

q() { curl -sS --fail-with-body "http://127.0.0.1:812$1/" --data-binary "$2"; }

seed() {
  q 1 "CREATE DATABASE IF NOT EXISTS BD1 ON CLUSTER ${CLUSTER}"
  q 1 "CREATE DATABASE IF NOT EXISTS BD2 ON CLUSTER ${CLUSTER}"
  q 1 "CREATE DATABASE IF NOT EXISTS test ON CLUSTER ${CLUSTER}"
  q 1 "CREATE FUNCTION IF NOT EXISTS lab_norm ON CLUSTER ${CLUSTER} AS (x) -> lower(trim(x))"
  # sharded replicated + distributed (path with {uuid})
  q 1 "CREATE TABLE BD1.dev_table_p ON CLUSTER ${CLUSTER} (id UInt64, ts DateTime, name String, v Float64, n String DEFAULT lab_norm(name))
       ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') PARTITION BY toYYYYMM(ts) ORDER BY id
       SETTINGS storage_policy = 'tiered'"
  q 1 "CREATE TABLE BD1.dev_table ON CLUSTER ${CLUSTER} AS BD1.dev_table_p ENGINE = Distributed('{cluster}', 'BD1', 'dev_table_p', cityHash64(id))"
  # path with {database}/{table} macros + replacing engine
  q 1 "CREATE TABLE BD1.users_p ON CLUSTER ${CLUSTER} (uid UInt64, ver UInt32, email String)
       ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}', ver) ORDER BY uid"
  q 1 "CREATE TABLE BD1.users ON CLUSTER ${CLUSTER} AS BD1.users_p ENGINE = Distributed(${CLUSTER}, BD1, users_p, uid)"
  # default engine args (default_replica_path)
  q 1 "CREATE TABLE BD1.agg_p ON CLUSTER ${CLUSTER} (d Date, id UInt64, c AggregateFunction(count)) ENGINE = ReplicatedAggregatingMergeTree ORDER BY (d, id)"
  q 1 "CREATE TABLE BD1.agg ON CLUSTER ${CLUSTER} AS BD1.agg_p ENGINE = Distributed(${CLUSTER}, BD1, agg_p, id)"
  # MV TO table
  q 1 "CREATE MATERIALIZED VIEW BD1.mv_agg ON CLUSTER ${CLUSTER} TO BD1.agg_p AS SELECT toDate(ts) AS d, id, countState() AS c FROM BD1.dev_table_p GROUP BY d, id"
  # MV with inner replicated table
  q 1 "CREATE MATERIALIZED VIEW BD1.mv_inner ON CLUSTER ${CLUSTER}
       ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') ORDER BY id
       AS SELECT id, sum(v) AS s FROM BD1.dev_table_p GROUP BY id"
  # view on distributed, view on view
  q 1 "CREATE VIEW BD2.v_users ON CLUSTER ${CLUSTER} AS SELECT uid, email FROM BD1.users WHERE uid > 0"
  q 1 "CREATE VIEW BD2.v_users2 ON CLUSTER ${CLUSTER} AS SELECT * FROM BD2.v_users"
  # dictionary + table with dictGet default
  q 1 "CREATE TABLE BD2.dim_p ON CLUSTER ${CLUSTER} (k UInt64, label String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/all/{database}/{table}', '{replica}') ORDER BY k"
  q 1 "CREATE DICTIONARY BD2.dim_dict ON CLUSTER ${CLUSTER} (k UInt64, label String) PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'dim_p' DB 'BD2')) LAYOUT(HASHED()) LIFETIME(300)"
  q 1 "CREATE TABLE BD2.facts_p ON CLUSTER ${CLUSTER} (k UInt64, label String DEFAULT dictGet('BD2.dim_dict', 'label', k)) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}') ORDER BY k"
  # bms objects (must be ignored) + MV that depends on bms
  q 1 "CREATE TABLE BD1.BMS_events_p ON CLUSTER ${CLUSTER} (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}') ORDER BY id"
  q 1 "CREATE TABLE BD1.events_copy_p ON CLUSTER ${CLUSTER} (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}') ORDER BY id"
  q 1 "CREATE MATERIALIZED VIEW BD1.mv_from_bms ON CLUSTER ${CLUSTER} TO BD1.events_copy_p AS SELECT id FROM BD1.BMS_events_p"
  q 1 "CREATE DATABASE IF NOT EXISTS bms_db ON CLUSTER ${CLUSTER}"
  q 1 "CREATE TABLE bms_db.x ON CLUSTER ${CLUSTER} (id UInt64) ENGINE = MergeTree ORDER BY id"
  q 1 "CREATE TABLE test.t_p ON CLUSTER ${CLUSTER} (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}') ORDER BY id"
  # Kafka (брокер не нужен для CREATE) + MV из него: проверка exclude_engines + каскада
  q 1 "CREATE TABLE BD1.kafka_events ON CLUSTER ${CLUSTER} (id UInt64) ENGINE = Kafka SETTINGS kafka_broker_list = '127.0.0.1:1', kafka_topic_list = 'events', kafka_group_name = 'ch_events', kafka_format = 'JSONEachRow'"
  q 1 "CREATE MATERIALIZED VIEW BD1.mv_kafka ON CLUSTER ${CLUSTER} TO BD1.events_copy_p AS SELECT id FROM BD1.kafka_events"
  q 1 "CREATE MATERIALIZED VIEW BD1.mv_copy_events ON CLUSTER ${CLUSTER} TO BD1.events_copy_p AS SELECT id FROM BD1.BMS_events_p"
  curl -sS --fail-with-body "http://127.0.0.1:8121/?database=BD1" --data-binary "CREATE VIEW BD1.v_unqual ON CLUSTER ${CLUSTER} AS SELECT count() AS c FROM dev_table_p"
  # misc engines
  q 1 "CREATE TABLE BD2.buf ON CLUSTER ${CLUSTER} AS BD1.dev_table_p ENGINE = Buffer('BD1', 'dev_table_p', 1, 10, 60, 1000, 100000, 1000000, 10000000)"
  q 1 "CREATE TABLE BD2.merge_all ON CLUSTER ${CLUSTER} (id UInt64) ENGINE = Merge('BD1', '^events_copy')"
  q 1 "CREATE TABLE BD2.null_sink ON CLUSTER ${CLUSTER} (id UInt64) ENGINE = Null"
  # drift on purpose: table that exists only on shard 02
  q 3 "CREATE TABLE BD2.only_on_shard2 (id UInt64) ENGINE = MergeTree ORDER BY id"
  q 4 "CREATE TABLE BD2.only_on_shard2 (id UInt64) ENGINE = MergeTree ORDER BY id"
  # data
  q 1 "INSERT INTO BD1.dev_table (id, ts, name, v) SELECT number, now() - number, toString(number), number/3 FROM numbers(100000)"
  q 1 "INSERT INTO BD2.dim_p SELECT number, concat('L', toString(number)) FROM numbers(100)"
  echo "seed: ok"
}

# Схема в стиле прода: Replicated* без аргументов -> путь из default_replica_path
seed_prod() {
  q 1 "CREATE DATABASE IF NOT EXISTS BD1 ON CLUSTER ${CLUSTER}"
  q 1 "CREATE DATABASE IF NOT EXISTS BD2 ON CLUSTER ${CLUSTER}"
  q 1 "CREATE DATABASE IF NOT EXISTS test ON CLUSTER ${CLUSTER}"
  q 1 "CREATE FUNCTION IF NOT EXISTS lab_norm ON CLUSTER ${CLUSTER} AS (x) -> lower(trim(x))"
  q 1 "CREATE TABLE BD1.dev_table_p ON CLUSTER ${CLUSTER} (id UInt64, ts DateTime, name String, v Float64, n String DEFAULT lab_norm(name))
       ENGINE = ReplicatedMergeTree PARTITION BY toYYYYMM(ts) ORDER BY id SETTINGS storage_policy = 'tiered'"
  q 1 "CREATE TABLE BD1.dev_table ON CLUSTER ${CLUSTER} AS BD1.dev_table_p ENGINE = Distributed('{cluster}', 'BD1', 'dev_table_p', cityHash64(id))"
  q 1 "CREATE TABLE BD1.users_p ON CLUSTER ${CLUSTER} (uid UInt64, ver UInt32, email String) ENGINE = ReplicatedReplacingMergeTree(ver) ORDER BY uid"
  q 1 "CREATE TABLE BD1.users ON CLUSTER ${CLUSTER} AS BD1.users_p ENGINE = Distributed('{cluster}', BD1, users_p, uid)"
  q 1 "CREATE TABLE BD1.agg_p ON CLUSTER ${CLUSTER} (d Date, id UInt64, c AggregateFunction(count)) ENGINE = ReplicatedAggregatingMergeTree ORDER BY (d, id)"
  q 1 "CREATE TABLE BD1.agg ON CLUSTER ${CLUSTER} AS BD1.agg_p ENGINE = Distributed('{cluster}', BD1, agg_p, id)"
  q 1 "CREATE MATERIALIZED VIEW BD1.mv_agg ON CLUSTER ${CLUSTER} TO BD1.agg_p AS SELECT toDate(ts) AS d, id, countState() AS c FROM BD1.dev_table_p GROUP BY d, id"
  # MV с inner-движком без аргументов: {table} = .inner_id.<uuid MV>
  q 1 "CREATE MATERIALIZED VIEW BD1.mv_inner ON CLUSTER ${CLUSTER} ENGINE = ReplicatedSummingMergeTree ORDER BY id AS SELECT id, sum(v) AS s FROM BD1.dev_table_p GROUP BY id"
  # переименованная таблица: путь в ZK остаётся со старым именем
  q 1 "CREATE TABLE BD2.old_name_p ON CLUSTER ${CLUSTER} (k UInt64) ENGINE = ReplicatedMergeTree ORDER BY k"
  q 1 "RENAME TABLE BD2.old_name_p TO BD2.events_p ON CLUSTER ${CLUSTER}"
  q 1 "CREATE TABLE BD2.events ON CLUSTER ${CLUSTER} AS BD2.events_p ENGINE = Distributed('{cluster}', BD2, events_p, rand())"
  q 1 "CREATE VIEW BD2.v_users ON CLUSTER ${CLUSTER} AS SELECT uid, email FROM BD1.users WHERE uid > 0"
  q 1 "CREATE TABLE BD2.dim_p ON CLUSTER ${CLUSTER} (k UInt64, label String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/all/{database}/{table}', '{replica}') ORDER BY k"
  q 1 "CREATE DICTIONARY BD2.dim_dict ON CLUSTER ${CLUSTER} (k UInt64, label String) PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'dim_p' DB 'BD2')) LAYOUT(HASHED()) LIFETIME(300)"
  q 1 "CREATE TABLE BD2.facts_p ON CLUSTER ${CLUSTER} (k UInt64, label String DEFAULT dictGet('BD2.dim_dict', 'label', k)) ENGINE = ReplicatedMergeTree ORDER BY k"
  q 1 "CREATE TABLE BD1.BMS_events_p ON CLUSTER ${CLUSTER} (id UInt64) ENGINE = ReplicatedMergeTree ORDER BY id"
  q 1 "CREATE TABLE BD1.events_copy_p ON CLUSTER ${CLUSTER} (id UInt64) ENGINE = ReplicatedMergeTree ORDER BY id"
  q 1 "CREATE MATERIALIZED VIEW BD1.mv_from_bms ON CLUSTER ${CLUSTER} TO BD1.events_copy_p AS SELECT id FROM BD1.BMS_events_p"
  q 1 "CREATE TABLE test.t_p ON CLUSTER ${CLUSTER} (id UInt64) ENGINE = ReplicatedMergeTree ORDER BY id"
  q 1 "CREATE TABLE BD2.null_sink ON CLUSTER ${CLUSTER} (id UInt64) ENGINE = Null"
  q 1 "INSERT INTO BD1.dev_table (id, ts, name, v) SELECT number, now() - number, toString(number), number/3 FROM numbers(100000) SETTINGS distributed_foreground_insert=1"
  q 1 "INSERT INTO BD2.dim_p SELECT number, concat('L', toString(number)) FROM numbers(100)"
  echo "seed_prod: ok"
}

expand() { # $1 = вес нового шарда (0 = только чтение, по умолчанию 1)
  local w=${1:-1}
  for i in 1 2 3 4 5 6; do
    remote_servers 3 "$w" >"$LAB/n$i/config.d/remote_servers.xml"
    q $i "SYSTEM RELOAD CONFIG"
  done
  echo "expand: все ноды видят 3 шарда, вес нового шарда = $w"
}

down() {
  for p in "$LAB"/n*/pid "$LAB"/keeper/pid; do [ -f "$p" ] && kill "$(cat "$p")" 2>/dev/null || true; done
  sleep 2; pkill -f "$LAB/" 2>/dev/null || true; rm -rf "$LAB"
}

case "${1:-}" in
  up) up;; seed) seed;; seed_prod) seed_prod;; expand) expand "${2:-1}";; down) down;;
  q) q "$2" "$3";;
  *) sed -n '2,14p' "$0"; exit 1;;
esac

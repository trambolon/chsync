#!/usr/bin/env bash
# Управление docker-стендом chsync.
#
#   ./lab.sh up              сертификаты (если нет) + запуск 3 keeper + 6 ClickHouse, ждать готовности
#   ./lab.sh seed            тестовая схема ON CLUSTER на старом кластере + данные
#   ./lab.sh expand [W]      remote_servers с новым шардом на старых узлах (W — вес шарда 03, по умолчанию 0)
#   ./lab.sh status          кто какие шарды видит, реплики, макросы
#   ./lab.sh q NODE "SQL"    запрос на узле, например: ./lab.sh q ch-s3-r1 "SHOW DATABASES"
#   ./lab.sh client NODE     интерактивный clickhouse-client на узле (admin)
#   ./lab.sh chsync ARGS     chsync внутри сети compose: ./lab.sh chsync check | plan | apply --execute --yes | diff
#   ./lab.sh down            остановить и удалить контейнеры И ДАННЫЕ (тома)
#   ./lab.sh certs           перевыпустить CA и сертификат узлов
set -euo pipefail
cd "$(dirname "$0")"

USER_NAME=admin
PASSWORD=123456qwe
CLUSTER=lab_cluster
OLD_NODES=(ch-s1-r1 ch-s1-r2 ch-s2-r1 ch-s2-r2)
NEW_NODES=(ch-s3-r1 ch-s3-r2)
dc() { docker compose "$@"; }

# remote_servers: $1 — сколько шардов (2 | 3), $2 — вес шарда 03
remote_servers() {
  local n=$1 w=${2:-0}
  cat <<EOF
<!-- Сгенерировано lab.sh: $n шард(а), вес нового шарда 03 = $w. Native TLS 9440, internal_replication = true. -->
<clickhouse>
    <remote_servers replace="replace">
        <$CLUSTER>
            <secret>lab_interserver_secret</secret>
EOF
  for s in $(seq 1 "$n"); do
    local weight=1
    [ "$s" = 3 ] && weight=$w
    cat <<EOF
            <shard>
                <weight>$weight</weight>
                <internal_replication>true</internal_replication>
                <replica><host>ch-s$s-r1</host><port>9440</port><secure>1</secure></replica>
                <replica><host>ch-s$s-r2</host><port>9440</port><secure>1</secure></replica>
            </shard>
EOF
  done
  cat <<EOF
        </$CLUSTER>
    </remote_servers>
</clickhouse>
EOF
}

certs() {
  mkdir -p certs
  local san="DNS:localhost,IP:127.0.0.1"
  for h in "${OLD_NODES[@]}" "${NEW_NODES[@]}"; do san+=",DNS:$h"; done
  openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -subj "/CN=chsync-lab-ca" \
    -keyout certs/ca.key -out certs/ca.crt 2>/dev/null
  openssl req -newkey rsa:2048 -nodes -subj "/CN=chsync-lab" -keyout certs/server.key -out certs/server.csr 2>/dev/null
  openssl x509 -req -in certs/server.csr -CA certs/ca.crt -CAkey certs/ca.key -CAcreateserial -days 3650 \
    -extfile <(printf "subjectAltName=%s\nextendedKeyUsage=serverAuth,clientAuth" "$san") -out certs/server.crt 2>/dev/null
  rm -f certs/server.csr certs/ca.srl
  chmod 644 certs/*.crt certs/server.key   # сервер работает под uid 101 — ключ должен читаться (только для стенда!)
  echo "certs: CA и сертификат узлов выпущены (SAN: $san)"
}

up() {
  [ -f certs/server.crt ] || certs
  [ -f config/cluster/old/remote_servers.xml ] || remote_servers 2 >config/cluster/old/remote_servers.xml
  [ -f config/cluster/new/remote_servers.xml ] || remote_servers 3 0 >config/cluster/new/remote_servers.xml
  mkdir -p chsync-out
  dc up -d --wait keeper1 keeper2 keeper3 "${OLD_NODES[@]}" "${NEW_NODES[@]}"
  status
}

q() { local node=$1; shift; dc exec -T "$node" clickhouse client -u "$USER_NAME" --password "$PASSWORD" --multiquery --query "$*"; }

seed() {
  if [ "$(q ch-s1-r1 "EXISTS TABLE shop.orders_p")" = 1 ]; then
    echo "seed: схема уже создана. Для чистого стенда: ./lab.sh down && ./lab.sh up && ./lab.sh seed"; return 0
  fi
  dc exec -T ch-s1-r1 clickhouse client -u "$USER_NAME" --password "$PASSWORD" --multiquery <seed/01-schema.sql
  # Дрейф специально: таблица только на шарде 02 (chsync покажет «есть только на 1 из 2 источников»)
  for n in ch-s2-r1 ch-s2-r2; do q "$n" "CREATE TABLE IF NOT EXISTS shop.shard2_only (id UInt64) ENGINE = MergeTree ORDER BY id"; done
  dc exec -T ch-s1-r1 clickhouse client -u "$USER_NAME" --password "$PASSWORD" --multiquery <seed/02-data.sql
  echo "seed: схема и данные созданы на старом кластере"
}

expand() {
  local w=${1:-0}
  remote_servers 3 "$w" >config/cluster/old/remote_servers.xml
  remote_servers 3 "$w" >config/cluster/new/remote_servers.xml
  for n in "${OLD_NODES[@]}" "${NEW_NODES[@]}"; do q "$n" "SYSTEM RELOAD CONFIG"; done
  echo "expand: все узлы видят 3 шарда, вес шарда 03 = $w"
}

status() {
  for n in "${OLD_NODES[@]}" "${NEW_NODES[@]}"; do
    printf '%-9s ' "$n"
    q "$n" "SELECT concat('shard=', getMacro('shard'), ' replica=', getMacro('replica'), '  видит шардов: ',
      toString((SELECT uniqExact(shard_num) FROM system.clusters WHERE cluster = '$CLUSTER')),
      '  таблиц: ', toString((SELECT count() FROM system.tables WHERE database NOT IN ('system','INFORMATION_SCHEMA','information_schema'))))"
  done
}

case "${1:-}" in
  up) up ;;
  seed) seed ;;
  expand) expand "${2:-0}" ;;
  status) status ;;
  q) shift; q "$@" ;;
  client) dc exec "${2:?узел}" clickhouse client -u "$USER_NAME" --password "$PASSWORD" ;;
  chsync) shift; mkdir -p chsync-out; dc run --rm chsync "$@" ;;
  certs) certs ;;
  down) dc --profile tools down -v --remove-orphans ;;
  *) sed -n '2,13p' "$0"; exit 1 ;;
esac

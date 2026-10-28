# Docker-стенд для chsync

3 ClickHouse Keeper + 6 ClickHouse 26.3.2.3: два «старых» шарда и один «новый», у каждого по 2 реплики.
Внутри сети compose всё как на проде: HTTPS **8443**, native TLS **9440** (в `remote_servers`), пользователь
**admin / 123456qwe**, Replicated-таблицы без аргументов движка (`default_replica_path`).

```
            старый кластер (source)                       новый шард (target)
   shard 01: ch-s1-r1, ch-s1-r2   shard 02: ch-s2-r1, ch-s2-r2     shard 03: ch-s3-r1, ch-s3-r2
                          keeper1, keeper2, keeper3 (Raft-кворум из 3)
```

## Порты на хосте

| Сервис    | HTTPS (8443) | native TLS (9440) | роль                    |
|-----------|--------------|-------------------|-------------------------|
| ch-s1-r1  | 18441        | 19441             | старый шард 01, source  |
| ch-s1-r2  | 18442        | 19442             | старый шард 01          |
| ch-s2-r1  | 18443        | 19443             | старый шард 02, source  |
| ch-s2-r2  | 18444        | 19444             | старый шард 02          |
| ch-s3-r1  | 18445        | 19445             | **новый шард 03**, target |
| ch-s3-r2  | 18446        | 19446             | **новый шард 03**, target |
| keeper1-3 | —            | —                 | 19181, 19182, 19183 → 9181 |

Порты 8123/9000 (без TLS) на хост не пробрасываются — они только для healthcheck и seed внутри контейнеров.

## Запуск

Нужны Docker с Compose v2 и openssl. Из каталога `testlab/docker`:

```bash
./lab.sh up          # выпустит CA и сертификат (certs/), поднимет 9 контейнеров, дождётся healthcheck
./lab.sh seed        # схема ON CLUSTER на старом кластере + данные
./lab.sh status      # кто сколько шардов видит и сколько таблиц
```

После `up` старые узлы видят 2 шарда, новые — 3 (новый шард с весом 0) — ровно состояние «новые хосты
подготовлены, старые ещё не переключены».

## Прогон chsync

**Внутри сети compose, по https 8443** (как с jump-хоста на проде; конфиг — `chsync.toml`):

```bash
./lab.sh chsync check
./lab.sh chsync plan                       # shop.products_p будет BLOCKED: путь без {shard}
# решили подключать глобальную таблицу: в chsync.toml global_replicated = "create"
./lab.sh chsync apply --execute --yes --targets ch-s3-r1
./lab.sh chsync apply --execute --yes
./lab.sh expand 0                          # remote_servers с новым шардом (вес 0) на старых узлах
./lab.sh chsync check
./lab.sh chsync diff                       # одно расхождение — shop.shard2_only, оставлено специально
```

`./lab.sh chsync …` = `docker compose run --rm chsync …`: бинарник берётся из `../../dist/chsync-linux-amd64`
(другой путь — переменная `CHSYNC_BIN`), отчёты пишутся в `./chsync-out`.

**С хоста, через проброшенные порты** (конфиг — `chsync.host.toml`, сертификат содержит SAN `localhost`):

```bash
../../dist/chsync-linux-amd64 check -c chsync.host.toml
```

Проверка сертификата включена: без `ca_file` подключение отклоняется (`x509: certificate signed by unknown authority`).

## Что в тестовой схеме (`seed/01-schema.sql`)

| Что | Объекты | Чем интересно для chsync |
|---|---|---|
| Пустые БД | `empty_db`, `staging` (с COMMENT) | создаются как есть |
| БД с движком Replicated | `repl_db` + `repl_db.kv` | chsync создаёт только БД, таблицы она раскладывает сама |
| Replicated без аргументов | `shop.orders_p`, `customers_p` (Replacing), `daily_revenue_p` (Summing), `analytics.sessions_p` (Collapsing) … | путь из `default_replica_path`, у каждой — свой Distributed |
| Сложный DDL | `orders_p`: партиции, TTL, кодеки, skip-индекс, проекция, `storage_policy = 'tiered'` | DDL переносится целиком, политика проверяется на target |
| Путь с `{uuid}` | `analytics.page_views_p` | обе реплики нового шарда получают один UUID |
| Глобальная таблица | `shop.products_p` (путь `/clickhouse/tables/all/…`) | BLOCKED по умолчанию, `global_replicated` |
| Переименованная таблица | `shop.payments_p` (была `legacy_payments_p`) | ZK-путь со старым именем |
| MV | `daily_revenue_mv` (TO), `hourly_status_mv` (inner-движок) | порядок создания, путь `.inner_id.<uuid>` |
| Словарь и функции | `products_dict`, `dictGetOrDefault` в DEFAULT, UDF `normalize_email` в DEFAULT | зависимости таблиц от словаря и функций |
| Нереплицируемые | `local_cache` (MergeTree) + Distributed над ней, `replacing_local`, Log, Memory, Null, Set, Join | предупреждения про данные на одной реплике |
| Прокси | `orders_buffer` (Buffer), `orders_all_local` (Merge) | создаются после своих таблиц |
| VIEW | обычный, параметризованный (`v_customer_orders(cid = 42)`), VIEW над VIEW | цепочка зависимостей |
| Исключения | БД `test`, БД `BMS_Legacy`, `shop.bms_sync_p` + MV над ней | фильтры и каскадное исключение |
| Дрейф | `shop.shard2_only` — только на шарде 02 | «есть только на 1 из 2 источников», DRIFT в diff |

## Прочее

```bash
./lab.sh q ch-s3-r1 "SELECT * FROM system.replicas FORMAT Vertical"   # запрос на узле
./lab.sh client ch-s1-r1                                              # интерактивный clickhouse-client
./lab.sh expand 1                                                     # открыть запись в шард 03
./lab.sh down                                                         # удалить контейнеры И данные
```

* Конфиги узлов: `config/server` (порты, TLS, keeper, макросы, storage policy), `config/users/admin.xml`,
  `config/keeper/keeper.xml`. Макросы `shard`/`replica` и `server_id` keeper'а задаются переменными окружения
  в `docker-compose.yml` (`from_env`) — файлы у всех узлов общие.
* `remote_servers` генерирует `lab.sh` в `config/cluster/old` (старые узлы) и `config/cluster/new` (новые);
  они монтируются в `/etc/clickhouse-server/conf.d` и перечитываются без рестарта (`SYSTEM RELOAD CONFIG`).
* Если Docker не даёт выставить `nofile = 262144` (`error setting rlimit type 7`), запускайте с
  `CH_NOFILE=20000 ./lab.sh up` или пропишите `CH_NOFILE=20000` в `.env`.
* Ключ сертификата лежит с правами 644 — так его читает сервер под uid 101. Это только для стенда.

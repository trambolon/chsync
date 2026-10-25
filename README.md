# chsync — схема ClickHouse на новые шарды

Утилита доводит новые хосты кластера до той же схемы, что и на старых: базы, SQL-функции,
словари, `Replicated*MergeTree`, `Distributed`, `Buffer/Merge`, `VIEW`, `MATERIALIZED VIEW` —
в правильном порядке, с правильными UUID и с проверкой ZooKeeper-путей.

* DDL берётся с живых хостов-источников (`system.tables.create_table_query`), выполняется
  **напрямую** на каждом новом хосте (без `ON CLUSTER`), только `CREATE ... IF NOT EXISTS`.
* Никогда не делает `DROP / ALTER / RENAME / INSERT`. Повторный запуск безопасен (идемпотентно).
* Один статический бинарник, Go, работает через HTTP-интерфейс (8123/8443). Зависимость одна —
  TOML-парсер, она лежит в `vendor/` (сборка без интернета).

Порядок работ по подключению шардов — в **RUNBOOK.md**. Здесь — только про утилиту.

## Структура проекта

| Файл | Что внутри |
|---|---|
| `main.go` | CLI, конфиг (TOML, умолчания, валидация), сценарий plan/apply, утилиты |
| `clickhouse.go` | HTTP-клиент ClickHouse, снимок хоста (БД, объекты, макросы, кластеры, окружение), запросы к ZK |
| `ddl.go` | Разбор DDL: вид объекта, UUID, аргументы движка, ZK-пути и макросы, ссылки между объектами; фильтры |
| `plan.go` | Объединение источников, каскадное исключение, порядок создания, план, проверки ZK, apply, verify, отчёты |
| `check.go` | `check` (в т.ч. контроль репликации) и `diff` |
| `chsync_test.go` | Юнит-тесты без сети |

## Шарды с репликацией и без

Движки таблиц переносятся **как есть** — так схема остаётся одинаковой на всех шардах. Сколько у шарда
реплик, задают `remote_servers` и макросы, а не движок:

* **Шард с одной репликой** — допустим. `Replicated*MergeTree` на нём работает (одна реплика в ZK),
  вторую реплику можно добавить позже без пересоздания таблиц. `check` предупредит, что нет отказоустойчивости.
* **Шард с несколькими репликами** — `Replicated*` реплицируются между ними; нереплицируемые `MergeTree`
  не синхронизируются (как на старых шардах) — `plan` предупреждает по каждой такой таблице.

Что сверяет `check` в разделе «Репликация шардов» (`target.replicas_per_shard = N` включает проверку числа реплик):

| Проблема | Последствие | Результат check |
|---|---|---|
| В `target.hosts` у нового шарда (по макросу `shard`) не N хостов | Шард не того размера | ✗ |
| В `remote_servers` у нового шарда не N реплик | Distributed не знает о реплике или знает о лишней | ✗ |
| `internal_replication=false` у шарда с репликами при наличии Replicated-таблиц | Distributed пишет в каждую реплику, а таблица ещё и реплицирует: двойная запись | ✗ |
| У реплик одного шарда разные макросы `shard` | По ZK это разные наборы реплик, а Distributed считает их копиями: неполные SELECT | ✗ |
| Один макрос `shard` у реплик разных шардов | По ZK это один набор реплик: данные задваиваются | ✗ |
| Один `host:port` в нескольких шардах `remote_servers` | Distributed читает его данные несколько раз | ✗ |
| Шард с одной репликой | Нет отказоустойчивости | ! |
| Distributed над нереплицируемой MergeTree на шардах с репликами | Данные попадают только на одну реплику | ! |

Макросы реплик читаются через `clusterAllReplicas` (те же креды, что у Distributed), поэтому
сверка охватывает и старые шарды, которых нет в `source.hosts`.

## Что именно она проверяет и почему это важно

| Проблема | Что будет без проверки | Что делает chsync |
|---|---|---|
| ZK-путь `/clickhouse/tables/{uuid}/{shard}` (дефолт CH). `{uuid}` хранится в метаданных **нераскрытым** | Если создать таблицу на двух репликах нового шарда без явного UUID — у них будут **разные UUID → разные ZK-пути → реплики не видят друг друга**, тихо | Переносит UUID таблицы с источника (`preserve_uuid=true`), MV — вместе с `TO INNER UUID` |
| Макрос `{shard}` нового хоста совпал со старым (опечатка в macros.xml) | Новый хост **становится третьей репликой старого шарда** и качает его данные | `check` и `plan` сравнивают развёрнутые ZK-пути target и всех источников → BLOCKED |
| В ZK остались узлы выведенного когда-то шарда / прошлой неудачной попытки | `Replica already exists` или подключение к чужим данным | Читает `<path>/replicas` через `system.zookeeper` на target → BLOCKED с готовой командой `SYSTEM DROP REPLICA ... FROM ZKPATH` |
| Таблица с путём **без** `{shard}` (полная копия на каждом хосте, «глобальная» справочная) | Новый хост присоединится к общему набору реплик и скачает все данные — может быть и нужно, и нет | По умолчанию BLOCKED, решение явно: `ddl.global_replicated = create|skip` |
| Порядок создания: MV до своей таблицы, VIEW до Distributed, `dictGet` в DEFAULT до словаря | `CREATE` падает | Граф зависимостей из DDL + приоритет по типу + повторные проходы при apply |
| MV/VIEW/Distributed поверх исключённой (`bms`, `test`) таблицы | `CREATE` падает или висит «полу-объект» | Каскадное исключение (`cascade_exclude=true`) |
| `Distributed('{cluster}', ...)`, кластера нет на target | Таблица создаётся, но не работает | BLOCKED, если кластера нет в `system.clusters` target |
| `storage_policy`/`disk` нет на новом хосте | `CREATE` падает | BLOCKED с указанием политики |
| Секреты в словарях/движках MySQL/PG/S3 показываются как `[HIDDEN]` | Создастся объект с паролем `[HIDDEN]` | BLOCKED, см. `ddl.display_secrets` |
| DDL одного объекта различается между старыми шардами (дрейф) | Непонятно, какую версию катить | BLOCKED (`on_conflict=error`) + показывает, где различие |
| Kafka/RabbitMQ/S3Queue | Потребление стартует сразу, ребаланс consumer group | Предупреждение в плане (или `exclude_engines`) |

## Команды

```
chsync check  -c chsync.toml          # предполётные проверки, ничего не меняет
chsync plan   -c chsync.toml          # план + SQL по каждому target (dry run)
chsync apply  -c chsync.toml --execute [--targets h1] [--yes]
chsync diff   -c chsync.toml          # «везде одинаково?» по source+target, exit 2 при расхождениях
chsync init                           # пример конфига
```

* `check` — связь, версии, макросы (уникальность shard/replica, новый shard ≠ старым), топология
  `remote_servers` на каждом хосте (кто сколько шардов видит, нашёл ли хост сам себя), репликация шардов
  (см. выше),
  связность `clusterAllReplicas` с кредами кластера, доступ к Keeper, storage policies, disks,
  пользователи/роли/UDF, различия `server_settings`, ключи шардирования всех Distributed.
* `plan` — для каждого target: `CREATE / EXISTS / DRIFT / BLOCKED / SKIP / EXTRA` + ожидаемые
  `zookeeper_path`/`replica_name`. Пишет `<target>.sql` (можно отдать на ревью/CAB или прогнать
  руками через `clickhouse-client --multiquery < file.sql`).
* `apply` — выполняет только CREATE. Без `--execute` (или `dry_run=false`) работает как `plan`.
  Есть BLOCKED → не запускается, пока не разберёте (или `--allow-blocked`, тогда они пропускаются).
  После создания перечитывает target: DDL совпадает с источником, реплики в `system.replicas`
  с ожидаемым путём и не в readonly.
* `diff` — сравнивает схему на всех хостах (в т.ч. показывает старый дрейф между старыми шардами).
  Подходит и для регулярного мониторинга «везде одинаково» по cron.

Флаги: `--targets a,b` (поэтапно, по одному хосту), `--databases a,b`, `--yes`, `--allow-blocked`,
`--stop-on-error`, `--scope all|sources|targets` (diff), `-v`.

Коды выхода: `0` — ок, `1` — ошибки, `2` — расхождения/BLOCKED, `3` — ошибка конфига.

Результаты каждого запуска: `output_dir/<дата-время>-<команда>/` — отчёт, SQL-скрипты, apply-логи.

## Фильтры (п.4–6 ТЗ)

```toml
[filter]
exclude_databases  = ["test"]       # system / INFORMATION_SCHEMA пропускаются всегда
exclude_name_regex = ["(?i)bms"]    # имя БД или объекта содержит bms в любом регистре
exclude_objects    = ["BD1.legacy_tmp"]
exclude_engines    = ["Kafka"]
cascade_exclude    = true           # MV/VIEW/Distributed/словари над исключёнными — тоже пропустить
```

## Установка на Debian 12/13

### Вариант A — готовый бинарник (на прод-хосте ничего ставить не нужно)

```bash
# с машины, где лежит архив
scp chsync-1.0.0.tar.gz admin@jump-host:/tmp/
ssh admin@jump-host
cd /tmp && tar xzf chsync-1.0.0.tar.gz && cd chsync
sha256sum -c dist/SHA256SUMS
sudo install -m 0755 dist/chsync-linux-amd64 /usr/local/bin/chsync   # arm64: chsync-linux-arm64
chsync version
```

### Вариант B — сборка из исходников

```bash
sudo apt update
sudo apt install -y golang-go make       # Debian 12: Go 1.19 — достаточно, Debian 13: Go 1.24
cd chsync
make test build                           # = go test + go build -mod=vendor (интернет не нужен)
sudo install -m 0755 chsync /usr/local/bin/chsync
```

### Запуск

Запускать с jump/admin-хоста, у которого есть доступ к **HTTP-порту (8123/8443) всех 10 хостов**.

```bash
mkdir -p ~/chsync && cd ~/chsync
chsync init > chsync.toml && vim chsync.toml      # cluster, source.hosts, target.hosts, фильтры
read -rs CHSYNC_PASSWORD && export CHSYNC_PASSWORD # пароль не попадает в history

chsync check
chsync plan            # читаем chsync-out/<run>/plan.txt и *.sql
chsync apply --execute --targets ch-s4-r1.prod.local
chsync apply --execute
chsync diff
```

Пользователь для chsync (если не хотите запускать под admin):

```sql
CREATE USER chsync IDENTIFIED WITH sha256_password BY '***' HOST IP '10.0.0.0/8';
GRANT SHOW, SELECT ON *.* TO chsync;          -- метаданные, system.*, system.zookeeper
GRANT REMOTE ON *.* TO chsync;                -- check: clusterAllReplicas
-- на НОВЫХ хостах дополнительно:
GRANT CREATE DATABASE, CREATE TABLE, CREATE VIEW, CREATE DICTIONARY, CREATE FUNCTION,
      INSERT, dictGet ON *.* TO chsync;
```

## Репетиция на стенде (рекомендуется перед продом)

`testlab/lab.sh` поднимает на одной машине Keeper + 6 инстансов ClickHouse (2 старых шарда × 2
реплики + новый шард × 2) с тестовой схемой: `{uuid}`-пути, `{database}/{table}`-пути, дефолтные
аргументы Replicated, MV c TO и с inner-движком, VIEW над VIEW, словарь + `dictGet` в DEFAULT,
Buffer/Merge/Null, глобальная таблица без `{shard}`, bms-объекты, БД test, дрейф между шардами.

```bash
# нужен бинарь clickhouse: пакет clickhouse-common-static из репозитория packages.clickhouse.com
# или CH_BIN=/path/to/clickhouse (сервер в системе запускать не нужно - стенд поднимает свои процессы)
cd testlab
./lab.sh up && ./lab.sh seed
# схема как на вашем проде (26.3, пути из default_replica_path, Replicated* без аргументов):
#   export REPLICA_PATH='/clickhouse/tables/{shard}/{database}/{table}'
#   ./lab.sh up && ./lab.sh seed_prod
../chsync check -c lab.toml
../chsync plan  -c lab.toml
../chsync apply -c lab.toml --execute --yes
./lab.sh expand 0     # remote_servers c новым шардом weight=0 на всех нодах
../chsync check -c lab.toml && ../chsync diff -c lab.toml
./lab.sh expand 1     # включить запись в новый шард
./lab.sh down
```

## Ограничения

* Сравнение DDL — текстовое после нормализации. Разные версии ClickHouse иногда форматируют
  DDL по-разному → ложные DRIFT. Ставьте на новые хосты ту же версию.
* Пользователи/роли/квоты (SQL RBAC), named collections, конфиги — не переносятся (`check`
  показывает, чего не хватает). Данные — не переносятся (см. RUNBOOK про перебалансировку).
* БД с движком `Replicated` — создаётся только сама БД, таблицы она синхронизирует сама.
  БД-движки MySQL/PostgreSQL/… — создаётся только БД.

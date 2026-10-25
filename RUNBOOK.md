# RUNBOOK: подключение шардов 4 и 5 к prod-кластеру ClickHouse

Было: 3 шарда × 2 реплики (6 хостов). Станет: 5 шардов × 2 реплики (10 хостов).
Имена в примерах: `ch-s{1..5}-r{1,2}.prod.local`, кластер `prod_cluster` — замените на свои.

## Принципы

1. **Старые хосты меняются ровно в одном месте** — `remote_servers` (перечитывается на лету,
   рестарт не нужен). Всё остальное делается на новых хостах, которые до последнего шага не
   видны ни приложениям, ни Distributed-таблицам старых хостов.
2. **Две фазы**: сначала новые шарды с `weight=0` — участвуют в SELECT, но INSERT через
   Distributed в них не идут (проверено на 24.8). Запись включается отдельным решением.
   До этого откат = вернуть старый `remote_servers.xml`.
3. **DDL-freeze на окно**: между снятием схемы и раскаткой `remote_servers` никаких
   `ON CLUSTER` DDL — иначе новые хосты пропустят изменение.
4. **Схема на новые хосты — утилитой chsync**, не руками: она переносит UUID (без этого реплики
   нового шарда не реплицируются друг с другом при путях `{uuid}`), проверяет ZK-пути, порядок
   зависимостей и фильтры.

## 1. Подготовка (за несколько дней до окна)

### 1.1 Решения, которые надо принять заранее

| Вопрос | Как узнать | Варианты |
|---|---|---|
| Формат макроса `shard` | `SELECT * FROM system.macros` на старом хосте | Как у старых: `01..03` → `04,05`; `1..3` → `4,5` |
| Ключи шардирования Distributed | `chsync check` → раздел «Distributed: ключи шардирования» | rand() → можно просто поднять веса. По ключу → см. раздел 3 |
| Таблицы без `{shard}` в ZK-пути («глобальные», полная копия на каждом хосте) | `chsync plan` → BLOCKED с этим текстом | `ddl.global_replicated = "create"` (хост скачает все данные) или `"skip"` |
| Kafka / RabbitMQ / S3Queue на шардах | `chsync plan` → предупреждения | Создавать **после** раскатки `remote_servers` (см. 2, шаг 7), иначе данные, потреблённые новыми шардами, временно не видны через старые хосты |
| Приложения пишут напрямую в `*_p` (своё шардирование, минуя Distributed)? | Опросить владельцев сервисов / `system.query_log` | Тогда веса не помогут — нужно менять конфиг приложения |

### 1.2 Железо и ОС

Как у старых: CPU/RAM/диски (лучше не меньше — новые шарды получат свою долю записи),
та же раскладка дисков под `storage_configuration`, те же sysctl/ulimits, THP, swappiness,
CPU governor, NTP/chrony, часовой пояс.

### 1.3 ClickHouse той же версии

```bash
# на старом хосте
clickhouse-server --version
# на новом (Debian), репозиторий packages.clickhouse.com уже подключён как на старых
sudo apt install -y clickhouse-server=24.8.14.39 clickhouse-client=24.8.14.39 clickhouse-common-static=24.8.14.39
sudo apt-mark hold clickhouse-server clickhouse-client clickhouse-common-static
```

Не запускать сервер, пока не положены конфиги (макросы читаются при старте).

### 1.4 Конфиги новых хостов

Берите `/etc/clickhouse-server/` со старой реплики целиком, затем меняйте только отмеченное.

| Что | Действие |
|---|---|
| `config.d/macros.xml` | **Новые** значения: `shard=04` (у обеих реплик шарда 4), `replica=<FQDN хоста>` — см. `examples/macros-ch-s4-r1.xml` |
| `config.d/remote_servers.xml` | **5-шардная** версия сразу, новые шарды `weight=0` — `examples/remote_servers.xml` |
| `interserver_http_host` | FQDN нового хоста; должен резолвиться со всех хостов |
| ZooKeeper/Keeper, `distributed_ddl.path` | Как на старых — **тот же** ансамбль, тот же путь очереди DDL |
| `storage_configuration` | Те же **имена** дисков и политик (пути могут отличаться) |
| `interserver_http_credentials`, `<secret>` кластера | Как на старых |
| `default_replica_path` / `default_replica_name` | Как на старых, если переопределены |
| users.xml / users.d, профили, квоты, system logs, merge_tree | Как на старых |

SQL-пользователи и роли (созданные через `CREATE USER`) хранятся локально на каждом хосте,
если не настроен replicated `user_directories`. `chsync check` покажет, кого не хватает на новых
хостах. Перенести: `SHOW ACCESS` на старом хосте → выполнить на новых (для показа хэшей паролей
нужны `display_secrets_in_show_and_select` + `format_display_secrets_in_show_and_select=1`).

### 1.5 Сеть

| Откуда → куда | Порт | Зачем |
|---|---|---|
| все 10 ↔ все 10 | 9000 (9440 TLS) | Distributed-запросы и вставки |
| реплики одного шарда | 9009 (interserver) | репликация частей |
| новые → Keeper | 2181 / 9181 | ZooKeeper/Keeper |
| jump-хост → все 10 | 8123 / 8443 | chsync, проверки |
| LB/chproxy → новые | 8123 / 9000 | на последнем шаге |

### 1.6 Keeper

Новые шарды добавят по одному набору znode на каждую Replicated-таблицу и столько же сессий/
watch. При тысячах таблиц проверьте запас Keeper по памяти и `znode_count`, снапшоты, latency.

### 1.7 Генеральная репетиция

`testlab/lab.sh` (README) — весь сценарий на одной машине. Плюс, если есть stage-кластер с той же
версией — полный прогон там.

## 2. Окно работ

| # | Действие | Где | Проверка | Откат |
|---|---|---|---|---|
| 0 | Бэкап конфигов: `tar czf /root/ch-conf-$(date +%F).tgz /etc/clickhouse-server /etc/metrika.xml 2>/dev/null` | все старые | архив есть | — |
| 1 | Запуск новых хостов: `systemctl enable --now clickhouse-server` | 4 новых | `chsync check`: 0 ошибок; новые видят 5 шардов и себя; старые — 3 («старый конфиг» — норма) | остановить новые |
| 2 | **DDL-freeze** объявлен. Убедиться, что очередь пуста: `SELECT count() FROM system.distributed_ddl_queue WHERE status != 'Finished'` | любой старый | 0 | — |
| 3 | `chsync plan` → ревью `plan.txt` и `*.sql`. BLOCKED = 0 (или осознанно `--allow-blocked`) | jump | create = ожидаемое, нет неожиданных DRIFT | — |
| 4 | `chsync apply --execute --targets ch-s4-r1.prod.local`, затем без `--targets` | jump | `verify: ... реплики в порядке`, failed=0 | `DROP DATABASE <db> SYNC` на новых (удаляет и их реплики в ZK) |
| 5 | `chsync diff` | jump | расхождений нет (кроме известного старого дрейфа) | — |
| 6 | `remote_servers.xml` (5 шардов, новые `weight=0`) на старые **по одному**: положить файл → `SYSTEM RELOAD CONFIG` → проверки справа → следующий | старые по 1 | см. блок «Проверка шага 6» ниже | вернуть старый файл + `SYSTEM RELOAD CONFIG` |
| 7 | `chsync check` + `chsync diff`. Если Kafka/RabbitMQ исключались на шаге 4 — убрать из `exclude_engines` и `chsync apply --execute` | jump | все 10 видят 5 шардов, топология одна, 0 ошибок | — |
| 8 | **Снять DDL-freeze.** Пробный DDL (ниже) | любой | ответили 10 хостов | — |
| 9 | Фаза 2 — запись в новые шарды (раздел 3). Можно отдельным окном | все 10 | раздел 4 | раздел 5 |
| 10 | Добавить новые хосты в LB / chproxy / клиентские конфиги | LB | запросы идут на новые | убрать |

Проверка шага 6 (на только что обновлённом хосте):

```sql
SELECT shard_num, shard_weight, groupArray(host_name) AS hosts
FROM system.clusters WHERE cluster = 'prod_cluster' GROUP BY 1, 2 ORDER BY 1;     -- 5 шардов, 4 и 5 с weight 0

SELECT count() FROM clusterAllReplicas('prod_cluster', system.one);               -- 10
SELECT count() FROM BD1.dev_table;      -- то же число, что до изменения (новые шарды пустые)
SELECT * FROM system.clusters WHERE cluster = 'prod_cluster' AND errors_count > 0; -- пусто
```

Пробный DDL (шаг 8) — что `ON CLUSTER` теперь доходит до всех 10:

```sql
CREATE TABLE default.chsync_probe ON CLUSTER prod_cluster (x UInt8) ENGINE = Null;  -- 10 строк в ответе
DROP TABLE default.chsync_probe ON CLUSTER prod_cluster SYNC;
```

Почему порядок именно такой: если раскатить `remote_servers` на старые хосты **до** создания
схемы на новых, Distributed-SELECT начнут ходить на шарды, где таблиц ещё нет (ошибки), а любой
`ON CLUSTER` DDL упадёт на новых хостах. Если создать схему, но не раскатить `remote_servers` —
`ON CLUSTER` с старых хостов не дойдёт до новых (отсюда DDL-freeze между шагами 3 и 8).

## 3. Фаза 2: запись в новые шарды и перебалансировка

ClickHouse не перераспределяет данные сам. Distributed выбирает шард как
`sharding_key % сумма_весов` → диапазон весов шарда. Отсюда три стратегии (их можно смешивать по
таблицам — `chsync check` выводит ключ шардирования каждой Distributed).

### A. Только новые данные (rand() или локальность ключа не важна)

Поднять `weight` новых шардов во всех 10 `remote_servers.xml` (по одному хосту + `SYSTEM RELOAD CONFIG`).

* `weight=1` у всех — новые данные равномерно на 5 шардов, старые остаются где были, перекос
  постепенно размывается (если есть TTL).
* Догоняющий режим: новым временно `weight=2..3`, когда объём сравняется (запрос в разделе 4) —
  вернуть всем 1. Доля записи в новые шарды = `2w / (3 + 2w)`: при w=2 — 57%, при w=3 — 67%.

### B. Перенос части старых партиций (rand(), нужно выровнять сразу)

Для таблиц, где не важно, на каком шарде лежит строка. Пример: партицию `202409` с шарда 1 на шард 4.

```sql
-- на ch-s4-r1: скачать части напрямую из ZK-пути шарда 1 (путь: system.replicas.zookeeper_path на шарде 1)
ALTER TABLE BD1.dev_table_p FETCH PARTITION '202409' FROM '/clickhouse/tables/<uuid>/01';
ALTER TABLE BD1.dev_table_p ATTACH PARTITION '202409';     -- реплицируется на ch-s4-r2
-- сверка: count()/sum() партиции на шарде 4 == на шарде 1
-- на ch-s1-r1 (реплицируется на ch-s1-r2):
ALTER TABLE BD1.dev_table_p DROP PARTITION '202409';
```

Проверено на стенде. Особенности: между ATTACH и DROP через Distributed видны дубли (делайте в
тихое время, партиция за партицией); MV при ATTACH **не** срабатывают — агрегаты по перенесённым
данным остаются на старом шарде (суммарно через Distributed всё сходится, переносить их не надо);
диск нового шарда должен вместить партицию.

### C. Решард (важна локальность ключа)

Если запросы полагаются на то, что все строки одного ключа на одном шарде —
`optimize_skip_unused_shards`, `distributed_group_by_no_merge`, JOIN/IN по локальным таблицам
(`distributed_product_mode=local`), дедупликация Replacing/Collapsing по ключу,
`uniqExact` по шардам — то любое `weight > 0` у новых шардов меняет `ключ % сумма_весов`
примерно для 80% ключей, и старые данные оказываются «не на своём» шарде → неверные результаты.
Варианты A и B для таких таблиц **не подходят**.

Схема решарда (по таблице, отдельный проект):

1. Новая пара таблиц на все 10 хостов (теперь `ON CLUSTER` доходит до всех):
   ```sql
   CREATE TABLE BD1.users_p_v2 ON CLUSTER prod_cluster AS BD1.users_p
     ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', ver) ORDER BY uid;
   CREATE TABLE BD1.users_v2 ON CLUSTER prod_cluster AS BD1.users
     ENGINE = Distributed(prod_cluster, BD1, users_p_v2, uid);
   ```
2. Двойная запись (приложение/MV пишет и в старую, и в `_v2`) **или** заморозка записи.
3. Переливка по партициям через Distributed (раскладывается по 5 шардам по новому правилу):
   ```sql
   INSERT INTO BD1.users_v2 SELECT * FROM BD1.users WHERE _partition_id = '...'
   SETTINGS distributed_foreground_insert = 1;
   ```
4. Сверка count/uniq/sum по партициям.
5. Переключение: `EXCHANGE TABLES BD1.users_p AND BD1.users_p_v2 ON CLUSTER prod_cluster;`
   (MV, которые пишут в таблицу, — пересоздать/проверить), затем удалить старую версию.

Нужен двойной объём диска на время переливки.

## 4. Мониторинг после включения записи

```sql
-- отложенные/ошибочные вставки Distributed (на старых хостах копятся пачки для новых шардов)
SELECT hostName(), database, table, data_path, is_blocked, error_count, data_files,
       formatReadableSize(data_compressed_bytes) AS pending, last_exception
FROM clusterAllReplicas('prod_cluster', system.distribution_queue)
WHERE data_files > 0 OR error_count > 0;

-- репликация
SELECT hostName(), database, table, queue_size, absolute_delay, is_readonly, active_replicas
FROM clusterAllReplicas('prod_cluster', system.replicas)
WHERE queue_size > 100 OR absolute_delay > 60 OR is_readonly OR active_replicas < total_replicas;

-- баланс объёма по шардам (cluster() = одна реплика на шард)
SELECT getMacro('shard') AS shard, formatReadableSize(sum(bytes_on_disk)) AS size, sum(rows) AS rows
FROM cluster('prod_cluster', system.parts)
WHERE active AND database NOT IN ('system')
GROUP BY shard ORDER BY shard;

-- ошибки соединений внутри кластера
SELECT host_name, shard_num, errors_count, slowdowns_count, estimated_recovery_time
FROM system.clusters WHERE cluster = 'prod_cluster' AND errors_count > 0;
```

Плюс `chsync diff` по cron (exit 2 = схема разъехалась) — как постоянная проверка «везде одинаково».

## 5. Откат

| Момент | Как откатить | Потери |
|---|---|---|
| До шага 6 | Старые хосты не тронуты. На новых: `DROP DATABASE <db> SYNC` для каждой БД (чистит их реплики в ZK) или переустановить; если хосты уже выключены — `SYSTEM DROP REPLICA '<replica>' FROM ZKPATH '<path>'` с любого живого хоста | нет |
| Шаги 6–8 (веса 0) | Вернуть старый `remote_servers.xml` на старые хосты + `SYSTEM RELOAD CONFIG`. Новые — как выше | нет (если Kafka-таблицы на новых не создавались) |
| После шага 9 | `weight=0` новым (стоп новой записи) → перенос данных обратно (FETCH PARTITION на старый шард, как в 3.B, в обратную сторону) → удалить шарды из конфига | время на перенос |

Шаг 9 — точка, после которой откат требует переноса данных. Поэтому он отдельным решением.

## 6. Troubleshooting

| Симптом | Причина / действие |
|---|---|
| chsync BLOCKED: «совпадает с путём на источнике … коллизия макроса шарда» | На новом хосте `shard` = старому. Исправить macros.xml **до** создания таблиц, рестарт |
| chsync BLOCKED: «в ZK уже есть реплика … а таблицы на хосте нет» | Остаток прошлой попытки. Выполнить предложенный `SYSTEM DROP REPLICA ... FROM ZKPATH ...` |
| chsync BLOCKED: «ZK-путь уже занят чужими репликами» | Узлы в ZK от выведенного шарда с тем же номером. Разобраться, удалить через `SYSTEM DROP REPLICA` |
| chsync BLOCKED: «без макроса шарда» | Глобальная таблица — решить `ddl.global_replicated` |
| chsync BLOCKED: `[HIDDEN]` | Словарь/движок с паролем. `ddl.display_secrets=true` + настройка сервера, или создать вручную |
| `Mapping for table with UUID=… already exists` | Таблицу дропнули без `SYNC`, Atomic держит UUID ~8 минут. `DROP TABLE … SYNC` и повторить |
| `Replica … already exists` при apply | См. «в ZK уже есть реплика» |
| Реплика `is_readonly=1` | Нет связи с Keeper / нет метаданных. Логи сервера, `system.zookeeper_connection` |
| `distribution_queue.error_count` растёт | Старые хосты не могут вставить в новые: сеть 9000, `<secret>`/пользователь кластера, нет таблицы |
| DRIFT при одинаковой схеме | Разные версии ClickHouse форматируют DDL по-разному — выровнять версии |

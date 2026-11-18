-- Тестовая схема для chsync. Выполняется на ch-s1-r1 ON CLUSTER lab_cluster:
-- старые узлы видят 2 шарда, поэтому объекты появляются только на старом кластере (ch-s1-*, ch-s2-*).
-- Replicated*-таблицы создаются БЕЗ аргументов движка — путь из default_replica_path, как на проде.

SET distributed_ddl_output_mode = 'none';   -- не печатать таблицу хостов на каждый ON CLUSTER (ошибки всё равно видны)

-- ═══════════════ Базы данных ═══════════════
CREATE DATABASE IF NOT EXISTS shop       ON CLUSTER lab_cluster;                         -- основная (Atomic)
CREATE DATABASE IF NOT EXISTS analytics  ON CLUSTER lab_cluster;
CREATE DATABASE IF NOT EXISTS empty_db   ON CLUSTER lab_cluster;                         -- пустая БД
CREATE DATABASE IF NOT EXISTS staging    ON CLUSTER lab_cluster COMMENT 'пустая БД с комментарием';
CREATE DATABASE IF NOT EXISTS archive    ON CLUSTER lab_cluster ENGINE = Atomic;          -- только Log-таблицы
CREATE DATABASE IF NOT EXISTS repl_db    ON CLUSTER lab_cluster
    ENGINE = Replicated('/clickhouse/databases/repl_db', '{shard}', '{replica}');       -- таблицами управляет сама БД
CREATE DATABASE IF NOT EXISTS test       ON CLUSTER lab_cluster;                         -- исключается фильтром
CREATE DATABASE IF NOT EXISTS BMS_Legacy ON CLUSTER lab_cluster;                         -- исключается фильтром (bms)

-- ═══════════════ SQL-функции ═══════════════
CREATE FUNCTION IF NOT EXISTS normalize_email ON CLUSTER lab_cluster AS (e) -> lower(trimBoth(e));
CREATE FUNCTION IF NOT EXISTS revenue_bucket  ON CLUSTER lab_cluster AS (x) -> multiIf(x < 100, 'small', x < 1000, 'medium', 'large');

-- ═══════════════ shop: Replicated + Distributed ═══════════════
-- Заказы: партиции, TTL, кодеки, skip-индекс, проекция, storage_policy
CREATE TABLE IF NOT EXISTS shop.orders_p ON CLUSTER lab_cluster
(
    order_id    UInt64,
    customer_id UInt64,
    ts          DateTime CODEC(Delta, ZSTD(3)),
    amount      Decimal(18, 2),
    status      LowCardinality(String),
    comment     String DEFAULT '',
    INDEX idx_status status TYPE set(16) GRANULARITY 4,
    PROJECTION by_status (SELECT status, count(), sum(amount) GROUP BY status)
)
ENGINE = ReplicatedMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (customer_id, ts, order_id)
TTL ts + INTERVAL 3 YEAR
SETTINGS storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS shop.orders ON CLUSTER lab_cluster AS shop.orders_p
ENGINE = Distributed('{cluster}', 'shop', 'orders_p', cityHash64(customer_id));

-- Клиенты: Replacing по версии, DEFAULT через SQL-функцию
CREATE TABLE IF NOT EXISTS shop.customers_p ON CLUSTER lab_cluster
(
    id         UInt64,
    email      String,
    email_norm String DEFAULT normalize_email(email),
    name       String,
    ver        UInt32
)
ENGINE = ReplicatedReplacingMergeTree(ver)
ORDER BY id;

CREATE TABLE IF NOT EXISTS shop.customers ON CLUSTER lab_cluster AS shop.customers_p
ENGINE = Distributed('{cluster}', 'shop', 'customers_p', id);

-- Справочник товаров: «глобальная» таблица, путь БЕЗ {shard} — полная копия на каждом узле
-- (chsync по умолчанию ставит BLOCKED: решение за DBA, ddl.global_replicated)
CREATE TABLE IF NOT EXISTS shop.products_p ON CLUSTER lab_cluster
(
    product_id UInt64,
    name       String,
    price      Decimal(18, 2)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/all/{database}/{table}', '{replica}')
ORDER BY product_id;

CREATE DICTIONARY IF NOT EXISTS shop.products_dict ON CLUSTER lab_cluster
(
    product_id UInt64,
    name       String,
    price      Decimal(18, 2)
)
PRIMARY KEY product_id
SOURCE(CLICKHOUSE(DB 'shop' TABLE 'products_p'))
LIFETIME(MIN 60 MAX 300)
LAYOUT(HASHED());

-- Позиции заказа: DEFAULT через dictGet (зависимость таблицы от словаря)
CREATE TABLE IF NOT EXISTS shop.order_items_p ON CLUSTER lab_cluster
(
    order_id     UInt64,
    product_id   UInt64,
    qty          UInt32,
    product_name String DEFAULT dictGetOrDefault('shop.products_dict', 'name', product_id, '')
)
ENGINE = ReplicatedMergeTree
ORDER BY (order_id, product_id);

CREATE TABLE IF NOT EXISTS shop.order_items ON CLUSTER lab_cluster AS shop.order_items_p
ENGINE = Distributed('{cluster}', 'shop', 'order_items_p', rand());

-- Агрегаты: MV TO SummingMergeTree
CREATE TABLE IF NOT EXISTS shop.daily_revenue_p ON CLUSTER lab_cluster
(
    day         Date,
    customer_id UInt64,
    orders      UInt64,
    revenue     Decimal(38, 2)
)
ENGINE = ReplicatedSummingMergeTree
ORDER BY (day, customer_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS shop.daily_revenue_mv ON CLUSTER lab_cluster TO shop.daily_revenue_p AS
SELECT toDate(ts) AS day, customer_id, count() AS orders, sum(amount) AS revenue
FROM shop.orders_p
GROUP BY day, customer_id;

CREATE TABLE IF NOT EXISTS shop.daily_revenue ON CLUSTER lab_cluster AS shop.daily_revenue_p
ENGINE = Distributed('{cluster}', 'shop', 'daily_revenue_p', customer_id);

-- MV с inner-движком (без TO): реплицируется inner-таблица .inner_id.<uuid MV>
CREATE MATERIALIZED VIEW IF NOT EXISTS shop.hourly_status_mv ON CLUSTER lab_cluster
ENGINE = ReplicatedAggregatingMergeTree
ORDER BY (hour, status)
AS SELECT toStartOfHour(ts) AS hour, status, countState() AS cnt, sumState(amount) AS total
FROM shop.orders_p
GROUP BY hour, status;

-- Переименованная таблица: ZK-путь остаётся со старым именем (legacy_payments_p)
CREATE TABLE IF NOT EXISTS shop.legacy_payments_p ON CLUSTER lab_cluster
(
    payment_id UInt64,
    order_id   UInt64,
    paid_at    DateTime,
    amount     Decimal(18, 2)
)
ENGINE = ReplicatedMergeTree
ORDER BY payment_id;
RENAME TABLE shop.legacy_payments_p TO shop.payments_p ON CLUSTER lab_cluster;

CREATE TABLE IF NOT EXISTS shop.payments ON CLUSTER lab_cluster AS shop.payments_p
ENGINE = Distributed('{cluster}', 'shop', 'payments_p', rand());

-- Архив заказов + Merge поверх локальных таблиц
CREATE TABLE IF NOT EXISTS shop.orders_archive_p ON CLUSTER lab_cluster AS shop.orders_p
ENGINE = ReplicatedMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (customer_id, ts, order_id);

CREATE TABLE IF NOT EXISTS shop.orders_all_local ON CLUSTER lab_cluster AS shop.orders_p
ENGINE = Merge('shop', '^orders(_archive)?_p$');

-- Buffer перед локальной таблицей
CREATE TABLE IF NOT EXISTS shop.orders_buffer ON CLUSTER lab_cluster AS shop.orders_p
ENGINE = Buffer('shop', 'orders_p', 4, 10, 60, 1000, 100000, 1000000, 10000000);

-- ═══════════════ shop: нереплицируемые движки ═══════════════
CREATE TABLE IF NOT EXISTS shop.local_cache ON CLUSTER lab_cluster
(
    key        String,
    value      String,
    updated_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY key
TTL updated_at + INTERVAL 7 DAY;

-- Distributed над НЕреплицируемой MergeTree (check предупредит: данные будут только на одной реплике шарда)
CREATE TABLE IF NOT EXISTS shop.local_cache_all ON CLUSTER lab_cluster AS shop.local_cache
ENGINE = Distributed('{cluster}', 'shop', 'local_cache', cityHash64(key));

CREATE TABLE IF NOT EXISTS shop.event_log   ON CLUSTER lab_cluster (ts DateTime, msg String) ENGINE = Log;
CREATE TABLE IF NOT EXISTS shop.tmp_session ON CLUSTER lab_cluster (sid String, data String) ENGINE = Memory;
CREATE TABLE IF NOT EXISTS shop.sink        ON CLUSTER lab_cluster (id UInt64, payload String) ENGINE = Null;
CREATE TABLE IF NOT EXISTS shop.vip_ids     ON CLUSTER lab_cluster (id UInt64) ENGINE = Set;
CREATE TABLE IF NOT EXISTS shop.customer_join ON CLUSTER lab_cluster (id UInt64, segment String) ENGINE = Join(ANY, LEFT, id);
CREATE TABLE IF NOT EXISTS shop.replacing_local ON CLUSTER lab_cluster (id UInt64, v UInt32) ENGINE = ReplacingMergeTree(v) ORDER BY id;

-- ═══════════════ shop: представления ═══════════════
CREATE VIEW IF NOT EXISTS shop.v_top_customers ON CLUSTER lab_cluster AS
SELECT customer_id, sum(amount) AS total, revenue_bucket(toFloat64(sum(amount))) AS bucket
FROM shop.orders
GROUP BY customer_id
ORDER BY total DESC
LIMIT 100;

-- Параметризованное представление: SELECT * FROM shop.v_customer_orders(cid = 42)
CREATE VIEW IF NOT EXISTS shop.v_customer_orders ON CLUSTER lab_cluster AS
SELECT order_id, ts, amount, status FROM shop.orders WHERE customer_id = {cid:UInt64};

-- VIEW над VIEW
CREATE VIEW IF NOT EXISTS shop.v_top10 ON CLUSTER lab_cluster AS
SELECT * FROM shop.v_top_customers LIMIT 10;

-- ═══════════════ analytics ═══════════════
CREATE TABLE IF NOT EXISTS analytics.sessions_p ON CLUSTER lab_cluster
(
    session_id UUID,
    user_id    UInt64,
    started    DateTime,
    duration   UInt32,
    sign       Int8
)
ENGINE = ReplicatedCollapsingMergeTree(sign)
PARTITION BY toYYYYMM(started)
ORDER BY (user_id, session_id);

CREATE TABLE IF NOT EXISTS analytics.sessions ON CLUSTER lab_cluster AS analytics.sessions_p
ENGINE = Distributed('{cluster}', 'analytics', 'sessions_p', user_id);

-- Явный путь с {uuid}: обе реплики нового шарда должны получить один и тот же UUID (preserve_uuid)
CREATE TABLE IF NOT EXISTS analytics.page_views_p ON CLUSTER lab_cluster
(
    ts      DateTime,
    user_id UInt64,
    url     String,
    tags    Array(String),
    attrs   Map(String, String)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toDate(ts)
ORDER BY (user_id, ts);

CREATE TABLE IF NOT EXISTS analytics.page_views ON CLUSTER lab_cluster AS analytics.page_views_p
ENGINE = Distributed('{cluster}', 'analytics', 'page_views_p', user_id);

CREATE VIEW IF NOT EXISTS analytics.v_daily_users ON CLUSTER lab_cluster AS
SELECT toDate(started) AS day, uniqExact(user_id) AS users FROM analytics.sessions GROUP BY day;

-- ═══════════════ Прочие БД ═══════════════
CREATE TABLE IF NOT EXISTS archive.old_events ON CLUSTER lab_cluster (ts DateTime, msg String) ENGINE = Log;

-- В БД с движком Replicated таблицы создаются без ON CLUSTER — их раскладывает сама БД
CREATE TABLE IF NOT EXISTS repl_db.kv (k String, v String) ENGINE = ReplicatedMergeTree ORDER BY k;

CREATE TABLE IF NOT EXISTS test.scratch ON CLUSTER lab_cluster (id UInt64) ENGINE = MergeTree ORDER BY id;

-- ═══════════════ Объекты с «bms» — chsync их пропускает (и всё, что на них ссылается) ═══════════════
CREATE TABLE IF NOT EXISTS shop.bms_sync_p ON CLUSTER lab_cluster (id UInt64, payload String) ENGINE = ReplicatedMergeTree ORDER BY id;
CREATE MATERIALIZED VIEW IF NOT EXISTS shop.sync_to_sink_mv ON CLUSTER lab_cluster TO shop.sink AS
SELECT id, payload FROM shop.bms_sync_p;                                        -- каскадно исключается
CREATE TABLE IF NOT EXISTS BMS_Legacy.raw ON CLUSTER lab_cluster (id UInt64) ENGINE = MergeTree ORDER BY id;

-- Тестовая схема для стенда БЕЗ Keeper (LAB=nokeeper). Выполняется на КАЖДОМ из 4 старых узлов по очереди:
-- без ZooKeeper нет distributed DDL, поэтому ON CLUSTER не работает (так же живёт и прод-кластер без Keeper).
-- Таблицы — MergeTree-семейство, у каждой «локальной» таблицы свой Distributed; копии на обе реплики шарда
-- пишет Distributed (internal_replication = false в remote_servers).

-- ═══════════════ Базы данных ═══════════════
CREATE DATABASE IF NOT EXISTS shop;
CREATE DATABASE IF NOT EXISTS analytics;
CREATE DATABASE IF NOT EXISTS empty_db;                                   -- пустая БД
CREATE DATABASE IF NOT EXISTS staging COMMENT 'пустая БД с комментарием';
CREATE DATABASE IF NOT EXISTS archive;
CREATE DATABASE IF NOT EXISTS test;                                       -- исключается фильтром
CREATE DATABASE IF NOT EXISTS BMS_Legacy;                                 -- исключается фильтром (bms)

-- ═══════════════ SQL-функции ═══════════════
CREATE FUNCTION IF NOT EXISTS normalize_email AS (e) -> lower(trimBoth(e));
CREATE FUNCTION IF NOT EXISTS revenue_bucket  AS (x) -> multiIf(x < 100, 'small', x < 1000, 'medium', 'large');

-- ═══════════════ shop: MergeTree + Distributed ═══════════════
-- Заказы: партиции, TTL, кодеки, skip-индекс, проекция, storage_policy
CREATE TABLE IF NOT EXISTS shop.orders_local
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
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (customer_id, ts, order_id)
TTL ts + INTERVAL 3 YEAR
SETTINGS storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS shop.orders AS shop.orders_local
ENGINE = Distributed('{cluster}', 'shop', 'orders_local', cityHash64(customer_id));

-- Клиенты: Replacing по версии, DEFAULT через SQL-функцию
CREATE TABLE IF NOT EXISTS shop.customers_local
(
    id         UInt64,
    email      String,
    email_norm String DEFAULT normalize_email(email),
    name       String,
    ver        UInt32
)
ENGINE = ReplacingMergeTree(ver)
ORDER BY id;

CREATE TABLE IF NOT EXISTS shop.customers AS shop.customers_local
ENGINE = Distributed('lab_cluster', 'shop', 'customers_local', id);       -- имя кластера литералом, без макроса

-- Справочник товаров: полная копия на каждом узле (заливается на все узлы), словарь поверх него
CREATE TABLE IF NOT EXISTS shop.products
(
    product_id UInt64,
    name       String,
    price      Decimal(18, 2)
)
ENGINE = MergeTree
ORDER BY product_id;

CREATE DICTIONARY IF NOT EXISTS shop.products_dict
(
    product_id UInt64,
    name       String,
    price      Decimal(18, 2)
)
PRIMARY KEY product_id
SOURCE(CLICKHOUSE(DB 'shop' TABLE 'products'))
LIFETIME(MIN 60 MAX 300)
LAYOUT(HASHED());

-- Позиции заказа: DEFAULT через dictGet (зависимость таблицы от словаря)
CREATE TABLE IF NOT EXISTS shop.order_items_local
(
    order_id     UInt64,
    product_id   UInt64,
    qty          UInt32,
    product_name String DEFAULT dictGetOrDefault('shop.products_dict', 'name', product_id, '')
)
ENGINE = MergeTree
ORDER BY (order_id, product_id);

CREATE TABLE IF NOT EXISTS shop.order_items AS shop.order_items_local
ENGINE = Distributed('{cluster}', 'shop', 'order_items_local', rand());

-- Агрегаты: MV TO SummingMergeTree (MV срабатывает на каждой реплике — Distributed вставляет в каждую)
CREATE TABLE IF NOT EXISTS shop.daily_revenue_local
(
    day         Date,
    customer_id UInt64,
    orders      UInt64,
    revenue     Decimal(38, 2)
)
ENGINE = SummingMergeTree
ORDER BY (day, customer_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS shop.daily_revenue_mv TO shop.daily_revenue_local AS
SELECT toDate(ts) AS day, customer_id, count() AS orders, sum(amount) AS revenue
FROM shop.orders_local
GROUP BY day, customer_id;

CREATE TABLE IF NOT EXISTS shop.daily_revenue AS shop.daily_revenue_local
ENGINE = Distributed('{cluster}', 'shop', 'daily_revenue_local', customer_id);

-- MV с inner-движком AggregatingMergeTree
CREATE MATERIALIZED VIEW IF NOT EXISTS shop.hourly_status_mv
ENGINE = AggregatingMergeTree
ORDER BY (hour, status)
AS SELECT toStartOfHour(ts) AS hour, status, countState() AS cnt, sumState(amount) AS total
FROM shop.orders_local
GROUP BY hour, status;

-- Архив + Merge поверх локальных таблиц, Buffer перед локальной таблицей
CREATE TABLE IF NOT EXISTS shop.orders_archive_local AS shop.orders_local
ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY (customer_id, ts, order_id);
CREATE TABLE IF NOT EXISTS shop.orders_all_local AS shop.orders_local
ENGINE = Merge('shop', '^orders(_archive)?_local$');
CREATE TABLE IF NOT EXISTS shop.orders_buffer AS shop.orders_local
ENGINE = Buffer('shop', 'orders_local', 4, 10, 60, 1000, 100000, 1000000, 10000000);

-- Прочие движки
CREATE TABLE IF NOT EXISTS shop.event_log   (ts DateTime, msg String) ENGINE = Log;
CREATE TABLE IF NOT EXISTS shop.tmp_session (sid String, data String) ENGINE = Memory;
CREATE TABLE IF NOT EXISTS shop.sink        (id UInt64, payload String) ENGINE = Null;
CREATE TABLE IF NOT EXISTS shop.vip_ids     (id UInt64) ENGINE = Set;
CREATE TABLE IF NOT EXISTS shop.customer_join (id UInt64, segment String) ENGINE = Join(ANY, LEFT, id);

-- ═══════════════ shop: представления ═══════════════
CREATE VIEW IF NOT EXISTS shop.v_top_customers AS
SELECT customer_id, sum(amount) AS total, revenue_bucket(toFloat64(sum(amount))) AS bucket
FROM shop.orders GROUP BY customer_id ORDER BY total DESC LIMIT 100;
CREATE VIEW IF NOT EXISTS shop.v_customer_orders AS
SELECT order_id, ts, amount, status FROM shop.orders WHERE customer_id = {cid:UInt64};
CREATE VIEW IF NOT EXISTS shop.v_top10 AS SELECT * FROM shop.v_top_customers LIMIT 10;

-- ═══════════════ analytics ═══════════════
CREATE TABLE IF NOT EXISTS analytics.sessions_local
(
    session_id UUID,
    user_id    UInt64,
    started    DateTime,
    duration   UInt32,
    sign       Int8
)
ENGINE = CollapsingMergeTree(sign)
PARTITION BY toYYYYMM(started)
ORDER BY (user_id, session_id);
CREATE TABLE IF NOT EXISTS analytics.sessions AS analytics.sessions_local
ENGINE = Distributed('{cluster}', 'analytics', 'sessions_local', user_id);

CREATE TABLE IF NOT EXISTS analytics.page_views_local
(
    ts      DateTime,
    user_id UInt64,
    url     String,
    tags    Array(String),
    attrs   Map(String, String)
)
ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY (user_id, ts);
CREATE TABLE IF NOT EXISTS analytics.page_views AS analytics.page_views_local
ENGINE = Distributed('{cluster}', 'analytics', 'page_views_local', user_id);

CREATE VIEW IF NOT EXISTS analytics.v_daily_users AS
SELECT toDate(started) AS day, uniqExact(user_id) AS users FROM analytics.sessions GROUP BY day;

-- ═══════════════ Прочие БД ═══════════════
CREATE TABLE IF NOT EXISTS archive.old_events (ts DateTime, msg String) ENGINE = Log;
CREATE TABLE IF NOT EXISTS test.scratch (id UInt64) ENGINE = MergeTree ORDER BY id;

-- ═══════════════ Объекты с «bms» — chsync их пропускает (и всё, что на них ссылается) ═══════════════
CREATE TABLE IF NOT EXISTS shop.bms_sync_local (id UInt64, payload String) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW IF NOT EXISTS shop.sync_to_sink_mv TO shop.sink AS SELECT id, payload FROM shop.bms_sync_local;
CREATE TABLE IF NOT EXISTS BMS_Legacy.raw (id UInt64) ENGINE = MergeTree ORDER BY id;

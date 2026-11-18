-- Данные для стенда без Keeper. Вставка через Distributed с синхронной доставкой: при internal_replication = false
-- Distributed сам пишет копию в КАЖДУЮ реплику шарда.
SET distributed_foreground_insert = 1;

-- Справочник shop.products (полная копия на каждом узле) lab.sh заливает на каждый узел отдельно.

INSERT INTO shop.customers (id, email, name, ver)
SELECT number, concat('  User', toString(number), '@Example.com '), concat('name-', toString(number)), 1 FROM numbers(1000);

INSERT INTO shop.orders (order_id, customer_id, ts, amount, status)
SELECT number, number % 1000, now() - toIntervalMinute(number), round((rand() % 500000) / 100, 2),
       ['new', 'paid', 'shipped', 'cancelled'][number % 4 + 1]
FROM numbers(50000);

INSERT INTO shop.order_items (order_id, product_id, qty) SELECT number % 50000, number % 200, 1 + number % 5 FROM numbers(100000);
INSERT INTO analytics.sessions SELECT generateUUIDv4(), number % 5000, now() - toIntervalSecond(number * 7), number % 3600, 1 FROM numbers(30000);
INSERT INTO analytics.page_views SELECT now() - toIntervalSecond(number), number % 5000, concat('/page/', toString(number % 50)),
       ['a', 'b'], map('ref', 'google') FROM numbers(50000);

-- Тестовые данные. Вставка через Distributed с синхронной доставкой — данные сразу раскладываются по шардам.
SET distributed_foreground_insert = 1;

INSERT INTO shop.products_p SELECT number, concat('product-', toString(number)), round(10 + rand() % 990, 2) FROM numbers(200);
SYSTEM RELOAD DICTIONARY shop.products_dict;

INSERT INTO shop.customers (id, email, name, ver)
SELECT number, concat('  User', toString(number), '@Example.com '), concat('name-', toString(number)), 1 FROM numbers(1000);

INSERT INTO shop.orders (order_id, customer_id, ts, amount, status)
SELECT number, number % 1000, now() - toIntervalMinute(number), round((rand() % 500000) / 100, 2),
       ['new', 'paid', 'shipped', 'cancelled'][number % 4 + 1]
FROM numbers(50000);

INSERT INTO shop.order_items (order_id, product_id, qty) SELECT number % 50000, number % 200, 1 + number % 5 FROM numbers(100000);
INSERT INTO shop.payments SELECT number, number, now() - toIntervalMinute(number), round((rand() % 500000) / 100, 2) FROM numbers(20000);
INSERT INTO shop.local_cache_all (key, value) SELECT toString(number), 'v' FROM numbers(1000);

INSERT INTO analytics.sessions SELECT generateUUIDv4(), number % 5000, now() - toIntervalSecond(number * 7), number % 3600, 1 FROM numbers(30000);
INSERT INTO analytics.page_views SELECT now() - toIntervalSecond(number), number % 5000, concat('/page/', toString(number % 50)),
       ['a', 'b'], map('ref', 'google') FROM numbers(50000);

INSERT INTO repl_db.kv VALUES ('version', '1'), ('owner', 'chsync-lab');

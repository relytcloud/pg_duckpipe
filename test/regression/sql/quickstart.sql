-- Verify the doc/QUICKSTART.md examples work end-to-end
SET client_min_messages = warning;
SELECT duckpipe.start_worker();
RESET client_min_messages;

-------------------------------------------------------
-- Section 1: Add Your First Table
-------------------------------------------------------
CREATE TABLE orders (
    id    BIGSERIAL PRIMARY KEY,
    customer TEXT,
    total    INT
);

INSERT INTO orders(customer, total) VALUES
    ('alice', 100),
    ('bob',   250),
    ('carol', 75);

SELECT duckpipe.add_table('public.orders');

SELECT pg_sleep(3);

-- Check sync state
SELECT source_table, target_table, state, rows_synced
FROM duckpipe.status()
WHERE source_table = 'public.orders';

-- Query the columnar table
SELECT * FROM orders_ducklake ORDER BY id;

-- Live DML streaming
INSERT INTO orders(customer, total) VALUES ('dave', 300);
UPDATE orders SET total = 500 WHERE customer = 'alice';
DELETE FROM orders WHERE customer = 'bob';

SELECT pg_sleep(3);

SELECT * FROM orders_ducklake ORDER BY id;

-------------------------------------------------------
-- Section 2: Remove a Table
-------------------------------------------------------
SELECT duckpipe.remove_table('public.orders');

-- Verify removal from status
SELECT count(*) AS remaining FROM duckpipe.status()
WHERE source_table = 'public.orders';

-------------------------------------------------------
-- Section 3: Re-Add a Table
-------------------------------------------------------
SELECT duckpipe.add_table('public.orders');

SELECT pg_sleep(3);

SELECT source_table, state, rows_synced
FROM duckpipe.status()
WHERE source_table = 'public.orders';

-- Target should have current source data (3 rows after earlier DML)
SELECT * FROM orders_ducklake ORDER BY id;

-- Clean up orders
SELECT duckpipe.remove_table('public.orders', drop_target => true);
DROP TABLE orders;

-------------------------------------------------------
-- Section 4: Add Multiple Tables
-------------------------------------------------------
CREATE TABLE orders (
    id    BIGSERIAL PRIMARY KEY,
    customer TEXT,
    total    INT
);

CREATE TABLE customers (
    id   BIGSERIAL PRIMARY KEY,
    name TEXT,
    email TEXT
);

CREATE TABLE products (
    id    BIGSERIAL PRIMARY KEY,
    name  TEXT,
    price INT
);

SELECT duckpipe.add_table('public.orders');
SELECT duckpipe.add_table('public.customers');
SELECT duckpipe.add_table('public.products');

SELECT pg_sleep(3);

SELECT source_table, target_table, sync_group, state
FROM duckpipe.status()
ORDER BY source_table;

-- Clean up
SELECT duckpipe.remove_table('public.orders', drop_target => true);
SELECT duckpipe.remove_table('public.customers', drop_target => true);
SELECT duckpipe.remove_table('public.products', drop_target => true);
DROP TABLE orders;
DROP TABLE customers;
DROP TABLE products;

-------------------------------------------------------
-- Section 5: Resync a Table
-------------------------------------------------------
CREATE TABLE orders (
    id    BIGSERIAL PRIMARY KEY,
    customer TEXT,
    total    INT
);

INSERT INTO orders(customer, total) VALUES ('alice', 100), ('bob', 250);

SELECT duckpipe.add_table('public.orders');

SELECT pg_sleep(3);

-- Verify initial snapshot
SELECT * FROM orders_ducklake ORDER BY id;

-- Add more rows, then resync
INSERT INTO orders(customer, total) VALUES ('carol', 75);

SELECT pg_sleep(2);

SELECT duckpipe.resync_table('public.orders');

SELECT pg_sleep(3);

-- After resync, target should have all 3 rows
SELECT source_table, state FROM duckpipe.status()
WHERE source_table = 'public.orders';

SELECT * FROM orders_ducklake ORDER BY id;

-- Clean up
SELECT duckpipe.remove_table('public.orders', drop_target => true);
DROP TABLE orders;

-------------------------------------------------------
-- Section 6: Monitor Sync Status
-------------------------------------------------------
CREATE TABLE orders (
    id    BIGSERIAL PRIMARY KEY,
    customer TEXT,
    total    INT
);

INSERT INTO orders(customer, total) VALUES ('alice', 100);

SELECT duckpipe.add_table('public.orders');

SELECT pg_sleep(3);

-- Per-table status
SELECT source_table, state, rows_synced, queued_changes
FROM duckpipe.status();

-- Group overview
SELECT name, enabled, table_count
FROM duckpipe.groups();

-- Worker health
SELECT is_backpressured
FROM duckpipe.worker_status();

-- Clean up
SELECT duckpipe.remove_table('public.orders', drop_target => true);
DROP TABLE orders;

SET client_min_messages = warning;
SELECT duckpipe.stop_worker();
RESET client_min_messages;

-- Fan-in streaming: multiple sources syncing into a single target table.
--
-- Two local groups (shard_a, shard_b) each with their own source table
-- (orders_a, orders_b) mapping to the same DuckLake target (orders_ducklake).
-- Verifies fan-in guard, schema validation, DML isolation, TRUNCATE, resync, remove.

SELECT duckpipe.start_worker();

-- Create two groups representing shards
SELECT duckpipe.create_group('shard_a');
SELECT duckpipe.create_group('shard_b');

-- Create source tables with identical schemas (simulating sharded tables)
CREATE TABLE orders_a (id int primary key, product text, qty int);
CREATE TABLE orders_b (id int primary key, product text, qty int);

-- Create a table with mismatched schema for validation test
CREATE TABLE orders_bad (id int primary key, product text, price numeric);

-- =============================================================
-- Test 1: First source adds normally
-- =============================================================
SELECT duckpipe.add_table('public.orders_a', 'public.orders_ducklake', 'shard_a', false);

-- =============================================================
-- Test 2: Accidental fan-in prevention (no fan_in => true)
-- =============================================================
SELECT duckpipe.add_table('public.orders_b', 'public.orders_ducklake', 'shard_b', false);

-- =============================================================
-- Test 3: Schema mismatch with fan_in => true
-- =============================================================
SELECT duckpipe.add_table('public.orders_bad', 'public.orders_ducklake', 'shard_b', false, true);

-- =============================================================
-- Test 4: Successful fan-in with fan_in => true
-- =============================================================
SELECT duckpipe.add_table('public.orders_b', 'public.orders_ducklake', 'shard_b', false, true);

-- =============================================================
-- Test 5: tables() shows source_count = 2 and source_label
-- =============================================================
SELECT source_table, target_table, sync_group, source_label, source_count
FROM duckpipe.tables()
WHERE target_table = 'public.orders_ducklake'
ORDER BY source_table;

-- =============================================================
-- Test 6: status() shows source_label for each mapping
-- =============================================================
SELECT source_table, sync_group, source_label
FROM duckpipe.status()
WHERE source_table IN ('public.orders_a', 'public.orders_b')
ORDER BY source_table;

-- =============================================================
-- Test 7: INSERT into both sources → verify all rows with correct _duckpipe_source
-- =============================================================
INSERT INTO orders_a VALUES (1, 'widget', 10), (2, 'gadget', 5);
INSERT INTO orders_b VALUES (101, 'sprocket', 20), (102, 'bolt', 50);

-- Longer sleep for initial replication setup across multiple groups
SELECT pg_sleep(8);

SELECT * FROM public.orders_ducklake ORDER BY id;

-- =============================================================
-- Test 8: UPDATE in one source → only that source's rows changed
-- =============================================================
UPDATE orders_a SET qty = 99 WHERE id = 1;

SELECT pg_sleep(3);

SELECT * FROM public.orders_ducklake ORDER BY id;

-- =============================================================
-- Test 9: DELETE in one source → other source unaffected
-- =============================================================
DELETE FROM orders_b WHERE id = 101;

SELECT pg_sleep(4);

SELECT * FROM public.orders_ducklake ORDER BY id;

-- =============================================================
-- Test 10: TRUNCATE one source → only that source's rows removed
-- =============================================================
TRUNCATE orders_a;

SELECT pg_sleep(3);

SELECT * FROM public.orders_ducklake ORDER BY id;

-- Re-insert into orders_a for subsequent tests
INSERT INTO orders_a VALUES (3, 'nut', 30);

SELECT pg_sleep(3);

SELECT * FROM public.orders_ducklake ORDER BY id;

-- =============================================================
-- Test 11: resync_table() one source → re-snapshot only that source
-- =============================================================
-- First add more data to orders_a so resync picks it up
INSERT INTO orders_a VALUES (4, 'washer', 40);

-- Wait for streaming to flush the new row before triggering resync,
-- to avoid a race between the snapshot DELETE and in-flight streaming flush.
SELECT pg_sleep(3);

-- Resync shard_a — should only delete and re-snapshot shard_a's rows
SELECT duckpipe.resync_table('public.orders_a', 'shard_a');

SELECT pg_sleep(5);

-- shard_a rows should match current orders_a; shard_b rows unchanged
SELECT * FROM public.orders_ducklake ORDER BY id;

-- =============================================================
-- Test 12: remove_table() with sync_group parameter
-- =============================================================
-- Remove shard_a's table (specifying group)
SET client_min_messages = warning;
SELECT duckpipe.stop_worker();
RESET client_min_messages;

SELECT duckpipe.remove_table('public.orders_a', false, 'shard_a');

-- Restart worker — shard_b should keep working
SELECT duckpipe.start_worker();

INSERT INTO orders_b VALUES (103, 'rivet', 15);

-- Longer sleep for worker restart + replication reconnect
SELECT pg_sleep(6);

-- shard_b rows should be present (shard_a rows may remain as orphaned data)
SELECT id, product, qty, _duckpipe_source FROM public.orders_ducklake
WHERE _duckpipe_source = 'shard_b/public.orders_b' ORDER BY id;

-- =============================================================
-- Cleanup
-- =============================================================
SET client_min_messages = warning;
SELECT duckpipe.stop_worker();
RESET client_min_messages;

SELECT duckpipe.remove_table('public.orders_b', false, 'shard_b');

SELECT duckpipe.drop_group('shard_a');
SELECT duckpipe.drop_group('shard_b');

DROP TABLE IF EXISTS public.orders_ducklake;
DROP TABLE orders_a;
DROP TABLE orders_b;
DROP TABLE orders_bad;

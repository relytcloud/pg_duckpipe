-- Test automatic DDL propagation from source to DuckLake target tables.
--
-- Schema changes (ADD/DROP/RENAME COLUMN) are detected via RELATION message
-- diffing and applied to the target table through a non-blocking queue barrier.

-- Set fast flush for regression tests (default 5s is too slow for pg_sleep waits)
SELECT duckpipe.set_config('flush_interval_ms', '1000');
SELECT duckpipe.set_config('flush_batch_threshold', '10000');

SET client_min_messages = warning;
SELECT duckpipe.stop_worker();
RESET client_min_messages;

-- Use a dedicated group to avoid stale replication slot from prior tests
SELECT duckpipe.create_group('ddl_grp');

SET client_min_messages = warning;
SELECT duckpipe.start_worker();
RESET client_min_messages;

CREATE TABLE ddl_test (id int primary key, a text);
SELECT duckpipe.add_table('public.ddl_test', NULL, 'ddl_grp', false);

-- Initial data
INSERT INTO ddl_test VALUES (1, 'hello');

SELECT pg_sleep(2);

SELECT * FROM public.ddl_test_ducklake ORDER BY id;

-- ============================================================
-- TEST 1: ADD COLUMN
-- ============================================================
ALTER TABLE ddl_test ADD COLUMN b integer;
INSERT INTO ddl_test VALUES (2, 'world', 42);

SELECT pg_sleep(3);

SELECT * FROM public.ddl_test_ducklake ORDER BY id;

-- ============================================================
-- TEST 2: DROP COLUMN
-- ============================================================
ALTER TABLE ddl_test DROP COLUMN a;
INSERT INTO ddl_test VALUES (3, 100);

SELECT pg_sleep(3);

SELECT * FROM public.ddl_test_ducklake ORDER BY id;

-- ============================================================
-- TEST 3: RENAME COLUMN
-- ============================================================
ALTER TABLE ddl_test RENAME COLUMN b TO score;
INSERT INTO ddl_test VALUES (4, 200);

SELECT pg_sleep(3);

SELECT * FROM public.ddl_test_ducklake ORDER BY id;

-- ============================================================
-- TEST 4: Multiple consecutive DDLs with interleaved DML
-- All issued without waiting — barriers queue in VecDeque and
-- each batch must be flushed with the correct schema version.
-- ============================================================
ALTER TABLE ddl_test ADD COLUMN x text;
INSERT INTO ddl_test VALUES (5, 300, 'v1');
ALTER TABLE ddl_test ADD COLUMN y integer;
INSERT INTO ddl_test VALUES (6, 400, 'v2', 10);
ALTER TABLE ddl_test DROP COLUMN x;
INSERT INTO ddl_test VALUES (7, 500, 20);

SELECT pg_sleep(10);

SELECT * FROM public.ddl_test_ducklake ORDER BY id;

-- Diagnostic: DuckLake column definitions
SELECT column_id, column_name, column_type, begin_snapshot, end_snapshot
FROM ducklake.ducklake_column ORDER BY column_id;
-- Schema version mappings
SELECT * FROM ducklake.ducklake_schema_versions ORDER BY begin_snapshot;

-- ============================================================
-- Cleanup
-- ============================================================
SELECT duckpipe.remove_table('public.ddl_test', false);
SELECT duckpipe.drop_group('ddl_grp');
DROP TABLE public.ddl_test_ducklake;
DROP TABLE ddl_test;

SET client_min_messages = warning;
SELECT duckpipe.stop_worker();
RESET client_min_messages;

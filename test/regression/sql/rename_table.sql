-- Test OID-based WAL routing after table rename
--
-- After ALTER TABLE RENAME, the publication still includes the table (by OID),
-- and pgoutput RELATION messages carry the new name with the same OID.
-- The OID-based routing should continue syncing and update source metadata,
-- but the target DuckLake table name stays unchanged.

-- Set fast flush for regression tests (default 5s is too slow for pg_sleep waits)
SELECT duckpipe.set_config('flush_interval_ms', '1000');
SELECT duckpipe.set_config('flush_batch_threshold', '10000');

SET client_min_messages = warning;
SELECT duckpipe.stop_worker();
RESET client_min_messages;

-- Use a dedicated group to avoid stale replication slot from prior tests
SELECT duckpipe.create_group('rename_grp');

SET client_min_messages = warning;
SELECT duckpipe.start_worker();
RESET client_min_messages;

CREATE TABLE rename_src (id int primary key, val text);
SELECT duckpipe.add_table('public.rename_src', NULL, 'rename_grp', false);

-- Insert initial data
INSERT INTO rename_src VALUES (1, 'before_rename');

SELECT pg_sleep(2);

-- Verify initial sync
SELECT * FROM public.rename_src_ducklake ORDER BY id;

-- Verify metadata has original name
SELECT source_table FROM duckpipe.table_mappings WHERE source_table = 'rename_src';

-- Rename the source table
ALTER TABLE rename_src RENAME TO rename_src_v2;

-- Insert more data using the new name
INSERT INTO rename_src_v2 VALUES (2, 'after_rename');

SELECT pg_sleep(3);

-- Target table keeps the original name (source rename does NOT rename target)
SELECT * FROM public.rename_src_ducklake ORDER BY id;

-- Verify source_table metadata was updated with new name
SELECT source_table FROM duckpipe.table_mappings
WHERE source_table IN ('rename_src', 'rename_src_v2');

-- Verify target_table is unchanged (still the original name)
SELECT target_table FROM duckpipe.table_mappings
WHERE source_table = 'rename_src_v2';

-- UPDATE and DELETE should also work after rename
UPDATE rename_src_v2 SET val = 'updated' WHERE id = 1;
DELETE FROM rename_src_v2 WHERE id = 2;

SELECT pg_sleep(2);

SELECT * FROM public.rename_src_ducklake ORDER BY id;

-- Cleanup (source uses new name, target keeps original name)
SELECT duckpipe.remove_table('public.rename_src_v2', false);
SELECT duckpipe.drop_group('rename_grp');
DROP TABLE public.rename_src_ducklake;
DROP TABLE rename_src_v2;

SET client_min_messages = warning;
SELECT duckpipe.stop_worker();
RESET client_min_messages;

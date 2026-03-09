-- Test that removing a table does not hold back replication for remaining tables.
-- Regression test for: stale per_table_lsn entries from removed tables preventing
-- confirmed_lsn advancement, causing unbounded lag growth.

SET client_min_messages = warning;
SELECT duckpipe.start_worker();
RESET client_min_messages;

-- Create two tables.
CREATE TABLE rtl_keep (id int primary key, val text);
CREATE TABLE rtl_drop (id int primary key, val text);

SELECT duckpipe.add_table('public.rtl_keep', NULL, 'default', false);
SELECT duckpipe.add_table('public.rtl_drop', NULL, 'default', false);

-- Insert initial data and wait for replication.
INSERT INTO rtl_keep VALUES (1, 'keep_one');
INSERT INTO rtl_drop VALUES (1, 'drop_one');
SELECT pg_sleep(4);

SELECT * FROM public.rtl_keep_ducklake ORDER BY id;
SELECT * FROM public.rtl_drop_ducklake ORDER BY id;

-- Remove the second table.  Without the prune fix, its stale per_table_lsn
-- entry would hold back confirmed_lsn for the entire group.
SELECT duckpipe.remove_table('public.rtl_drop', false);

-- Insert more data into the kept table AFTER removing the other.
INSERT INTO rtl_keep VALUES (2, 'keep_two');
INSERT INTO rtl_keep VALUES (3, 'keep_three');

-- Wait for replication — this should succeed within a normal cycle.
-- Without the fix, confirmed_lsn is frozen and lag grows unbounded.
SELECT pg_sleep(6);

SELECT * FROM public.rtl_keep_ducklake ORDER BY id;

-- Verify the new rows replicated (proves confirmed_lsn was not held back).
SELECT count(*) = 3 AS all_rows_replicated FROM public.rtl_keep_ducklake;

-- Cleanup
SELECT duckpipe.remove_table('public.rtl_keep', false);
DROP TABLE public.rtl_keep_ducklake;
DROP TABLE public.rtl_drop_ducklake;
DROP TABLE rtl_keep;
DROP TABLE rtl_drop;

SET client_min_messages = warning;
SELECT duckpipe.stop_worker();
RESET client_min_messages;

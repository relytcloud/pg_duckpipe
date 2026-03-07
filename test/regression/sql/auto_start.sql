-- Test auto-start: add_table() should start background worker automatically

-- Verify no worker is running yet
SELECT count(*) AS workers_before FROM pg_stat_activity WHERE backend_type LIKE 'pg_duckpipe:%';

-- Create source table
CREATE TABLE auto_test (id int primary key, val text);

-- add_table should auto-start the worker (no start_worker() call)
SELECT duckpipe.add_table('public.auto_test', NULL, 'default', false);

-- Give the worker time to register in pg_stat_activity
SELECT pg_sleep(1);

-- Verify worker is now running
SELECT count(*) AS workers_after FROM pg_stat_activity WHERE backend_type LIKE 'pg_duckpipe:%';

-- Verify start_worker() reports already running
SELECT duckpipe.start_worker();

-- Adding a second table should NOT start another worker
CREATE TABLE auto_test2 (id int primary key, val text);
SELECT duckpipe.add_table('public.auto_test2', NULL, 'default', false);

-- Still exactly one worker
SELECT count(*) AS workers_still_one FROM pg_stat_activity WHERE backend_type LIKE 'pg_duckpipe:%';

-- Cleanup
SELECT duckpipe.remove_table('public.auto_test', false);
SELECT duckpipe.remove_table('public.auto_test2', false);
DROP TABLE public.auto_test_ducklake;
DROP TABLE public.auto_test2_ducklake;
DROP TABLE auto_test;
DROP TABLE auto_test2;

SET client_min_messages = warning;
SELECT duckpipe.stop_worker();
RESET client_min_messages;

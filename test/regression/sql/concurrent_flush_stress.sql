-- Stress test: concurrent flush snapshot_id conflict + DuckLake retry
--
-- Reproduces a crash where multiple flush threads commit DuckLake transactions
-- simultaneously, causing ducklake_snapshot PK conflicts. DuckLake's retry
-- logic can corrupt internal state, leading to SIGSEGV or SIGABRT.
--
-- Uses dblink to run parallel DML from 4 concurrent sessions, mimicking the
-- soak test's sysbench workload pattern.

CREATE EXTENSION IF NOT EXISTS dblink;

-- Configure aggressive flushing
SELECT duckpipe.set_config('flush_interval_ms', '200');
SELECT duckpipe.set_config('flush_batch_threshold', '50');
SELECT duckpipe.set_config('max_concurrent_flushes', '4');

ALTER SYSTEM SET duckpipe.poll_interval = 100;
SELECT pg_reload_conf();

SET client_min_messages = warning;
SELECT duckpipe.start_worker();
RESET client_min_messages;

-- Create 4 source tables
CREATE TABLE cfs_a (id serial primary key, val text, num int);
CREATE TABLE cfs_b (id serial primary key, val text, num int);
CREATE TABLE cfs_c (id serial primary key, val text, num int);
CREATE TABLE cfs_d (id serial primary key, val text, num int);

SELECT duckpipe.add_table('public.cfs_a', NULL, 'default', false);
SELECT duckpipe.add_table('public.cfs_b', NULL, 'default', false);
SELECT duckpipe.add_table('public.cfs_c', NULL, 'default', false);
SELECT duckpipe.add_table('public.cfs_d', NULL, 'default', false);

-- Seed each table with 200 rows
INSERT INTO cfs_a (val, num) SELECT 'a_' || g, g FROM generate_series(1, 200) g;
INSERT INTO cfs_b (val, num) SELECT 'b_' || g, g FROM generate_series(1, 200) g;
INSERT INTO cfs_c (val, num) SELECT 'c_' || g, g FROM generate_series(1, 200) g;
INSERT INTO cfs_d (val, num) SELECT 'd_' || g, g FROM generate_series(1, 200) g;
SELECT pg_sleep(3);

-- Launch 4 parallel dblink sessions that hammer DML concurrently.
-- Each session targets ONE table with INSERT+UPDATE in a tight loop,
-- maximizing concurrent flush commits across all 4 tables.

-- Helper function for a single worker's DML loop
CREATE OR REPLACE FUNCTION cfs_worker_loop(tbl text, rounds int) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  FOR i IN 1..rounds LOOP
    -- INSERT batch
    EXECUTE format(
      'INSERT INTO %I (val, num) SELECT %L || %s || ''_'' || g, g FROM generate_series(1, 50) g',
      tbl, left(tbl, 5), i
    );
    -- UPDATE overlapping rows (generates DELETE+INSERT in DuckLake)
    EXECUTE format(
      'UPDATE %I SET val = %L || %s, num = num + 1 WHERE id BETWEEN %s AND %s',
      tbl, 'upd_' || left(tbl, 5), i,
      ((i * 7) % 150) + 1, ((i * 7) % 150) + 30
    );
  END LOOP;
END;
$$;

-- Open 4 concurrent dblink connections
SELECT dblink_connect('w1', 'dbname=' || current_database() || ' port=' || current_setting('port'));
SELECT dblink_connect('w2', 'dbname=' || current_database() || ' port=' || current_setting('port'));
SELECT dblink_connect('w3', 'dbname=' || current_database() || ' port=' || current_setting('port'));
SELECT dblink_connect('w4', 'dbname=' || current_database() || ' port=' || current_setting('port'));

-- Fire all 4 workers asynchronously (each does 2000 rounds of INSERT+UPDATE)
SELECT dblink_send_query('w1', 'SELECT cfs_worker_loop(''cfs_a'', 500)');
SELECT dblink_send_query('w2', 'SELECT cfs_worker_loop(''cfs_b'', 500)');
SELECT dblink_send_query('w3', 'SELECT cfs_worker_loop(''cfs_c'', 500)');
SELECT dblink_send_query('w4', 'SELECT cfs_worker_loop(''cfs_d'', 500)');

-- Wait for all 4 workers to complete
SELECT * FROM dblink_get_result('w1') AS t(result text);
SELECT * FROM dblink_get_result('w2') AS t(result text);
SELECT * FROM dblink_get_result('w3') AS t(result text);
SELECT * FROM dblink_get_result('w4') AS t(result text);

SELECT dblink_disconnect('w1');
SELECT dblink_disconnect('w2');
SELECT dblink_disconnect('w3');
SELECT dblink_disconnect('w4');

-- Wait for flushes to drain
SELECT pg_sleep(15);

-- Primary assertion: worker did not crash
SELECT count(*) > 0 AS worker_alive FROM pg_stat_activity
  WHERE backend_type LIKE 'pg_duckpipe:%';

-- Data integrity check
SELECT count(*) > 0 AS cfs_a_has_data FROM public.cfs_a_ducklake;
SELECT count(*) > 0 AS cfs_b_has_data FROM public.cfs_b_ducklake;
SELECT count(*) > 0 AS cfs_c_has_data FROM public.cfs_c_ducklake;
SELECT count(*) > 0 AS cfs_d_has_data FROM public.cfs_d_ducklake;

-- Info: verify snapshots were created (count varies by timing)
SELECT count(*) > 100 AS many_snapshots FROM ducklake.ducklake_snapshot;

-- Cleanup: stop worker FIRST to avoid deadlock
DROP FUNCTION cfs_worker_loop;

SELECT duckpipe.remove_table('public.cfs_a', false);
SELECT duckpipe.remove_table('public.cfs_b', false);
SELECT duckpipe.remove_table('public.cfs_c', false);
SELECT duckpipe.remove_table('public.cfs_d', false);

SET client_min_messages = warning;
SELECT duckpipe.stop_worker();
RESET client_min_messages;

SELECT pg_sleep(2);

DROP TABLE IF EXISTS public.cfs_a_ducklake;
DROP TABLE IF EXISTS public.cfs_b_ducklake;
DROP TABLE IF EXISTS public.cfs_c_ducklake;
DROP TABLE IF EXISTS public.cfs_d_ducklake;

DROP TABLE cfs_a;
DROP TABLE cfs_b;
DROP TABLE cfs_c;
DROP TABLE cfs_d;

SELECT duckpipe.set_config('flush_interval_ms', NULL);
SELECT duckpipe.set_config('flush_batch_threshold', NULL);
SELECT duckpipe.set_config('max_concurrent_flushes', NULL);

ALTER SYSTEM RESET duckpipe.poll_interval;
SELECT pg_reload_conf();

DROP EXTENSION dblink;

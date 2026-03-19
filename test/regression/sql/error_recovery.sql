-- Test error detection and self-healing recovery
--
-- Scenario: drop the DuckLake target table while sync is active,
-- verify flush failures are recorded in error_message, then
-- re-create the target and verify sync self-heals.

ALTER SYSTEM SET duckpipe.poll_interval = 100;
SELECT pg_reload_conf();

SELECT duckpipe.start_worker();

CREATE TABLE err_src (id int primary key, val text);
SELECT duckpipe.add_table('public.err_src', NULL, 'default', false);

-- Insert and wait for initial sync
INSERT INTO err_src VALUES (1, 'one'), (2, 'two');
SELECT pg_sleep(2);

-- Verify initial sync works
SELECT * FROM public.err_src_ducklake ORDER BY id;

-- Drop the target table to cause flush failures
DROP TABLE public.err_src_ducklake;

-- Insert data to trigger a flush that will fail
INSERT INTO err_src VALUES (3, 'three');
SELECT pg_sleep(3);

-- Verify error_message is recorded (flush failure detected)
SELECT state,
       error_message IS NOT NULL AND error_message != '' AS has_error
FROM duckpipe.table_mappings
WHERE source_table = 'err_src';

-- Re-create the target table (fix the problem)
CREATE TABLE public.err_src_ducklake (LIKE err_src) USING ducklake;
ALTER TABLE public.err_src_ducklake ADD COLUMN _duckpipe_source text;

-- Insert new data to trigger sync recovery
INSERT INTO err_src VALUES (4, 'four');
SELECT pg_sleep(3);

-- Verify sync self-healed: new data should appear
-- (row 3 was lost during the error window, but row 4 arrives after recovery)
SELECT count(*) > 0 AS has_synced_data FROM public.err_src_ducklake;

-- Verify error_message is cleared after successful flush
SELECT error_message IS NULL OR error_message = '' AS error_cleared
FROM duckpipe.table_mappings
WHERE source_table = 'err_src';

-- Cleanup
SELECT duckpipe.remove_table('public.err_src', false);
DROP TABLE public.err_src_ducklake;
DROP TABLE err_src;

ALTER SYSTEM RESET duckpipe.poll_interval;
SELECT pg_reload_conf();

SET client_min_messages = warning;
SELECT duckpipe.stop_worker();
RESET client_min_messages;

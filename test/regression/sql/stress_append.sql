-- Stress test: many single-row transactions should be batched into few files

-- Configure batching parameters
ALTER SYSTEM SET duckpipe.poll_interval = 100;
SELECT pg_reload_conf();

CREATE TABLE stress_src (id int primary key, val text);
SELECT duckpipe.add_table('public.stress_src', NULL, 'default', false);

-- Helper: insert one row per transaction
CREATE PROCEDURE stress_insert_one_row_txn(start_id int, num_txns int)
LANGUAGE plpgsql AS $$
DECLARE
  i int;
  rid int;
BEGIN
  FOR i IN 0..num_txns-1 LOOP
    rid := start_id + i + 1;
    INSERT INTO stress_src VALUES (rid, 'row_' || rid);
    COMMIT;
  END LOOP;
END;
$$;

-- 200 transactions x 1 row each
CALL stress_insert_one_row_txn(0, 200);

-- Wait for sync
SELECT pg_sleep(5);

-- Verify all rows arrived
SELECT count(*) AS synced_rows FROM public.stress_src_ducklake;

-- Batching assertion: active files should be far fewer than 200 rows
WITH c AS (
  SELECT count(*) AS active_file_count
  FROM ducklake.ducklake_data_file df
  JOIN ducklake.ducklake_table t ON df.table_id = t.table_id
  JOIN ducklake.ducklake_schema s ON t.schema_id = s.schema_id
  WHERE s.schema_name = 'public'
    AND t.table_name = 'stress_src_ducklake'
    AND t.end_snapshot IS NULL
    AND df.end_snapshot IS NULL
)
SELECT c.active_file_count < 200 AS file_batching_ok
FROM c;

-- Verify worker is still alive after stress load
SELECT count(*) > 0 AS worker_alive FROM pg_stat_activity
  WHERE backend_type LIKE 'pg_duckpipe:%';

-- Cleanup
DROP PROCEDURE stress_insert_one_row_txn;
SELECT duckpipe.remove_table('public.stress_src', false);
DROP TABLE public.stress_src_ducklake;
DROP TABLE stress_src;

ALTER SYSTEM RESET duckpipe.poll_interval;
SELECT pg_reload_conf();

-- Test duckpipe.metrics() JSON endpoint

-- Create and add table (no worker needed — metrics() reads SHM + PG)
CREATE TABLE metrics_test (id int primary key, val text);
SELECT duckpipe.add_table('public.metrics_test', NULL, 'default', false);

-- Call metrics() and verify it returns valid JSON with expected structure
SELECT jsonb_typeof(duckpipe.metrics()::jsonb) AS top_type;
SELECT jsonb_typeof((duckpipe.metrics()::jsonb)->'tables') AS tables_type;
SELECT jsonb_typeof((duckpipe.metrics()::jsonb)->'groups') AS groups_type;

-- Verify the table entry has expected fields
SELECT
  (t->>'group') AS group_name,
  (t->>'source_table') AS source_table,
  (t->>'state') AS state,
  (t->>'rows_synced')::bigint AS rows_synced,
  (t->>'queued_changes')::bigint AS queued_changes,
  (t->>'duckdb_memory_bytes')::bigint AS duckdb_memory_bytes,
  (t->>'consecutive_failures')::int AS consecutive_failures,
  (t->>'flush_count')::bigint AS flush_count,
  (t->>'flush_duration_ms')::bigint AS flush_duration_ms,
  (t->>'avg_row_bytes')::bigint AS avg_row_bytes,
  (t->>'snapshot_duration_ms') IS NOT NULL AS has_snapshot_duration_key,
  (t->>'snapshot_rows') IS NOT NULL AS has_snapshot_rows_key,
  (t->>'applied_lsn') IS NOT NULL AS has_applied_lsn_key
FROM jsonb_array_elements((duckpipe.metrics()::jsonb)->'tables') AS t;

-- Verify the group entry has expected fields
SELECT
  (g->>'name') AS group_name,
  (g->>'total_queued_changes')::bigint AS total_queued,
  (g->>'is_backpressured')::boolean AS is_bp
FROM jsonb_array_elements((duckpipe.metrics()::jsonb)->'groups') AS g;

-- Cleanup
SELECT duckpipe.remove_table('public.metrics_test', false);
DROP TABLE public.metrics_test_ducklake;
DROP TABLE metrics_test;

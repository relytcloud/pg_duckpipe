-- Test monitoring functions

-- Test duckpipe.groups() returns default group info
SELECT name, publication, slot_name, enabled, table_count FROM duckpipe.groups();

-- Create and add table
CREATE TABLE mon_test (id int primary key, val text);

SELECT duckpipe.add_table('public.mon_test', NULL, 'default', false);

-- Test duckpipe.tables() returns the mapping
SELECT source_table, target_table, sync_group, enabled FROM duckpipe.tables();

-- Test duckpipe.status() returns state info including new observability columns
SELECT sync_group, source_table, target_table, state, enabled,
       queued_changes, consecutive_failures, retry_at, applied_lsn,
       snapshot_duration_ms, snapshot_rows
FROM duckpipe.status();

-- Test duckpipe.worker_status() returns runtime state
SELECT total_queued_changes, is_backpressured FROM duckpipe.worker_status();

-- Cleanup
SELECT duckpipe.remove_table('public.mon_test', false);
DROP TABLE public.mon_test_ducklake;
DROP TABLE mon_test;


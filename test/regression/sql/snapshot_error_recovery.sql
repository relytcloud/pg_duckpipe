-- Test snapshot error recovery with exponential backoff
--
-- Scenario: force a snapshot that fails (target table missing), verify
-- consecutive_failures counting and error_message recording, then fix
-- the target and verify the snapshot self-heals.

-- Fast polling for quick feedback
ALTER SYSTEM SET duckpipe.poll_interval = 100;
SELECT pg_reload_conf();

SELECT duckpipe.start_worker();

-- Step 1: Create source, add table, wait for initial sync
CREATE TABLE snap_err_src (id int primary key, val text);
SELECT duckpipe.add_table('public.snap_err_src', NULL, 'default', false);
INSERT INTO snap_err_src VALUES (1, 'one'), (2, 'two');

SELECT pg_sleep(3);

-- Verify initial snapshot succeeded
SELECT * FROM public.snap_err_src_ducklake ORDER BY id;

-- Step 2: Drop the ducklake target to break future snapshots
DROP TABLE public.snap_err_src_ducklake;

-- Step 3: Force a re-snapshot by resetting state directly
-- (We bypass resync_table() because it TRUNCATEs the target which is already gone)
UPDATE duckpipe.table_mappings
SET state = 'SNAPSHOT',
    rows_synced = 0, last_sync_at = NULL,
    error_message = NULL, applied_lsn = NULL, snapshot_lsn = NULL,
    consecutive_failures = 0, retry_at = NULL,
    snapshot_duration_ms = NULL, snapshot_rows = NULL
WHERE source_table = 'snap_err_src';

-- Step 4: Wait for 3+ consecutive snapshot failures
SELECT pg_sleep(5);

-- Assert: failures counted, error message recorded
SELECT consecutive_failures >= 3 AS enough_failures,
       error_message LIKE '%not found%' AS error_mentions_not_found
FROM duckpipe.table_mappings
WHERE source_table = 'snap_err_src';

-- Step 5: Fix the problem — recreate the ducklake target
CREATE TABLE public.snap_err_src_ducklake (LIKE snap_err_src) USING ducklake;

-- Step 6: Wait for snapshot to succeed now that target exists
SELECT pg_sleep(4);

-- Assert: recovered — state should be STREAMING or CATCHUP, errors cleared
SELECT state IN ('STREAMING', 'CATCHUP') AS recovered,
       consecutive_failures = 0 AS failures_cleared,
       error_message IS NULL AS error_cleared
FROM duckpipe.table_mappings
WHERE source_table = 'snap_err_src';

-- Step 7: Verify new data syncs after recovery
INSERT INTO snap_err_src VALUES (3, 'three');
SELECT pg_sleep(2);

SELECT * FROM public.snap_err_src_ducklake ORDER BY id;

-- Cleanup
SELECT duckpipe.remove_table('public.snap_err_src', false);
DROP TABLE public.snap_err_src_ducklake;
DROP TABLE snap_err_src;

ALTER SYSTEM RESET duckpipe.poll_interval;
SELECT pg_reload_conf();

SET client_min_messages = warning;
SELECT duckpipe.stop_worker();
RESET client_min_messages;

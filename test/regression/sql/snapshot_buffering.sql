-- Test: WAL changes during snapshot are buffered and applied after completion
--
-- Verifies the decoupled snapshot design:
--   1. Snapshot runs as a fire-and-forget background task
--   2. WAL streaming starts concurrently (does not wait for snapshot)
--   3. WAL changes for SNAPSHOT tables are pushed to paused flush queues
--   4. After snapshot completes -> CATCHUP -> flush queue unpaused -> changes flushed
--
-- If the flush queue were NOT paused during snapshot, the flush thread could
-- write WAL changes to the target, then the snapshot's DELETE FROM target
-- would wipe them -- causing permanent data loss.

ALTER SYSTEM SET duckpipe.poll_interval = 100;
SELECT pg_reload_conf();

-- Ensure a clean worker: stop any leftover, then start fresh
SET client_min_messages = warning;
SELECT duckpipe.stop_worker();
SELECT pg_sleep(1);
SELECT duckpipe.start_worker();
RESET client_min_messages;

-- Create table with initial data for snapshot
CREATE TABLE snap_buf (id int PRIMARY KEY, val text);
INSERT INTO snap_buf SELECT g, 'initial_' || g FROM generate_series(1, 1000) g;

-- Add table (triggers SNAPSHOT; streaming starts concurrently)
SELECT duckpipe.add_table('public.snap_buf');

-- Immediately insert more rows -- these generate WAL while the table may
-- still be in SNAPSHOT state.  The flush queue must buffer them (paused)
-- and only flush after the snapshot completes and the queue is unpaused.
INSERT INTO snap_buf SELECT g, 'concurrent_' || g FROM generate_series(1001, 2000) g;

-- Wait for snapshot + catchup
SELECT pg_sleep(15);

-- All 2000 rows must be present regardless of timing
SELECT count(*) AS total_rows FROM public.snap_buf_ducklake;

-- Verify both cohorts exist
SELECT count(*) AS initial_rows FROM public.snap_buf_ducklake WHERE val LIKE 'initial_%';
SELECT count(*) AS concurrent_rows FROM public.snap_buf_ducklake WHERE val LIKE 'concurrent_%';

-- Spot-check boundary rows
SELECT id, val FROM public.snap_buf_ducklake WHERE id IN (1, 1000, 1001, 2000) ORDER BY id;

-- Table must have progressed past SNAPSHOT (CATCHUP or STREAMING)
SELECT state IN ('CATCHUP', 'STREAMING') AS past_snapshot FROM duckpipe.status() WHERE source_table = 'public.snap_buf';

-- Phase 2: mixed DML during a second snapshot (resync)
-- This tests that UPDATE/DELETE WAL changes are also correctly buffered.
SELECT duckpipe.resync_table('public.snap_buf');

-- Concurrent mixed DML while re-snapshot is running
UPDATE snap_buf SET val = 'updated_' || id WHERE id <= 100;
DELETE FROM snap_buf WHERE id BETWEEN 1901 AND 2000;
INSERT INTO snap_buf SELECT g, 'phase2_' || g FROM generate_series(2001, 2100) g;

SELECT pg_sleep(15);

-- Expected: 1000 initial (100 updated) + 900 concurrent (1001-1900) + 100 phase2 = 2000
SELECT count(*) AS total_after_resync FROM public.snap_buf_ducklake;

-- Verify updates applied
SELECT count(*) AS updated_rows FROM public.snap_buf_ducklake WHERE val LIKE 'updated_%';

-- Verify deletes applied (rows 1901-2000 should be gone)
SELECT count(*) AS deleted_range FROM public.snap_buf_ducklake WHERE id BETWEEN 1901 AND 2000;

-- Verify new inserts present
SELECT count(*) AS phase2_rows FROM public.snap_buf_ducklake WHERE val LIKE 'phase2_%';

-- Must have progressed past SNAPSHOT again
SELECT state IN ('CATCHUP', 'STREAMING') AS past_snapshot FROM duckpipe.status() WHERE source_table = 'public.snap_buf';

-- Cleanup
SELECT duckpipe.remove_table('public.snap_buf', false);
DROP TABLE public.snap_buf_ducklake;
DROP TABLE snap_buf;

ALTER SYSTEM RESET duckpipe.poll_interval;
SELECT pg_reload_conf();

SET client_min_messages = warning;
SELECT duckpipe.stop_worker();
RESET client_min_messages;

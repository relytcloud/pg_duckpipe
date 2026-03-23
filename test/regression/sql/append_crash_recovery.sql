-- Test: append-mode crash recovery deduplication.
--
-- Simulates the crash window between DuckDB COMMIT and PG applied_lsn
-- update by manually rolling back applied_lsn after a successful flush.
-- On restart, WAL replays from the rolled-back LSN — without dedup,
-- already-flushed changes would be appended again as duplicates.

SET client_min_messages = warning;
SELECT duckpipe.start_worker();
RESET client_min_messages;

CREATE TABLE append_crash (id int primary key, val text);

-- Append mode, skip snapshot
SELECT duckpipe.add_table('public.append_crash', NULL, 'default', false, false, 'append');

-- Insert rows and wait for flush
INSERT INTO append_crash VALUES (1, 'one'), (2, 'two'), (3, 'three');

SELECT pg_sleep(8);

-- Verify: 3 rows flushed with op='I'
SELECT count(*) AS before_count FROM public.append_crash_ducklake;

-- Record the current applied_lsn (should be > 0 after flush)
SELECT applied_lsn IS NOT NULL AS has_applied_lsn
FROM duckpipe.table_mappings
WHERE source_table = 'append_crash';

-- Stop worker
SET client_min_messages = warning;
SELECT duckpipe.stop_worker();
RESET client_min_messages;

-- Simulate crash window: roll back applied_lsn AND confirmed_lsn to 0/0.
-- This mimics what happens if the process crashes after DuckDB COMMIT
-- but before PG applied_lsn update completes.
UPDATE duckpipe.table_mappings
SET applied_lsn = '0/0'::pg_lsn
WHERE source_table = 'append_crash';

UPDATE duckpipe.sync_groups
SET confirmed_lsn = '0/0'::pg_lsn
WHERE name = 'default';

-- Restart worker — WAL replays from 0/0, re-delivering all changes
SET client_min_messages = warning;
SELECT duckpipe.start_worker();
RESET client_min_messages;

-- Insert one more row to trigger a new flush cycle
INSERT INTO append_crash VALUES (4, 'four');

SELECT pg_sleep(8);

-- Without dedup fix: 7 rows (3 original + 3 replayed duplicates + 1 new)
-- With dedup fix:    4 rows (3 original + 1 new, replayed duplicates skipped)
SELECT count(*) AS after_recovery_count FROM public.append_crash_ducklake;

-- Verify no duplicate op='I' rows for ids 1-3
SELECT id, count(*) AS copies
FROM public.append_crash_ducklake
WHERE id <= 3
GROUP BY id
ORDER BY id;

-- Verify the new row (id=4) arrived
SELECT count(*) AS new_row_count
FROM public.append_crash_ducklake
WHERE id = 4 AND "_duckpipe_op" = 'I';

-- Cleanup
SELECT duckpipe.remove_table('public.append_crash', false);
DROP TABLE public.append_crash_ducklake;
DROP TABLE append_crash;

SET client_min_messages = warning;
SELECT duckpipe.stop_worker();
RESET client_min_messages;

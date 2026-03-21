-- Test append sync mode: immutable changelog with _duckpipe_op and _duckpipe_lsn

SET client_min_messages = warning;
SELECT duckpipe.start_worker();
RESET client_min_messages;

CREATE TABLE append_src (id int primary key, val text);

-- Add table in append mode (skip initial snapshot for streaming-only test)
SELECT duckpipe.add_table('public.append_src', NULL, 'default', false, false, 'append');

-- Phase 1: INSERT 3 rows
INSERT INTO append_src VALUES (1, 'one');
INSERT INTO append_src VALUES (2, 'two');
INSERT INTO append_src VALUES (3, 'three');

SELECT pg_sleep(8);

-- Verify: 3 rows with _duckpipe_op = 'I'
SELECT count(*) AS insert_count FROM public.append_src_ducklake WHERE "_duckpipe_op" = 'I';

-- All LSNs should be > 0 (streaming, not snapshot)
SELECT count(*) AS positive_lsn_count FROM public.append_src_ducklake WHERE "_duckpipe_lsn" > 0;

-- Phase 2: UPDATE a row — should add a NEW row with op='U', old row still present
UPDATE append_src SET val = 'one_updated' WHERE id = 1;

SELECT pg_sleep(8);

-- Should now have 4 rows total (3 inserts + 1 update)
SELECT count(*) AS total_after_update FROM public.append_src_ducklake;

-- Verify update row exists
SELECT count(*) AS update_count FROM public.append_src_ducklake WHERE "_duckpipe_op" = 'U';

-- Original insert for id=1 should still be there
SELECT count(*) AS id1_rows FROM public.append_src_ducklake WHERE id = 1;

-- Phase 3: DELETE a row — should add a NEW row with op='D'
DELETE FROM append_src WHERE id = 2;

SELECT pg_sleep(8);

-- Should now have 5 rows total (3 inserts + 1 update + 1 delete)
SELECT count(*) AS total_after_delete FROM public.append_src_ducklake;

-- Verify delete row exists
SELECT count(*) AS delete_count FROM public.append_src_ducklake WHERE "_duckpipe_op" = 'D';

-- Verify LSNs are monotonically increasing (ordered by operation sequence)
SELECT count(*) AS monotonic_ok
FROM (
    SELECT "_duckpipe_lsn",
           lag("_duckpipe_lsn") OVER (ORDER BY "_duckpipe_lsn") AS prev_lsn
    FROM public.append_src_ducklake
) sub
WHERE prev_lsn IS NOT NULL AND "_duckpipe_lsn" >= prev_lsn;

-- Phase 4: Verify total op distribution
SELECT "_duckpipe_op", count(*) AS cnt
FROM public.append_src_ducklake
GROUP BY "_duckpipe_op"
ORDER BY "_duckpipe_op";

-- Cleanup
SELECT duckpipe.remove_table('public.append_src', false);
DROP TABLE public.append_src_ducklake;
DROP TABLE append_src;

SET client_min_messages = warning;
SELECT duckpipe.stop_worker();
RESET client_min_messages;

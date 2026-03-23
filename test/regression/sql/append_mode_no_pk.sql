-- Test append sync mode for tables with no primary key

SET client_min_messages = warning;
SELECT duckpipe.start_worker();
RESET client_min_messages;

CREATE TABLE nopk_src (id int, val text);

-- Append mode should succeed for no-PK tables
SELECT duckpipe.add_table('public.nopk_src', NULL, 'default', false, false, 'append');

-- Upsert mode must be rejected for no-PK tables
CREATE TABLE nopk_src2 (id int, val text);
SELECT duckpipe.add_table('public.nopk_src2', NULL, 'default', false, false, 'upsert');

-- Phase 1: INSERT 3 rows
INSERT INTO nopk_src VALUES (1, 'one');
INSERT INTO nopk_src VALUES (2, 'two');
INSERT INTO nopk_src VALUES (3, 'three');

SELECT pg_sleep(8);

-- Verify: 3 rows with _duckpipe_op = 'I' and all values populated
SELECT count(*) AS insert_count FROM public.nopk_src_ducklake WHERE "_duckpipe_op" = 'I';

-- All values should be non-NULL
SELECT count(*) AS non_null_count FROM public.nopk_src_ducklake WHERE id IS NOT NULL AND val IS NOT NULL;

-- Phase 2: UPDATE a row — changelog gets op='U' with full new values
UPDATE nopk_src SET val = 'one_updated' WHERE id = 1;

SELECT pg_sleep(8);

-- Should now have 4 rows total (3 inserts + 1 update)
SELECT count(*) AS total_after_update FROM public.nopk_src_ducklake;

-- Update row should have all columns populated
SELECT id, val, "_duckpipe_op" FROM public.nopk_src_ducklake WHERE "_duckpipe_op" = 'U';

-- Phase 3: DELETE a row — changelog gets op='D' with full old values (not NULLs)
DELETE FROM nopk_src WHERE id = 2;

SELECT pg_sleep(8);

-- Should now have 5 rows total
SELECT count(*) AS total_after_delete FROM public.nopk_src_ducklake;

-- Delete row should have all columns populated (REPLICA IDENTITY FULL)
SELECT id, val, "_duckpipe_op" FROM public.nopk_src_ducklake WHERE "_duckpipe_op" = 'D';

-- Phase 4: Verify total op distribution
SELECT "_duckpipe_op", count(*) AS cnt
FROM public.nopk_src_ducklake
GROUP BY "_duckpipe_op"
ORDER BY "_duckpipe_op";

-- Cleanup
SELECT duckpipe.remove_table('public.nopk_src', false);
DROP TABLE public.nopk_src_ducklake;
DROP TABLE nopk_src;
DROP TABLE nopk_src2;

SET client_min_messages = warning;
SELECT duckpipe.stop_worker();
RESET client_min_messages;

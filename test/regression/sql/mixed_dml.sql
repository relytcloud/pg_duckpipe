-- Test single-table mixed DML (UPDATE + DELETE at scale)
--
-- Reproduces the mixed DML consistency failure where DELETEs in the flush
-- path return 0 rows deleted, causing duplicate rows from UPDATEs and
-- missing DELETEs.

SET client_min_messages = warning;
SELECT duckpipe.start_worker();
RESET client_min_messages;

CREATE TABLE mixed_test (id int primary key, val text, num int);

SELECT duckpipe.add_table('public.mixed_test', NULL, 'default', false);

-- Phase 1: Bulk INSERT 200 rows
INSERT INTO mixed_test SELECT g, 'val_' || g, g FROM generate_series(1, 200) g;

SELECT pg_sleep(3);

-- Verify initial sync
SELECT count(*) AS initial_count FROM public.mixed_test_ducklake;

-- Phase 2: UPDATE 100 rows (should be DELETE + INSERT in flush path)
UPDATE mixed_test SET val = 'updated_' || id, num = num * 10 WHERE id <= 100;

SELECT pg_sleep(3);

-- Verify: count should still be 200 (no duplicates from failed DELETEs)
SELECT count(*) AS after_update_count FROM public.mixed_test_ducklake;

-- Spot-check updated values
SELECT id, val, num FROM public.mixed_test_ducklake WHERE id IN (1, 50, 100) ORDER BY id;

-- Phase 3: DELETE 50 rows
DELETE FROM mixed_test WHERE id > 150;

SELECT pg_sleep(3);

-- Verify: count should be 150
SELECT count(*) AS after_delete_count FROM public.mixed_test_ducklake;

-- Phase 4: Mixed in single transaction
BEGIN;
INSERT INTO mixed_test SELECT g, 'new_' || g, g FROM generate_series(201, 250) g;
UPDATE mixed_test SET val = 'batch_updated_' || id WHERE id BETWEEN 50 AND 70;
DELETE FROM mixed_test WHERE id BETWEEN 140 AND 150;
COMMIT;

SELECT pg_sleep(3);

-- Verify: 150 - 11 (deleted 140..150) + 50 (inserted 201..250) = 189
SELECT count(*) AS final_count FROM public.mixed_test_ducklake;

-- Cleanup
SELECT duckpipe.remove_table('public.mixed_test', false);
DROP TABLE public.mixed_test_ducklake;
DROP TABLE mixed_test;

SET client_min_messages = warning;
SELECT duckpipe.stop_worker();
RESET client_min_messages;

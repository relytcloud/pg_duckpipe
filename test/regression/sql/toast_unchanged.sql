-- TOAST unchanged columns become NULL
--
-- Bug: When an UPDATE doesn't modify a TOASTed column (>2KB), pgoutput sends
-- status 'u' (unchanged) with no data. decoder.c converts this to NULL.
-- Since UPDATEs are DELETE + INSERT, the INSERT writes NULL for the unchanged
-- TOAST column, silently corrupting data.

SET client_min_messages = warning;
SELECT duckpipe.start_worker();
RESET client_min_messages;

CREATE TABLE toast_test (id int PRIMARY KEY, small_col text, big_col text);

-- Force TOAST storage for big_col so pgoutput sends 'u' for unchanged values
ALTER TABLE toast_test ALTER COLUMN big_col SET STORAGE EXTERNAL;

SELECT duckpipe.add_table('public.toast_test', NULL, 'default', false);

-- Insert rows with large text (well above TOAST threshold)
INSERT INTO toast_test VALUES (1, 'small_1', repeat('x', 10000));
INSERT INTO toast_test VALUES (2, 'small_2', repeat('y', 10000));

SELECT pg_sleep(2);

-- Verify initial sync
SELECT id, small_col, length(big_col) AS big_col_len
FROM public.toast_test_ducklake ORDER BY id;

-- Update only small_col (big_col unchanged → pgoutput sends status 'u')
UPDATE toast_test SET small_col = 'updated_1' WHERE id = 1;
UPDATE toast_test SET small_col = 'updated_2' WHERE id = 2;

SELECT pg_sleep(2);

-- Verify: big_col must be preserved, not NULL
SELECT id, small_col, length(big_col) AS big_col_len,
       big_col IS NULL AS big_col_is_null
FROM public.toast_test_ducklake ORDER BY id;

-- Also test: update big_col itself (should work normally)
UPDATE toast_test SET big_col = repeat('z', 10000) WHERE id = 1;

SELECT pg_sleep(2);

SELECT id, small_col, length(big_col) AS big_col_len
FROM public.toast_test_ducklake ORDER BY id;

-- Cleanup
SELECT duckpipe.remove_table('public.toast_test', false);
DROP TABLE public.toast_test_ducklake;
DROP TABLE toast_test;

SET client_min_messages = warning;
SELECT duckpipe.stop_worker();
RESET client_min_messages;

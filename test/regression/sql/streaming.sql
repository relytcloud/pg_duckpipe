CREATE EXTENSION pg_duckpipe CASCADE;
SELECT duckpipe.start_worker();

CREATE TABLE stream_test (id int primary key, val text);

SELECT duckpipe.add_table('public.stream_test', NULL, 'default', false);

INSERT INTO stream_test VALUES (1, 'one');
INSERT INTO stream_test VALUES (2, 'two');

SELECT pg_sleep(2);

SELECT * FROM public.stream_test_ducklake ORDER BY id;

-- Test UPDATE (should be handled as DELETE + INSERT)
UPDATE stream_test SET val = 'updated_two' WHERE id = 2;

SELECT pg_sleep(2);

SELECT * FROM public.stream_test_ducklake ORDER BY id;

DELETE FROM stream_test WHERE id = 1;

SELECT pg_sleep(2);

SELECT * FROM public.stream_test_ducklake ORDER BY id;

SELECT duckpipe.remove_table('public.stream_test', false);
DROP TABLE public.stream_test_ducklake;
DROP TABLE stream_test;

SET client_min_messages = warning;
SELECT duckpipe.stop_worker();
RESET client_min_messages;
DROP EXTENSION pg_duckpipe CASCADE;

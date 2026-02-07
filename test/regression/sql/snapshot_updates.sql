-- Test snapshot copy (initial load) and UPDATE sync
CREATE EXTENSION pg_duckpipe CASCADE;
SELECT duckpipe.start_worker();

CREATE TABLE existing_data (id int primary key, val text);
INSERT INTO existing_data VALUES (1, 'one'), (2, 'two'), (3, 'three');

SELECT duckpipe.add_table('public.existing_data');

SELECT pg_sleep(2);

SELECT * FROM public.existing_data_ducklake ORDER BY id;

UPDATE existing_data SET val = 'updated_two' WHERE id = 2;

SELECT pg_sleep(4);

SELECT * FROM public.existing_data_ducklake ORDER BY id;

SELECT duckpipe.remove_table('public.existing_data', false);
DROP TABLE public.existing_data_ducklake;
DROP TABLE existing_data;

SET client_min_messages = warning;
SELECT duckpipe.stop_worker();
RESET client_min_messages;
DROP EXTENSION pg_duckpipe CASCADE;

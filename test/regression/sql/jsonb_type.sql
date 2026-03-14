-- Test JSONB column type support
SET client_min_messages = warning;
SELECT duckpipe.start_worker();
RESET client_min_messages;

CREATE TABLE jsonb_test (
    id int primary key,
    data jsonb
);

-- Insert data before add_table so snapshot captures it
INSERT INTO jsonb_test VALUES (1, '{"key": "value"}'::jsonb);

SELECT duckpipe.add_table('public.jsonb_test');

SELECT pg_sleep(5);

SELECT * FROM public.jsonb_test_ducklake ORDER BY id;

SELECT duckpipe.remove_table('public.jsonb_test', false);
DROP TABLE public.jsonb_test_ducklake;
DROP TABLE jsonb_test;

SET client_min_messages = warning;
SELECT duckpipe.stop_worker();
RESET client_min_messages;

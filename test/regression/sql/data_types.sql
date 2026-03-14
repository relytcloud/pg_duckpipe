-- Test various PostgreSQL data types
SELECT duckpipe.start_worker();

CREATE TABLE dtype_test (
    id serial primary key,
    col_int int,
    col_bigint bigint,
    col_smallint smallint,
    col_float float,
    col_double double precision,
    col_text text,
    col_varchar varchar(100),
    col_bool boolean,
    col_ts timestamp,
    col_date date,
    col_json json,
    col_jsonb jsonb,
    col_uuid uuid,
    col_numeric numeric(10,2),
    col_timestamptz timestamptz,
    col_time time,
    col_char char(10),
    col_smallserial smallserial
);

SELECT duckpipe.add_table('public.dtype_test', NULL, 'default', false);

-- Insert rows with various types
INSERT INTO dtype_test (col_int, col_bigint, col_smallint, col_float, col_double, col_text, col_varchar, col_bool, col_ts, col_date, col_json, col_jsonb, col_uuid, col_numeric, col_timestamptz, col_time, col_char)
VALUES (42, 9999999999, 1, 3.14, 2.718281828, 'hello world', 'varchar val', true, '2024-01-15 10:30:00', '2024-01-15', '{"a":1}', '{"b":2}', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 12345.67, '2024-01-15 10:30:00+00', '14:30:00', 'fixed');

-- Insert with NULLs
INSERT INTO dtype_test (col_int, col_bigint, col_smallint, col_float, col_double, col_text, col_varchar, col_bool, col_ts, col_date, col_json, col_jsonb, col_uuid, col_numeric, col_timestamptz, col_time, col_char)
VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

-- Insert with empty string
INSERT INTO dtype_test (col_int, col_text, col_varchar)
VALUES (0, '', '');

SELECT pg_sleep(2);

SELECT id, col_int, col_bigint, col_smallint, col_bool, col_text, col_varchar FROM public.dtype_test_ducklake ORDER BY id;

SELECT id, col_json, col_jsonb, col_uuid, col_numeric FROM public.dtype_test_ducklake ORDER BY id;

SELECT id, col_timestamptz, col_time, col_char, col_smallserial FROM public.dtype_test_ducklake ORDER BY id;

SELECT duckpipe.remove_table('public.dtype_test', false);
DROP TABLE public.dtype_test_ducklake;
DROP TABLE dtype_test;

SET client_min_messages = warning;
SELECT duckpipe.stop_worker();
RESET client_min_messages;

CREATE EXTENSION pg_ducklake_sync CASCADE;
SELECT ducklake_sync.start_worker();

CREATE TABLE stream_test (id int primary key, val text);
CREATE TABLE ducklake.stream_test (id int, val text) USING ducklake;

SELECT ducklake_sync.add_table('public.stream_test', 'ducklake.stream_test', 'default', false);

INSERT INTO stream_test VALUES (1, 'one');
INSERT INTO stream_test VALUES (2, 'two');

SELECT pg_sleep(2);

SELECT * FROM ducklake.stream_test ORDER BY id;

-- Test UPDATE (should be handled as DELETE + INSERT)
UPDATE stream_test SET val = 'updated_two' WHERE id = 2;

SELECT pg_sleep(2);

SELECT * FROM ducklake.stream_test ORDER BY id;

DELETE FROM stream_test WHERE id = 1;

SELECT pg_sleep(2);

SELECT * FROM ducklake.stream_test ORDER BY id;

SELECT ducklake_sync.remove_table('public.stream_test', false);
DROP TABLE ducklake.stream_test;
DROP TABLE stream_test;

SELECT ducklake_sync.stop_worker();
DROP EXTENSION pg_ducklake_sync CASCADE;

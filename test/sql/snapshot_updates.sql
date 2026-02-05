-- Test snapshot copy (initial load) and UPDATE sync
CREATE EXTENSION pg_ducklake_sync CASCADE;
SELECT ducklake_sync.start_worker();

CREATE TABLE existing_data (id int primary key, val text);
INSERT INTO existing_data VALUES (1, 'one'), (2, 'two'), (3, 'three');

CREATE TABLE ducklake.existing_data (id int, val text) USING ducklake;

SELECT ducklake_sync.add_table('public.existing_data', 'ducklake.existing_data', 'default', true);

SELECT pg_sleep(2);

SELECT * FROM ducklake.existing_data ORDER BY id;

UPDATE existing_data SET val = 'updated_two' WHERE id = 2;

SELECT pg_sleep(4);

SELECT * FROM ducklake.existing_data ORDER BY id;

SELECT ducklake_sync.remove_table('public.existing_data', false);
DROP TABLE ducklake.existing_data;
DROP TABLE existing_data;

SELECT ducklake_sync.stop_worker();
DROP EXTENSION pg_ducklake_sync CASCADE;

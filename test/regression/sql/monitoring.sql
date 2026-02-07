-- Test monitoring functions
CREATE EXTENSION pg_duckpipe CASCADE;

-- Test duckpipe.groups() returns default group info
SELECT name, publication, slot_name, enabled, table_count FROM duckpipe.groups();

-- Create and add table
CREATE TABLE mon_test (id int primary key, val text);

SELECT duckpipe.add_table('public.mon_test', NULL, 'default', false);

-- Test duckpipe.tables() returns the mapping
SELECT source_table, target_table, sync_group, enabled FROM duckpipe.tables();

-- Test duckpipe.status() returns state info
SELECT sync_group, source_table, target_table, state, enabled FROM duckpipe.status();

-- Cleanup
SELECT duckpipe.remove_table('public.mon_test', false);
DROP TABLE public.mon_test_ducklake;
DROP TABLE mon_test;

DROP EXTENSION pg_duckpipe CASCADE;

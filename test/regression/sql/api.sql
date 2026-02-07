-- Test API functions and metadata management
CREATE EXTENSION pg_duckpipe CASCADE;

-- Verify default group exists
SELECT name, publication, slot_name FROM duckpipe.sync_groups;

-- Test create_group (STRICT bug fixed - should now succeed)
SELECT duckpipe.create_group('analytics');
SELECT name, publication, slot_name FROM duckpipe.sync_groups WHERE name = 'analytics';

-- Test add_table to the new group
CREATE TABLE t1 (id int primary key, val text);

SELECT duckpipe.add_table('public.t1', NULL, 'analytics', false);

SELECT source_schema, source_table, group_id, state
FROM duckpipe.table_mappings
WHERE source_table = 't1';

-- Test move_table
SELECT duckpipe.move_table('public.t1', 'default');

SELECT m.source_schema, m.source_table, g.name as group_name
FROM duckpipe.table_mappings m
JOIN duckpipe.sync_groups g ON m.group_id = g.id
WHERE m.source_table = 't1';

-- Test remove_table
SELECT duckpipe.remove_table('public.t1', false);

SELECT count(*) FROM duckpipe.table_mappings WHERE source_table = 't1';

-- Test enable/disable group
SELECT duckpipe.disable_group('analytics');
SELECT enabled FROM duckpipe.sync_groups WHERE name = 'analytics';

SELECT duckpipe.enable_group('analytics');
SELECT enabled FROM duckpipe.sync_groups WHERE name = 'analytics';

-- Test drop_group cleans up
SELECT duckpipe.drop_group('analytics');
SELECT count(*) FROM duckpipe.sync_groups WHERE name = 'analytics';

DROP TABLE public.t1_ducklake;
DROP TABLE t1;
DROP EXTENSION pg_duckpipe CASCADE;

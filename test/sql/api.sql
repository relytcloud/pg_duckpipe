-- Test API functions and metadata management
CREATE EXTENSION pg_ducklake_sync CASCADE;

SELECT name, publication, slot_name FROM ducklake_sync.sync_groups;

SELECT ducklake_sync.create_group('analytics');
SELECT name, publication, slot_name FROM ducklake_sync.sync_groups WHERE name = 'analytics';

CREATE TABLE t1 (id int primary key, val text);

SELECT ducklake_sync.add_table('public.t1', 'ducklake.t1', 'analytics', false);

SELECT source_schema, source_table, group_id, state 
FROM ducklake_sync.table_mappings 
WHERE source_table = 't1';

SELECT ducklake_sync.move_table('public.t1', 'default');

SELECT m.source_schema, m.source_table, g.name as group_name
FROM ducklake_sync.table_mappings m
JOIN ducklake_sync.sync_groups g ON m.group_id = g.id
WHERE m.source_table = 't1';

SELECT ducklake_sync.remove_table('public.t1', false);

SELECT count(*) FROM ducklake_sync.table_mappings WHERE source_table = 't1';

-- Test enable/disable group
SELECT ducklake_sync.disable_group('analytics');
SELECT enabled FROM ducklake_sync.sync_groups WHERE name = 'analytics';

SELECT ducklake_sync.enable_group('analytics');
SELECT enabled FROM ducklake_sync.sync_groups WHERE name = 'analytics';

SELECT ducklake_sync.drop_group('analytics');
SELECT count(*) FROM ducklake_sync.sync_groups WHERE name = 'analytics';

DROP TABLE t1;
DROP EXTENSION pg_ducklake_sync CASCADE;

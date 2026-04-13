-- Test ducklake_catalog_connstr config key (string type)
-- Exercises global and per-group CRUD for the remote DuckLake catalog connstr.

-- 1. Initially unset (not in global_config table)
SELECT duckpipe.get_config('ducklake_catalog_connstr');

-- 2. Set at global level
SELECT duckpipe.set_config('ducklake_catalog_connstr',
    'host=analytics.example.com port=5432 dbname=lake user=catalog');
SELECT duckpipe.get_config('ducklake_catalog_connstr');

-- 3. Visible in full config JSON
SELECT duckpipe.get_config()::jsonb ? 'ducklake_catalog_connstr' AS has_key;

-- 4. Empty string rejected
SELECT duckpipe.set_config('ducklake_catalog_connstr', '');

-- 5. Per-group override
SELECT duckpipe.set_group_config('default', 'ducklake_catalog_connstr',
    'host=group-catalog port=5432 dbname=mydb');
SELECT duckpipe.get_group_config('default', 'ducklake_catalog_connstr');

-- 6. Resolved JSON shows group override
SELECT duckpipe.get_group_config('default')::jsonb->>'ducklake_catalog_connstr' AS catalog_connstr;

-- 7. Cleanup
DELETE FROM duckpipe.global_config WHERE key = 'ducklake_catalog_connstr';
UPDATE duckpipe.sync_groups SET config = '{}'::jsonb WHERE name = 'default';

-- 8. Verify unset after cleanup
SELECT duckpipe.get_config('ducklake_catalog_connstr');
SELECT duckpipe.get_group_config('default', 'ducklake_catalog_connstr');

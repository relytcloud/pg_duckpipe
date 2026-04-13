-- Test ducklake_catalog_connstr: config CRUD + E2E flush through configured connstr.

------------------------------------------------------------
-- Part 1: Config CRUD (global + per-group)
------------------------------------------------------------

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

-- 7. Cleanup CRUD state
DELETE FROM duckpipe.global_config WHERE key = 'ducklake_catalog_connstr';
UPDATE duckpipe.sync_groups SET config = '{}'::jsonb WHERE name = 'default';

-- 8. Verify unset after cleanup
SELECT duckpipe.get_config('ducklake_catalog_connstr');
SELECT duckpipe.get_group_config('default', 'ducklake_catalog_connstr');

------------------------------------------------------------
-- Part 2: E2E — flush through configured catalog connstr
------------------------------------------------------------
-- Creates a dedicated sync group with ducklake_catalog_connstr set to a Unix
-- socket loopback connstr (same PG). A fresh bgworker reads the config at
-- startup and uses it for DuckLake ATTACH. If not propagated, no data arrives.

DO $$
DECLARE
    v_user text;
    v_port text;
    v_sockdir text;
BEGIN
    v_user := current_user;
    v_port := current_setting('port');
    v_sockdir := current_setting('unix_socket_directories');
    PERFORM duckpipe.create_group('catalog_e2e');
    PERFORM duckpipe.set_group_config('catalog_e2e', 'ducklake_catalog_connstr',
        'host=' || v_sockdir || ' port=' || v_port
        || ' dbname=regression user=' || v_user);
    PERFORM duckpipe.set_group_config('catalog_e2e', 'flush_interval_ms', '1000');
END;
$$;

SELECT duckpipe.get_group_config('catalog_e2e', 'ducklake_catalog_connstr') IS NOT NULL AS has_connstr;

CREATE TABLE t_catalog_e2e(id int primary key, val text);
SELECT duckpipe.add_table('t_catalog_e2e', sync_group => 'catalog_e2e', copy_data => false);
SELECT pg_sleep(2);

INSERT INTO t_catalog_e2e VALUES (1, 'hello'), (2, 'world');
SELECT pg_sleep(3);

SELECT id, val FROM public.t_catalog_e2e_ducklake ORDER BY id;

-- Cleanup
SET client_min_messages = warning;
SELECT duckpipe.remove_table('t_catalog_e2e', true, 'catalog_e2e');
SELECT pg_sleep(1);
SELECT duckpipe.drop_group('catalog_e2e');
RESET client_min_messages;
DROP TABLE t_catalog_e2e;

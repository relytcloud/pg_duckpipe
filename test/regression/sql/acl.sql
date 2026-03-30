-- Test access control: target table permissions, schema USAGE, function ACLs

-- Create a non-superuser role that owns the source table
CREATE ROLE acl_owner LOGIN;
GRANT CREATE ON SCHEMA public TO acl_owner;

-- Create source table as superuser, then transfer ownership
CREATE TABLE acl_source (id int primary key, val text);
ALTER TABLE acl_source OWNER TO acl_owner;

-- Add table to duckpipe (no copy — avoids needing a running worker)
SELECT duckpipe.add_table('public.acl_source', NULL, 'default', false);

-- Verify the target table exists
SELECT count(*) FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relname = 'acl_source_ducklake';

-- Verify acl_owner has SELECT privilege on the target table
SELECT has_table_privilege('acl_owner', 'public.acl_source_ducklake', 'SELECT') AS has_select;

-- Verify acl_owner does NOT have INSERT/UPDATE/DELETE (read-only grant)
SELECT has_table_privilege('acl_owner', 'public.acl_source_ducklake', 'INSERT') AS has_insert;
SELECT has_table_privilege('acl_owner', 'public.acl_source_ducklake', 'UPDATE') AS has_update;
SELECT has_table_privilege('acl_owner', 'public.acl_source_ducklake', 'DELETE') AS has_delete;

-- Verify management functions are NOT callable by non-superusers
SET ROLE acl_owner;
SELECT duckpipe.add_table('public.acl_source');  -- should fail: permission denied
RESET ROLE;

-- Verify config-write functions are NOT callable by non-superusers
SET ROLE acl_owner;
SELECT duckpipe.set_config('duckdb_threads', '2');  -- should fail: permission denied
SELECT duckpipe.set_group_config('default', 'duckdb_threads', '2');  -- should fail: permission denied
RESET ROLE;

-- Verify config-read functions ARE callable by non-superusers
SET ROLE acl_owner;
SELECT duckpipe.get_config('duckdb_threads') IS NOT NULL OR TRUE AS can_call_get_config;
SELECT duckpipe.get_group_config('default', 'duckdb_threads') IS NOT NULL OR TRUE AS can_call_get_group_config;
RESET ROLE;

-- Verify monitoring functions ARE callable by non-superusers (schema USAGE granted to PUBLIC)
SET ROLE acl_owner;
SELECT count(*) >= 0 AS can_call_groups FROM duckpipe.groups();
SELECT count(*) >= 0 AS can_call_tables FROM duckpipe.tables();
SELECT count(*) >= 0 AS can_call_status FROM duckpipe.status();
RESET ROLE;

-- Verify non-superusers cannot directly query internal tables
SET ROLE acl_owner;
SELECT * FROM duckpipe.sync_groups;  -- should fail: permission denied
SELECT * FROM duckpipe.table_mappings;  -- should fail: permission denied
SELECT * FROM duckpipe.global_config;  -- should fail: permission denied
RESET ROLE;

-- Cleanup
SELECT duckpipe.remove_table('public.acl_source', false);
DROP TABLE public.acl_source_ducklake;
DROP TABLE acl_source;
REVOKE CREATE ON SCHEMA public FROM acl_owner;
DROP ROLE acl_owner;

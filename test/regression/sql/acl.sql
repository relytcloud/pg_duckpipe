-- Test access control: target table permissions
-- Verify that add_table grants SELECT on the target to the source table owner.

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

-- Verify that non-superusers cannot access the duckpipe schema (no USAGE grant)
SET ROLE acl_owner;
SELECT duckpipe.add_table('public.acl_source');  -- should fail: permission denied
RESET ROLE;

-- Cleanup
SELECT duckpipe.remove_table('public.acl_source', false);
DROP TABLE public.acl_source_ducklake;
DROP TABLE acl_source;
REVOKE CREATE ON SCHEMA public FROM acl_owner;
DROP ROLE acl_owner;

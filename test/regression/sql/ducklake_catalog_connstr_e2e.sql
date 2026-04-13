-- E2E test: verify flush threads use ducklake_catalog_connstr for DuckDB ATTACH.
--
-- Creates a dedicated sync group with ducklake_catalog_connstr set to a Unix
-- socket loopback connstr (same PG instance). A fresh bgworker is spawned for
-- this group, reads the config at startup, and uses it for DuckLake ATTACH.
-- If the connstr is not propagated correctly, the flush would fail and no
-- data would appear in the target table.

-- Set up: create group with catalog connstr override + fast flush
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

-- Verify config is set
SELECT duckpipe.get_group_config('catalog_e2e', 'ducklake_catalog_connstr') IS NOT NULL AS has_connstr;

-- Create source table
CREATE TABLE t_catalog_e2e(id int primary key, val text);

-- Add table (auto-starts bgworker which reads ducklake_catalog_connstr)
SELECT duckpipe.add_table('t_catalog_e2e', sync_group => 'catalog_e2e', copy_data => false);
SELECT pg_sleep(2);

-- Insert and wait for streaming flush
INSERT INTO t_catalog_e2e VALUES (1, 'hello'), (2, 'world');
SELECT pg_sleep(3);

-- Verify data flushed through the configured catalog connstr
SELECT id, val FROM public.t_catalog_e2e_ducklake ORDER BY id;

-- Cleanup
SET client_min_messages = warning;
SELECT duckpipe.remove_table('t_catalog_e2e', true, 'catalog_e2e');
SELECT pg_sleep(1);
SELECT duckpipe.drop_group('catalog_e2e');
RESET client_min_messages;
DROP TABLE t_catalog_e2e;

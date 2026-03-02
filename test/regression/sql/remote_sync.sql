-- Test remote sync: exercises the tokio-postgres DDL code path by using
-- a Unix socket loopback conninfo pointing at the same PG instance.
--
-- pg_regress disables TCP (listen_addresses=''), so we use Unix sockets.
-- This still validates the full remote code path: create_group with conninfo,
-- add_table with remote DDL via tokio-postgres, groups() SRF conninfo column,
-- streaming INSERT sync, remove_table/drop_group remote cleanup.
--
-- NOTE: DML sync (UPDATE/DELETE) is skipped because concurrent DuckLake commits
-- from the bgworker flush thread and the test client trigger a known crash in
-- pg_ducklake DucklakeUtilityHook. INSERT-only streaming is tested instead.

-- Disable the default group (its slot doesn't exist on a fresh instance)
SELECT duckpipe.disable_group('default');

SELECT duckpipe.start_worker();

-- Build conninfo using the PG instance's Unix socket directory and port.
-- The temp instance socket lives in a pg_regress-created temp directory.
DO $$
DECLARE
    v_user text;
    v_port text;
    v_sockdir text;
BEGIN
    v_user := current_user;
    v_port := current_setting('port');
    v_sockdir := current_setting('unix_socket_directories');
    PERFORM duckpipe.create_group(
        'remote_test',
        conninfo => 'host=' || v_sockdir || ' port=' || v_port
                     || ' dbname=regression user=' || v_user
    );
END;
$$;

-- Verify group created with conninfo
SELECT name, publication, slot_name, conninfo IS NOT NULL AS has_conninfo
FROM duckpipe.sync_groups WHERE name = 'remote_test';

-- Verify slot + publication created on "remote" (same PG via tokio-postgres)
SELECT slot_name FROM pg_replication_slots
WHERE slot_name = 'duckpipe_slot_remote_test';

SELECT pubname FROM pg_publication
WHERE pubname = 'duckpipe_pub_remote_test';

-- Verify groups() SRF includes conninfo
SELECT name, conninfo IS NOT NULL AS has_conninfo
FROM duckpipe.groups() WHERE name = 'remote_test';

-- Create source table (on the "remote" = same PG)
CREATE TABLE t_remote(id int primary key, val text);

-- Add table to remote group (without initial snapshot for simplicity)
SELECT duckpipe.add_table('t_remote', sync_group => 'remote_test', copy_data => false);

-- Verify target table was created (explicit DDL path, not LIKE)
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 't_remote_ducklake'
ORDER BY ordinal_position;

-- Verify mapping exists with remote source OID
SELECT source_schema, source_table, target_table, state,
       source_oid IS NOT NULL AS has_oid
FROM duckpipe.table_mappings
WHERE source_table = 't_remote';

-- Verify table added to remote publication
SELECT tablename FROM pg_publication_tables
WHERE pubname = 'duckpipe_pub_remote_test' AND tablename = 't_remote';

-- Wait for streaming sync (INSERT only — avoids concurrent commit crash)
INSERT INTO t_remote VALUES (1, 'alpha'), (2, 'beta');
SELECT pg_sleep(3);

-- Verify data synced
SELECT * FROM public.t_remote_ducklake ORDER BY id;

-- Cleanup: stop worker first to avoid concurrent commit issues
SET client_min_messages = warning;
SELECT duckpipe.stop_worker();
RESET client_min_messages;

-- Remove table (should drop from remote publication)
SELECT duckpipe.remove_table('t_remote');

-- Verify removed from publication
SELECT count(*) AS pub_table_count FROM pg_publication_tables
WHERE pubname = 'duckpipe_pub_remote_test' AND tablename = 't_remote';

-- Drop group (should drop remote slot + publication)
SELECT duckpipe.drop_group('remote_test');

-- Verify slot + publication removed
SELECT count(*) AS slot_count FROM pg_replication_slots
WHERE slot_name = 'duckpipe_slot_remote_test';

SELECT count(*) AS pub_count FROM pg_publication
WHERE pubname = 'duckpipe_pub_remote_test';

-- Clean up local objects
DROP TABLE IF EXISTS public.t_remote_ducklake;
DROP TABLE t_remote;

-- Complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_duckpipe" to load this file. \quit

-- Schema is created by the extension (schema = 'duckpipe' in control file)

-- Sync groups
CREATE TABLE duckpipe.sync_groups (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    publication     TEXT NOT NULL UNIQUE,
    slot_name       TEXT NOT NULL UNIQUE,
    enabled         BOOLEAN DEFAULT true,
    confirmed_lsn   pg_lsn,
    last_sync_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Table mappings
CREATE TABLE duckpipe.table_mappings (
    id              SERIAL PRIMARY KEY,
    group_id        INTEGER NOT NULL REFERENCES duckpipe.sync_groups(id),
    source_schema   TEXT NOT NULL,
    source_table    TEXT NOT NULL,
    target_schema   TEXT NOT NULL,
    target_table    TEXT NOT NULL,
    state           TEXT NOT NULL DEFAULT 'PENDING',
    snapshot_lsn    pg_lsn,
    enabled         BOOLEAN DEFAULT true,
    rows_synced     BIGINT DEFAULT 0,
    last_sync_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(source_schema, source_table)
);

-- Default sync group
INSERT INTO duckpipe.sync_groups (name, publication, slot_name)
VALUES ('default',
        'duckpipe_pub_' || current_database(),
        'duckpipe_slot_' || current_database());

-- API Functions
CREATE FUNCTION duckpipe.create_group(
    name TEXT,
    publication TEXT DEFAULT NULL,
    slot_name TEXT DEFAULT NULL
) RETURNS TEXT
AS 'MODULE_PATHNAME', 'duckpipe_create_group'
LANGUAGE C;

CREATE FUNCTION duckpipe.drop_group(
    name TEXT,
    drop_slot BOOLEAN DEFAULT true
) RETURNS void
AS 'MODULE_PATHNAME', 'duckpipe_drop_group'
LANGUAGE C STRICT;

CREATE FUNCTION duckpipe.enable_group(name TEXT) RETURNS void
AS 'MODULE_PATHNAME', 'duckpipe_enable_group'
LANGUAGE C STRICT;

CREATE FUNCTION duckpipe.disable_group(name TEXT) RETURNS void
AS 'MODULE_PATHNAME', 'duckpipe_disable_group'
LANGUAGE C STRICT;

CREATE FUNCTION duckpipe.add_table(
    source_table TEXT,
    target_table TEXT DEFAULT NULL,
    sync_group TEXT DEFAULT 'default',
    copy_data BOOLEAN DEFAULT true
) RETURNS void
AS 'MODULE_PATHNAME', 'duckpipe_add_table'
LANGUAGE C;

CREATE FUNCTION duckpipe.remove_table(
    source_table TEXT,
    drop_target BOOLEAN DEFAULT false
) RETURNS void
AS 'MODULE_PATHNAME', 'duckpipe_remove_table'
LANGUAGE C;

CREATE FUNCTION duckpipe.move_table(
    source_table TEXT,
    new_group TEXT
) RETURNS void
AS 'MODULE_PATHNAME', 'duckpipe_move_table'
LANGUAGE C STRICT;

CREATE FUNCTION duckpipe.resync_table(source_table TEXT) RETURNS void
AS 'MODULE_PATHNAME', 'duckpipe_resync_table'
LANGUAGE C STRICT;

-- Monitoring Views
CREATE FUNCTION duckpipe.groups() RETURNS TABLE(
    name TEXT,
    publication TEXT,
    slot_name TEXT,
    enabled BOOLEAN,
    table_count INTEGER,
    lag_bytes BIGINT,
    last_sync TIMESTAMPTZ
)
AS 'MODULE_PATHNAME', 'duckpipe_groups'
LANGUAGE C STRICT;

CREATE FUNCTION duckpipe.tables() RETURNS TABLE(
    source_table TEXT,
    target_table TEXT,
    sync_group TEXT,
    enabled BOOLEAN,
    rows_synced BIGINT,
    last_sync TIMESTAMPTZ
)
AS 'MODULE_PATHNAME', 'duckpipe_tables'
LANGUAGE C STRICT;

CREATE FUNCTION duckpipe.status() RETURNS TABLE(
    sync_group TEXT,
    source_table TEXT,
    target_table TEXT,
    state TEXT,
    enabled BOOLEAN,
    rows_synced BIGINT,
    last_sync TIMESTAMPTZ
)
AS 'MODULE_PATHNAME', 'duckpipe_status'
LANGUAGE C STRICT;

CREATE FUNCTION duckpipe.start_worker() RETURNS void
AS 'MODULE_PATHNAME', 'duckpipe_start_worker'
LANGUAGE C;

CREATE FUNCTION duckpipe.stop_worker() RETURNS void
AS 'MODULE_PATHNAME', 'duckpipe_stop_worker'
LANGUAGE C;

-- Restrict sensitive functions to superusers / extension owner
REVOKE ALL ON FUNCTION duckpipe.create_group(TEXT, TEXT, TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION duckpipe.drop_group(TEXT, BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION duckpipe.enable_group(TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION duckpipe.disable_group(TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION duckpipe.add_table(TEXT, TEXT, TEXT, BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION duckpipe.remove_table(TEXT, BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION duckpipe.move_table(TEXT, TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION duckpipe.resync_table(TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION duckpipe.start_worker() FROM PUBLIC;
REVOKE ALL ON FUNCTION duckpipe.stop_worker() FROM PUBLIC;

-- Complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_ducklake_sync" to load this file. \quit

-- Schema is created by the extension (schema = 'ducklake_sync' in control file)

-- Sync groups
CREATE TABLE ducklake_sync.sync_groups (
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
CREATE TABLE ducklake_sync.table_mappings (
    id              SERIAL PRIMARY KEY,
    group_id        INTEGER NOT NULL REFERENCES ducklake_sync.sync_groups(id),
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

-- Relation cache
CREATE TABLE ducklake_sync.relation_cache (
    group_id        INTEGER REFERENCES ducklake_sync.sync_groups(id),
    remote_relid    INTEGER,
    source_schema   TEXT,
    source_table    TEXT,
    columns         JSONB,
    PRIMARY KEY (group_id, remote_relid)
);

-- Default sync group
INSERT INTO ducklake_sync.sync_groups (name, publication, slot_name)
VALUES ('default', 
        'ducklake_sync_pub_' || current_database(), 
        'ducklake_sync_slot_' || current_database());

-- API Functions
CREATE FUNCTION ducklake_sync.create_group(
    name TEXT,
    publication TEXT DEFAULT NULL,
    slot_name TEXT DEFAULT NULL
) RETURNS TEXT
AS 'MODULE_PATHNAME', 'ducklake_sync_create_group'
LANGUAGE C STRICT;

CREATE FUNCTION ducklake_sync.drop_group(
    name TEXT,
    drop_slot BOOLEAN DEFAULT true
) RETURNS void
AS 'MODULE_PATHNAME', 'ducklake_sync_drop_group'
LANGUAGE C STRICT;

CREATE FUNCTION ducklake_sync.enable_group(name TEXT) RETURNS void
AS 'MODULE_PATHNAME', 'ducklake_sync_enable_group'
LANGUAGE C STRICT;

CREATE FUNCTION ducklake_sync.disable_group(name TEXT) RETURNS void
AS 'MODULE_PATHNAME', 'ducklake_sync_disable_group'
LANGUAGE C STRICT;

CREATE FUNCTION ducklake_sync.add_table(
    source_table TEXT,
    target_table TEXT DEFAULT NULL,
    sync_group TEXT DEFAULT 'default',
    copy_data BOOLEAN DEFAULT true
) RETURNS void
AS 'MODULE_PATHNAME', 'ducklake_sync_add_table'
LANGUAGE C;

CREATE FUNCTION ducklake_sync.remove_table(
    source_table TEXT,
    drop_target BOOLEAN DEFAULT false
) RETURNS void
AS 'MODULE_PATHNAME', 'ducklake_sync_remove_table'
LANGUAGE C;

CREATE FUNCTION ducklake_sync.move_table(
    source_table TEXT,
    new_group TEXT
) RETURNS void
AS 'MODULE_PATHNAME', 'ducklake_sync_move_table'
LANGUAGE C STRICT;

CREATE FUNCTION ducklake_sync.resync_table(source_table TEXT) RETURNS void
AS 'MODULE_PATHNAME', 'ducklake_sync_resync_table'
LANGUAGE C STRICT;

-- Monitoring Views
CREATE FUNCTION ducklake_sync.groups() RETURNS TABLE(
    name TEXT,
    publication TEXT,
    slot_name TEXT,
    enabled BOOLEAN,
    table_count INTEGER,
    lag_bytes BIGINT,
    last_sync TIMESTAMPTZ
)
AS 'MODULE_PATHNAME', 'ducklake_sync_groups'
LANGUAGE C STRICT;

CREATE FUNCTION ducklake_sync.tables() RETURNS TABLE(
    source_table TEXT,
    target_table TEXT,
    sync_group TEXT,
    enabled BOOLEAN,
    rows_synced BIGINT,
    last_sync TIMESTAMPTZ
)
AS 'MODULE_PATHNAME', 'ducklake_sync_tables'
LANGUAGE C STRICT;

CREATE FUNCTION ducklake_sync.status() RETURNS TABLE(
    sync_group TEXT,
    source_table TEXT,
    target_table TEXT,
    enabled BOOLEAN,
    lag_bytes BIGINT,
    rows_synced BIGINT,
    last_sync TIMESTAMPTZ
)
AS 'MODULE_PATHNAME', 'ducklake_sync_status'
LANGUAGE C STRICT;

CREATE FUNCTION ducklake_sync.start_worker() RETURNS void
AS 'MODULE_PATHNAME', 'ducklake_sync_start_worker'
LANGUAGE C;

CREATE FUNCTION ducklake_sync.stop_worker() RETURNS void
AS 'MODULE_PATHNAME', 'ducklake_sync_stop_worker'
LANGUAGE C;

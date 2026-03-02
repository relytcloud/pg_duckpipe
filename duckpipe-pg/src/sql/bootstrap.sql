-- Bootstrap SQL for pg_duckpipe
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
    applied_lsn     pg_lsn,
    enabled         BOOLEAN DEFAULT true,
    rows_synced     BIGINT DEFAULT 0,
    queued_changes  BIGINT DEFAULT 0,
    last_sync_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    source_oid      BIGINT UNIQUE,
    error_message   TEXT,
    retry_at        TIMESTAMPTZ,
    consecutive_failures INTEGER DEFAULT 0,
    snapshot_duration_ms BIGINT,
    snapshot_rows        BIGINT,
    UNIQUE(source_schema, source_table)
);

-- Worker runtime state (single-row, updated each sync cycle)
CREATE TABLE duckpipe.worker_state (
    id                   INTEGER PRIMARY KEY DEFAULT 1,
    total_queued_changes BIGINT DEFAULT 0,
    is_backpressured     BOOLEAN DEFAULT false,
    updated_at           TIMESTAMPTZ
);

INSERT INTO duckpipe.worker_state (id) VALUES (1);

-- Default sync group
INSERT INTO duckpipe.sync_groups (name, publication, slot_name)
VALUES ('default',
        'duckpipe_pub_' || current_database(),
        'duckpipe_slot_' || current_database());

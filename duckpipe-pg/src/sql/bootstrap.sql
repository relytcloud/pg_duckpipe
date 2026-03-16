-- Bootstrap SQL for pg_duckpipe
-- Schema is created by the extension (schema = 'duckpipe' in control file)

-- Sync groups
CREATE TABLE duckpipe.sync_groups (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    publication     TEXT NOT NULL UNIQUE,
    slot_name       TEXT NOT NULL UNIQUE,
    conninfo        TEXT,
    enabled         BOOLEAN DEFAULT true,
    mode            TEXT NOT NULL DEFAULT 'bgworker' CHECK (mode IN ('bgworker', 'daemon')),
    config          JSONB NOT NULL DEFAULT '{}'::jsonb,
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
    duckdb_memory_bytes  BIGINT DEFAULT 0,
    UNIQUE(source_schema, source_table)
);

CREATE INDEX ON duckpipe.table_mappings (group_id);

-- Worker runtime state (one row per sync group, upserted each sync cycle)
CREATE TABLE duckpipe.worker_state (
    group_id             INTEGER PRIMARY KEY REFERENCES duckpipe.sync_groups(id) ON DELETE CASCADE,
    total_queued_changes BIGINT DEFAULT 0,
    is_backpressured     BOOLEAN DEFAULT false,
    updated_at           TIMESTAMPTZ
);

-- Global config (key-value rows, seeded with defaults)
CREATE TABLE duckpipe.global_config (
    key    TEXT PRIMARY KEY,
    value  TEXT NOT NULL
);
INSERT INTO duckpipe.global_config VALUES
    ('duckdb_buffer_memory_mb', '16'),
    ('duckdb_flush_memory_mb', '512'),
    ('duckdb_threads', '1'),
    ('flush_interval_ms', '5000'),
    ('flush_batch_threshold', '50000'),
    ('max_concurrent_flushes', '4'),
    ('max_queued_changes', '500000');

-- Default sync group
INSERT INTO duckpipe.sync_groups (name, publication, slot_name)
VALUES ('default',
        'duckpipe_pub_' || current_database(),
        'duckpipe_slot_' || current_database());

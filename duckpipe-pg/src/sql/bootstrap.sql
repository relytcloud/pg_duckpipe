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
    source_oid      BIGINT,
    target_oid      BIGINT,
    error_message   TEXT,
    retry_at        TIMESTAMPTZ,
    consecutive_failures INTEGER DEFAULT 0,
    snapshot_duration_ms BIGINT,
    snapshot_rows        BIGINT,
    source_label    TEXT,
    sync_mode       TEXT NOT NULL DEFAULT 'upsert' CHECK (sync_mode IN ('upsert', 'append')),
    config          JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE(group_id, source_schema, source_table),
    UNIQUE(group_id, source_oid)
);

CREATE INDEX ON duckpipe.table_mappings (group_id);


-- Global config (key-value rows, seeded with defaults)
CREATE TABLE duckpipe.global_config (
    key    TEXT PRIMARY KEY,
    value  TEXT NOT NULL
);
INSERT INTO duckpipe.global_config VALUES
    ('drain_poll_ms', '100'),
    ('duckdb_buffer_memory_mb', '16'),
    ('duckdb_flush_memory_mb', '512'),
    ('duckdb_threads', '1'),
    ('flush_interval_ms', '5000'),
    ('flush_batch_threshold', '50000'),
    ('max_concurrent_flushes', '4'),
    ('max_queued_bytes', '1000000000');

-- Default sync group
INSERT INTO duckpipe.sync_groups (name, publication, slot_name)
VALUES ('default',
        'duckpipe_pub_' || current_database(),
        'duckpipe_slot_' || current_database());

-- Allow all users to call monitoring functions (management functions have
-- explicit REVOKE ALL FROM PUBLIC so they remain superuser-only).
GRANT USAGE ON SCHEMA duckpipe TO PUBLIC;

-- Keep internal tables private — only accessible via API functions.
REVOKE ALL ON duckpipe.sync_groups FROM PUBLIC;
REVOKE ALL ON duckpipe.table_mappings FROM PUBLIC;
REVOKE ALL ON duckpipe.global_config FROM PUBLIC;

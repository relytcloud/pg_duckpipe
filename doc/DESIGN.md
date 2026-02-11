# pg_duckpipe: PostgreSQL HTAP Sync Extension

## Technical Design Document v4.0

### Date: 2026-02-06

---

## 1. Executive Summary

**pg_duckpipe** enables automatic CDC (Change Data Capture) synchronization from PostgreSQL heap tables (row store) to pg_ducklake DuckLake tables (column store), achieving HTAP within a single PostgreSQL instance.

### Design Philosophy: Simple AND Production-Ready

Key insight: PostgreSQL already provides **both** the production output plugin (pgoutput) **and** the parsing functions (`logicalrep_read_*`). We reuse both.

| Component | Our Approach |
|-----------|--------------|
| Output plugin | **pgoutput** (production, not test_decoding) |
| Protocol parsing | Reuse PostgreSQL's `logicalrep_read_*` functions |
| Data access | `pg_logical_slot_get_binary_changes()` via SPI |
| Execution | On-demand background worker with polling |

---

## 2. Why This Approach?

### Why NOT test_decoding?

test_decoding is explicitly documented as:
> "an **example** of a logical decoding output plugin. **It doesn't do anything especially useful**, but can serve as a starting point for developing your own output plugin."

It's not meant for production.

### Why pgoutput + built-in parsers?

| Aspect | pgoutput | test_decoding |
|--------|----------|---------------|
| Purpose | Production replication | Example/testing |
| Format | Efficient binary | Human-readable text |
| Parsing | Built-in `logicalrep_read_*` | Custom parser needed |
| Schema info | Rich (RELATION messages) | Basic (inline types) |
| TRUNCATE | Supported | Not supported |

**Key realization**: We don't need to implement binary parsing ourselves. PostgreSQL's `logicalrep_read_*` functions (in `proto.c`) do all the work.

---

## 3. Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                      PostgreSQL Instance                          │
│                                                                   │
│  ┌───────────────┐              ┌────────────────────────────┐   │
│  │  Heap Tables  │              │  DuckLake Tables           │   │
│  │  (OLTP)       │              │  (OLAP via pg_ducklake)    │   │
│  └───────┬───────┘              └─────────────▲──────────────┘   │
│          │                                    │                   │
│          │ WAL                                │ INSERT/DELETE     │
│          ▼                                    │                   │
│  ┌─────────────────────┐                     │                   │
│  │ Logical Decoding    │                     │                   │
│  │ Slot (pgoutput)     │                     │                   │
│  └──────────┬──────────┘                     │                   │
│             │                                 │                   │
│             │ Binary (via SPI)                │                   │
│             ▼                                 │                   │
│  ┌───────────────────────────────────────────┴──────────────────┐│
│  │                   Background Worker                           ││
│  │                                                               ││
│  │  1. SPI: pg_logical_slot_get_binary_changes()                ││
│  │  2. Parse: logicalrep_read_insert/update/delete (built-in!)  ││
│  │  3. Batch: Accumulate changes per table in hash table        ││
│  │  4. Apply: DELETE + INSERT to DuckLake via SPI               ││
│  │  5. Checkpoint: Update confirmed_lsn and last_sync_at        ││
│  │                                                               ││
│  └───────────────────────────────────────────────────────────────┘│
└──────────────────────────────────────────────────────────────────┘
```

---

## 4. Protocol Message Flow

pgoutput sends these message types (single-byte identifiers defined in `logicalproto.h`):

```
'B' = BEGIN        - Transaction start
'R' = RELATION     - Table schema definition (cached per poll round)
'I' = INSERT       - Row insert
'U' = UPDATE       - Row update (old key + new tuple)
'D' = DELETE       - Row delete (old key)
'T' = TRUNCATE     - Table truncate
'C' = COMMIT       - Transaction commit (triggers batch flush)
```

Our `decode_message()` processes them:

```c
switch (msgtype) {
case LOGICAL_REP_MSG_RELATION:
    rel = logicalrep_read_rel(buf);
    entry = hash_search(rel_cache, &rel->remoteid, HASH_ENTER, &found);
    entry->rel = rel;
    entry->mapping = NULL;  // reset cached mapping
    break;

case LOGICAL_REP_MSG_INSERT:
    relid = logicalrep_read_insert(buf, &newtup);
    // lookup cached mapping, skip if CATCHUP and lsn <= snapshot_lsn
    batch_add_change(batches, mapping, change, entry->rel);
    break;

case LOGICAL_REP_MSG_UPDATE:
    relid = logicalrep_read_update(buf, &has_old, &oldtup, &newtup);
    // decompose into DELETE(old key) + INSERT(new tuple)
    batch_add_change(batches, mapping, del_change, entry->rel);
    batch_add_change(batches, mapping, ins_change, entry->rel);
    break;

case LOGICAL_REP_MSG_DELETE:
    relid = logicalrep_read_delete(buf, &oldtup);
    batch_add_change(batches, mapping, change, entry->rel);
    break;

case LOGICAL_REP_MSG_COMMIT:
    logicalrep_read_commit(buf, &commit_data);
    group->pending_lsn = commit_data.end_lsn;
    flush_all_batches(batches);  // apply on commit boundary
    break;

case LOGICAL_REP_MSG_TRUNCATE:
    relid_list = logicalrep_read_truncate(buf, &cascade, &restart_seqs);
    flush_all_batches(batches);  // flush pending before truncate
    // DELETE FROM each mapped target table (DuckLake compatibility)
    break;
}
```

---

## 5. Core Components

### 5.1 Metadata Schema

Two tables in the `duckpipe` schema:

```sql
-- Sync groups: each group = 1 publication + 1 replication slot
-- Multiple tables share one group (resource efficient)
CREATE TABLE duckpipe.sync_groups (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    publication     TEXT NOT NULL UNIQUE,
    slot_name       TEXT NOT NULL UNIQUE,
    enabled         BOOLEAN DEFAULT true,
    confirmed_lsn   PG_LSN,
    last_sync_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Table mappings: which source tables sync to which target tables
CREATE TABLE duckpipe.table_mappings (
    id              SERIAL PRIMARY KEY,
    group_id        INTEGER NOT NULL REFERENCES duckpipe.sync_groups(id),
    source_schema   TEXT NOT NULL,
    source_table    TEXT NOT NULL,
    target_schema   TEXT NOT NULL,
    target_table    TEXT NOT NULL,
    state           TEXT NOT NULL DEFAULT 'PENDING',
    snapshot_lsn    PG_LSN,
    enabled         BOOLEAN DEFAULT true,
    rows_synced     BIGINT DEFAULT 0,
    last_sync_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(source_schema, source_table)
);

-- Default sync group (created on extension install, name includes current database)
INSERT INTO duckpipe.sync_groups (name, publication, slot_name)
VALUES ('default',
        'duckpipe_pub_' || current_database(),
        'duckpipe_slot_' || current_database());
```

**Resource usage**:

| Tables | Groups | Publications | Slots |
|--------|--------|--------------|-------|
| 10 | 1 | 1 | 1 |
| 50 | 1 | 1 | 1 |
| 100 (grouped) | 5 | 5 | 5 |

### 5.2 Data Structures (pg_duckpipe.h)

```c
/* Sync state machine: PENDING → SNAPSHOT → CATCHUP → STREAMING */
typedef enum SyncState {
    SYNC_STATE_PENDING,
    SYNC_STATE_SNAPSHOT,
    SYNC_STATE_CATCHUP,
    SYNC_STATE_STREAMING
} SyncState;

typedef struct SyncGroup {
    int id;
    char *name;
    char *publication;
    char *slot_name;
    XLogRecPtr pending_lsn;
} SyncGroup;

typedef struct TableMapping {
    int id;
    int group_id;
    char *source_schema;
    char *source_table;
    char *target_schema;
    char *target_table;
    SyncState state;
    XLogRecPtr snapshot_lsn;
    bool enabled;
} TableMapping;

typedef struct RelationCacheEntry {
    LogicalRepRelId remote_relid;  /* Hash key */
    LogicalRepRelation *rel;       /* Deep copied schema info */
    TableMapping *mapping;         /* Cached mapping (avoids SPI per message) */
} RelationCacheEntry;

typedef struct SyncChange {
    SyncChangeType type;           /* INSERT, UPDATE, DELETE */
    XLogRecPtr lsn;
    List *col_values;              /* List of char* (stringified column values) */
    List *key_values;              /* List of char* (PK values for DELETE) */
} SyncChange;

typedef struct SyncBatch {
    char target_table[NAMEDATALEN * 2 + 2];  /* Hash key: "schema.table\0" */
    List *changes;                             /* List of SyncChange* */
    int count;
    XLogRecPtr last_lsn;
    List *attnames;     /* Column names */
    int nkeyattrs;      /* Number of key attributes */
    int *keyattrs;      /* Key attribute indices (0-based) */
} SyncBatch;
```

### 5.3 Background Worker

The worker is started automatically by `add_table()` if no worker is already running for the current database. It can also be started manually via `start_worker()`. Both paths register a dynamic background worker with `bgw_restart_time = 10` (auto-restart on crash).

```c
void duckpipe_worker_main(Datum main_arg) {
    BackgroundWorkerInitializeConnectionByOid(dboid, InvalidOid, 0);

    SyncMemoryContext = AllocSetContextCreate(TopMemoryContext, ...);

    while (!got_sigterm) {
        if (!duckpipe_enabled) { wait; continue; }

        StartTransactionCommand();
        PushActiveSnapshot(GetTransactionSnapshot());
        SPI_connect_ext(SPI_OPT_NONATOMIC);  /* allows mid-loop commits */

        groups = get_enabled_sync_groups();  /* SPI query */
        foreach(group, groups) {
            processed = process_sync_group(group);
        }

        SPI_finish();
        PopActiveSnapshot();
        CommitTransactionCommand();
        MemoryContextReset(SyncMemoryContext);

        WaitLatch(any_work ? 10ms : poll_interval);
    }
}
```

Key design choices:
- **SPI_OPT_NONATOMIC**: Allows `SPI_commit()`/`SPI_start_transaction()` within the loop for snapshot processing (DuckDB writes require separate transactions).
- **SyncMemoryContext**: Dedicated memory context reset after each round to prevent leaks.
- **Adaptive polling**: 10ms when work was found, `poll_interval` (default 1s) when idle.

### 5.4 Processing Pipeline

`process_sync_group()` handles one group per call:

1. **Snapshot processing**: Find tables in `SNAPSHOT` state, copy data, transition to `CATCHUP`
2. **Create hash tables**: `batches` (keyed by target table name) and `rel_cache` (keyed by pgoutput relation ID)
3. **Fetch WAL changes**: `pg_logical_slot_get_binary_changes(slot, NULL, batch_size_per_group, 'publication_names', pub)`
4. **Decode and batch**: Each message is parsed via `decode_message()`, changes accumulated in per-table `SyncBatch` entries
5. **Flush batches**: `flush_all_batches()` applies all accumulated changes via `apply_batch()`
6. **State transitions**: Promote `CATCHUP` tables to `STREAMING` where `snapshot_lsn <= pending_lsn`
7. **Checkpoint**: Update `confirmed_lsn` and `last_sync_at` in `sync_groups`

### 5.5 Table Mapping Cache

To avoid an SPI query per WAL message, table mappings are cached in `RelationCacheEntry`:

```c
// In decode_message(), for INSERT/UPDATE/DELETE:
if (entry->mapping == NULL)
    entry->mapping = get_table_mapping(group, entry->rel->nspname, entry->rel->relname);
mapping = entry->mapping;
```

Cache lifetime is one poll round (destroyed with `rel_cache` hash table). Cache is invalidated when a new `RELATION` message arrives for the same table (`entry->mapping = NULL`).

### 5.6 Change Application (apply.c)

`apply_batch()` generates SQL for each batch:

- **DELETEs**: One per row: `DELETE FROM target WHERE pk1 = v1 AND pk2 = v2`
- **INSERTs**: Batched multi-row: `INSERT INTO target VALUES (v1, v2, ...), (v3, v4, ...), ...`

INSERTs are accumulated and flushed together. A pending INSERT batch is flushed before any DELETE to maintain correct ordering.

UPDATEs are decomposed into DELETE + INSERT at decode time. This is efficient for column stores where in-place updates are expensive.

Changes are applied within the caller's SPI session (no nested `SPI_connect`).

### 5.7 Batch Memory Management (batch.c)

`SyncChange` structs contain nested Lists (`col_values`, `key_values`) that hold palloc'd strings. A custom `free_change_list()` properly frees the entire hierarchy:

```c
static void free_change_list(List *changes) {
    foreach(lc, changes) {
        SyncChange *change = lfirst(lc);
        // Manually iterate to handle NULL entries (list_free_deep can't)
        if (change->col_values != NIL) {
            foreach(vc, change->col_values) { if (lfirst(vc)) pfree(lfirst(vc)); }
            list_free(change->col_values);
        }
        if (change->key_values != NIL) {
            foreach(vc, change->key_values) { if (lfirst(vc)) pfree(lfirst(vc)); }
            list_free(change->key_values);
        }
        pfree(change);
    }
    list_free(changes);
}
```

---

## 6. Table Sync State Machine

```c
typedef enum SyncState {
    SYNC_STATE_PENDING,    // Registered, not yet processed
    SYNC_STATE_SNAPSHOT,   // Copying existing data
    SYNC_STATE_CATCHUP,    // Skipping WAL changes already in snapshot
    SYNC_STATE_STREAMING   // Normal operation
} SyncState;
```

```
                add_table(copy_data=true)          add_table(copy_data=false)
                       │                                    │
                       ▼                                    │
┌─────────────────────────────────────┐                     │
│            SNAPSHOT                   │                     │
│  1. Record pg_current_wal_lsn()     │                     │
│  2. INSERT INTO target SELECT *     │                     │
│     FROM source                      │                     │
│  3. Commit DuckDB write             │                     │
│  4. Store snapshot_lsn               │                     │
└──────────────────┬──────────────────┘                     │
                   │                                         │
                   ▼                                         │
┌─────────────────────────────────────┐                     │
│            CATCHUP                    │                     │
│  Skip WAL changes with              │                     │
│  lsn <= snapshot_lsn (already in    │                     │
│  snapshot copy).                     │                     │
│  Apply changes with lsn >           │                     │
│  snapshot_lsn normally.              │                     │
└──────────────────┬──────────────────┘                     │
                   │ when pending_lsn >= snapshot_lsn        │
                   ▼                                         ▼
┌─────────────────────────────────────────────────────────────┐
│                      STREAMING                                │
│  Normal operation: apply all changes as they arrive          │
└─────────────────────────────────────────────────────────────┘
```

### CATCHUP Logic

The CATCHUP state prevents duplicate data when a table is added with `copy_data=true`. Between creating the replication slot and completing the snapshot copy, WAL changes may be generated that are already reflected in the copied data.

State is tracked **per table**, not per group. Each table in a group transitions independently based on its own `snapshot_lsn`.

In `decoder.c`, the skip logic:

```c
if (mapping->state == SYNC_STATE_CATCHUP &&
    mapping->snapshot_lsn != InvalidXLogRecPtr &&
    lsn <= mapping->snapshot_lsn)
    return;  /* Already included in snapshot, skip */
```

Note: changes with `lsn > snapshot_lsn` are applied normally even while in CATCHUP state. CATCHUP only skips messages at or below the snapshot boundary.

After a poll round, CATCHUP tables are promoted to STREAMING only when the group's WAL consumption has advanced past their `snapshot_lsn`:

```sql
UPDATE duckpipe.table_mappings SET state = 'STREAMING'
WHERE group_id = $1 AND state = 'CATCHUP'
  AND snapshot_lsn <= $2;  -- $2 = group->pending_lsn (last consumed commit LSN)
```

This LSN-gated promotion prevents premature transition when `batch_size_per_group` limits cause partial WAL consumption across multiple poll rounds. Without it, a CATCHUP table promoted too early would process remaining WAL messages without skip logic, causing duplicate rows.

### resync_table()

Resyncing a table resets it to `SNAPSHOT` state:

1. `TRUNCATE` the target DuckLake table
2. `UPDATE state = 'SNAPSHOT', rows_synced = 0` in `table_mappings`
3. Worker picks it up and re-copies data on next round

---

## 7. API

All functions are in the `duckpipe` schema. Sensitive functions have `REVOKE ALL ... FROM PUBLIC` for security.

### 7.1 Sync Group Management

```sql
-- Create a new sync group (creates publication + replication slot)
duckpipe.create_group(
    name TEXT,
    publication TEXT DEFAULT NULL,   -- defaults to 'duckpipe_pub_{name}'
    slot_name TEXT DEFAULT NULL      -- defaults to 'duckpipe_slot_{name}'
) RETURNS TEXT

-- Drop a sync group and optionally its replication slot
duckpipe.drop_group(name TEXT, drop_slot BOOLEAN DEFAULT true) RETURNS void

-- Enable/disable a sync group
duckpipe.enable_group(name TEXT) RETURNS void
duckpipe.disable_group(name TEXT) RETURNS void
```

### 7.2 Table Sync Management

```sql
-- Add a table to sync (always auto-creates target DuckLake table)
duckpipe.add_table(
    source_table TEXT,               -- 'schema.table' (default schema: public)
    target_table TEXT DEFAULT NULL,   -- defaults to '{schema}.{table}_ducklake', auto-created
    sync_group TEXT DEFAULT 'default',
    copy_data BOOLEAN DEFAULT true   -- SNAPSHOT initial data
) RETURNS void
-- Target table is created via: CREATE TABLE IF NOT EXISTS target (LIKE source) USING ducklake
-- Target schema is created if it doesn't exist

-- Remove a table from sync
duckpipe.remove_table(
    source_table TEXT,
    drop_target BOOLEAN DEFAULT false
) RETURNS void

-- Move a table to a different sync group
duckpipe.move_table(source_table TEXT, new_group TEXT) RETURNS void

-- Force full resync (truncate target + re-snapshot)
duckpipe.resync_table(source_table TEXT) RETURNS void
```

### 7.3 Worker Management

```sql
-- Start the background worker for the current database
duckpipe.start_worker() RETURNS void

-- Stop all running workers
duckpipe.stop_worker() RETURNS void
```

The worker auto-restarts after 10 seconds on crash (`bgw_restart_time = 10`).

### 7.4 Monitoring (Set-Returning Functions)

```sql
-- Group-level overview
duckpipe.groups() RETURNS TABLE(
    name TEXT,
    publication TEXT,
    slot_name TEXT,
    enabled BOOLEAN,
    table_count INTEGER,
    lag_bytes BIGINT,       -- bytes behind current WAL position
    last_sync TIMESTAMPTZ
)

-- Table-level overview
duckpipe.tables() RETURNS TABLE(
    source_table TEXT,
    target_table TEXT,
    sync_group TEXT,
    enabled BOOLEAN,
    rows_synced BIGINT,
    last_sync TIMESTAMPTZ
)

-- Detailed per-table status with state
duckpipe.status() RETURNS TABLE(
    sync_group TEXT,
    source_table TEXT,
    target_table TEXT,
    state TEXT,             -- PENDING/SNAPSHOT/CATCHUP/STREAMING
    enabled BOOLEAN,
    rows_synced BIGINT,
    last_sync TIMESTAMPTZ
)
```

### 7.5 Configuration (GUC Parameters)

```sql
duckpipe.poll_interval = 1000          -- ms between polls (100-3600000)
duckpipe.batch_size_per_table = 1000   -- changes per table before flush
duckpipe.batch_size_per_group = 10000  -- total changes per group per round
duckpipe.enabled = on                  -- enable/disable worker processing
duckpipe.debug_log = off               -- emit critical-path timing logs
```

All GUC parameters are `PGC_SIGHUP` — changeable via `ALTER SYSTEM` or config reload without restart.

---

## 8. Source File Structure

```
pg_duckpipe/
├── pg_duckpipe.control          # Extension metadata (requires pg_duckdb)
├── Makefile                     # PGXS build, delegates tests to test/regression/
├── sql/
│   └── pg_duckpipe--1.0.sql     # Schema, functions, REVOKE statements
├── src/
│   ├── pg_duckpipe.h            # Header: SyncState enum, structs, function decls
│   ├── pg_duckpipe.c            # Entry point (_PG_init), GUC definitions
│   ├── worker.c                 # Background worker loop, snapshot, WAL processing
│   ├── decoder.c                # Message dispatch (logicalrep_read_*), CATCHUP skip
│   ├── batch.c                  # Per-table batch accumulation, memory management
│   ├── apply.c                  # SQL generation for DELETE/INSERT to DuckLake
│   └── api.c                    # SQL function implementations, SRF monitoring views
├── test/
│   └── regression/
│       ├── Makefile             # pg_regress_installcheck with --temp-instance
│       ├── regression.conf      # wal_level=logical, shared_preload_libraries
│       ├── schedule             # Test execution order
│       ├── sql/                 # Test SQL files
│       │   ├── auto_start.sql   # Worker auto-start on add_table()
│       │   ├── api.sql          # Group/table management API tests
│       │   ├── monitoring.sql   # groups()/tables()/status() SRF tests
│       │   ├── streaming.sql    # INSERT/UPDATE/DELETE CDC tests
│       │   ├── snapshot_updates.sql  # Initial copy + concurrent updates
│       │   ├── multiple_tables.sql   # Multiple tables in same group
│       │   ├── data_types.sql   # Various PostgreSQL data types
│       │   ├── resync.sql       # resync_table() functionality
│       │   ├── truncate.sql     # TRUNCATE propagation
│       │   └── premature_catchup.sql  # CATCHUP→STREAMING LSN-gated transition
│       └── expected/            # Expected outputs for pg_regress
└── doc/
    └── DESIGN.md                # This document
```

### Build & Test

```bash
make                        # Build the extension
make install                # Install to PostgreSQL
make installcheck           # Build + install + run all 9 regression tests
make check-regression       # Run regression tests only
make check-regression TEST=api  # Run a single test
make clean-regression       # Remove test artifacts
make format                 # clang-format src/*.c src/*.h
```

Tests run on a temporary PostgreSQL instance (port 5555) with `wal_level=logical` and `shared_preload_libraries = 'pg_duckdb,pg_duckpipe'`.

---

## 9. Performance Characteristics

### 9.1 Overhead

| Aspect | Impact |
|--------|--------|
| Source table writes | Zero (no triggers, WAL already written) |
| WAL retention | Grows until consumed by slot |
| CPU (parsing) | Low (efficient binary format, built-in parsers) |
| Memory | batch_size * avg_row_size per poll round |

### 9.2 Latency

| Setting | Typical Latency |
|---------|-----------------|
| poll_interval=1000ms | 1-3 seconds |
| poll_interval=100ms | 100-500ms |

When work is found, the next poll happens after 10ms (adaptive).

---

## 10. Multi-Table Fairness

### Within a Sync Group

Changes from one slot are interleaved in WAL order. The worker batches by table and flushes when:
1. A COMMIT message arrives (primary flush trigger)
2. End of poll round (flush remaining)

All tables in a group make progress together.

### Across Sync Groups

Groups are processed sequentially (round-robin) within each poll round. Each group processes up to `batch_size_per_group` changes per round.

---

## 11. Security

All mutating API functions (`create_group`, `drop_group`, `enable_group`, `disable_group`, `add_table`, `remove_table`, `move_table`, `resync_table`, `start_worker`, `stop_worker`) have:

```sql
REVOKE ALL ON FUNCTION duckpipe.<func>(...) FROM PUBLIC;
```

Only the extension owner (typically superuser) can call them.

---

## 12. Limitations (V1)

1. **Primary key required** on source tables (needed for UPDATE/DELETE key identification)
2. **No DDL sync** — schema changes need manual intervention
3. **No row filtering** — all rows synced
4. **No transformation** — columns copied as-is
5. **Single worker per database** — one background worker handles all sync groups
6. **Eventual consistency** — configurable latency (1-5 seconds typical)
7. **No streaming replication protocol** — uses polling via `pg_logical_slot_get_binary_changes()` (not walsender)

---

## 13. PostgreSQL Internals Used

Functions reused from `src/backend/replication/logical/proto.c`:

| Function | Purpose |
|----------|---------|
| `logicalrep_read_begin()` | Parse BEGIN message |
| `logicalrep_read_commit()` | Parse COMMIT message (provides end_lsn) |
| `logicalrep_read_rel()` | Parse RELATION message (table schema) |
| `logicalrep_read_insert()` | Parse INSERT message |
| `logicalrep_read_update()` | Parse UPDATE message (old key + new tuple) |
| `logicalrep_read_delete()` | Parse DELETE message (old key) |
| `logicalrep_read_truncate()` | Parse TRUNCATE message (list of relation IDs) |

Data structures from `src/include/replication/logicalproto.h`:

| Structure | Purpose |
|-----------|---------|
| `LogicalRepRelation` | Table schema from RELATION message |
| `LogicalRepTupleData` | Row data (colvalues array + colstatus) |
| `LogicalRepBeginData` | Transaction info from BEGIN |
| `LogicalRepCommitData` | Commit info including end_lsn |

---

## 14. Comparison

### vs. External ETL

| Aspect | pg_duckpipe | External ETL |
|--------|-------------|--------------|
| Deployment | Extension only | Separate service |
| Protocol | Reuses PG internals | Implements from scratch |
| Latency | Lower (in-process) | Higher (network) |
| Scalability | Single node | Distributed |

### vs. Custom Output Plugin

| Aspect | pg_duckpipe (pgoutput) | Custom plugin |
|--------|------------------------|---------------|
| Maintenance | None (PG maintains pgoutput) | Must maintain |
| Parsing | Built-in `logicalrep_read_*` | Write from scratch |
| Protocol changes | Handled by PG | Must track manually |

---

## 15. Example Usage

### Basic (Single Default Group)

```sql
CREATE EXTENSION pg_duckpipe CASCADE;  -- installs pg_duckdb dependency

-- Create source table
CREATE TABLE orders (id SERIAL PRIMARY KEY, customer_id INT, amount NUMERIC);

-- Add to sync (auto-creates orders_ducklake, initial snapshot + streaming)
-- The background worker starts automatically if not already running
SELECT duckpipe.add_table('public.orders');

-- OLTP writes
INSERT INTO orders (customer_id, amount) VALUES (1, 99.99);

-- OLAP reads (after ~1s sync delay)
SELECT customer_id, SUM(amount) FROM orders_ducklake GROUP BY customer_id;

-- Monitor
SELECT * FROM duckpipe.groups();
SELECT * FROM duckpipe.status();
```

### Advanced (Multiple Groups)

```sql
-- Create separate group for high-volume tables
SELECT duckpipe.create_group('analytics');

-- Add tables to separate groups
SELECT duckpipe.add_table('public.events', sync_group := 'analytics');
SELECT duckpipe.add_table('public.users');  -- uses 'default' group

-- Check resource usage: 2 groups = 2 publications + 2 slots
SELECT * FROM duckpipe.groups();
```

### Resync

```sql
-- Force re-snapshot of a table (truncates target, re-copies)
SELECT duckpipe.resync_table('public.orders');
```

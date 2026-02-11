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

---

## 16. Remote Sync Design (Planned)

### 16.1 Motivation

Current pg_duckpipe syncs heap → DuckLake within a single PostgreSQL instance. Remote sync extends this to replicate from a **remote** PostgreSQL server into local DuckLake tables, enabling:

- **Offload analytics**: OLTP on a dedicated server, OLAP on a separate analytics node with DuckLake
- **Data consolidation**: Replicate tables from multiple remote PostgreSQL instances into one analytics database
- **Read replica replacement**: Instead of a full streaming replica, selectively sync specific tables into columnar format

```
┌──────────────────────┐              ┌──────────────────────────────────┐
│  Remote PostgreSQL   │              │  Local PostgreSQL + DuckLake     │
│                      │   network    │                                  │
│  ┌──────────────┐    │              │  ┌────────────────────────────┐  │
│  │ Heap Tables  │    │  ─────────►  │  │ DuckLake Tables (OLAP)     │  │
│  │ (OLTP)       │    │   pgoutput   │  └────────────────────────────┘  │
│  └──────────────┘    │   binary     │                                  │
│                      │              │  ┌────────────────────────────┐  │
│  Replication Slot    │              │  │ pg_duckpipe worker         │  │
│  Publication         │              │  │ (fetches + decodes + apply)│  │
│                      │              │  └────────────────────────────┘  │
└──────────────────────┘              └──────────────────────────────────┘
```

### 16.2 What Changes (and What Doesn't)

The pgoutput binary wire format is identical regardless of whether the source is local or remote. This means the core decoding, batching, and apply pipeline is reusable without modification.

| Component | Local (current) | Remote | Changes needed |
|-----------|-----------------|--------|----------------|
| WAL fetch | `pg_logical_slot_get_binary_changes()` via SPI | `dblink` calling same function on remote | worker.c: swap SPI call for dblink call |
| Message parsing | `logicalrep_read_*` on binary data | Same — wire format is identical | None (decoder.c unchanged) |
| Batching | Per-table hash accumulation | Same | None (batch.c unchanged) |
| Apply | INSERT/DELETE to local DuckLake via SPI | Same — target is always local | None (apply.c unchanged) |
| Snapshot copy | `INSERT INTO target SELECT * FROM source` (local) | `dblink` or `postgres_fdw` to remote source | worker.c: snapshot copy path |
| Slot management | Local SQL (`pg_create_logical_replication_slot`) | `dblink` to remote | api.c: group creation |
| Publication management | Local SQL (`CREATE PUBLICATION`) | Must exist on remote (user-managed or via `dblink`) | api.c or user responsibility |
| Connection info | None needed | `conninfo` string stored per sync group | Schema + api.c |

### 16.3 Background: dblink

`dblink` is a PostgreSQL contrib module that allows SQL queries against remote PostgreSQL servers from within a local session. It's relevant here because it provides the simplest path to remote WAL consumption.

**Key dblink concepts:**

```sql
-- Open a persistent named connection (survives across queries in a session)
SELECT dblink_connect('myconn', 'host=remote port=5432 dbname=prod user=repl');

-- Execute a query on the remote server, returns setof record
SELECT * FROM dblink('myconn',
    'SELECT lsn, data FROM pg_logical_slot_get_binary_changes(
        ''duckpipe_slot'', NULL, 1000,
        ''proto_version'', ''1'',
        ''publication_names'', ''duckpipe_pub'')'
) AS t(lsn pg_lsn, data bytea);

-- Close the connection
SELECT dblink_disconnect('myconn');
```

**Why dblink fits pg_duckpipe's architecture:**

1. **Minimal code change**: The current worker calls `pg_logical_slot_get_binary_changes()` via SPI. With dblink, the same SQL runs on the remote server — only the transport changes. The returned `(lsn, data)` rows have the identical schema.

2. **Persistent connections**: `dblink_connect` creates a named connection that persists for the session. The background worker can open one connection per remote group at startup and reuse it across poll rounds, avoiding per-round connection overhead.

3. **No new dependencies**: `dblink` ships with PostgreSQL as a contrib module. It's already available on virtually all PostgreSQL installations. No external libraries or custom protocol handling needed.

4. **Binary data passthrough**: `dblink` returns `bytea` columns faithfully. The pgoutput binary messages pass through the network as-is — the local `logicalrep_read_*` functions parse them exactly as they would for local data.

**dblink vs. postgres_fdw:**

| Aspect | dblink | postgres_fdw |
|--------|--------|--------------|
| Query style | Explicit `dblink()` function calls | Transparent via foreign tables |
| Connection management | Manual (`dblink_connect`/`dblink_disconnect`) | Automatic (cached per session) |
| Return types | `setof record` (must cast) | Typed foreign tables |
| Binary data | Works (returns `bytea` as-is) | Works but designed for row access |
| Overhead | Minimal — direct SQL passthrough | Higher — query planning, FDW API |
| Use for WAL fetch | Natural fit — same SQL, different server | Awkward — WAL functions aren't tables |
| Use for snapshot copy | `dblink` + `COPY` or row-at-a-time | Natural fit — `SELECT * FROM foreign_table` |

**Recommendation**: Use `dblink` for WAL fetching (the hot path) and optionally `postgres_fdw` for snapshot copy (one-time bulk data transfer where transparent SQL is convenient).

**dblink limitations to be aware of:**

- **Authentication**: The remote connection string must include credentials or use `.pgpass`/`pg_service.conf`. Storing passwords in `sync_groups` requires encryption or referencing a `pg_foreign_server` with `password_required = false`.
- **Error handling**: If the remote server disconnects, `dblink` calls raise errors. The worker must catch these and reconnect.
- **No async streaming**: `dblink` is request-response (polling). For push-based streaming, you'd need libpq's replication protocol directly. But polling matches pg_duckpipe's existing architecture.
- **Two-phase not needed**: pg_duckpipe doesn't need distributed transactions — it's one-directional replication with idempotent apply.

### 16.4 Approach: dblink Polling (Recommended for V2)

This approach maximizes code reuse. The worker's poll loop stays nearly identical — the only difference is where the SQL executes.

**Current local flow (worker.c):**
```c
// SPI call to local server
appendStringInfo(&query,
    "SELECT lsn, data FROM pg_logical_slot_get_binary_changes("
    "%s, NULL, %d, 'proto_version', '1', 'publication_names', %s)",
    quote_literal_cstr(group->slot_name),
    duckpipe_batch_size_per_group,
    quote_literal_cstr(group->publication));
ret = SPI_execute(query.data, true, 0);
```

**Remote flow (proposed change):**
```c
if (group->conninfo != NULL) {
    // Remote: wrap the same query in dblink()
    appendStringInfo(&query,
        "SELECT * FROM dblink(%s, "
        "'SELECT lsn, data FROM pg_logical_slot_get_binary_changes("
        "''" SLOT "'', NULL, %d, "
        "''proto_version'', ''1'', "
        "''publication_names'', ''" PUB "'')"
        "') AS t(lsn pg_lsn, data bytea)",
        quote_literal_cstr(group->conninfo), ...);
    ret = SPI_execute(query.data, true, 0);
} else {
    // Local: existing path (unchanged)
    ...
}
```

The result set schema is identical: `(lsn pg_lsn, data bytea)`. All downstream code (message copy, decode, batch, apply) is unchanged.

### 16.5 Schema Changes

```sql
-- Add conninfo column to sync_groups
ALTER TABLE duckpipe.sync_groups ADD COLUMN conninfo TEXT;
-- NULL = local sync (current behavior, backward compatible)
-- Non-NULL = remote sync via dblink

-- Examples:
-- Local group (default, unchanged):
--   conninfo = NULL

-- Remote group:
--   conninfo = 'host=prod-db.internal port=5432 dbname=production user=replicator'
```

No changes to `table_mappings` — remote vs. local is a group-level property. All tables in a remote group sync from the same remote server.

### 16.6 API Changes

```sql
-- Extended create_group with optional conninfo
duckpipe.create_group(
    name TEXT,
    publication TEXT DEFAULT NULL,
    slot_name TEXT DEFAULT NULL,
    conninfo TEXT DEFAULT NULL          -- NEW: NULL = local, non-NULL = remote
) RETURNS TEXT

-- Extended add_table for remote sources
duckpipe.add_table(
    source_table TEXT,
    target_table TEXT DEFAULT NULL,
    sync_group TEXT DEFAULT 'default',
    copy_data BOOLEAN DEFAULT true
) RETURNS void
-- When sync_group has conninfo set:
--   - Publication must already exist on remote (user-managed)
--   - Snapshot copy uses dblink to fetch from remote source
--   - Target table schema derived from remote table via dblink query
```

**Example usage:**

```sql
-- Create extension (dblink must also be available)
CREATE EXTENSION dblink;
CREATE EXTENSION pg_duckpipe CASCADE;

-- On the REMOTE server (one-time setup by DBA):
--   CREATE PUBLICATION analytics_pub FOR TABLE orders, customers;
--   -- Replication slot is created by pg_duckpipe via dblink

-- On the LOCAL analytics server:
SELECT duckpipe.create_group(
    'remote_prod',
    publication := 'analytics_pub',
    conninfo := 'host=prod-db port=5432 dbname=production user=replicator'
);

-- Add remote tables (auto-creates local DuckLake targets)
SELECT duckpipe.add_table('public.orders', sync_group := 'remote_prod');
SELECT duckpipe.add_table('public.customers', sync_group := 'remote_prod');

-- Query local DuckLake tables for analytics
SELECT customer_id, sum(amount) FROM orders_ducklake GROUP BY customer_id;
```

### 16.7 Snapshot Copy for Remote Tables

The snapshot path (`process_snapshot` in worker.c) currently does:

```sql
INSERT INTO target SELECT * FROM source;
```

For remote sources, this becomes:

```sql
-- Option A: dblink (simple, row-at-a-time over network)
INSERT INTO target
SELECT * FROM dblink(conninfo,
    'SELECT * FROM source_schema.source_table'
) AS t(col1 type1, col2 type2, ...);

-- Option B: postgres_fdw (transparent, bulk transfer)
-- Requires one-time foreign server + user mapping setup
CREATE FOREIGN TABLE temp_remote_source (LIKE target)
    SERVER remote_prod OPTIONS (schema_name 'public', table_name 'orders');
INSERT INTO target SELECT * FROM temp_remote_source;
DROP FOREIGN TABLE temp_remote_source;

-- Option C: COPY protocol via libpq (most efficient for bulk)
-- Remote: COPY source TO STDOUT (BINARY)
-- Local:  COPY target FROM STDIN (BINARY)
-- Requires libpq connection, not SPI — more implementation effort
```

**Recommendation**: Start with Option A (dblink) for simplicity. The column type list can be derived by querying `information_schema.columns` on the remote server via dblink. Optimize to Option C if snapshot copy performance becomes a bottleneck for large tables.

### 16.8 Connection Lifecycle

```
Worker startup
    │
    ▼
For each remote group:
    dblink_connect('group_{id}', conninfo)
    │
    ▼
Poll loop (repeated):
    dblink('group_{id}', 'SELECT ... pg_logical_slot_get_binary_changes(...)')
    │
    ▼
On error / disconnect:
    dblink_disconnect('group_{id}')
    dblink_connect('group_{id}', conninfo)   ← reconnect
    │
    ▼
Worker shutdown:
    dblink_disconnect('group_{id}')
```

Named connections persist across poll rounds within the worker's session, avoiding per-round TCP handshake and authentication overhead.

### 16.9 Remote Slot and Publication Management

| Operation | Local (current) | Remote |
|-----------|-----------------|--------|
| Create slot | `pg_create_logical_replication_slot()` via SPI | `dblink(conninfo, 'SELECT pg_create_logical_replication_slot(...)')` |
| Drop slot | `pg_drop_replication_slot()` via SPI | `dblink(conninfo, 'SELECT pg_drop_replication_slot(...)')` |
| Create publication | `CREATE PUBLICATION ... FOR TABLE ...` via SPI | User-managed on remote server |
| Add table to publication | `ALTER PUBLICATION ... ADD TABLE ...` via SPI | User-managed on remote server |

Publication management on the remote server is left to the DBA because:
1. It requires DDL privileges on the remote server
2. The remote server's publication may serve multiple subscribers
3. Network partitions during DDL create hard-to-recover states

The replication slot, however, is managed by pg_duckpipe because:
1. It's specific to this subscriber (one slot per sync group)
2. Slot creation can be done via `dblink` with replication-capable credentials
3. Slot lifecycle must be tied to group lifecycle (leaked slots cause unbounded WAL retention)

### 16.10 Implementation Phases

**Phase 1: Core remote WAL streaming**
- Add `conninfo` column to `sync_groups` schema
- Add `conninfo` parameter to `create_group()`
- Create/drop replication slot on remote via dblink
- Worker: branch WAL fetch to use dblink when `conninfo` is set
- Worker: dblink connection lifecycle (connect, reconnect on error, disconnect)
- No snapshot support yet — `copy_data=false` only

**Phase 2: Remote snapshot copy**
- Query remote `information_schema.columns` via dblink for target table creation
- Snapshot copy via dblink SELECT
- Temporary replication slot on remote for atomic snapshot + LSN
- Full SNAPSHOT → CATCHUP → STREAMING state machine for remote tables

**Phase 3: Operational hardening**
- Connection health monitoring in `duckpipe.groups()` view
- Credential management (reference `pg_foreign_server` instead of inline conninfo)
- Configurable connection timeout and retry policy
- Remote server version compatibility checks

### 16.11 Limitations of Remote Sync

1. **Polling latency**: dblink is request-response. Minimum latency is `poll_interval` + network RTT + query execution. For sub-second latency, consider reducing `poll_interval` but expect higher remote server load.
2. **No push-based streaming**: True streaming replication (walsender protocol via `START_REPLICATION`) would eliminate polling overhead but requires implementing the full replication protocol client in C with libpq async APIs. This is a possible future optimization (Phase 4+).
3. **Publication is user-managed**: The DBA must create and maintain the publication on the remote server. pg_duckpipe cannot auto-add tables to a remote publication.
4. **Network partition handling**: If the remote becomes unreachable, the worker retries on the next poll round. The replication slot on the remote continues to retain WAL. Prolonged outages may cause WAL disk pressure on the remote — monitoring `pg_replication_slots.wal_status` on the remote is the DBA's responsibility.
5. **No conflict resolution**: Remote sync is one-directional. If the local DuckLake target is modified outside pg_duckpipe, there is no conflict detection.

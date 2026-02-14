# pg_duckpipe v2: Architecture Design

## Executive Summary

pg_duckpipe v2 is a complete redesign of the CDC sync service from PostgreSQL heap tables to DuckLake columnar tables. The redesign addresses v1's single-threaded processing and adds parallelism, dual deployment capability, and DuckLake-specific optimizations.

### Design Goals

| Goal | Approach |
|------|----------|
| At-least-once with idempotent replay | Per-table LSN tracking + idempotent MERGE applies |
| Parallel batch apply | In-memory staging queues + per-table flush workers |
| No streaming disruption when adding tables | Independent snapshot workers |
| Shared replication slots | One slot per sync group (not per table) |
| Dual deployment | Same core runs as PostgreSQL extension or standalone binary |
| DuckLake-optimized | DuckDB native MERGE, Appender API, data inlining |

### Why Rust?

- **Tokio async runtime** provides clean single-producer/multi-consumer concurrency
- **Memory safety without GC** — safe when running inside PostgreSQL process
- **pgrx** for PostgreSQL extension (Mode 1), same codebase compiles to standalone binary (Mode 2)
- **tokio-postgres** supports both loopback (Mode 1) and streaming replication (Mode 2)

### Key Architectural Decisions

1. **No SPI** — Use loopback tokio-postgres connections instead of SPI
2. **In-memory staging queues** — Per-table `VecDeque` in Rust, no external dependencies for staging
3. **Unified WAL consumption** — Use START_REPLICATION protocol for all modes
4. **Per-table staging** — One queue per source table, drained by flush workers
5. **DuckDB buffer + MERGE flush path** — Flush workers continuously drain queue into DuckDB buffer table, compact and MERGE into DuckLake on threshold
6. **Backpressure-aware** — Slot consumer monitors queue depth and throttles
7. **Source table OID** — Use OID as identifier to survive table renames
8. **URI-based source tracking** — Follow pg_mooncake pattern for source identification

---

## Terminology and Naming Conventions

### Component Names

| Term | Definition |
|-------|------------|
| **Source** | PostgreSQL database containing heap tables to replicate from |
| **Target** | PostgreSQL database with DuckLake extension containing columnar tables |
| **Sync Service** | The Rust-based service performing CDC (runs as bgworker or standalone) |
| **Sync Group** | A collection of tables sharing one publication and one replication slot |
| **Slot Consumer** | Task that reads WAL from replication slot and dispatches to per-table staging queues |
| **Flush Worker** | Task that continuously drains a staging queue into a DuckDB buffer, compacts and MERGEs into DuckLake on threshold |
| **Snapshot Worker** | Task that performs initial full copy of a table |
| **Checkpoint Manager** | Task that computes and advances confirmed_flush_lsn |

### Table State Names

| State | Meaning |
|-------|---------|
| **PENDING** | Table added but not yet started |
| **SNAPSHOT** | Initial full copy in progress |
| **CATCHUP** | Catching up from snapshot_lsn to current WAL |
| **STREAMING** | Normal operation, applying all changes |
| **ERRORED** | Error encountered, awaiting resolution |

### Database Object Names

| Object | Naming Pattern |
|---------|----------------|
| Metadata schema | `duckpipe` |
| Sync groups table | `duckpipe.sync_groups` |
| Table mappings table | `duckpipe.table_mappings` |
| Staging queue | Per-table `VecDeque<StagingRow>` in Rust process memory |
| DuckDB buffer table | `buffer_<source_oid>` (persistent table in embedded DuckDB, one per source table) |
| GUC parameters | `duckpipe.<name>` |

### LSN Terminology

| Term | Definition |
|-------|------------|
| **LSN** | Log Sequence Number - position in PostgreSQL WAL |
| **snapshot_lsn** | WAL LSN at which snapshot was taken (per table) |
| **applied_lsn** | WAL LSN up to which changes have been applied (per table) |
| **confirmed_flush_lsn** | WAL LSN confirmed as processed by replication slot (per group) |

### Acronyms

| Acronym | Full Name |
|----------|-----------|
| CDC | Change Data Capture |
| WAL | Write-Ahead Log |
| LSN | Log Sequence Number |
| OID | Object Identifier |
| MERGE | SQL MERGE statement (upsert/delete in single operation) |
| TOAST | The Oversized-Attribute Storage Technique |

### Deployment Modes

| Mode | Description |
|------|-------------|
| **Mode 1 / Extension** | Runs as PostgreSQL bgworker, connects via loopback |
| **Mode 2 / Standalone** | Runs as separate process, connects via streaming replication |

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                          Sync Service Process                                │
│                                                                              │
│  Slot Consumer (1/group)     Per-Table Queues      Flush Workers (1/table)   │
│    ──START_REPLICATION─►       ┌──────────┐                                  │
│       decode pgoutput ──────►  │ Queue T1 │ ──drain──► FW1 ──►┐             │
│                        ──────► │ Queue T2 │ ──drain──► FW2 ──►┤ Embedded    │
│                        ──────► │ Queue T3 │ ──drain──► FW3 ──►┤ DuckDB      │
│                                └──────────┘                   │ (per-worker) │
│  Snapshot Workers (parallel, independent)                     │              │
│                                                               ▼              │
│  ┌─────────────────────┐    ┌─────────────────────────────────────────────┐  │
│  │ Source PostgreSQL   │    │ DuckLake (attached in DuckDB)               │  │
│  │                     │    │   Appender ──► buffer ──► compact ──► MERGE │  │
│  │ Heap Tables, WAL,   │    │   Metadata catalog: PostgreSQL              │  │
│  │ Slot, Publication   │    │   Data files: local/S3                      │  │
│  └─────────────────────┘    └─────────────────────────────────────────────┘  │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Deployment

Standalone service runs as a separate process. The bgworker is a thin launcher that spawns and monitors it.

| Mode | Process Model | Connection Method |
|------|---------------|-------------------|
| Extension | PostgreSQL bgworker with embedded sync service | PostgreSQL loopback to localhost |
| Standalone | Standalone binary service | Streaming replication to remote PostgreSQL |

Both modes use identical `duckpipe-core` sync logic.

---

## Component: Slot Consumer

Single async task per sync group. Responsible for reading WAL and dispatching decoded changes to per-table in-memory staging queues.

### Responsibilities

1. **WAL consumption** — Use START_REPLICATION protocol (unified for all modes)
2. **pgoutput decoding** — Parse binary protocol using `postgres-replication` crate
3. **Route to queues** — Append decoded changes to per-table staging queues
4. **Backpressure control** — Monitor total queue depth, throttle if behind
5. **Checkpoint coordination** — Track per-table `applied_lsn`, compute `confirmed_flush_lsn`
6. **Replication slot advancement** — Send `StandbyStatusUpdate` message via replication protocol with confirmed LSN

### Backpressure Design

The slot consumer tracks the total row count across all staging queues AND DuckDB buffer tables. If the combined count exceeds threshold:

```
total_pending_rows = sum(queue.len() + buffer_row_count for each table)
if total_pending_rows > backpressure_max_staging_rows:
    - Slow down/stop polling from replication slot
    - Wait for flush workers to flush buffers to DuckLake
    - Resume when row count reduces
```

Monitoring both queue depth and buffer depth is necessary because the flush worker drains the queue quickly into DuckDB — the queue alone would undercount the actual memory pressure. The buffer row counts are reported by flush workers to the slot consumer via the same completion channel used for `applied_lsn`.

Row-count-based backpressure directly measures memory pressure, unlike LSN-gap-based backpressure which can misfire (large LSN gaps from unrelated WAL consume no staging memory, while small LSN gaps with many large rows can exhaust memory).

---

## Component: Flush Workers

One async task per table. Each flush worker continuously drains its in-memory staging queue into a persistent DuckDB buffer table, and periodically flushes the buffer to DuckLake via MERGE when a size or time threshold is reached.

### Responsibilities

1. **Continuously drain staging queue** — Append incoming rows from the per-table queue to a DuckDB buffer table via Appender
2. **Trigger flush** — When buffer row count reaches `batch_size_per_table` or `batch_max_fill_ms` elapses since last flush
3. **Compact duplicate PKs** — Deduplicate buffer rows in DuckDB, keeping only the last operation per PK
4. **MERGE into DuckLake** — Execute native DuckDB `MERGE INTO` from compacted buffer into attached DuckLake table
5. **Truncate buffer** — Clear the buffer table after successful MERGE
6. **Track progress** — Update `duckpipe.table_mappings.applied_lsn`
7. **Report to checkpoint manager** — Send `applied_lsn` for slot advancement
8. **Handle errors** — Retry, log, or enter ERRORED state

### Flush Worker Loop

```
loop {
    // Phase 1: DRAIN — continuously move rows from queue to DuckDB buffer
    while let Some(rows) = queue.try_drain() {
        appender.append_rows(rows);
        appender.flush();
    }

    // Phase 2: CHECK FLUSH TRIGGER
    if buffer_row_count >= batch_size_per_table
       || elapsed_since_last_flush >= batch_max_fill_ms {

        // Phase 3: COMPACT — dedup in DuckDB by PK
        compact_buffer();

        // Phase 4: MERGE — apply compacted buffer to DuckLake
        merge_into_ducklake();

        // Phase 5: TRUNCATE — clear buffer
        truncate_buffer();

        // Phase 6: REPORT
        update_applied_lsn();
        report_to_checkpoint_manager();
    }

    // Sleep briefly if queue was empty
    tokio::time::sleep(poll_interval).await;
}
```

### Why Per-Table Queues + DuckDB Buffer?

- **Queue** — Fast, lock-free handoff between slot consumer (producer) and flush worker (consumer). Decouples WAL consumption rate from DuckDB write rate.
- **DuckDB buffer** — Absorbs bursts, enables SQL-based compaction, and provides natural batching for DuckLake writes. DuckDB handles memory pressure via automatic disk spill if buffer grows large.

### Compaction (Deduplication)

The buffer table can accumulate multiple operations on the same PK (e.g., INSERT pk=5, UPDATE pk=5, DELETE pk=5). DuckDB MERGE requires unique keys in the USING source. Compaction deduplicates by PK directly in DuckDB:

```sql
-- Create compacted view: for each PK, keep only the last operation by seq
CREATE TEMP TABLE compacted AS
    SELECT * FROM buffer
    WHERE (pk, seq) IN (
        SELECT pk, MAX(seq) FROM buffer GROUP BY pk
    );
```

This runs entirely in DuckDB — no data leaves the process. After MERGE, both the buffer and compacted tables are cleaned up.

### Flush Path: DuckDB Buffer + MERGE

Each flush worker holds an embedded DuckDB connection with DuckLake attached. The buffer table is created once and persists for the lifetime of the flush worker.

**DuckDB Session Setup (once per flush worker lifecycle):**

```sql
INSTALL ducklake;
LOAD ducklake;

-- Attach the DuckLake catalog using PostgreSQL as metadata store
ATTACH 'ducklake:postgres:dbname=<target_db> host=<host>' AS lake (
    METADATA_SCHEMA 'ducklake'
);

-- Create persistent buffer table (typed to match source schema)
CREATE TABLE buffer_<oid> (
    seq         BIGINT NOT NULL,
    lsn         BIGINT NOT NULL,
    op_type     SMALLINT NOT NULL,   -- 0=INSERT, 1=UPDATE, 2=DELETE
    pk          <pk_type> NOT NULL,
    col_a       <col_a_type>,
    col_b       <col_b_type>,
    col_a_unchanged BOOLEAN DEFAULT false,
    col_b_unchanged BOOLEAN DEFAULT false
);
```

**Continuous Append (Phase 1):**

```rust
// Drain queue and append to DuckDB buffer via Appender (binary, fast)
let mut appender = conn.appender("buffer_<oid>")?;
for row in drained_rows {
    appender.append_row([row.seq, row.lsn, row.op_type, row.pk, ...])?;
}
appender.flush()?;
```

DuckDB's Appender writes directly to columnar storage in binary format — no SQL parsing, no text serialization.

**Flush (Phases 3-5):**

```sql
-- Step 1: Compact — dedup by PK, keep last operation
CREATE TEMP TABLE compacted AS
    SELECT * FROM buffer_<oid>
    WHERE (pk, seq) IN (
        SELECT pk, MAX(seq) FROM buffer_<oid> GROUP BY pk
    );

-- Step 2: MERGE compacted → DuckLake
MERGE INTO lake.target_schema.target_table AS t
USING compacted AS s
ON t.pk = s.pk
WHEN MATCHED AND s.op_type = 2 THEN DELETE
WHEN MATCHED AND s.op_type = 1 THEN UPDATE SET
    col_a = CASE WHEN s.col_a_unchanged THEN t.col_a ELSE s.col_a END,
    col_b = CASE WHEN s.col_b_unchanged THEN t.col_b ELSE s.col_b END
WHEN NOT MATCHED AND s.op_type = 0 THEN INSERT (pk, col_a, col_b)
    VALUES (s.pk, s.col_a, s.col_b);

-- Step 3: Clean up
DROP TABLE compacted;
DELETE FROM buffer_<oid>;
```

The MERGE executes entirely within DuckDB — both the buffer and DuckLake target are in the same process. No cross-boundary data transfer.

### Flush Triggers

| Trigger | Condition | Rationale |
|---------|-----------|-----------|
| **Size** | `buffer_row_count >= batch_size_per_table` | Bound memory usage, efficient batch size for DuckLake writes |
| **Time** | `elapsed >= batch_max_fill_ms` since last flush | Bound latency for low-throughput tables |

Whichever trigger fires first initiates the flush. This ensures both throughput (large batches) and freshness (time-bound latency).

### Concurrent DuckLake Access

Both pg_duckdb (inside PostgreSQL) and the flush worker's embedded DuckDB may access the same DuckLake catalog simultaneously. DuckLake handles this via:

- **Optimistic concurrency** — Metadata catalog (PostgreSQL) serializes schema changes
- **Retry with backoff** — Configurable via `ducklake_retry_wait_ms`, `ducklake_retry_backoff`, `ducklake_max_retry_count`
- **File-level isolation** — Each write creates new Parquet files; no in-place modification

The flush worker should configure appropriate retry settings:
```sql
SET ducklake_retry_wait_ms = 100;
SET ducklake_retry_backoff = 2.0;
SET ducklake_max_retry_count = 10;
```

### DuckDB Connection Model

Each flush worker creates its own embedded DuckDB instance with DuckLake attached. This provides:

- **Full parallelism** — No single-writer contention. Each worker's Appender and MERGE execute independently without serialization.
- **Process isolation** — A crash or memory issue in one DuckDB instance does not affect other flush workers.
- **Simple ownership** — Buffer table (`buffer_<oid>`) is local to the instance. No cross-worker visibility concerns. TEMP tables work naturally.

**Trade-off:** Higher memory overhead (each DuckDB instance has its own buffer pool). Acceptable because flush workers are long-lived (one per table) and the buffer tables are the primary memory consumer regardless of instance count.

**Note:** Each DuckDB instance independently attaches to the same DuckLake catalog. DuckLake's optimistic concurrency and file-level isolation ensure correctness when multiple instances write concurrently.

---

## Component: Snapshot Workers

Independent tokio task per table. Responsible for initial full copy from source to DuckLake target.

### Responsibilities

1. **Separate PostgreSQL connection** — From connection pool
2. **Create temporary replication slot** — `CREATE_REPLICATION_SLOT <name> TEMPORARY LOGICAL pgoutput` to obtain a consistent (LSN, snapshot) pair
3. **Import exported snapshot** — `SET TRANSACTION SNAPSHOT <snapshot_id>` within REPEATABLE READ transaction
4. **Bulk copy** — Read from source via PostgreSQL connection, write to DuckLake target via embedded DuckDB (`INSERT INTO lake.schema.table ...`)
5. **Record `consistent_point`** — Use the slot's `consistent_point` as `snapshot_lsn`
6. **State transition** — Set table to CATCHUP with recorded `snapshot_lsn`
7. **Cleanup** — Drop temporary slot, return connection to pool

### Why Temporary Replication Slot?

Using `pg_current_wal_lsn()` with REPEATABLE READ has a data loss window: in-flight transactions may have WAL records with LSN < snapshot_lsn but haven't committed yet, so they're invisible to the snapshot. During CATCHUP, these changes would be skipped (`lsn <= snapshot_lsn`), causing data loss.

A temporary replication slot's `consistent_point` is guaranteed to be consistent with the exported snapshot — all committed transactions visible in the snapshot have LSN <= consistent_point, and no invisible transactions have LSN <= consistent_point. This is the same approach used by v1 and pg_dump.

### Why Independent?

- **Does NOT touch the group's replication slot** — Uses its own temporary slot, zero impact on streaming tables
- **Separate connection** — No coordination needed with slot consumer
- **Parallel execution** — Multiple tables snapshot simultaneously

---

## Component: Checkpoint Manager

Background task per sync group. Responsible for tracking progress and advancing replication slot.

### Responsibilities

1. **Collect `applied_lsn`** — From all active flush workers
2. **Compute `confirmed_flush_lsn`** — `min(all applied_lsn)` for slot advancement
3. **Advance slot** — Send `StandbyStatusUpdate` message with computed LSN via replication protocol

### Why Minimum LSN?

The slot's `confirmed_flush_lsn` must NOT advance beyond any table's `applied_lsn`. This prevents WAL from being discarded before a table has processed it.

---

## Component: In-Memory Staging Queues

Per-table in-memory queues for buffering decoded WAL changes between the slot consumer and flush workers.

### Data Structures

```rust
/// A single decoded change from the WAL stream
struct StagingRow {
    seq: u64,                     // Monotonic sequence number (for ordering)
    lsn: u64,                     // WAL LSN of this change
    op_type: OpType,              // INSERT=0, UPDATE=1, DELETE=2
    pk_values: Vec<Value>,        // Primary key column values (typed)
    col_values: Vec<Option<Value>>, // Column values (None = TOAST unchanged)
    col_unchanged: Vec<bool>,     // true = column not sent by pgoutput (TOAST)
}

/// Per-table staging queue (one per source table)
struct TableQueue {
    queue: Mutex<VecDeque<StagingRow>>,  // Append by slot consumer, drain by flush worker
    schema: Arc<TableSchema>,            // Cached column names, types, PK columns
}

/// The slot consumer holds a HashMap<u32, TableQueue> keyed by source OID
```

### Queue Operations

- **Append** (slot consumer): `queue.lock().push_back(row)` — O(1) amortized
- **Drain** (flush worker): `queue.lock().drain(..).collect()` — Atomically takes all rows, releases lock immediately. The flush worker then owns the drained `Vec<StagingRow>` exclusively.
- **Backpressure query** (slot consumer): `queue.lock().len()` — O(1)

### Properties

- **Per-table** — One queue per source table, keyed by source OID
- **In-memory only** — Lost on restart; resume from `applied_lsn` via WAL replay
- **Lock-free drain** — Flush worker drains available rows in one lock acquisition, then appends to DuckDB buffer without holding the lock
- **Lightweight handoff** — The queue is a thin transfer layer between the slot consumer and flush worker. The DuckDB buffer table is the primary staging store.
- **Typed values** — Column values stored as Rust `Value` enum (matching PostgreSQL types from pgoutput), not as strings. This avoids serialization/deserialization overhead and allows direct binary encoding when loading via DuckDB Appender.

### Write Path

```
Slot Consumer                              Flush Worker
    │                                              │
    ├─ decode pgoutput                             │
    ├─ append ──► TableQueue (Mutex<VecDeque>)     │
    │                                              ├─ drain ◄── TableQueue
    │                                              ├─ Appender ──► DuckDB buffer table
    │                                              ├─ (on threshold) compact in DuckDB
    │                                              ├─ MERGE compacted ──► DuckLake
    │                                              └─ report applied_lsn
    │
    ├─ receive applied_lsn from flush workers
    └─ advance replication slot
```

---

## Table State Machine

```
                add_table(copy_data=true)          add_table(copy_data=false)
                       │                                    │
                       ▼                                    │
              ┌─────────────────┐                          │
              │     SNAPSHOT     │                          │
              │  - Record snapshot_lsn                       │
              │  - Copy data to DuckLake                    │
              │  - Set state = CATCHUP                      │
              └──────────┬──────────┘                      │
                         │                                    ▼
              ┌─────────────────────┐                     │
              │     CATCHUP         │                     │
              │  - Skip lsn <= snapshot_lsn                 │
              │  - Apply lsn > snapshot_lsn                 │
              │  - Promote when past snapshot_lsn            │
              └──────────┬──────────┘                     │
                         │                                    ▼
              ┌─────────────────────────────────────────────┐ │
              │              STREAMING                         │
              │  - Apply all changes                         │
              │  - Track applied_lsn persistently            │
              └─────────────────────────────────────────────┘ │
                         │                                    ▼
              ┌─────────────────┐                          │
              │     ERRORED      │                           │
              │  - Stop processing                            │
              │  - Log error                               │
              │  - Retry policy:                            │
              │    * Transient: auto-retry                  │
              │    * Configuration: manual fix               │
              │    * Resource: manual fix                    │
              └────────────────┘                          │
```

### CATCHUP → STREAMING Promotion

A table transitions from CATCHUP to STREAMING when **any change with `lsn > snapshot_lsn` is encountered and applied**. This is detected locally by the flush worker — no global coordination required.

### Transaction Boundary Handling

**Design choice: batch boundaries do NOT respect source transaction boundaries.** A batch may contain partial transactions — some rows from a source transaction may be in one batch while others appear in the next.

This means readers of DuckLake target tables may observe intermediate states (partial transaction application) between flush cycles. This is an acceptable trade-off for CDC workloads where:
- The target is primarily used for analytical queries, not transactional reads
- Strict transactional consistency on the target would require buffering entire source transactions, which can be arbitrarily large
- Eventually (after the next flush cycle), all changes are applied and the target converges to the source state

If strict transaction boundary alignment is needed in the future, the slot consumer would need to track `BEGIN`/`COMMIT` message boundaries and only allow flushes at commit points.

---

## DuckLake-Specific Optimizations

### Native MERGE for TOAST-Safe UPDATEs

Fixes v1's TOAST unchanged column bug. The flush worker uses DuckDB's native MERGE with `CASE WHEN unchanged` to selectively preserve TOAST-unchanged columns in a single statement. See [Flush Workers: Flush Path](#flush-path-duckdb-appender--merge) for the full process.

For TOAST unchanged columns, the `col_unchanged` boolean array tracks which columns were not sent by pgoutput (TOAST optimization). The MERGE uses `CASE WHEN unchanged THEN target.col ELSE staging.col END` to correctly distinguish between a column explicitly set to NULL and a column omitted due to TOAST. Using COALESCE would incorrectly conflate these two cases.

### Data Inlining

DuckLake can store small inserts directly in metadata catalog instead of creating Parquet files.

**Why this matters for CDC:** CDC produces many small batches. Without inlining, each flush creates a tiny Parquet file → "small files problem".

**Strategy:**
- Enable data inlining with limit matching `batch_size_per_table`
- Small CDC batches go inline → no file overhead
- Periodic `ducklake.flush_inlined_data()` converts inline data to Parquet files

### Known Issues (Future Work)

- **VACUUM** — Merging small Parquet files and removing tombstone files
- **Delete files** — DuckLake uses tombstone files; periodic cleanup may be needed
- **File handle management** — Large number of Parquet files may cause issues

---

## Publication Management

When a table is added or removed from a sync group, the publication must be updated accordingly:

- **`add_table()`** — Executes `ALTER PUBLICATION <pub_name> ADD TABLE <schema.table>` on the source database
- **`remove_table()`** — Executes `ALTER PUBLICATION <pub_name> DROP TABLE <schema.table>` on the source database
- **`create_group()`** — Creates the publication with `CREATE PUBLICATION <pub_name>` (initially empty)
- **`drop_group()`** — Drops the publication and replication slot

Publication changes take effect immediately for the slot consumer — new tables appear in the WAL stream after the next transaction on those tables.

---

## Schema Change Handling (DDL)

**Current scope: not supported.** Schema changes on source tables (ALTER TABLE ADD/DROP COLUMN, type changes) are not handled automatically.

pgoutput sends updated RELATION messages when the schema changes. The slot consumer will detect a schema mismatch between the cached relation info and the new RELATION message. When this occurs:

1. The table transitions to **ERRORED** state with a descriptive error message
2. The flush worker stops processing that table
3. Manual intervention is required: update the target DuckLake table schema, then call `resync_table()` or manually resolve

**Future work:** Automatic DDL propagation (ALTER TABLE on target to match source) could be added, but requires careful handling of type compatibility between PostgreSQL and DuckLake column types.

---

## Connection Pool

The sync service maintains connections to both the source PostgreSQL and an embedded DuckDB instance:

**PostgreSQL connections (via `tokio-postgres`):**

| Connection | Count | Target | Purpose |
|------------|-------|--------|---------|
| Replication connection | 1 per sync group | Source PG | START_REPLICATION protocol for WAL streaming |
| Snapshot read connections | Up to `max_snapshot_workers` (default 4) | Source PG | REPEATABLE READ snapshots for reading source data |
| Snapshot write connection | shared with flush workers | DuckDB (embedded) | Writing snapshot data to DuckLake via attached catalog |
| Metadata connection | 1 | Source PG | Reading/writing `duckpipe.*` metadata tables |

**DuckDB connections (via `duckdb-rs`):**

| Connection | Count | Purpose |
|------------|-------|---------|
| Flush worker DuckDB instances | 1 per table | Appender load + MERGE into DuckLake (attached) |

PostgreSQL connections are managed via `deadpool-postgres` for pooling. Each flush worker owns an independent embedded DuckDB instance with DuckLake attached for the duration of the table's lifecycle.

---

## Delivery Guarantee: At-Least-Once with Idempotent Replay

### Why At-Least-Once (Not Exactly-Once)

True exactly-once would require atomic commits spanning both the DuckLake data write (via pg_duckdb) and the `applied_lsn` metadata update (PostgreSQL catalog) in a single transaction. Since these are different storage backends, a crash between the two could cause:
- Data written to DuckLake but `applied_lsn` not updated → batch replayed on restart
- `applied_lsn` updated but DuckLake write failed → data lost

Instead, we use **at-least-once delivery with idempotent replay**: on restart, batches may be re-applied, but the MERGE semantics ensure the final state is correct regardless of how many times a batch is applied (INSERT of existing row → matches on PK and updates to same values; DELETE of missing row → no-op).

### Mechanism

1. **Per-table `applied_lsn`** — Each table tracks its progress persistently
2. **MERGE-based batch apply** — Idempotent: re-applying the same batch produces the same result
3. **Checkpoint calculation** — `confirmed_flush_lsn = min(all applied_lsn)`
4. **Crash recovery** — On restart, replay from `confirmed_flush_lsn`, skip or re-apply idempotently

### On Restart

```
Restart:
    1. In-memory staging queues are empty (process restarted)
    2. DuckDB buffer tables are empty (in-memory DuckDB, lost on restart)
    3. Load persisted applied_lsn for each table
    4. Slot consumer resumes from confirmed_flush_lsn
    5. Slot consumer creates fresh per-table queues
    6. Flush workers create fresh buffer tables in DuckDB
    7. Any changes with lsn <= applied_lsn: skipped by flush worker (already applied)
    8. Changes near applied_lsn boundary: may be re-applied (idempotent via MERGE)
```

**Restart replay cost:** Since the slot resumes from `confirmed_flush_lsn` (the minimum across all tables), tables whose `applied_lsn` is far ahead of the minimum will re-receive and discard WAL they've already processed. This is correct but adds replay overhead proportional to the gap between the fastest and slowest table. The backpressure mechanism limits this gap during normal operation.

---

## New Table Handoff (Simplified)

With per-table in-memory queues:

```
Time: add_table(T) called
─────────────────────────────────────────────────────────────────────►

Slot Consumer:                         Flush Worker:
─ create TableQueue for T ─►           │
─ append changes ──► queue             │
                                       └──────────────────────┐
                                       │ FW: drain queue (atomic swap)
                                       │ FW: compact (dedup by PK)
                                       │ FW: Appender → DuckDB buffer (continuous)
                                       │ FW: (on threshold) compact + MERGE → DuckLake
                                       │ FW: report applied_lsn
                                       └──────────────────────┘
```

### Key Simplifications

- **No version switching** — Each table has its own queue
- **No handoff protocol** — New table gets a new queue immediately
- **No cleanup needed** — Drain empties the queue atomically

---

## Concurrency Model

```
tokio runtime
├── Slot Consumer (1 per sync group)
│   └── Per-table staging queues (Mutex<VecDeque<StagingRow>>)
├── Flush Workers (1 per table)
│   └── Each owns a DuckDB instance (DuckLake attached)
├── Snapshot Workers (M per tables in SNAPSHOT state)
└── Checkpoint Manager (1 per sync group)
```

### Task Communication

- **Slot consumer** → per-table queues (appends decoded WAL changes)
- **Flush workers** ← per-table queues (drains via atomic swap)
- **Flush workers** → DuckLake (Appender + MERGE via per-worker DuckDB instance with DuckLake attached)
- **Completion channel** — Flush workers send `applied_lsn` to checkpoint manager after successful MERGE
- **Backpressure** — Slot consumer monitors `sum(queue.len())` across all tables

---

## Error Handling

Errors are classified for retry policy:

| Class | Examples | Action |
|--------|----------|--------|
| **Transient** | Connection timeout, lock contention, DuckDB busy | Auto-retry with exponential backoff |
| **Configuration** | Schema mismatch, table not found, permission denied | Manual fix required |
| **Resource** | Disk full, replication slot limit exceeded | Manual fix required |

---

## Metadata Schema

Metadata is stored in **the source PostgreSQL database** under the `duckpipe` schema. This is the natural location because:

- **Publications and replication slots live on the source** — metadata references them by name
- **Source OIDs are local** — `source_oid` is only meaningful on the source instance
- **Mode 1 (extension)** — the bgworker runs inside the source PostgreSQL, direct access
- **Mode 2 (standalone)** — the service connects to the source PostgreSQL via the metadata connection

The sync service reads metadata at startup to discover groups, tables, and resume LSNs. It writes back `applied_lsn`, `confirmed_flush_lsn`, `state`, and `error_message` as processing progresses.

```sql
-- Sync groups (one slot per group)
CREATE TABLE duckpipe.sync_groups (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    publication     TEXT NOT NULL UNIQUE,
    slot_name       TEXT NOT NULL UNIQUE,
    conninfo        TEXT,                      -- NULL = local, non-NULL = remote
    enabled         BOOLEAN DEFAULT true,
    confirmed_flush_lsn   PG_LSN,
    last_sync_at    TIMESTAMPTZ
);

-- Table mappings with per-table progress tracking
CREATE TABLE duckpipe.table_mappings (
    id              SERIAL PRIMARY KEY,
    group_id        INTEGER NOT NULL REFERENCES duckpipe.sync_groups(id),
    source_oid      OID NOT NULL,             -- NEW: use OID to survive renames
    source_uri      TEXT NOT NULL,            -- NEW: pg_mooncake style URI
    source_schema   TEXT NOT NULL,
    source_table    TEXT NOT NULL,
    target_schema   TEXT NOT NULL,
    target_table    TEXT NOT NULL,
    state           TEXT NOT NULL DEFAULT 'PENDING',
    snapshot_lsn    PG_LSN,
    applied_lsn     PG_LSN,                  -- Per-table progress tracking
    error_message   TEXT,                    -- Diagnostic info
    retry_at        TIMESTAMPTZ,             -- Next retry time for auto-retry
    enabled         BOOLEAN DEFAULT true,
    rows_synced     BIGINT DEFAULT 0,
    last_sync_at    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(source_oid)                        -- OID is unique within PG
);
```

### Source Table Identification

Following pg_mooncake's design:
- **source_uri** — PostgreSQL connection string (e.g., `postgresql://user@host/db`)
- **source_oid** — Table OID (survives table renames)
- **source_schema, source_table** — Human-readable names (for display, queries)

---

## Configuration Parameters

```sql
-- Core settings
duckpipe.poll_interval = 1000                -- ms between polls (default: 1000)
duckpipe.batch_size_per_table = 1000         -- buffer rows per table before flush
duckpipe.batch_max_fill_ms = 5000            -- max ms before flush (latency bound)
duckpipe.enabled = on                         -- enable/disable worker processing
duckpipe.debug_log = off                      -- emit critical-path timing logs

-- Backpressure settings
duckpipe.backpressure_enabled = on            -- enable backpressure throttling
duckpipe.backpressure_max_staging_rows = 100000  -- max total staging rows before throttling

-- DuckLake optimization settings
duckpipe.data_inlining_row_limit = 1000      -- inline small batches

-- Connection settings
duckpipe.max_snapshot_workers = 4            -- concurrent snapshot copies
duckpipe.connection_timeout = 30000          -- ms for PG connection timeout
```

---

## Comparison: v1 vs v2

| Aspect | v1 | v2 |
|---------|-----|------|
| Language | C | Rust |
| PG interaction | SPI (in-process) | Loopback tokio-postgres (no SPI) |
| Concurrency | Single-threaded poll loop | Per-table parallel batch processors |
| WAL consumption | pg_logical_slot_get_binary_changes via SPI | START_REPLICATION protocol (unified) |
| UPDATE handling | DELETE + INSERT (TOAST bug) | DuckDB MERGE with TOAST preservation (correct, efficient) |
| Batch flush | Size only | Size + time (no commit boundary alignment) |
| Staging | None (direct apply) | Per-table in-memory queues (Rust VecDeque) |
| Flush path | Direct SPI INSERT/DELETE | Queue → DuckDB buffer → compact → MERGE |
| DuckLake optimization | None | Data inlining, native DuckDB MERGE |
| Error handling | Log and continue | Classified errors with retry policies |
| Deployment | PG extension only | PG extension + standalone binary |
| State tracking | Group-level confirmed_lsn | Per-table applied_lsn + group confirmed_flush |
| Adding tables | Snapshot blocks poll loop | Independent snapshot workers |
| Backpressure | None | Queue-depth throttling (row count) |
| Source identification | Schema.table name | OID + URI (survives renames) |

---

## References

- Supabase etl architecture: https://github.com/MaterializeInc/etl
- pg_mooncake/moonlink architecture: `/Users/xiaoyuwei/Desktop/workspace_ducklake/pg_mooncake`
- postgres-replication crate: https://github.com/MaterializeInc/rust-postgres
- DuckLake storage model: pg_duckdb extension

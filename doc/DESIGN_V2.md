# pg_duckpipe: Architecture Design

## Executive Summary

pg_duckpipe is a CDC sync service from PostgreSQL heap tables to DuckLake columnar tables. It uses parallel per-table flush workers, in-memory staging queues, and DuckLake-specific optimizations to provide real-time replication with crash safety.

### Design Goals

| Goal | Approach |
|------|----------|
| At-least-once with idempotent replay | Per-table LSN tracking + idempotent DELETE+INSERT applies |
| Parallel batch apply | In-memory staging queues + per-table flush workers |
| No streaming disruption when adding tables | Independent snapshot workers |
| Shared replication slots | One slot per sync group (not per table) |
| Dual deployment | Same core runs as PostgreSQL extension or standalone binary |
| DuckLake-optimized | DuckDB Appender API, data inlining |

### Key Architectural Decisions

1. **No SPI** — Use loopback tokio-postgres connections instead of SPI
2. **In-memory staging queues** — Per-table `VecDeque` in Rust, no external dependencies for staging
3. **Unified WAL consumption** — Use START_REPLICATION protocol for all modes
4. **Per-table staging** — One queue per source table, drained by flush workers
5. **DuckDB buffer + DELETE+INSERT flush path** — Flush workers drain queue into DuckDB buffer table, compact and apply to DuckLake via DELETE+INSERT
6. **Backpressure-aware** — Per-queue byte tracking pauses WAL consumer when total queued bytes exceed threshold
7. **Source table OID** — Use OID as identifier to survive table renames
8. **REPLICA IDENTITY FULL** — Required on source tables to avoid TOAST-unchanged column issues

---

## Terminology

### Components

| Term | Definition |
|------|------------|
| **Source** | PostgreSQL database containing heap tables to replicate from |
| **Target** | PostgreSQL database with DuckLake extension containing columnar tables |
| **Sync Service** | The Rust-based service performing CDC (runs as bgworker or standalone) |
| **Sync Group** | A collection of tables sharing one publication and one replication slot |
| **Slot Consumer** | Task that reads WAL from replication slot and dispatches to per-table staging queues |
| **Flush Worker** | OS thread that drains a staging queue into a DuckDB buffer, compacts and applies to DuckLake |
| **Snapshot Worker** | Task that performs initial full copy of a table |

### Table States

| State | Meaning |
|-------|---------|
| **PENDING** | Table added but not yet started |
| **SNAPSHOT** | Initial full copy in progress |
| **CATCHUP** | Catching up from snapshot_lsn to current WAL |
| **STREAMING** | Normal operation, applying all changes |
| **ERRORED** | Error encountered, auto-retries with exponential backoff |

### Deployment Modes

| Mode | Description |
|------|-------------|
| **Extension** | Runs as PostgreSQL bgworker, connects via loopback |
| **Standalone** | Runs as separate process, connects via streaming replication |

Both modes use identical `duckpipe-core` sync logic.

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                      Sync Service Process                            │
│                                                                      │
│  Slot Consumer (1/group)    Per-Table Queues    Flush Threads        │
│    ─START_REPLICATION─►      ┌──────────┐       (OS threads)         │
│      decode pgoutput ──────► │ Queue T1 │ ──► FW1 ──►┐              │
│                       ─────► │ Queue T2 │ ──► FW2 ──►┤ Embedded     │
│                       ─────► │ Queue T3 │ ──► FW3 ──►┤ DuckDB       │
│                              └──────────┘            │ (per-worker)  │
│                                                      ▼              │
│  Snapshot Workers            ┌───────────────────────────────────┐  │
│  (tokio tasks, parallel)     │ DuckLake (attached in DuckDB)     │  │
│                              │   Appender ──► buffer ──► compact │  │
│                              │   ──► DELETE+INSERT ──► target    │  │
│                              └───────────────────────────────────┘  │
│                                                                      │
│  ┌─────────────────────┐                                            │
│  │ Source PostgreSQL    │    Backpressure: per-queue byte tracking   │
│  │ Heap Tables, WAL,   │    pauses slot consumer when > threshold   │
│  │ Slot, Publication   │                                            │
│  └─────────────────────┘                                            │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Component: Slot Consumer

Single async task per sync group. Reads WAL and dispatches decoded changes to per-table in-memory staging queues.

### Responsibilities

1. **WAL consumption** — START_REPLICATION protocol (unified for all modes)
2. **pgoutput decoding** — Parse binary protocol
3. **Route to queues** — Append decoded changes to per-table staging queues
4. **Backpressure control** — Sum per-queue byte counters, pause if total exceeds `max_queued_bytes`
5. **Checkpoint coordination** — Track per-table `applied_lsn`, compute `confirmed_flush_lsn`
6. **Replication slot advancement** — Send `StandbyStatusUpdate` with confirmed LSN

### Backpressure Design

The WAL consumer sums per-queue byte counters across all tables. When the total exceeds `max_queued_bytes` (default 256 MB), WAL consumption pauses until flush workers drain enough data.

This is byte-size-based rather than LSN-gap-based, which directly measures memory pressure. LSN gaps can misfire (large gaps from unrelated WAL consume no staging memory, while small gaps with many rows can exhaust memory). Byte tracking is more accurate than row counting because row sizes vary widely across tables.

---

## Component: Flush Workers

One **OS thread** per table (via `std::thread::spawn`). Each flush thread owns an embedded DuckDB connection with DuckLake attached, and a local single-threaded tokio runtime for async PG metadata updates.

### Responsibilities

1. **Drain staging queue** — Take changes from the per-table queue (capped at batch threshold per drain)
2. **Append to DuckDB buffer** — Via Appender API (binary, no SQL parsing)
3. **Compact duplicate PKs** — Deduplicate buffer rows using windowing function
4. **Apply to DuckLake** — DELETE matching PKs from target, then INSERT new/updated rows
5. **Track progress** — Update `applied_lsn` in metadata
6. **Handle errors** — Retry or enter ERRORED state with exponential backoff

### Flush Trigger

Flush is triggered by either condition:
- **Size**: queue reaches `flush_batch_threshold`
- **Time**: `flush_interval` elapsed since last flush

Whichever fires first initiates the flush. This ensures both throughput (large batches) and freshness (time-bound latency).

### Flush Path: Buffer + DELETE+INSERT

Each flush worker holds an in-memory DuckDB connection with DuckLake attached.

**DuckDB Session Setup (once per flush worker):**

```sql
INSTALL ducklake;
LOAD ducklake;
ATTACH 'ducklake:postgres:dbname=<db> host=<host>' AS lake (
    METADATA_SCHEMA 'ducklake'
);
```

**Append (via Appender API):**

```rust
let mut appender = conn.appender("buffer")?;
for change in &changes {
    appender.append_row([seq, op_type, pk, col_a, col_b, ...])?;
}
appender.flush()?;
```

**Compact (dedup by PK, keep last operation):**

```sql
CREATE TEMP TABLE compacted AS
    SELECT * EXCLUDE (_rn) FROM (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY pk_col ORDER BY _seq DESC
        ) AS _rn
        FROM buffer
    ) sub WHERE _rn = 1;
```

**Apply (DELETE+INSERT):**

```sql
-- Remove existing rows that match any PK in the batch
DELETE FROM target WHERE EXISTS (
    SELECT 1 FROM compacted WHERE compacted.pk = target.pk
);

-- Insert new and updated rows
INSERT INTO target (pk, col_a, col_b)
    SELECT pk, col_a, col_b FROM compacted
    WHERE _op_type IN (0, 1);  -- INSERT or UPDATE
```

### Skip-Delete Optimization

For pure-INSERT batches (no UPDATEs or DELETEs), the expensive DELETE scan against DuckLake Parquet files can be skipped. After the initial WAL-replay conflict window closes (detected when a pure-INSERT batch returns 0 deleted rows), the flush worker sets a `skip_delete` flag to avoid unnecessary Parquet scans.

### DuckDB Connection Model

Each flush worker creates its own in-memory DuckDB instance. This provides:

- **Full parallelism** — No single-writer contention
- **Process isolation** — A crash in one DuckDB instance does not affect others
- **Simple ownership** — Buffer table is local to the instance

**Trade-off:** Higher memory overhead (each instance has its own buffer pool). Acceptable because flush workers are long-lived and buffer tables are the primary memory consumer regardless.

---

## Component: Snapshot Workers

Independent tokio task per table. Performs initial full copy from source to DuckLake target.

### Mechanism

1. Open a **control connection** and begin `REPEATABLE READ` transaction
2. Create a temporary logical slot via SQL: `pg_create_logical_replication_slot(name, 'pgoutput', true)` — returns `consistent_point` LSN
3. Export the snapshot: `pg_export_snapshot()`
4. Open a **data connection**, import the snapshot: `SET TRANSACTION SNAPSHOT <id>`
5. Copy data: `DELETE FROM target; INSERT INTO target SELECT * FROM source`
6. Record `consistent_point` as `snapshot_lsn`, transition table to CATCHUP
7. Close control connection (temp slot auto-drops with session)

### Why Independent?

- **Does NOT touch the group's replication slot** — Uses its own temporary slot
- **Separate connections** — No coordination with slot consumer
- **Parallel execution** — Multiple tables snapshot simultaneously

---

## Table State Machine

```
            add_table(copy_data=true)       add_table(copy_data=false)
                    │                                │
                    ▼                                │
           ┌────────────────┐                       │
           │    SNAPSHOT     │                       │
           │ Copy data       │                       │
           └───────┬────────┘                       │
                    │                                │
           ┌───────▼────────┐                       │
           │    CATCHUP      │                       │
           │ Skip lsn <=     │                       │
           │ snapshot_lsn    │                       │
           └───────┬────────┘                       │
                    │                                ▼
           ┌───────▼────────────────────────────────┐
           │            STREAMING                    │
           │ Apply all changes, track applied_lsn    │
           └───────┬────────────────────────────────┘
                    │
           ┌───────▼────────┐
           │    ERRORED      │
           │ Auto-retry with │
           │ exponential     │
           │ backoff         │
           └────────────────┘
```

### Transaction Boundary Handling

Batch boundaries do NOT respect source transaction boundaries. A batch may contain partial transactions. This means DuckLake readers may observe intermediate states between flush cycles. This is an acceptable trade-off:

- The target is primarily for analytical queries, not transactional reads
- Strict boundary alignment would require buffering entire source transactions (unbounded size)
- After the next flush, the target converges to the source state

---

## Concurrency Model

```
Main thread (tokio current_thread runtime)
├── Slot Consumer (1 per sync group, async task)
│   └── Per-table staging queues (Mutex<VecDeque>)
├── Snapshot Workers (tokio tasks, parallel)
└── Backpressure: per-queue AtomicI64 byte counters, summed by WAL consumer

Flush Threads (OS threads via std::thread::spawn, 1 per table)
├── Each owns: DuckDB in-memory connection (DuckLake attached)
├── Each owns: local single-threaded tokio runtime (for async PG metadata updates)
└── Communicates via: Mutex<Vec<>> shared queue + mpsc channel
```

### Why OS Threads for Flush Workers?

Flush workers perform heavy DuckDB operations (Appender, buffer scans, DuckLake writes) that can block for extended periods. OS threads provide better isolation than tokio tasks:

- No risk of blocking the shared tokio runtime
- Each thread's local tokio runtime handles async PG metadata updates independently
- Simpler shutdown semantics

---

## Delivery Guarantee: At-Least-Once

True exactly-once would require atomic commits spanning both the DuckLake data write and the `applied_lsn` metadata update. Since these are different storage backends, at-least-once with idempotent replay is used instead.

### Mechanism

1. **Per-table `applied_lsn`** — Each table tracks progress persistently
2. **DELETE+INSERT apply** — Idempotent: re-applying the same batch produces the same result
3. **Checkpoint calculation** — `confirmed_flush_lsn = min(all applied_lsn)`
4. **Crash recovery** — On restart, replay from `confirmed_flush_lsn`, skip or re-apply idempotently

### On Restart

1. In-memory staging queues are empty (process restarted)
2. DuckDB buffer tables are empty (in-memory, lost on restart)
3. Load persisted `applied_lsn` for each table
4. Slot consumer resumes from `confirmed_flush_lsn`
5. Changes with `lsn <= applied_lsn` are skipped by flush workers
6. Changes near the boundary may be re-applied (idempotent)

---

## REPLICA IDENTITY FULL Requirement

Source tables must use `REPLICA IDENTITY FULL`. This ensures pgoutput sends all column values on every UPDATE, avoiding TOAST-unchanged column issues.

Without this, pgoutput omits unchanged TOAST columns (sending a special "unchanged" marker). Handling this correctly would require the flush worker to read the current target row and merge values — adding complexity and a read-before-write penalty.

By requiring FULL identity, the buffer schema is simplified (no `{col}_unchanged` flags) and every change is self-contained.

---

## Metadata Schema

Stored in the source PostgreSQL under the `duckpipe` schema.

```sql
CREATE TABLE duckpipe.sync_groups (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    publication     TEXT NOT NULL UNIQUE,
    slot_name       TEXT NOT NULL UNIQUE,
    enabled         BOOLEAN DEFAULT true,
    confirmed_flush_lsn  PG_LSN,
    last_sync_at    TIMESTAMPTZ
);

CREATE TABLE duckpipe.table_mappings (
    id              SERIAL PRIMARY KEY,
    group_id        INTEGER REFERENCES duckpipe.sync_groups(id),
    source_oid      BIGINT UNIQUE,
    source_schema   TEXT NOT NULL,
    source_table    TEXT NOT NULL,
    target_schema   TEXT NOT NULL,
    target_table    TEXT NOT NULL,
    state           TEXT NOT NULL DEFAULT 'PENDING',
    snapshot_lsn    PG_LSN,
    applied_lsn     PG_LSN,
    error_message   TEXT,
    retry_at        TIMESTAMPTZ,
    consecutive_failures INTEGER DEFAULT 0,
    enabled         BOOLEAN DEFAULT true,
    rows_synced     BIGINT DEFAULT 0,
    last_sync_at    TIMESTAMPTZ DEFAULT NOW()
);
```

---

## Publication Management

- **`add_table()`** — `ALTER PUBLICATION <pub> ADD TABLE <table>`
- **`remove_table()`** — `ALTER PUBLICATION <pub> DROP TABLE <table>`
- **`create_group()`** — `CREATE PUBLICATION <pub>` (initially empty)
- **`drop_group()`** — Drops publication and replication slot

---

## Schema Change Handling (DDL)

**Not supported.** Schema changes on source tables (ALTER TABLE ADD/DROP COLUMN, type changes) are not handled automatically.

When pgoutput sends an updated RELATION message that mismatches the cached schema, the table transitions to ERRORED. Manual intervention is required: update the target table schema, then `resync_table()`.

---

## Error Handling

| Class | Examples | Action |
|-------|----------|--------|
| **Transient** | Connection timeout, lock contention, DuckDB busy | Auto-retry with exponential backoff (30s x 2^n, max ~30 min) |
| **Configuration** | Schema mismatch, table not found, permission denied | Manual fix required |
| **Resource** | Disk full, replication slot limit exceeded | Manual fix required |

Per-table error isolation ensures one table's failures do not block other tables in the same sync group.

---

## Configuration Parameters

See [USAGE.md](./USAGE.md) for the full GUC reference with ranges and tuning examples.

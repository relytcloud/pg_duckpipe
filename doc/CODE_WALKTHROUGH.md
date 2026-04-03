# pg_duckpipe Code Walkthrough

A ground-up guide to understanding the pg_duckpipe codebase. This document is derived entirely from reading the source code, not from other documentation.

---

## Table of Contents

1. [What This Project Does](#1-what-this-project-does)
2. [Workspace Layout](#2-workspace-layout)
3. [The Big Picture: Data Flow](#3-the-big-picture-data-flow)
4. [Metadata Schema (The Database Tables)](#4-metadata-schema)
5. [duckpipe-core: The Shared Engine](#5-duckpipe-core-the-shared-engine)
   - 5.1 [types.rs — Data Structures](#51-typesrs)
   - 5.2 [state.rs — Per-Table State Machine](#52-staters)
   - 5.3 [decoder.rs — pgoutput Binary Parser](#53-decoderrs)
   - 5.4 [queue.rs — Per-Table Change Queues](#54-queuerrs)
   - 5.5 [metadata.rs — Async Database Operations](#55-metadatars)
   - 5.6 [slot_consumer.rs — Streaming Replication](#56-slot_consumerrs)
   - 5.7 [snapshot.rs — Initial Data Copy](#57-snapshotrs)
   - 5.8 [duckdb_flush.rs — DuckDB Buffer + Compact + Apply](#58-duckdb_flushrs)
   - 5.9 [flush_worker.rs — Flush Orchestration](#59-flush_workerrs)
   - 5.10 [service.rs — The Orchestrator](#510-servicers)
6. [duckpipe-pg: The PostgreSQL Extension](#6-duckpipe-pg-the-postgresql-extension)
   - 6.1 [lib.rs — Extension Init + GUC Parameters](#61-librs)
   - 6.2 [api.rs — SQL API](#62-apirs)
   - 6.3 [worker.rs — Background Worker Shell](#63-workerrs)
   - 6.4 [bootstrap.sql — Schema DDL](#64-bootstrapsql)
7. [duckpipe-daemon: The Standalone Daemon](#7-duckpipe-daemon-the-standalone-daemon)
8. [How a Change Flows End-to-End](#8-how-a-change-flows-end-to-end)
9. [Key Algorithms In Depth](#9-key-algorithms-in-depth)
   - 9.1 [DuckDB Flush: Buffer → Compact → Apply](#91-duckdb-flush)
   - 9.2 [TOAST Column Preservation](#92-toast-column-preservation)
   - 9.3 [Crash-Safe Slot Advancement](#93-crash-safe-slot-advancement)
   - 9.4 [CATCHUP Skip Logic](#94-catchup-skip-logic)
   - 9.5 [OID-Based Rename Safety](#95-oid-based-rename-safety)
   - 9.6 [Error Recovery with Exponential Backoff](#96-error-recovery)
10. [Testing](#10-testing)
11. [Build and Run](#11-build-and-run)

---

## 1. What This Project Does

pg_duckpipe is a CDC (Change Data Capture) system that automatically replicates data from regular PostgreSQL heap tables into DuckLake columnar tables (provided by the pg_duckdb extension). This enables HTAP — your OLTP writes go to heap tables, and pg_duckpipe continuously mirrors them into columnar storage for fast analytics.

The system has two deployment modes:
- **Mode 1 (Extension)**: Runs as a PostgreSQL background worker inside the database process.
- **Mode 2 (Standalone)**: Runs as a separate `duckpipe` daemon process, connecting over TCP.

Both modes share the same core engine (`duckpipe-core`).

---

## 2. Workspace Layout

```
pg_duckpipe/
├── Cargo.toml                      # Workspace root (3 member crates)
├── Makefile                        # Build + test orchestration
│
├── duckpipe-core/                  # Shared CDC engine (pure Rust, no PG deps)
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs                  # Module exports
│       ├── connstr.rs              # Shared connstr parsing (parse_connstr, build_tokio_pg_connstr)
│       ├── types.rs                # Value enum, Change, SyncGroup, TableMapping, LSN helpers
│       ├── state.rs                # SyncState enum + transition validation
│       ├── error.rs                # DuckPipeError + ErrorClass
│       ├── decoder.rs              # pgoutput binary protocol parser (typed values)
│       ├── queue.rs                # Per-table VecDeque<Change> staging
│       ├── metadata.rs             # Async reads/writes to duckpipe.* tables
│       ├── slot_consumer.rs        # START_REPLICATION streaming via pgwire-replication
│       ├── snapshot.rs             # Initial snapshot via SQL functions
│       ├── duckdb_flush.rs         # Embedded DuckDB: Appender → buffer → compact → apply
│       ├── flush_coordinator.rs    # Decoupled producer-consumer coordinator with self-triggered flush threads
│       ├── flush_worker.rs         # PG metrics update + error state handling utility
│       └── service.rs              # Orchestrator: run_sync_cycle(), WAL processing
│
├── duckpipe-pg/                    # PostgreSQL extension (pgrx)
│   ├── Cargo.toml
│   ├── build.rs                    # Links libpq
│   └── src/
│       ├── lib.rs                  # _PG_init, GUC registration, SharedMetrics SHM
│       ├── api.rs                  # SQL functions: add_table, create_group, metrics(), etc.
│       ├── worker.rs               # BGWorker entry point → calls duckpipe-core
│       ├── bin/pgrx_embed.rs       # pgrx build stub
│       └── sql/bootstrap.sql       # CREATE TABLE duckpipe.sync_groups, etc.
│
├── duckpipe-daemon/                # Standalone daemon
│   ├── Cargo.toml
│   └── src/
│       └── main.rs                 # CLI (clap) + tokio::main → calls duckpipe-core
│
└── test/regression/                # SQL regression tests
    ├── regression.conf             # PG config: wal_level=logical, etc.
    ├── schedule                    # Test execution order
    ├── sql/                        # Test SQL files
    └── expected/                   # Expected output
```

**Dependencies** (from workspace `Cargo.toml`):

| Crate | Purpose |
|-------|---------|
| `tokio` | Async runtime |
| `tokio-postgres` | Async PG client (metadata queries, snapshot connections) |
| `pgwire-replication` | START_REPLICATION streaming protocol |
| `duckdb` (bundled) | Embedded DuckDB for flush operations |
| `pgrx` | PostgreSQL extension framework (duckpipe-pg only) |
| `clap` | CLI argument parsing (daemon only) |
| `tracing` | Structured logging |
| `thiserror` | Error derive macros |

---

## 3. The Big Picture: Data Flow

```
┌──────────────────────────────────────────────────────────────────────┐
│                        PostgreSQL Instance                           │
│                                                                      │
│  ┌─────────────-┐        ┌──────────────────────────────────────────┐│
│  │ Heap Tables  │        │ DuckLake Tables (via pg_duckdb)          ││
│  │ (your OLTP   │        │ (columnar storage for analytics)         ││
│  │  writes)     │        │                                          ││
│  └──────┬───────┘        └────────────────────▲─────────────────────┘│
│         │ WAL                                 │ DELETE + INSERT      │
│         ▼                                     │                      │
│  ┌───────────────────┐                        │                      │
│  │ Replication Slot  │                        │                      │
│  │ (pgoutput binary) │                        │                      │
│  └────────┬──────────┘                        │                      │
└───────────┼───────────────────────────────────┼──────────────────────┘
            │                                   │
            ▼                                   │
┌───────────────────────────────────────────────────────────────────────┐
│                    duckpipe-core (Rust)                               │
│                                                                       │
│  ┌──────────────┐   ┌──────────┐   ┌────────┐   ┌─────────────────┐  │
│  │SlotConsumer  │──▶│ Decoder  │──▶│ Queues │──▶│ DuckDB Flush    │  │
│  │(streaming    │   │(pgoutput │   │(per-   │   │ (buffer→compact │──┘
│  │ replication) │   │ binary)  │   │ table) │   │  →apply)        │
│  └──────────────┘   └──────────┘   └────────┘   └─────────────────┘
│                                                                       │
│  Invoked by either:                                                   │
│    duckpipe-pg (BGWorker, Unix socket)                                │
│    duckpipe-daemon (standalone, TCP)                                  │
└───────────────────────────────────────────────────────────────────────┘
```

WAL consumption uses **streaming replication** exclusively: `START_REPLICATION` via the `pgwire-replication` crate. This provides near-zero latency and crash-safe slot advancement (explicit `StandbyStatusUpdate` after flush, rather than automatic slot advancement).

---

## 4. Metadata Schema

Created by `bootstrap.sql` on `CREATE EXTENSION pg_duckpipe`:

### `duckpipe.sync_groups`

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL PK | Group ID |
| `name` | TEXT UNIQUE | Human-readable name (e.g., `'default'`) |
| `publication` | TEXT UNIQUE | PG publication name (e.g., `'duckpipe_pub_mydb'`) |
| `slot_name` | TEXT UNIQUE | Replication slot name (e.g., `'duckpipe_slot_mydb'`) |
| `enabled` | BOOLEAN | Whether this group is being processed |
| `confirmed_lsn` | pg_lsn | Crash-safe LSN: min(applied_lsn) across all active tables |
| `last_sync_at` | TIMESTAMPTZ | Last successful sync timestamp |

A `'default'` group is auto-created during extension installation.

### `duckpipe.table_mappings`

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL PK | Mapping ID |
| `group_id` | INTEGER FK | References sync_groups(id) |
| `source_schema` | TEXT | Source table schema |
| `source_table` | TEXT | Source table name |
| `target_schema` | TEXT | DuckLake target schema |
| `target_table` | TEXT | DuckLake target table name |
| `state` | TEXT | One of: PENDING, SNAPSHOT, CATCHUP, STREAMING, ERRORED |
| `snapshot_lsn` | pg_lsn | WAL position when snapshot was taken |
| `applied_lsn` | pg_lsn | Highest LSN successfully applied to this table |
| `enabled` | BOOLEAN | Whether this table is being synced |
| `rows_synced` | BIGINT | Cumulative rows synced (for monitoring) |
| `source_oid` | BIGINT UNIQUE | PG `pg_class.oid` for rename-safe WAL routing |
| `error_message` | TEXT | Most recent error (NULL when healthy) |
| `retry_at` | TIMESTAMPTZ | Next auto-retry time (for ERRORED state) |
| `consecutive_failures` | INTEGER | Counter for exponential backoff |

UNIQUE constraint on `(source_schema, source_table)` prevents duplicate mappings.

---

## 5. duckpipe-core: The Shared Engine

### 5.1 `types.rs`

Defines the fundamental data structures used everywhere.

**`Value`** — typed column value parsed from pgoutput text representation:
```rust
pub enum Value {
    Null,
    Bool(bool),
    Int16(i16),     // PG int2 (type OID 21)
    Int32(i32),     // PG int4 (type OID 23), oid (26)
    Int64(i64),     // PG int8 (type OID 20)
    Float32(f32),   // PG float4 (type OID 700)
    Float64(f64),   // PG float8 (type OID 701)
    Text(String),   // Everything else (DuckDB auto-casts)
}
```

Type OIDs come from pgoutput RELATION messages. Unrecognized types fall back to `Text(String)`, which DuckDB auto-casts to the buffer table's declared column type at Appender insert time.

**`ChangeType`** — what happened to a row:
```rust
pub enum ChangeType {
    Insert,   // New row
    Delete,   // Row removed (carries key_values only)
    Update,   // Row modified with TOAST unchanged columns
}
```

**`Change`** — a single decoded WAL change:
```rust
pub struct Change {
    pub change_type: ChangeType,
    pub lsn: u64,                    // WAL position
    pub col_values: Vec<Value>,      // Full row values (INSERT/UPDATE)
    pub key_values: Vec<Value>,      // Primary key values (DELETE/UPDATE)
    pub col_unchanged: Vec<bool>,    // TOAST unchanged flags (UPDATE only)
}
```

`Value::Null` replaces the old `Option::None`. The `col_unchanged` flag remains separate since unchanged columns have no value available — they are `Value::Null` with `col_unchanged[i] = true`.

**`RelCacheEntry`** — cached schema info from pgoutput RELATION messages:
```rust
pub struct RelCacheEntry {
    pub nspname: String,          // Schema name
    pub relname: String,          // Table name
    pub attnames: Vec<String>,    // Column names in ordinal order
    pub attkeys: Vec<usize>,      // Indices of primary key columns
    pub atttypes: Vec<u32>,       // PostgreSQL type OIDs (one per column)
}
```

**`SyncGroup`** — a publication + replication slot pair:
```rust
pub struct SyncGroup {
    pub id: i32,
    pub name: String,
    pub publication: String,
    pub slot_name: String,
    pub pending_lsn: u64,       // Highest LSN seen this cycle (from COMMIT messages)
    pub confirmed_lsn: u64,     // Crash-safe LSN from metadata
}
```

**`TableMapping`** — maps source → target with state:
```rust
pub struct TableMapping {
    pub id: i32,
    pub source_schema: String, pub source_table: String,
    pub target_schema: String, pub target_table: String,
    pub state: String,          // "PENDING", "SNAPSHOT", "CATCHUP", "STREAMING", "ERRORED"
    pub snapshot_lsn: u64,
    pub applied_lsn: u64,
    pub enabled: bool,
    pub source_oid: Option<i64>,
    pub error_message: Option<String>,
}
```

**LSN helpers:**
- `parse_lsn("0/1C463F8") -> u64`: Splits on `/`, parses each half as hex, combines into 64-bit value.
- `format_lsn(u64) -> "0/1C463F8"`: Reverse operation.

### 5.2 `state.rs`

Per-table state machine with validated transitions.

```
                   ┌──────────┐
                   │  PENDING │
                   └────┬──┬──┘
          copy_data=true│  │copy_data=false
                        ▼  ▼
               ┌──────────┐  ┌───────────┐
               │ SNAPSHOT │  │ STREAMING │◀─── auto-retry ───┐
               └─────┬────┘  └──┬──────┬─┘                   │
                     │          │      │                      │
                     ▼          │      ▼                      │
               ┌──────────┐    │  ┌─────────┐                │
               │ CATCHUP  │────┘  │ ERRORED │────────────────┘
               └──────────┘       └────┬────┘
                                       │
                                  resync_table()
                                       │
                                       ▼
                                  ┌──────────┐
                                  │ SNAPSHOT  │  (restart)
                                  └──────────┘
```

Key method: `can_transition_to(target) -> bool` enforces valid paths.

### 5.3 `decoder.rs`

Pure-Rust parser for PostgreSQL's pgoutput binary protocol (version 1). No PostgreSQL library dependencies — works identically in the background worker and standalone daemon.

**Low-level readers** (all big-endian, advance cursor):
- `read_byte`, `read_i16`, `read_i32`, `read_i64`, `read_string` (null-terminated), `read_bytes`

**High-level parsers:**

`parse_relation_message(data, cursor) -> (rel_id, RelCacheEntry)` — decodes 'R' messages:
```
[rel_id:i32] [nspname:string] [relname:string] [replica_ident:u8]
[natts:i16] [flags:u8 name:string type_oid:i32 type_mod:i32]...
```
Builds a `RelCacheEntry` with column names, primary key indices (bit 0 of flags), and type OIDs (retained for typed value parsing).

`parse_text_value(text, type_oid) -> Value` — converts pgoutput text representation to a typed `Value` using the column's PostgreSQL type OID. Recognized types: bool (16), int2 (21), int4 (23), oid (26), int8 (20), float4 (700), float8 (701). Everything else falls back to `Text(String)`.

`parse_tuple_data(data, cursor, atttypes) -> (Vec<Value>, unchanged_flags)` — decodes row data:
```
[natts:i16] for each:
  'n' → Value::Null (unchanged=false)
  'u' → Value::Null (unchanged=true)  ← critical for UPDATE
  't' → [len:i32] [bytes:len] → parse_text_value(text, atttypes[i])
```

`extract_key_values(values, key_attrs) -> Vec<Value>` — picks out PK columns by index.

### 5.4 `queue.rs`

Simple per-table staging area.

**`TableQueue`**: Holds a `VecDeque<Change>` plus metadata (target_key, mapping_id, column names, key column indices, type OIDs, last_lsn). Supports `push`, `drain`, `len`, `is_empty`. Used by flush threads to pass batched changes to `FlushWorker::flush()`.

### 5.5 `metadata.rs`

Async operations against `duckpipe.sync_groups` and `duckpipe.table_mappings` via `tokio-postgres`.

**`MetadataClient<'a>`** wraps a `&'a tokio_postgres::Client` and provides:

**Sync group ops:**
- `get_enabled_sync_groups()` — fetch all groups where `enabled = true`
- `update_confirmed_lsn(group_id, lsn)` — update crash-safe LSN + last_sync_at
- `get_min_applied_lsn(group_id)` — `MIN(applied_lsn)` across STREAMING/CATCHUP tables; returns 0 if any table has NULL applied_lsn (not yet flushed)

**Table mapping ops:**
- `get_table_mapping(group_id, schema, table)` — name-based lookup
- `get_table_mapping_by_oid(group_id, oid)` — OID fallback for rename safety
- `update_source_name(id, new_schema, new_table)` — called when rename detected
- `update_applied_lsn(id, lsn)` — per-table flush checkpoint

**State transitions:**
- `set_catchup_state(id, snapshot_lsn, duration_ms, snapshot_rows)` — SNAPSHOT → CATCHUP, stores snapshot timing
- `transition_catchup_to_streaming(group_id, pending_lsn)` — bulk promote CATCHUP tables where `snapshot_lsn <= pending_lsn`

**Error tracking:**
- `record_error_message(id, msg)` — store error without changing state
- `increment_consecutive_failures(id) -> i32` — bump counter, return new value
- `clear_consecutive_failures(id)` — reset to 0 on success
- `set_errored_with_retry(id, msg, backoff_secs)` — set ERRORED + retry_at
- `get_retryable_errored_tables(group_id)` — tables where `retry_at <= now()`
- `retry_errored_table(id)` — ERRORED → STREAMING, clear error state

**Snapshot support:**
- `get_snapshot_tasks(group_id)` — all tables in SNAPSHOT state
- `load_batch_metadata_from_source(schema, table)` — query `pg_attribute` + `pg_index` to get column names and PK indices
- `slot_exists(slot_name)` — check `pg_replication_slots`

### 5.6 `slot_consumer.rs`

Streaming replication consumer using the `pgwire-replication` crate.

**`SlotConsumer`** wraps a `ReplicationClient` and provides:

Two constructors:
- `connect(socket_dir, port, user, database, slot, publication, start_lsn)` — Unix socket (bgworker)
- `connect_tcp(host, port, user, password, database, slot, publication, start_lsn)` — TCP (daemon)

Both configure: start_lsn (typically confirmed_lsn), status_interval = 500ms, wakeup_interval = 100ms.

**`poll_messages(max_count, timeout_ms) -> Vec<(u64, Vec<u8>)>`**

Receives up to `max_count` WAL messages within `timeout_ms`. Returns `(lsn, pgoutput_binary)` tuples.

The `pgwire-replication` crate pre-parses BEGIN/COMMIT into structured events. To keep both consumption paths feeding identical data into the decoder, `SlotConsumer` synthesizes the raw pgoutput binary format:
- `ReplicationEvent::Begin` → 21-byte 'B' message
- `ReplicationEvent::Commit` → 26-byte 'C' message
- `ReplicationEvent::XLogData` → passed through as-is

**`send_status_update(lsn)`** — report that all WAL up to `lsn` has been durably applied. The crate sends the actual `StandbyStatusUpdate` on its next status interval.

**`close()`** — graceful shutdown: `CopyDone` + final status update.

**Design: reconnect-per-cycle.** Each sync cycle creates a fresh `SlotConsumer`, processes available WAL, then disconnects. Unix socket reconnect is ~1ms. This avoids long-lived streaming connections that can have keepalive issues with the pgrx `block_on()` boundary.

### 5.7 `snapshot.rs`

Initial data copy using SQL functions (not the replication protocol).

**Why SQL-based?** `tokio-postgres` 0.7 does not support the `replication=database` startup parameter. The SQL function `pg_create_logical_replication_slot()` (PG14+) provides the same consistent_point LSN.

**`process_snapshot_task(source_schema, source_table, target_schema, target_table, connstr, timing, task_id) -> Result<u64, String>`**

Returns the consistent_point LSN on success.

Algorithm (two connections):

```
Connection A (control — holds snapshot open):
  1. BEGIN ISOLATION LEVEL REPEATABLE READ
  2. SELECT pg_create_logical_replication_slot('duckpipe_snap_{task_id}', 'pgoutput', true)
     → consistent_point LSN (snapshot established at this query)
  3. SELECT pg_export_snapshot()
     → snapshot_name

Connection B (data — performs copy):
  4. BEGIN ISOLATION LEVEL REPEATABLE READ
  5. SET TRANSACTION SNAPSHOT '{snapshot_name}'
  6. DELETE FROM target   (clear stale data)
  7. INSERT INTO target SELECT * FROM source
  8. COMMIT

Connection A (cleanup):
  9. COMMIT (exported snapshot becomes invalid)
  10. Disconnect (temp slot auto-drops)
```

Each snapshot task uses a unique slot name (`duckpipe_snap_{task_id}`) so concurrent snapshot tasks for different tables don't collide. The copy result is wrapped in an inner async block to ensure control connection cleanup always runs.

### 5.8 `duckdb_flush.rs`

The most complex module. Uses an embedded DuckDB instance to buffer, deduplicate, and apply changes to DuckLake.

**Why DuckDB?** DuckLake tables are managed by the DuckDB engine (via pg_duckdb). Direct SQL INSERT/DELETE from PostgreSQL is slow. Using an embedded DuckDB instance that ATTACHes the same DuckLake catalog gives native DuckDB performance for batch operations.

**Buffer loading** uses DuckDB's native Appender API (not SQL INSERT). Typed `Value` variants map directly to DuckDB `ToSql` types via the `push_value_to_row()` helper. This bypasses SQL parsing entirely, eliminates SQL injection risk from manual quoting, and is faster for numeric-heavy tables where string formatting/parsing overhead is avoided. `Value::Text` strings are auto-cast by DuckDB to the buffer table's declared column types.

**`FlushWorker`** — persistent per-table DuckDB session:

```rust
pub struct FlushWorker {
    db: duckdb::Connection,              // In-memory DuckDB
    lake_info: Option<LakeTableInfo>,    // Cached schema info
}
```

Created once per target table (expensive one-time setup):
1. `Connection::open_in_memory()`
2. `INSTALL ducklake; LOAD ducklake;`
3. `ATTACH 'ducklake:postgres:{connstr}&schema={ducklake_schema}' AS lake`

> **Note: dual ducklake extension in bgworker mode.** Each FlushWorker opens its own
> in-memory DuckDB connection via duckdb-rs (linked against `libduckdb.so`). That DuckDB
> instance does not have ducklake built-in, so it `INSTALL`s the `.duckdb_extension` from
> the offline cache (`~/.duckdb/extensions/`). In bgworker mode, pg_ducklake is also loaded
> into the same process with ducklake statically linked — meaning two copies of the ducklake
> code coexist. This is safe because they operate on separate DuckDB instances with separate
> state, and DuckDB's extension loader uses `RTLD_LOCAL` symbol scoping. However, version
> parity between the two copies is important — the `.duckdb_extension` version must match the
> `libduckdb.so` version shipped by pg_ducklake. In daemon mode this is not a concern since
> pg_ducklake is not loaded into the daemon process.

Cached `LakeTableInfo` holds the actual schema name inside DuckLake and column types (from `information_schema` with `table_catalog = 'lake'`).

**`flush(queue: TableQueue) -> Result<DuckDbFlushResult>`** — the core algorithm (see [Section 9.1](#91-duckdb-flush) for detailed walkthrough).

Workers are managed by the `FlushCoordinator` (see section 5.8b). Each persistent flush thread lazily creates its own `FlushWorker` and drops it on error for automatic recovery.

### 5.8b `flush_coordinator.rs`

Decoupled producer-consumer coordinator with self-triggered per-table flush threads. Flush threads are fully independent from the WAL consumer — no synchronous barrier between WAL processing and flush.

**`FlushCoordinator`** — manages shared queues, long-lived flush threads, and backpressure:

```rust
pub struct FlushCoordinator {
    pg_connstr: String,
    ducklake_schema: String,
    threads: HashMap<String, FlushThreadEntry>,  // Per-table thread + queue
    result_tx: mpsc::Sender<FlushThreadResult>,
    result_rx: mpsc::Receiver<FlushThreadResult>,
    backpressure: Arc<BackpressureState>,         // Per-queue byte tracking + max_queued_bytes
    flush_batch_threshold: usize,
    flush_interval_ms: u64,
    flush_gate: Arc<FlushGate>,                  // Limits concurrent flush operations
}
```

Key data structures:
- `SharedTableQueue` — `Mutex`-protected queue with `Vec<Change>`, last_lsn, and `QueueMeta` (schema info)
- `TableQueueHandle` — `Arc`-shared between producer and consumer, with `Condvar` for waking the flush thread
- `ThreadControl` — `AtomicBool` flags for shutdown and drain_requested (lock-free signaling)
- `FlushThreadEntry` — per-table coordinator state: queue handle, control, join handle, drain_complete barrier
- `BackpressureState` — per-queue `AtomicI64` byte counters incremented by producer and decremented by flush threads; `max_queued_bytes` threshold; WAL consumer sums all queues to check backpressure
- `FlushGate` — ticket-based FIFO semaphore limiting concurrent `flush_buffer()` calls (see below)

**FlushGate** — controls flush parallelism:

With 100+ tables, all flush threads trying to `flush_buffer()` concurrently would cause memory spikes (each gets `duckdb_flush_memory_mb`) and DuckLake commit lock contention. `FlushGate` is a ticket-based FIFO semaphore that limits how many threads flush at once (`max_concurrent_flushes`, default 4). Threads that can't acquire a slot continue buffering in low-memory mode.

Fairness: threads take a ticket on arrival and wait in strict FIFO order — no starvation possible. The timeout adapts to the median of the last 64 flush durations (floor 1s), falling back to `flush_interval` until enough data is collected. On timeout, the thread forfeits its ticket so it doesn't block the queue. Drain requests (TRUNCATE/shutdown) bypass the gate entirely for correctness.

**Public API:**
- `new(pg_connstr, ducklake_schema, group_name, flush_batch_threshold, flush_interval_ms, max_queued_bytes, duckdb_buffer_memory_mb, duckdb_flush_memory_mb, max_concurrent_flushes)` — create coordinator with FlushGate, no threads yet
- `ensure_queue(target_key, mapping_id, attnames, key_attrs, atttypes)` — create queue + spawn flush thread if new (or respawn if dead); passes `Arc<FlushGate>` to thread
- `push_change(target_key, change)` — lock queue, push change, notify condvar, increment per-queue byte counter
- `is_backpressured() -> bool` — true when sum of per-queue byte counters >= max_queued_bytes
- `get_meta(mapping_id) -> Option<QueueMeta>` — clone current queue metadata (used to build TRUNCATE barriers)
- `drain_and_wait_all() -> Vec<FlushThreadResult>` — synchronous barrier (retained for shutdown only)
- `collect_results() -> Vec<FlushThreadResult>` — non-blocking drain of mpsc result channel
- `set_max_concurrent_flushes(n)` — dynamically update gate limit (for runtime GUC changes)
- `active_flush_count() -> usize` — current number of threads actively flushing
- `shutdown()` — signal all threads to stop, join all
- `clear()` — `shutdown()` + recreate mpsc channel (panic recovery)

**Flush thread main loop** (`flush_thread_main`) — self-triggered:

Each flush thread creates its own `tokio::runtime::Runtime` for async PG metadata updates (avoids deadlock with the main thread's `current_thread` runtime since `tokio_postgres::connect` spawns a connection driver task).

1. Calculate `wait_timeout` = remaining time until next flush_interval trigger
2. Lock queue, `wait_timeout` on Condvar (not indefinite wait)
3. If shutdown → flush accumulated, break
4. Drain new changes from shared queue to local accumulator, decrement per-queue byte counter
5. Self-trigger check: accumulated >= `batch_threshold` OR elapsed >= `flush_interval`
6. If should flush:
   a. Acquire `FlushGate` slot (ticket-based FIFO wait with adaptive timeout; drain requests bypass gate)
   b. Build `TableQueue` from accumulated changes
   c. `FlushWorker.flush(table_queue)` → returns `DuckDbFlushResult` with `applied_count`, `last_lsn`, `flush_duration_ms`
   d. `rt.block_on(update_metrics_via_pg(...))` — write applied_lsn to PG
   e. `rt.block_on(clear_error_on_success(...))` — clear error state
   f. Send `FlushThreadResult::Success { mapping_id, applied_count, last_lsn, flush_duration_ms }` via mpsc
   g. Record flush duration in `FlushGate` ring buffer (feeds adaptive timeout)
   h. Release `FlushGate` slot
   i. On error: `rt.block_on(update_error_state(...))` — record error, ERRORED transition
   i. Reset timer
7. If drain_requested → signal drain_complete (for TRUNCATE compatibility)

**Thread safety:**
| Component | Mechanism |
|-----------|-----------|
| `SharedTableQueue.changes` | `Mutex` — producer locks to push, consumer locks to drain |
| `Condvar` | Wakes consumer with timeout; spurious wakeups handled by condition check |
| `ThreadControl` flags | `AtomicBool` with Acquire/Release ordering |
| `drain_complete` | `Mutex<bool>` + `Condvar` — main thread waits, flush thread signals |
| `BackpressureState` per-queue byte counters | `AtomicI64` per queue — producer increments, flush threads decrement; WAL consumer sums for backpressure check |
| `FlushGate` | `Mutex` + `Condvar` — ticket-based FIFO; threads wait on condvar for their turn |
| `FlushWorker` | `Send` + `!Sync` — exclusive ownership within each flush thread |
| `tokio::runtime::Runtime` | Per-thread — each flush thread owns its own runtime for PG ops |
| `mpsc` channel | `Sender` cloned per-thread, `Receiver` on main thread only |

### 5.9 `flush_worker.rs`

Utilities for PostgreSQL metadata updates, called by flush threads via `rt.block_on()`.

**`update_metrics_via_pg(connstr, mapping_id, applied_count, last_lsn)`**:
- Creates a short-lived `tokio-postgres` connection
- Updates `rows_synced += applied_count`, `last_sync_at = now()`
- Updates `applied_lsn` if `last_lsn > 0`
- Called from each flush thread after successful DuckDB flush

**`update_error_state(connstr, mapping_id, error, errored_threshold)`**:
- Records error message, increments consecutive_failures
- If failures >= errored_threshold (3): transitions to ERRORED with exponential backoff
- Called from each flush thread on flush failure

**`clear_error_on_success(connstr, mapping_id)`**:
- Resets consecutive_failures to 0 and clears error_message
- Called from each flush thread after successful flush

### 5.10 `service.rs`

The orchestrator. Contains the main sync cycle and WAL message processing.

**`ServiceConfig`** — all tuning knobs:
```rust
pub struct ServiceConfig {
    pub poll_interval_ms: i32,       // Sleep between cycles when idle
    pub batch_size_per_group: i32,   // Max WAL messages per cycle (default 100000)
    pub debug_log: bool,             // Timing diagnostics
    pub connstr: String,             // PG connection string
    pub duckdb_pg_connstr: String,   // PG connstr for DuckDB ATTACH
    pub ducklake_schema: String,     // DuckLake schema name
    pub flush_interval_ms: i32,      // Time trigger for flush
    pub flush_batch_threshold: i32,  // Size trigger for flush
    pub max_queued_bytes: i64,       // Backpressure threshold (bytes)
    pub duckdb_buffer_memory_mb: i32,   // DuckDB memory during buffering
    pub duckdb_flush_memory_mb: i32,    // DuckDB memory during flush
    pub max_concurrent_flushes: i32, // FlushGate concurrency limit
}
```

**`SlotConnectParams`** — how to connect the streaming consumer:
```rust
pub enum SlotConnectParams {
    Unix { socket_dir, port, user, dbname },      // BGWorker
    Tcp { host, port, user, password, dbname },    // Daemon
}
```

**`run_sync_cycle(config, coordinator, slot_params) -> Result<bool>`**

The top-level function called by both the bgworker and daemon. One complete cycle:

1. **Connect** to PostgreSQL via `tokio-postgres` (local metadata connection)
2. **Get enabled sync groups** from metadata (includes `conninfo`)
3. **For each group:**
   - **Remote groups** (`conninfo IS NOT NULL`): open a per-group `tokio-postgres` connection to the remote PG for catalog queries; build per-group `SlotConnectParams` from conninfo for WAL replication; use remote connstr for snapshot COPY
   - Process snapshots (concurrent `tokio::spawn` per table)
   - Stream WAL via `SlotConsumer` (streaming replication, routed to remote PG for remote groups)
   - Pass `source_client` through to PK metadata queries (uses remote PG for remote groups, local for local groups)
4. **Disconnect** (remote source connections closed per-group)
5. Return whether any work was done (caller uses this to decide sleep vs. immediate re-poll)

**`process_snapshots(meta, group, connstr, timing)`** — spawns all SNAPSHOT tables as concurrent tokio tasks. Each runs `snapshot::process_snapshot_task()` independently. On success: `set_catchup_state()` stores the snapshot_lsn, duration_ms, and rows_copied. On failure: `record_error_message()` (table stays in SNAPSHOT for retry).

**`process_sync_group_streaming(client, meta, group, config, consumer, coordinator)`** — streaming WAL consumption:
1. Check backpressure — if flush threads are lagging, skip this poll round (but still read `min(applied_lsn)` from PG and advance slot)
2. `consumer.poll_messages(batch_size_per_group, timeout)` — poll up to 100K WAL messages with ~500ms timeout
3. Feed to `process_wal_messages()` — decode all, push to coordinator queues (no synchronous flush barrier)
4. `consumer.send_status_update(confirmed_lsn)`

**`process_wal_messages(...)` — the core decoder loop:**

Iterates over `(lsn, data)` tuples, parsing each pgoutput binary message:

| Message | Action |
|---------|--------|
| `'R'` (RELATION) | Cache schema info in `rel_cache`; detect DDL changes via diffing (see §5.1) |
| `'I'` (INSERT) | Decode typed values, resolve mapping, skip if disabled/ERRORED/CATCHUP, push to queue |
| `'U'` (UPDATE) | Decode old+new, if TOAST unchanged → `Update` change, else → `Delete` + `Insert` |
| `'D'` (DELETE) | Decode old key values, push `Delete` change |
| `'B'` (BEGIN) | Parse but no action (LSN tracked from COMMIT) |
| `'C'` (COMMIT) | Update `group.pending_lsn = end_lsn` |
| `'T'` (TRUNCATE) | Enqueue barrier (`DdlCommand::Truncate`); flush thread drains then executes `DELETE FROM` |

After the message loop:
1. `coordinator.collect_results()` — non-blocking drain of flush results for error logging (no synchronous barrier)
2. Auto-retry ERRORED tables whose `retry_at` has passed
3. `transition_catchup_to_streaming()` — promote CATCHUP tables where `snapshot_lsn <= pending_lsn`
4. Calculate `confirmed_lsn = min(applied_lsn)` from PG metadata (reads whatever flush threads have written)
5. Update `confirmed_lsn` in metadata

**Decoupled flush** — WAL consumer and flush threads are fully independent:
- Each table's flush thread runs in its own OS thread with its own `FlushWorker` (owns `duckdb::Connection`) and its own tokio runtime (for PG metadata updates)
- Flush threads self-trigger based on queue size (`flush_batch_threshold`) or time (`flush_interval`)
- On success: flush threads update `rows_synced` and `applied_lsn` in PG directly; send `FlushThreadResult::Success` with `flush_duration_ms` back to main thread via mpsc
- The main thread (`collect_results()`) tracks `per_table_flush_count` and `per_table_flush_duration` in-memory; the bgworker writes these to PG shared memory (SHM) after each sync cycle
- On failure: `FlushWorker` is dropped (lazily recreated), flush thread records error in PG, transitions to ERRORED after 3 consecutive failures
- Per-table error isolation: one table failing doesn't block others
- Backpressure: WAL consumer pauses when total queued bytes exceed `max_queued_bytes`
- TRUNCATE: non-blocking barrier queue (same as DDL); flush thread drains pending changes then executes DELETE
- Crash safety: replication slot only advances past what all tables have durably flushed (confirmed_lsn = min(applied_lsn) read from PG)

### 5.1. Schema DDL Sync

Schema changes (ADD/DROP/RENAME COLUMN) are automatically propagated from source to DuckLake target tables. Source table renames are tracked (metadata updated) but do **not** rename the target — the target table name is stable and independent of the source name.

**Detection** — pgoutput sends a RELATION message before the first DML after a schema change. The `'R'` handler in `process_one_wal_message()` compares the new RELATION with the cached entry via `detect_schema_changes()`:

- **ADD COLUMN**: column names in new but not in old → query `pg_attribute` for type
- **DROP COLUMN**: column names in old but not in new
- **RENAME COLUMN**: same position, different name, same type OID

**Target resolution** — `apply_ddl_commands()` resolves the current target table name from `target_oid` via `pg_class` rather than using a stored name string. This means the pipeline survives user-initiated renames of the target table. For old mappings without `target_oid`, it falls back to the name stored in metadata.

**Propagation** — DDL is treated as a non-blocking barrier event in the per-table queue (`PendingDdl`). The WAL consumer sets the barrier and continues immediately; the flush thread handles it autonomously:

1. WAL consumer detects schema diff → calls `coordinator.set_pending_ddl()` with `DdlCommand` list and new `QueueMeta`
2. While barrier is set, `push_change()` routes new-schema changes to `pending_after_ddl`
3. Flush thread drains and flushes old-schema changes from the buffer
4. Flush thread applies `ALTER TABLE` commands to the DuckLake target via a short-lived PG connection (`apply_ddl_commands()`)
5. Flush thread resets `FlushWorker`, merges `pending_after_ddl` into main queue with updated `QueueMeta`, continues normally

The barrier ensures old-schema data is flushed before ALTER TABLE, preventing column mismatch errors or data loss.

---

## 6. duckpipe-pg: The PostgreSQL Extension

### 6.1 `lib.rs`

Extension entry point. `_PG_init` registers GUC parameters, the background worker, and shared memory.

GUCs registered: `poll_interval`, `batch_size_per_group`, `enabled`, `debug_log`, `data_inlining_row_limit`, `flush_interval`, `flush_batch_threshold`, `max_queued_bytes`, `duckdb_buffer_memory_mb`, `duckdb_flush_memory_mb`, `max_concurrent_flushes`. See [USAGE.md](./USAGE.md#configuration-gucs) for full parameter reference.

**Shared memory (SHM)**: `_PG_init` also registers `METRICS_SHM` — a `PgLwLock<SharedMetrics>` struct in PG shared memory containing per-table slots (`queued_changes`, `duckdb_memory_bytes`, `flush_count`, `flush_duration_ms`) and per-group slots (`total_queued_bytes`, `is_backpressured`, `active_flushes`). Fixed-size: 128 table slots + 8 group slots (~5KB). The bgworker writes to SHM after each sync cycle; SQL functions (`status()`, `worker_status()`, `metrics()`) read from SHM, avoiding PG round-trips for transient observability data.

### 6.2 `api.rs`

SQL-callable functions. All are REVOKE'd from PUBLIC by default. See [USAGE.md](./USAGE.md#sql-api) for full API reference with parameters and examples.

Functions implemented: `create_group`, `drop_group`, `enable_group`, `disable_group`, `add_table`, `remove_table`, `move_table`, `resync_table`, `start_worker`, `stop_worker`, monitoring SRFs (`groups()`, `tables()`, `status()`, `worker_status()`), and `metrics()` (JSON snapshot).

**SHM-backed monitoring**: `status()` and `worker_status()` read transient metrics (`queued_changes`, `duckdb_memory_bytes`, `flush_count`, `flush_duration_ms`, `total_queued_bytes`, `is_backpressured`, `active_flushes`) from PG shared memory instead of PG tables. `metrics()` merges SHM data with persisted PG data (state, rows_synced, applied_lsn, etc.) into a single JSON document.

**`add_table` implementation** — the most complex function:
1. Parse source table name (default schema: `public`)
2. Generate target name: `{schema}.{table}_ducklake` (or use provided name)
3. Look up sync group to get publication + slot
4. Add table to publication (`ALTER PUBLICATION ... ADD TABLE ...`)
5. Create target table: `CREATE TABLE IF NOT EXISTS ... (LIKE source) USING ducklake`
6. Insert into `table_mappings` with state = SNAPSHOT (if copy_data) or STREAMING
7. Store `pg_class.oid` as `source_oid` for rename safety
8. Auto-start background worker if not running

### 6.3 `worker.rs`

Thin shell around `duckpipe-core`. The BGWorker entry point (`duckpipe_worker_main`):

1. **Init**: Attach signal handlers (SIGHUP, SIGTERM), connect to database by OID
2. **Setup**: Read PG config (port, socket_dir, database name), build connstr, create tokio runtime
3. **Precompute**: `SlotConnectParams::Unix { socket_dir, port, user, dbname }` and `FlushCoordinator`
4. **Main loop**:
   ```
   while !shutdown_requested:
       if sighup: reload config
       if !enabled: sleep, continue
       config = read GUC values
       result = catch_unwind(|| rt.block_on(service::run_sync_cycle(config, coordinator, params)))
       match result:
           Ok(Ok(true))  → immediate re-poll (more work likely)
           Ok(Ok(false)) → sleep poll_interval (idle)
           Ok(Err(msg))  → log error, sleep
           Err(panic)    → coordinator.clear(), log, sleep
   coordinator.shutdown()  // join all flush threads before exit
   ```

The `catch_unwind` wrapper catches both Rust panics and PostgreSQL errors (`CaughtError`). On panic, `coordinator.clear()` shuts down all flush threads and recreates the mpsc channel to destroy any inconsistent state.

After each successful sync cycle, the bgworker writes transient metrics to PG shared memory (SHM): per-group `total_queued_bytes`, `is_backpressured`, and `active_flushes`, and per-table `queued_changes`, `duckdb_memory_bytes`, `flush_count`, and `flush_duration_ms`. This eliminates 3 PG round-trips per cycle that were previously used for observability-only updates. The worker also calls `coordinator.set_max_concurrent_flushes()` each cycle so runtime GUC changes take effect without restart.

### 6.4 `bootstrap.sql`

Executed on `CREATE EXTENSION pg_duckpipe`. Creates:
- `duckpipe.sync_groups` table
- `duckpipe.table_mappings` table
- Default sync group named `'default'` with auto-generated publication and slot names

---

## 7. duckpipe-daemon: The Standalone Daemon

Standalone daemon that reuses `duckpipe-core` over TCP with a REST API for control.

**CLI** (via `clap`):
```
duckpipe --connstr "host=localhost port=5432 dbname=mydb user=replicator password=secret"
         [--group <name>]            # Pre-bind to a sync group at startup
         [--api-port 8080]           # REST API port (0 to disable)
         [--poll-interval 1000]
         [--batch-size-per-group 100000]
         [--ducklake-schema ducklake]
         [--flush-interval 5000]
         [--flush-batch-threshold 10000]
         [--max-queued-bytes 256000000]
         [--duckdb-buffer-memory-mb 16]
         [--duckdb-flush-memory-mb 512]
         [--max-concurrent-flushes 4]
         [--debug]
```

**Unique to the daemon:**
- Uses `duckpipe_core::connstr::{parse_connstr, build_tokio_pg_connstr, to_slot_connect_params}` — shared connection string parsing module
- Uses `tracing-subscriber` with env-filter for structured logging
- REST API via `axum` for group binding/unbinding (`POST /groups`, `DELETE /groups`)
- Advisory lock prevents two daemons from running for the same group
- Graceful shutdown via `signal::ctrl_c()`
- Self-healing: on error, calls `coordinator.clear()` and retries next cycle
- Supports late binding: daemon can start unbound and wait for `POST /groups` to assign a sync group

**Main loop (outer/inner):**
```rust
loop {  // outer: wait for group binding
    wait_for_group_binding();
    loop {  // inner: sync loop for bound group
        if group_unbound: break  // back to outer
        tokio::select! {
            _ = signal::ctrl_c() => return,
            _ = stop_signal => break,
            result = service::run_group_sync_cycle(...) => { ... }
        }
    }
}
```

Key difference from bgworker: uses `SlotConnectParams::Tcp` instead of `::Unix`. Transient metrics (flush_count, flush_duration, queued_changes) are tracked in `FlushCoordinator` in-process and cached in `AppState::metrics_cache` after each sync cycle. The `GET /metrics` endpoint reads this cache and merges with PG persisted data, matching the same JSON shape as the PG `duckpipe.metrics()` function.

---

## 8. How a Change Flows End-to-End

Trace a single `INSERT INTO orders (id, product, qty) VALUES (42, 'widget', 10)`:

**1. PostgreSQL writes WAL.** The INSERT is recorded in the write-ahead log.

**2. Replication slot captures the change.** pgoutput encodes it as binary:
```
[msg_type='I'] [rel_id:u32] [marker='N'] [natts=3]
  [status='t'] [len=2] [data="42"]
  [status='t'] [len=6] [data="widget"]
  [status='t'] [len=2] [data="10"]
```

**3. SlotConsumer receives it.** Via `START_REPLICATION` streaming replication. Returns `(lsn, binary_data)`.

**4. service.rs decodes it.** `process_wal_messages()` reads byte `'I'`, calls `decoder::read_i32` for rel_id, `decoder::parse_tuple_data` with type OIDs from the relation cache to produce typed `Vec<Value>` values.

**5. Mapping is resolved.** `resolve_mapping()` looks up `orders` by name (or OID fallback). Returns `TableMapping` with target = `orders_ducklake`.

**6. Skip checks.** If table is disabled, ERRORED, or in CATCHUP with lsn <= snapshot_lsn → skip.

**7. Change is queued.** Pushed into `TableQueue` for `"public.orders_ducklake"`.

**8. Flush trigger.** Changes are pushed to the `FlushCoordinator`'s shared queues during the message loop. Flush threads self-trigger independently based on queue size (`flush_batch_threshold`) or time (`flush_interval`). Before flushing, each thread acquires a `FlushGate` slot (ticket-based FIFO, max `max_concurrent_flushes` concurrent flushes). Threads waiting for a slot continue buffering in low-memory mode. No synchronous barrier — WAL consumer continues without waiting for flushes.

**9. DuckDB flush.** `FlushWorker::flush()`:
   - Creates buffer table in DuckDB (with real column types from DuckLake catalog)
   - Appends change via DuckDB Appender: typed values (Int32(42), Text("widget"), Int32(10)) pass through DuckDB's type system directly — no SQL parsing or string escaping
   - Compacts (no-op for single row)
   - `DELETE FROM lake.public.orders_ducklake WHERE id = compacted.id` (idempotent)
   - `INSERT INTO lake.public.orders_ducklake SELECT id, product, qty FROM compacted WHERE _op_type IN (0, 1)`
   - Drops buffer tables

**10. Metrics update.** The flush thread calls `update_metrics_via_pg()` via its own tokio runtime to bump `rows_synced` and set `applied_lsn` in PG metadata. The flush result (including `flush_duration_ms`) is sent back to the main thread via mpsc, where `FlushCoordinator::collect_results()` tracks cumulative `flush_count` and latest `flush_duration_ms` per table. The bgworker then writes these transient metrics to PG shared memory (SHM) after each sync cycle — no PG round-trips needed for observability data.

**11. Checkpoint.** The WAL consumer periodically reads `get_min_applied_lsn()` from PG (whatever flush threads have written). `update_confirmed_lsn()` persists it. `send_status_update()` tells PostgreSQL to advance the slot.

---

## 9. Key Algorithms In Depth

### 9.1 DuckDB Flush

The flush algorithm in `duckdb_flush.rs` handles deduplication, TOAST preservation, and atomic application.

**Step 1: Create buffer table**
```sql
CREATE TABLE buffer (
    _seq INTEGER,             -- Ordering sequence
    _op_type INTEGER,         -- 0=INSERT, 1=UPDATE, 2=DELETE
    id INTEGER,               -- Real column (with real DuckDB type)
    product VARCHAR,
    qty INTEGER,
    id_unchanged BOOLEAN,     -- TOAST tracking per column
    product_unchanged BOOLEAN,
    qty_unchanged BOOLEAN
)
```

Column types come from DuckLake's `information_schema` (not always VARCHAR).

**Step 2: Load changes** (DuckDB Appender API)

Each `Change` becomes a row appended via DuckDB's native Appender (bypasses SQL parsing entirely). Typed `Value` variants map directly to DuckDB `ToSql` types — no manual quoting or string escaping. `Value::Text` strings are auto-cast by DuckDB to the buffer table's declared column types. For DELETE: only PK columns populated, rest NULL.

**Step 3: Compact** (deduplicate by PK)
```sql
CREATE TABLE compacted AS
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY _seq DESC) AS _rn
    FROM buffer
) WHERE _rn = 1
```
This keeps only the last operation per primary key. Handles composite PKs correctly.

**Step 4: Resolve TOAST** — for UPDATE rows with unchanged columns, fetch old values from target:
```sql
UPDATE compacted SET product = (
    SELECT product FROM lake.schema.table
    WHERE lake.schema.table.id = compacted.id
) WHERE product_unchanged = true AND _op_type = 1
```

**Step 5: Apply** (atomic transaction)
```sql
BEGIN;
DELETE FROM lake.schema.table WHERE id IN (SELECT id FROM compacted);
INSERT INTO lake.schema.table (id, product, qty)
    SELECT id, product, qty FROM compacted WHERE _op_type IN (0, 1);
COMMIT;
```

On failure: `ROLLBACK`. DELETE+INSERT instead of MERGE because DuckLake's MERGE only supports a single action clause.

**Step 6: Cleanup** — DROP both buffer and compacted tables.

### 9.2 TOAST Column Preservation

PostgreSQL's TOAST mechanism stores large column values out-of-line. When a row is UPDATEd, pgoutput marks unchanged TOAST columns with status byte `'u'` instead of sending the (potentially large) value.

pg_duckpipe handles this:
1. **Decoder**: `parse_tuple_data()` sets `unchanged[i] = true` and `Value::Null` for `'u'` columns
2. **Service**: Creates a `ChangeType::Update` with `col_unchanged` flags
3. **DuckDB flush**: Buffer table has `{col}_unchanged BOOLEAN` columns
4. **TOAST resolution**: Before DELETE, fetches old values from DuckLake target for unchanged columns
5. **Final INSERT**: Includes the resolved values

### 9.3 Crash-Safe Slot Advancement

The replication slot must only advance to an LSN where **all** active tables have durably flushed. Otherwise, a crash could cause data loss for lagging tables.

```
confirmed_lsn = MIN(applied_lsn) across all STREAMING/CATCHUP tables

If ANY table has NULL applied_lsn (hasn't flushed yet):
    confirmed_lsn = 0 (don't advance)
```

This is computed by `get_min_applied_lsn()` after every flush round. The slot consumer reports this LSN (not `pending_lsn`) via `StandbyStatusUpdate`.

### 9.4 CATCHUP Skip Logic

After a snapshot at `consistent_point` LSN:
- The table transitions to CATCHUP state with `snapshot_lsn = consistent_point`
- WAL changes with `lsn <= snapshot_lsn` are skipped (already in the snapshot)
- WAL changes with `lsn > snapshot_lsn` are applied normally
- When `pending_lsn >= snapshot_lsn` (the group has consumed past the snapshot point), the table transitions to STREAMING

The promotion check is a bulk UPDATE: `transition_catchup_to_streaming(group_id, pending_lsn)` transitions all qualifying tables at once.

### 9.5 OID-Based Rename Safety

When a table is renamed (`ALTER TABLE foo RENAME TO bar`), pgoutput RELATION messages carry the new name but the same OID.

1. WAL dispatch first tries name-based lookup: `get_table_mapping(group_id, schema, relname)`
2. If not found, falls back to OID: `get_table_mapping_by_oid(group_id, rel_id)`
3. If OID match found, updates metadata: `update_source_name(mapping_id, new_schema, new_table)`

The `source_oid` column (BIGINT UNIQUE) is populated when `add_table()` is called.

### 9.6 Error Recovery

Per-table error isolation with exponential backoff:

```
Flush failure:
  → record_error_message(table_id, error_text)
  → increment_consecutive_failures(table_id) → count

  if count < 3:
      → Continue (transient, will retry next cycle)
      → Error message visible via status() SRF

  if count >= 3:
      → set_errored_with_retry(table_id, error_text, backoff_secs)
      → backoff_secs = 30 * 2^min(count - 3, 6)   (30s, 60s, 120s, ... max ~32 min)
      → Table transitions to ERRORED, WAL changes skipped

Auto-retry:
  → Each cycle checks get_retryable_errored_tables(group_id)
  → Tables where retry_at <= now() transition ERRORED → STREAMING
  → consecutive_failures and error_message cleared

On success:
  → clear_consecutive_failures() (resets counter to 0)
  → record_error_message("") (clears message)
```

One table's error never blocks other tables in the same group.

---

## 10. Testing

19 SQL regression tests in `test/regression/`. They run on a temporary PostgreSQL instance (port 5555) with special config:

```
wal_level = logical
max_replication_slots = 10
shared_preload_libraries = 'pg_duckdb,pg_duckpipe'
```

**Test pattern** (every test follows this structure):
```sql
-- Setup
CREATE TABLE source_table (id int primary key, val text);
SELECT duckpipe.add_table('public.source_table', ...);

-- Make changes
INSERT/UPDATE/DELETE ...
SELECT pg_sleep(N);   -- Wait for background worker to process

-- Verify target
SELECT * FROM public.source_table_ducklake ORDER BY id;

-- Verify metrics
SELECT state, error_message FROM duckpipe.status() WHERE ...;

-- Cleanup
SELECT duckpipe.remove_table('public.source_table');
```

**Test coverage:**

| Test | What It Validates |
|------|-------------------|
| `auto_start` | `add_table()` auto-launches bgworker |
| `api` | All group/table management SQL functions |
| `monitoring` | `groups()`, `tables()`, `status()` SRFs (including SHM-backed columns) |
| `metrics` | `metrics()` JSON output structure, `status()` flush columns |
| `streaming` | INSERT → UPDATE → DELETE replication |
| `snapshot_updates` | Initial copy + concurrent writes during snapshot |
| `multiple_tables` | Multiple tables in one sync group |
| `data_types` | Various PG types (int, text, bool, timestamp, numeric, etc.) |
| `resync` | `resync_table()` re-snapshots from scratch |
| `truncate` | TRUNCATE propagation |
| `stress_append` | Many single-row transactions batched efficiently |
| `toast_unchanged` | TOAST unchanged column preservation on UPDATE |
| `composite_pk` | Multi-column primary key INSERT/UPDATE/DELETE + compaction |
| `large_transaction` | 500-row single txn, large batch flush |
| `multi_group` | Multiple independent sync groups, disable/re-enable with WAL catchup |
| `error_recovery` | Flush error detection, error_message recording, self-healing after fix |
| `rename_table` | OID-based routing after `ALTER TABLE RENAME`, metadata name update |
| `premature_catchup` | CATCHUP→STREAMING only when WAL consumption passes snapshot_lsn |
| `snapshot_race` | Concurrent writes during snapshot don't cause data loss |

---

## 11. Build, Run, and Usage

See [USAGE.md](./USAGE.md) for build commands, SQL API reference, configuration parameters, monitoring, and usage examples.

# PROCESS.md — pg_duckpipe v2 Implementation Progress

## Summary

### Done

- [x] Pure-Rust pgoutput decoder (RELATION, INSERT, UPDATE, DELETE, TRUNCATE, BEGIN, COMMIT)
- [x] Typed `Value` enum with OID-based parsing (Bool, Int16, Int32, Int64, Float32, Float64, Text)
- [x] Per-table state machine (PENDING, SNAPSHOT, CATCHUP, STREAMING, ERRORED)
- [x] DuckDB embedded flush (buffer → compact → TOAST resolve → DELETE+INSERT via Appender API)
- [x] Persistent per-table FlushWorker sessions (reuse DuckDB connection + cached schema)
- [x] Streaming replication via `START_REPLICATION` (pgwire-replication crate)
- [x] Reconnect-per-cycle design (~1ms Unix socket reconnect)
- [x] Crash-safe slot advancement (`confirmed_lsn = min(applied_lsn)`)
- [x] Snapshot via SQL functions (`pg_create_logical_replication_slot` + `pg_export_snapshot`)
- [x] Parallel snapshot tasks (concurrent tokio::spawn per table)
- [x] CATCHUP skip logic (skip WAL changes <= snapshot_lsn, promote when pending_lsn >= snapshot_lsn)
- [x] OID-based WAL routing (handles ALTER TABLE RENAME)
- [x] TOAST unchanged column preservation on UPDATE
- [x] TRUNCATE propagation (per-table drain + DELETE FROM)
- [x] Per-table error isolation (one table failing doesn't block others)
- [x] ERRORED state with exponential backoff auto-retry (30s * 2^n, max ~30min)
- [x] Producer-consumer flush architecture (persistent per-table OS threads)
- [x] **Fully decoupled WAL/flush** — no synchronous barrier between WAL consumer and flush threads
  - [x] Self-triggered flush (batch threshold OR time interval)
  - [x] Backpressure via AtomicI64 (WAL consumer pauses when queues full)
  - [x] Flush threads handle PG metadata updates independently (own tokio runtime each)
  - [x] Per-table drain for TRUNCATE; drain_and_wait_all retained for shutdown only
- [x] PostgreSQL extension (pgrx): SQL API, GUCs, bgworker, bootstrap DDL
- [x] Standalone daemon (duckpipe-daemon) over TCP
- [x] 19 regression tests all passing
- [x] Observability: `status()` SRF exposes `consecutive_failures`, `retry_at`, `applied_lsn` per table
- [x] Observability: `worker_status()` SRF exposes runtime `total_queued_changes`, `is_backpressured`
- [x] Observability: `status()` SRF now exposes `queued_changes` per table (shared queue + flush thread local accumulator)

### TODO

#### Performance / Scalability
- [ ] Flush thread pool — currently 1 OS thread + 1 tokio runtime + 1 DuckDB connection per table; for 50+ tables, a fixed-size thread pool would be more efficient
- [ ] Batch compaction tuning — explore DuckLake-level compaction to reduce Parquet file proliferation under sustained small-batch writes
- [ ] Inline data flush
- [x] benchmark and identify bottleneck (see benchmark findings below)
- [x] **confirmed_lsn resets to 0 on table re-add** — fixed: `get_min_applied_lsn()` and `get_active_table_lsns()` now use `COALESCE(applied_lsn, CASE WHEN state='CATCHUP' THEN snapshot_lsn END)` so CATCHUP tables with NULL applied_lsn use snapshot_lsn as effective floor; `seed_table_lsns()` uses `max(current, seed)` so zero-seeded entries get corrected to snapshot_lsn. **Result**: repeated benchmark run catch-up improved ~2× (51.8s → 25.6s, 3850 → 7885 rows/sec).
- [x] **Large catch-up batch stall (pure-insert path)** — fixed: `FlushWorker` now tracks `may_have_conflicts: bool` (init `true`). DELETE always runs while the flag is set. After the first pure-insert batch returns zero rows deleted (WAL-replay window is over), the flag clears to `false` and all subsequent pure-insert batches skip the O(lake_size) Parquet scan entirely. Flag resets to `true` on worker recreation (error recovery or resync). Confirmed `premature_catchup` test still passes (DELETE runs conservatively until zero-match proves no conflicts). **Impact**: pure-insert streaming workloads (append-only CDC) save the full DELETE scan cost per flush after the initial sync window.
  - **Remaining / mixed-DML path**: for batches containing UPDATEs or DELETEs in WAL (`has_non_inserts = true`), DELETE is always necessary (removes old row version before inserting new version) and cannot be skipped. The bottleneck for mixed-DML OLTP catch-up is fundamental DuckLake write throughput (~4.5k–10k rows/sec Parquet-over-PG). No pg_duckpipe-side fix without DuckLake-native upsert or index support.
- [ ] **Parquet-over-PG write throughput** — DuckLake stores Parquet in PostgreSQL (large objects or bytea). INSERT of 200k rows takes ~20s (≈10k rows/sec write throughput). For very large catch-up batches the bottleneck shifts from DELETE to the INSERT path itself.

#### Features
- [ ] `source_uri` column for pg_mooncake compatibility
- [ ] `conninfo` column in sync_groups for remote PG support
- [ ] Schema DDL sync (ALTER TABLE ADD/DROP COLUMN propagation)

#### Monitoring / Observability
- [x] `rows_synced` = 0 during snapshot — snapshot row count not credited to `rows_synced`; fixed by returning row count from `process_snapshot_task()` and calling `update_table_metrics()` after `set_catchup_state()`
- [ ] `applied_lsn` stays NULL during SNAPSHOT/CATCHUP until first WAL flush — should be set to `snapshot_lsn` after snapshot completes
- [x] No per-table accumulator visibility — fixed: `table_mappings.queued_changes` updated each cycle from `coordinator.table_pending_counts()` (shared queue + local accumulator); exposed in `duckpipe.status()` as `queued_changes BIGINT`
- [ ] `worker_state` not updated during snapshot processing — stale `total_queued_changes`/`is_backpressured` while snapshots run

#### Robustness
- [ ] Snapshot failures have no retry backoff — unlike ERRORED flush state (30s×2^n backoff), snapshot failures retry on every cycle immediately; risk of thrash on repeated failures
- [ ] Graceful handling of DuckLake schema drift (target table altered outside duckpipe)
- [ ] Connection pooling for flush thread PG metadata updates (currently short-lived connections per flush)
- [x] Standardize logging: shared `init_subscriber`, all `eprintln!` replaced with `tracing` macros
- [ ] regressions / tests for crash / error cases

#### Benchmark Script
- [x] Catch-up rate displayed as cumulative average (falls during stalls, misleading) — fixed to windowed per-interval rate
- [x] Final `lag_bytes` misleading after OLTP — WAL keeps advancing from checkpoints/autovacuum, lag stays large even when all DML is applied; fixed summary output to clarify
- [x] Snapshot monitor `Pending: N` label misleading — N is the count of tables NOT in STREAMING state, not a queued-change count; renamed to `non-STREAMING: N`

### Phase 7: Standardized Logging
- `duckpipe-core/src/log.rs` (new) — `init_subscriber(debug: bool)`: shared tracing-subscriber setup; `RUST_LOG` overrides the default filter
- `duckpipe-core/Cargo.toml` — added `tracing-subscriber.workspace = true`
- `duckpipe-pg/Cargo.toml` — added `tracing.workspace = true`
- `duckpipe-daemon/Cargo.toml` — removed `tracing-subscriber` (now owned by `duckpipe-core`)
- Replaced all `eprintln!` across core crates with typed tracing macros:
  - Connection errors → `tracing::error!`
  - ERRORED transition / periodic refresh failure → `tracing::warn!`
  - Auto-retry events → `tracing::info!`
  - Critical-path timing logs → `tracing::debug!`
- `duckpipe-daemon/src/main.rs` — inline subscriber setup replaced with `duckpipe_core::log::init_subscriber(args.debug)`
- `duckpipe-pg/src/worker.rs` — calls `init_subscriber(DEBUG_LOG.get())` once at bgworker startup (stderr captured by PostgreSQL log collector)

---

## Detailed Implementation History

### Phase 1: Foundation (types, decoder, queues, state machine)
- `duckpipe-core/src/types.rs` — Change, Batch, RelCacheEntry, SyncGroup, TableMapping, LSN parse/format
- `duckpipe-core/src/decoder.rs` — Pure-Rust pgoutput binary decoder (RELATION, INSERT, UPDATE, DELETE, TRUNCATE, BEGIN, COMMIT), TOAST unchanged detection
- `duckpipe-core/src/queue.rs` — Per-table `TableQueue` with `VecDeque<Change>`
- `duckpipe-core/src/state.rs` — SyncState enum: Pending, Snapshot, Catchup, Streaming, Errored; validated transitions
- `duckpipe-core/src/error.rs` — DuckPipeError with ErrorClass (Transient, Configuration, Resource)

### Phase 2: Flush Worker with DuckDB Buffer
- `duckpipe-core/src/duckdb_flush.rs` — Embedded DuckDB flush path:
  - `discover_lake_table_info()` — Introspects DuckLake catalog via `information_schema` (table_catalog='lake') to resolve schema name and column types
  - Buffer table created with real DuckLake column types (not VARCHAR)
  - Compaction via `ROW_NUMBER() OVER (PARTITION BY pk ORDER BY _seq DESC)` (correct for composite PKs)
  - TOAST resolution: fills unchanged columns from DuckLake target before DELETE
  - DELETE+INSERT pattern (DuckLake MERGE only supports single action)
  - **Atomicity**: DELETE+INSERT wrapped in BEGIN/COMMIT with ROLLBACK on failure
  - **DuckDB Appender API**: Buffer loading uses DuckDB's native Appender (bypasses SQL parsing entirely). Values pass through DuckDB's type system directly — no manual quoting/escaping, no SQL injection risk.
- `duckpipe-core/src/flush_worker.rs` — Dual-path flush: DuckDB primary, SQL fallback; per-table `applied_lsn` tracking
- `duckpipe-core/src/apply.rs` — SQL fallback applier via tokio-postgres
- `duckpipe-core/src/sql_gen.rs` — SQL generation for INSERT/DELETE/UPDATE batches

### Phase 3: WAL Processing Pipeline
- `duckpipe-core/src/service.rs` — WAL processing pipeline: fetch → decode → dispatch → flush → checkpoint
  - Streaming replication via `SlotConsumer` (`START_REPLICATION` protocol)
  - CATCHUP→STREAMING transition based on `pending_lsn > snapshot_lsn`
  - End-of-cycle `drain_and_wait_all()` — one flush per table per cycle (no mid-cycle early flush)
  - Graceful per-table error handling via `FlushThreadResult` (one table failing doesn't block others)
  - **Crash-safe checkpoint**: `confirmed_lsn = min(applied_lsn)` across all active tables (not `pending_lsn`)

### Phase 4: Snapshot Workers
- `duckpipe-pg/src/worker.rs` — Parallel snapshot via `tokio::spawn`:
  - Each table gets own libpq replication connection + tokio-postgres data connection
  - Temp replication slot for consistent snapshot export
  - `SET TRANSACTION SNAPSHOT` for data copy
  - Error recording in metadata (`record_error_message`)

### Phase 5a: ERRORED State with Auto-Retry
- `SyncState::Errored` variant with transitions from/to Snapshot, Catchup, Streaming
- `set_errored_state()`, `record_error_message()`, `clear_error_state()` in MetadataClient
- Snapshot failures record error_message (table stays in SNAPSHOT for auto-retry)
- **Flush error → ERRORED transition**: `consecutive_failures` counter in table_mappings; after 3 failures, transitions to ERRORED with exponential backoff `retry_at` (30s × 2^n, max ~30min)
- **Auto-retry**: Each WAL processing round checks for ERRORED tables with `retry_at <= now()` and transitions them back to STREAMING
- **ERRORED table skip**: WAL dispatch skips ERRORED tables (changes discarded; recovery via `resync_table()`)
- `resync_table()` clears `error_message`, `applied_lsn`, `snapshot_lsn`, `consecutive_failures`, `retry_at`

### Phase 5b: OID-Based WAL Routing
- `source_oid BIGINT UNIQUE` in table_mappings
- `add_table()` stores `pg_class.oid` via catalog lookup
- `get_table_mapping_by_oid()` for rename-safe WAL routing
- **Active OID routing**: WAL dispatch falls back to OID-based lookup when name-based lookup fails (handles table renames)
- `update_source_name()` — Updates metadata with new schema/table name when rename detected via OID match

### Phase 5c: Metadata Schema
- `source_oid BIGINT UNIQUE`, `error_message TEXT`, `retry_at TIMESTAMPTZ`, `applied_lsn pg_lsn`, `consecutive_failures INTEGER` columns
- `status()` SRF exposes `error_message`
- `resync_table()` clears all error/retry state

### Phase 5d: Parallel Snapshot Workers
- Snapshot tasks spawned as independent tokio tasks (one per table in SNAPSHOT state)

### DuckDB Flush Bugs Fixed
- **Catalog Error** — `information_schema` queried with `table_catalog = 'lake'` (not `lake.information_schema`)
- **Type Mismatch** — Buffer table uses real column types from DuckLake catalog
- **MERGE Limitation** — Replaced multi-action MERGE with DELETE+INSERT
- **Compaction Bug** — `ROW_NUMBER()` replaces `WHERE _seq IN (SELECT MAX)` for composite PK correctness

### PostgreSQL Extension (pgrx)
- `duckpipe-pg/src/lib.rs` — `_PG_init`, GUC registration, bgworker registration
- `duckpipe-pg/src/api.rs` — SQL API: create/drop/enable/disable group, add/remove/move/resync table, start/stop worker, groups()/tables()/status() SRFs
- `duckpipe-pg/src/worker.rs` — Background worker main loop with panic recovery
- `duckpipe-pg/src/sql/bootstrap.sql` — Schema DDL

### Phase 5e: Persistent Per-Table FlushWorker Sessions
- `duckpipe-core/src/duckdb_flush.rs` — `FlushWorker` struct with persistent `duckdb::Connection` and cached `LakeTableInfo`:
  - One-time setup: `Connection::open_in_memory()`, `INSTALL ducklake; LOAD ducklake;`, `ATTACH 'ducklake:postgres:...' AS lake`
  - Per-flush: only creates/drops lightweight buffer table; reuses connection and cached schema info
- `duckpipe-core/src/flush_worker.rs` — `update_metrics_via_pg()` for async PG metrics updates after DuckDB flush
- `duckpipe-core/src/service.rs` — `FlushCoordinator` manages persistent flush threads; removed `FlushConfig` and `build_flush_config()`
- `duckpipe-pg/src/worker.rs` — Coordinator created before main loop, passed into async block per cycle, `clear()` on panic recovery

### Phase 5f: Pure Rust Snapshot + Standalone Daemon
- `duckpipe-core/src/snapshot.rs` — Pure Rust snapshot using SQL functions (not replication protocol):
  - Replaces libpq FFI (`PQconnectdb`, `PQexec`, etc.) from `duckpipe-pg/src/worker.rs`
  - Uses `pg_create_logical_replication_slot()` + `pg_export_snapshot()` in a REPEATABLE READ transaction
    (`tokio-postgres` 0.7 does not support the `replication=database` startup parameter)
  - Control connection creates temp slot (unique name: `duckpipe_snap_{task_id}`) + exports snapshot
  - Data connection imports snapshot and copies data
  - `SnapshotResult` struct moved to core (shared by bgworker and daemon)
  - Uses `tracing` instead of `eprintln!` for logging
- `duckpipe-core/src/slot_consumer.rs` — Added `connect_tcp()` for TCP-based streaming replication:
  - Uses `ReplicationConfig::new()` with `with_port()` builder
  - Enables standalone daemon to connect over the network instead of Unix sockets
  - Existing `connect()` (Unix) unchanged for bgworker backward compatibility
- `duckpipe-core/src/service.rs` — Shared `run_sync_cycle()` coordination function:
  - `SlotConnectParams` enum with `Unix` and `Tcp` variants
  - `run_sync_cycle()` — Full cycle: connect → get groups → snapshots → streaming → disconnect
  - `process_snapshots()` — Parallel snapshot task spawning with metadata recording
  - Both bgworker and daemon delegate to `run_sync_cycle()`, eliminating duplicated coordination code
- `duckpipe-pg/src/worker.rs` — Thin bgworker shell:
  - Removed all coordination logic (connect, get groups, snapshot loop, WAL processing)
  - Removed `tokio-postgres`, `MetadataClient`, `SlotConsumer`, `snapshot` imports
  - Inner async block is single call: `service::run_sync_cycle(&config, coord, &slot_params)`
  - Precomputes `SlotConnectParams::Unix` before main loop
  - Retains bgworker-specific: GUC reading, SIGHUP handling, panic recovery, `wait_latch`
- `duckpipe-daemon/` — Thin daemon shell:
  - Main loop calls `service::run_sync_cycle()` directly with `SlotConnectParams::Tcp`
  - Retains daemon-specific: CLI arg parsing, `parse_connstr()`, tracing-subscriber, `signal::ctrl_c()`

### Phase 5g: Typed Values + DuckDB Appender + Remove Per-Table Early Flush
- `duckpipe-core/src/types.rs` — `Value` enum (Null, Bool, Int16, Int32, Int64, Float32, Float64, Text) with `Display` impl; `Change.col_values`/`key_values` changed from `Vec<Option<String>>` to `Vec<Value>`
- `duckpipe-core/src/decoder.rs` — `RelCacheEntry.atttypes: Vec<u32>` retains PostgreSQL type OIDs from RELATION messages; `parse_text_value()` converts pgoutput text to typed `Value` using type OID; `parse_tuple_data()` and `extract_key_values()` updated to `Vec<Value>`
- `duckpipe-core/src/queue.rs` — `TableQueue.atttypes: Vec<u32>` threaded through queue creation
- `duckpipe-core/src/service.rs` — `atttypes` threaded through `ensure_queue()` and queue creation; all `parse_tuple_data()` call sites pass `atttypes`
- `duckpipe-core/src/duckdb_flush.rs` — Replaced string-interpolated multi-row INSERT VALUES with DuckDB Appender API; `push_value_to_row()` maps `Value` variants to DuckDB `ToSql` types; eliminates SQL injection risk from manual quoting
- **Removed per-table mid-cycle early flush**: `batch_size_per_table` GUC and `flush_single_queue()` removed. All changes accumulate during WAL processing and flush once per table at end of cycle via `drain_and_wait_all()`. Batch size adapts naturally to throughput — the polling timeout (~500ms) acts as the batching window. Larger batches improve DuckLake flush efficiency (fewer Parquet files, better compaction).
- **`batch_size_per_group` default raised to 100,000** (was 10,000) to allow more WAL messages per cycle

### Phase 5h: Producer-Consumer Flush Architecture
- `duckpipe-core/src/flush_coordinator.rs` — New `FlushCoordinator` with persistent per-table flush threads:
  - Producer (main thread) pushes decoded WAL changes to `Mutex`-protected shared queues, notifying flush threads via `Condvar`
  - Consumer (flush threads) are long-lived OS threads, each owning a `FlushWorker` with its own `duckdb::Connection`
  - `drain_and_wait_all()` — synchronous barrier: signals all threads to flush, waits for completion via per-thread `drain_complete` condvar
  - `FlushThreadResult` (Success/Error) communicated via `mpsc` channel
  - Thread lifecycle: `ensure_queue()` spawns if new or dead, `shutdown()` joins all, `clear()` for panic recovery
  - Lock-free shutdown/drain signals via `AtomicBool`
- `duckpipe-core/src/service.rs` — Replaced `parallel_flush()` + `QueueRegistry` + `FlushWorkerRegistry` with `FlushCoordinator`:
  - `ensure_coordinator_queue()` — loads metadata and calls `coordinator.ensure_queue()`
  - `process_flush_results()` — processes `Vec<FlushThreadResult>`: metrics updates, error recording, ERRORED transitions
  - WAL dispatch pushes changes via `coordinator.push_change()`
  - Cycle-end and TRUNCATE use `coordinator.drain_and_wait_all()` for synchronous barrier
- `duckpipe-core/src/duckdb_flush.rs` — Removed `FlushWorkerRegistry` (replaced by `FlushCoordinator`)
- `duckpipe-core/src/flush_worker.rs` — Removed `flush_table_queue()` and `FlushResult` (flush threads call `FlushWorker::flush()` directly)
- `duckpipe-core/src/queue.rs` — Removed `QueueRegistry` (replaced by `FlushCoordinator`'s shared queues)
- `duckpipe-pg/src/worker.rs` — Uses `FlushCoordinator` instead of `FlushWorkerRegistry`; calls `coordinator.shutdown()` before exit
- `duckpipe-daemon/src/main.rs` — Uses `FlushCoordinator`; `#[tokio::main(flavor = "current_thread")]` for `mpsc::Receiver` (!Sync) compatibility

### Phase 5i: Fully Decoupled WAL Processing / Flush Architecture
- **Removed synchronous barrier** between WAL processing and flush — WAL consumer runs continuously without waiting for flushes
- `duckpipe-core/src/flush_coordinator.rs` — Major rewrite:
  - `BackpressureState` with `AtomicI64` total_queued counter (producer increments, flush threads decrement)
  - Flush threads self-trigger based on queue size (`flush_batch_threshold`) or time (`flush_interval`)
  - Each flush thread creates its own `tokio::runtime::Runtime` for async PG metadata updates (avoids deadlock with main thread's `current_thread` runtime)
  - Flush threads handle PG metadata updates independently: `update_metrics_via_pg()`, `update_error_state()`, `clear_error_on_success()`
  - `is_backpressured()` — WAL consumer skips polling when total queued changes exceed threshold
  - `drain_and_wait_table()` — per-table synchronous drain for TRUNCATE
  - `drain_and_wait_all()` — retained for shutdown only
  - `condvar.wait_timeout()` replaces indefinite `condvar.wait()` for time-based self-trigger
  - Local accumulator: changes drain from shared queue into local Vec, then batch-flush
- `duckpipe-core/src/flush_worker.rs` — Added `update_error_state()` and `clear_error_on_success()` for flush thread self-contained error handling
- `duckpipe-core/src/service.rs` — Decoupled from flush:
  - Removed `process_flush_results()` — flush threads handle metrics and errors themselves
  - Removed `drain_and_wait_all()` at cycle end — replaced with non-blocking `collect_results()` for error logging
  - TRUNCATE uses `drain_and_wait_table()` for per-table synchronous drain before DELETE
  - Backpressure check at start of `process_sync_group_streaming()`
- `duckpipe-pg/src/lib.rs` — New GUCs: `flush_interval` (1000ms), `flush_batch_threshold` (10000), `max_queued_changes` (500000)
- `duckpipe-core/src/service.rs` — `ServiceConfig` extended with `flush_interval_ms`, `flush_batch_threshold`, `max_queued_changes`
- `duckpipe-pg/src/worker.rs` — Passes new GUCs to `FlushCoordinator::new()`
- `duckpipe-daemon/src/main.rs` — New CLI args: `--flush-interval`, `--flush-batch-threshold`, `--max-queued-changes`
- **Crash safety**: Slot never advances past what all tables have durably flushed
- **Test update**: `multiple_tables` sleep increased from 2s to 4s for decoupled flush latency

### Phase 6: Streaming Replication Consumer
- `duckpipe-core/src/slot_consumer.rs` — Real `pgwire-replication` crate integration:
  - `SlotConsumer::connect()` via `ReplicationConfig::unix()` with `start_lsn` from `confirmed_lsn`
  - `poll_messages()` — receives `ReplicationEvent`s, synthesizes pgoutput binary B/C messages from Begin/Commit events, XLogData passes through directly
  - `send_status_update()` — crash-safe: reports `min(applied_lsn)` (not `pending_lsn`) via `update_applied_lsn()`
  - `close()` — graceful shutdown sends CopyDone + final StandbyStatusUpdate
  - **Reconnect-per-cycle**: fresh connection each worker cycle (~1ms Unix socket), avoids keepalive issues with pgrx `block_on()` boundary
  - **Streaming-only**: SQL polling fallback removed for crash safety (slot advancement is explicit via StandbyStatusUpdate)
- `duckpipe-core/src/types.rs` — Added `confirmed_lsn: u64` to `SyncGroup`
- `duckpipe-core/src/metadata.rs` — `get_enabled_sync_groups()` fetches `confirmed_lsn`
- `duckpipe-core/src/service.rs` — `ProcessResult.confirmed_lsn` for crash-safe StandbyStatusUpdate
- `duckpipe-pg/src/worker.rs` — Streaming-only WAL consumption via `run_sync_cycle()`

---

## Test Coverage

All 19 regression tests pass. See `doc/CODE_WALKTHROUGH.md` section 10 for the full test coverage table.

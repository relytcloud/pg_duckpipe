# PROGRESS.md — pg_duckpipe v2 Implementation Progress

## Done

- [x] Pure-Rust pgoutput decoder (RELATION, INSERT, UPDATE, DELETE, TRUNCATE, BEGIN, COMMIT)
- [x] Typed `Value` enum with OID-based parsing (Bool, Int16, Int32, Int64, Float32, Float64, Text)
- [x] Per-table state machine (PENDING, SNAPSHOT, CATCHUP, STREAMING, ERRORED)
- [x] DuckDB embedded flush (buffer → compact → TOAST resolve → DELETE+INSERT via Appender API)
- [x] Persistent per-table FlushWorker sessions (reuse DuckDB connection + cached schema)
- [x] Streaming replication via `START_REPLICATION` (pgwire-replication crate)
- [x] Reconnect-per-cycle design (~1ms Unix socket reconnect)
- [x] Crash-safe slot advancement (`confirmed_lsn = min(applied_lsn)`)
- [x] Snapshot via SQL functions (`pg_create_logical_replication_slot` + `pg_export_snapshot`) *(legacy — refactored to chunked COPY pipeline)*
- [x] Parallel snapshot tasks (concurrent tokio::spawn per table)
- [x] CATCHUP skip logic (skip WAL changes <= snapshot_lsn, promote when pending_lsn >= snapshot_lsn)
- [x] OID-based WAL routing (handles ALTER TABLE RENAME)
- [x] TOAST unchanged column preservation on UPDATE
- [x] TRUNCATE propagation (per-table drain + DELETE FROM)
- [x] Per-table error isolation (one table failing doesn't block others)
- [x] ERRORED state with exponential backoff auto-retry (30s * 2^n, max ~30min)
- [x] Fully decoupled WAL/flush — self-triggered flush (batch threshold OR time interval), backpressure via AtomicI64, flush threads own tokio runtime + PG metadata updates
- [x] PostgreSQL extension (pgrx): SQL API, GUCs, bgworker, bootstrap DDL
- [x] Standalone daemon (duckpipe-daemon) over TCP
- [x] 24 regression tests all passing
- [x] Observability: `status()` SRF exposes `consecutive_failures`, `retry_at`, `applied_lsn`, `queued_changes` per table
- [x] Observability: `worker_status()` SRF exposes `total_queued_changes`, `is_backpressured`
- [x] Standardized logging: shared `init_subscriber`, all `eprintln!` replaced with `tracing` macros
- [x] `rows_synced` credited during snapshot
- [x] `confirmed_lsn` resets to 0 on table re-add — fixed via `COALESCE(applied_lsn, snapshot_lsn)` floor
- [x] Large catch-up batch stall (pure-insert path) — fixed via `may_have_conflicts` flag skipping DELETE scan
- [x] `lag_bytes` flat during catch-up — fixed: `StandbyStatusUpdate` sent each cycle even when no new WAL
- [x] Flush-thread drain capped at `batch_threshold` for incremental progress visibility
- [x] Decoupled snapshots from sync cycle — SnapshotManager spawns fire-and-forget snapshot tasks; sync cycle kicks new ones and collects completed results non-blocking. WAL changes for SNAPSHOT tables are buffered in paused flush queues during snapshot, then flushed on completion.
- [x] Mixed DML correctness — flush DELETE WHERE clause was using all columns (REPLICA IDENTITY FULL `attkeys`) instead of real PK from `pg_index`; fixed by caching `pk_key_attrs` per relation and using it for `extract_key_values` and flush queue setup

## TODO

### Performance / Scalability
- [ ] Snapshot WAL buffering memory — during SNAPSHOT, WAL changes for snapshotting tables accumulate in paused flush queues unbounded; long-running snapshots on high-write tables can cause memory bloat. Consider spilling to disk or capping buffer size with snapshot-aware backpressure.
- [x] Parallel snapshot writes crash on concurrent COMMIT (pg_duckdb bug) — **Fixed**: replaced PG-side `INSERT INTO target SELECT * FROM source` (which went through pg_ducklake and crashed on concurrent COMMIT) with direct DuckDB connections per snapshot task via chunked COPY pipeline. Snapshots now run truly in parallel. Benchmark (bench_suite 30s): 4T append=26,353 rows/s (~31k/s per table) vs previous sequential 13,127 rows/s (2x improvement); 4T mixed=38,858 rows/s (~23k/s per table). Single-table: 1T append=14,569 rows/s (125k/s per table), 1T mixed=6,020 rows/s.
- [ ] Multi-table streaming lag 20x higher than single-table (benchmark suite) — 4T append avg lag=55 MB vs 1T=2.7 MB during OLTP (bench_suite 30s run). Standalone multi-table runs show 3.7 MB avg lag (fresh DB), confirming the regression is specific to running scenarios sequentially in `bench_suite.sh`. Likely cause: leftover flush threads from earlier scenarios (cleanup removes table mappings but does not shut down the bgworker's FlushCoordinator entries for tables outside the current scenario's range). Mixed DML lag significantly higher: 1T=154 MB, 4T=332 MB — expected due to UPDATE/DELETE WAL amplification and Parquet-scan DELETE phase.
- [ ] Snapshot detection delay up to `poll_interval` — when bgworker is already running, `add_table()` is not detected until the next sync cycle. Benchmark shows ~10s delay (poll_interval=10s) in scenario 3 vs instant detection in scenario 1 (where `add_table()` starts the bgworker). This inflates measured snapshot throughput: single-table mixed DML shows 5,956 rows/s (16.8s wall clock) vs expected ~14,700 rows/s (6.8s) if detected immediately. The 16.8s breaks down as: ~10s detection delay + 0.15s copy + ~5s streaming timeout before result collection + ~1.6s CATCHUP transition. Consider: wake bgworker immediately on `add_table()` via pg_notify or latch signal.
- [ ] Flush thread pool — 1 OS thread + 1 tokio runtime + 1 DuckDB connection per table; fixed-size pool needed for 50+ tables
- [ ] Batch compaction tuning — reduce Parquet file proliferation under sustained small-batch writes
- [ ] Inline data flush
- [ ] Parquet-over-PG write throughput — ~10k rows/sec cap; bottleneck for large catch-up batches
- [ ] DELETE phase dominates mixed DML flush — 15.5ms avg (50% of flush time) for single-table `oltp_read_write`; cross-catalog `EXISTS` join scans full Parquet each time. Consider DuckLake-native delete-by-PK API or batched positional deletes to avoid full-table scan.
- [ ] DuckLake commit contention in multi-table flushes — commit phase jumps from 1.7ms (1 table) to 10.9ms (4 tables); likely DuckLake metadata lock contention across parallel flush workers. Investigate DuckLake-level concurrency improvements or flush batching across tables.
- [ ] Mixed DML catch-up throughput appears as 0 rows/s — `rows_synced` counter only reflects net row count change, so UPDATE/DELETE-heavy workloads show no progress even while actively flushing. Need a separate `changes_applied` counter to track actual WAL processing throughput.
- [ ] Mixed DML replication lag 50-100x higher than append — 170-377 MB avg vs 3-64 MB; each UPDATE generates DELETE+INSERT in WAL (REPLICA IDENTITY FULL sends full old+new tuples) and flush must scan Parquet for DELETE. Investigate streaming-mode flush during OLTP (currently flush only happens during catch-up idle periods).
- [x] WAL consumer: inline `recv → decode → push_change` hot path — eliminated collect-all-then-process Vec buffer. Bookkeeping (flush results, auto-retry, CATCHUP→STREAMING, confirmed_lsn) now runs every 10k commits or 500ms via `run_heartbeat`, eliminating the trailing 1-row straddle flush and producing steady ~10k-row Parquet files.

### Features
- [ ] Dedicated bgworker per group — currently one bgworker per database iterates all groups sequentially; refactor to spawn one bgworker per sync group so groups are fully isolated (own FlushCoordinator, SnapshotManager, SlotState). Pack `db_oid + group_id` into bgw_main_arg Datum. Update launch/stop/check APIs for per-group semantics.
- [ ] Per-group GUC overrides — add nullable config columns to `sync_groups` (poll_interval, batch_size, flush_interval, flush_batch_threshold, max_queued_changes); NULL falls back to global GUC. Add `alter_group()` SQL API. Each per-group worker reads its own overrides on each cycle.
- [ ] `source_uri` column for pg_mooncake compatibility
- [ ] `conninfo` column in sync_groups for remote PG support
- [ ] Schema DDL sync (ALTER TABLE ADD/DROP COLUMN propagation)
- [x] **`REPLICA IDENTITY FULL` enforced** — `add_table()` always issues `ALTER TABLE <src> REPLICA IDENTITY FULL`. The flush path has no TOAST-unchanged fallback: buffer table has no `{col}_unchanged` columns, appender rows are narrower (ncols not 2×ncols), and TOAST resolution is gone. Any `col_unchanged = true` in WAL (source identity changed after add) surfaces as a hard flush error triggering the existing backoff retry path. Trade-off: higher WAL volume on source.
- [x] Dockerfile for setting up a self-contained playground env
- [ ] CI: `cargo chef` pattern to cache Rust dependency compilation across GHA runs
- [ ] Sync tables that have no PK and ensure e2e EOS

### Monitoring / Observability
- [ ] `applied_lsn` stays NULL during SNAPSHOT/CATCHUP — should be set to `snapshot_lsn` after snapshot completes
- [ ] `worker_state` not updated during snapshot processing — stale metrics while snapshots run
- [x] Expose snapshot timing in `duckpipe.status()` — added `snapshot_duration_ms` and `snapshot_rows` columns to `table_mappings` and `duckpipe.status()`. Snapshot duration is always measured (not gated on `timing` flag) and stored on CATCHUP transition. `resync_table()` resets both columns.
- [x] Benchmark suite (`bench_suite.sh`) — 4 scenarios (single/multi × insert/mixed) with automated analysis report (`analyze_results.py`)

### Bugs
- [x] Mixed DML (oltp_read_write) consistency failure — fixed: flush DELETE used all-column WHERE clause instead of PK-only (see Done section)
- [ ] Benchmark suite cleanup incomplete — `bench_suite.sh` only calls `remove_table` for `sbtest1..sbtest{N}` where N is the current scenario's table count. When a multi-table scenario (N=4) is followed by a single-table scenario (N=1), orphaned mappings for sbtest2-4 remain in `duckpipe.status()`, inflating `sum(rows_synced)` and potentially leaving stale flush threads running.

### Robustness
- [ ] Snapshot failures have no retry backoff — risk of thrash on repeated failures
- [ ] Graceful handling of DuckLake schema drift (target table altered outside duckpipe)
- [ ] Connection pooling for flush thread PG metadata updates (currently short-lived connections per flush)
- [ ] Regression tests for crash / error cases

# PROGRESS.md — pg_duckpipe v2 Implementation Progress

## Done

### Core Engine
- [x] Pure-Rust pgoutput decoder (RELATION, INSERT, UPDATE, DELETE, TRUNCATE, BEGIN, COMMIT)
- [x] Typed `Value` enum with OID-based parsing (Bool, Int16, Int32, Int64, Float32, Float64, Text)
- [x] Per-table state machine (PENDING, SNAPSHOT, CATCHUP, STREAMING, ERRORED)
- [x] DuckDB embedded flush (buffer → compact → DELETE+INSERT via Appender API)
- [x] Persistent per-table FlushWorker sessions (reuse DuckDB connection + cached schema)
- [x] Streaming replication via `START_REPLICATION` (pgwire-replication crate)
- [x] Reconnect-per-cycle design (~1ms Unix socket reconnect)
- [x] Crash-safe slot advancement (`confirmed_lsn = min(applied_lsn)`)
- [x] OID-based WAL routing (handles ALTER TABLE RENAME)
- [x] TOAST unchanged column preservation on UPDATE
- [x] TRUNCATE propagation (per-table drain + DELETE FROM)
- [x] Per-table error isolation (one table failing doesn't block others)
- [x] ERRORED state with exponential backoff auto-retry (30s × 2^n, max ~30min)
- [x] Fully decoupled WAL/flush — self-triggered flush (batch threshold OR time interval), backpressure via AtomicI64, flush threads own tokio runtime + PG metadata updates
- [x] `REPLICA IDENTITY FULL` enforced — `add_table()` auto-sets identity; no TOAST-unchanged fallback needed

### Snapshots
- [x] Chunked COPY pipeline — direct DuckDB connections per snapshot task, replacing PG-side INSERT INTO (which crashed on concurrent COMMIT via pg_ducklake)
- [x] Parallel snapshot tasks (concurrent tokio::spawn per table)
- [x] Decoupled snapshots from sync cycle — SnapshotManager runs fire-and-forget; WAL changes buffered in paused flush queues during snapshot
- [x] CATCHUP skip logic (skip WAL changes ≤ snapshot_lsn, promote when pending_lsn ≥ snapshot_lsn)

### Performance
- [x] WAL consumer inline hot path �� eliminated collect-all-then-process Vec buffer; bookkeeping via `run_heartbeat` every 10k commits or 500ms
- [x] Large catch-up batch stall — fixed via `may_have_conflicts` flag skipping DELETE scan on pure-insert path
- [x] Flush-thread drain capped at `batch_threshold` for incremental progress visibility
- [x] Mixed DML correctness — flush DELETE used all-column WHERE instead of PK-only; fixed by caching `pk_key_attrs` per relation

### Observability
- [x] `status()` SRF: `consecutive_failures`, `retry_at`, `applied_lsn`, `queued_changes`, `snapshot_duration_ms`, `snapshot_rows`, `duckdb_memory_bytes`
- [x] `worker_status()` SRF: `total_queued_changes`, `is_backpressured`
- [x] Standardized logging: shared `init_subscriber`, all output via `tracing` macros
- [x] `rows_synced` credited during snapshot
- [x] Benchmark suite (`bench_suite.sh`) — 4 scenarios with automated analysis

### Infrastructure
- [x] PostgreSQL extension (pgrx): SQL API, GUCs, bgworker, bootstrap DDL
- [x] Standalone daemon (duckpipe-daemon) over TCP
- [x] Regression tests all passing
- [x] Dockerfile for self-contained playground env

### Bug Fixes
- [x] `confirmed_lsn` resets to 0 on table re-add — fixed via `COALESCE(applied_lsn, snapshot_lsn)` floor
- [x] `lag_bytes` flat during catch-up — fixed: `StandbyStatusUpdate` sent each cycle even when no new WAL

## TODO

### Performance / Scalability
- [ ] Snapshot WAL buffering memory — unbounded accumulation in paused flush queues during long snapshots; consider spill-to-disk or snapshot-aware backpressure
- [x] Multi-table streaming lag 20x single-table — two bugs: (1) benchmark `prepare_env` only cleaned up `sbtest1..args.tables` mappings, leaving orphans from wider prior scenarios; (2) `FlushCoordinator` never pruned stale `per_table_lsn` entries and flush threads for removed tables, freezing `confirmed_lsn`. Fixed via `prune_removed_tables()` in coordinator (called each cycle with `get_all_mapping_ids`) + benchmark cleanup queries all existing mappings
- [x] Snapshot detection delay up to `poll_interval` — LISTEN/NOTIFY wakeup: `add_table()`, `resync_table()`, `enable_group()` fire `NOTIFY duckpipe_wakeup_{group}`; bgworker LISTENs and wakes immediately
- [ ] Snapshot producers block WAL consumer — snapshot CSV producers (`run_csv_producer`) do sync file I/O (`fs::File::write_all`) and byte-by-byte quote tracking on the single-threaded tokio runtime, blocking the WAL consumer and all other async tasks during those windows; move producers to `spawn_blocking` or a dedicated thread so snapshots never interfere with WAL streaming
- [ ] Flush thread pool — 1 OS thread + 1 tokio runtime + 1 DuckDB connection per table; need fixed-size pool for 50+ tables
- [ ] Batch compaction tuning — reduce Parquet file proliferation under sustained small-batch writes
- [ ] Inline data flush
- [ ] Parquet-over-PG write throughput — ~10k rows/sec cap; bottleneck for large catch-up batches
- [ ] DELETE phase dominates mixed DML flush — cross-catalog EXISTS join scans full Parquet; consider DuckLake-native delete-by-PK
- [ ] DuckLake commit contention in multi-table flushes — commit jumps from 1.7ms (1T) to 10.9ms (4T); metadata lock contention
- [ ] Mixed DML catch-up throughput appears as 0 rows/s — `rows_synced` only reflects net change; need `changes_applied` counter
- [ ] Mixed DML replication lag 50-100x append — WAL amplification from REPLICA IDENTITY FULL + Parquet-scan DELETE phase

### Features
- [ ] Daemon REST API — expose monitoring/control endpoints (status, health, metrics) from the standalone daemon binary so operators can integrate with orchestration and alerting without a PG connection
- [x] Dedicated bgworker per group — one worker per sync group for full isolation (own FlushCoordinator, SnapshotManager, SlotState)
- [x] Per-group NOTIFY channels (`duckpipe_wakeup_{group}`) — avoid thundering herd wakeups; per-group bgworker spawns its own LISTEN channel
- [ ] Per-group config (`sync_groups.config JSONB`) — persistent per-group settings accessible from both bgworker and daemon modes; SQL API `set_group_config(group, key, value)` / `get_group_config(group)`; initial keys: `duckdb_memory_limit` (default '256MB'), `duckdb_threads` (default 1); future: migrate `flush_interval_ms`, `flush_batch_threshold`, `max_queued_changes` from GUCs; NULL/absent keys fall back to global defaults
- [ ] Staged storage between WAL and DuckLake — persist changes into a durable intermediate delta/staging layer, then merge/rewrite into DuckLake in larger maintenance-friendly jobs so CDC durability is decoupled from DuckLake file proliferation
- [x] `conninfo` column in sync_groups for remote PG support — group-level conninfo routes WAL replication, snapshots, and catalog queries to a remote PG while metadata and DuckLake targets stay local. `create_group(conninfo=>...)` creates slot+publication on remote, `add_table()` introspects remote pg_catalog for explicit DDL, `remove_table()`/`drop_group()` clean up remote objects. Shared `connstr` module extracted to `duckpipe-core/src/connstr.rs`.
- [ ] Schema DDL sync (ALTER TABLE ADD/DROP COLUMN propagation)
- [ ] Sync tables with no PK (ensure e2e EOS)
- [ ] CI: `cargo chef` pattern for cached Rust dependency compilation
- [ ] CI: run `cargo test` for unit tests (e.g. `connstr` module) — currently only `make installcheck` regression tests are in CI

### Monitoring / Observability
- [ ] Prometheus-compatible metrics endpoint — expose runtime metrics (`duckdb_memory_bytes`, `rows_synced`, `queued_changes`, flush latencies, etc.) as time-series counters/gauges for scraping; `status()` is a point-in-time snapshot view, not suited for time-series observability. Daemon: HTTP `/metrics` endpoint; bgworker: `pg_stat` integration or metrics table with timestamps
- [ ] `applied_lsn` stays NULL during SNAPSHOT/CATCHUP — should be set to `snapshot_lsn` after snapshot completes
- [ ] `worker_state` not updated during snapshot processing — stale metrics while snapshots run
- [ ] Flush runtime stats for fixed-interval scraping — expose per-table/group counters such as flush count, rows per flush, avg rows/flush, and recent flush timings so Prometheus-style systems can sample every 30s without reconstructing rates from logs

### Bugs
- [x] Benchmark suite cleanup incomplete — orphaned mappings for sbtest2-4 remain after multi→single-table scenario transition; fixed: `prepare_env` now queries `duckpipe.status()` to remove ALL existing mappings + drops leftover sbtest tables beyond current scenario count

### Robustness
- [x] Missing index on `table_mappings.group_id` — FK column has no index; nearly every hot-path query filters by `group_id` (state checks, flush lookups, retry scans); also slows FK constraint checks on `sync_groups` DELETE
- [x] Snapshot failures have no retry backoff — risk of thrash on repeated failures; fixed via exponential backoff
- [ ] Graceful handling of DuckLake schema drift (target table altered outside duckpipe)
- [ ] Unbounded DuckDB memory — no `memory_limit` set per FlushWorker; N tables × default limit (~80% RAM each) is theoretically unbounded; depends on per-group config (`duckdb_memory_limit`)
- [ ] Connection pooling for flush thread PG metadata updates
- [ ] Regression tests for crash / error cases

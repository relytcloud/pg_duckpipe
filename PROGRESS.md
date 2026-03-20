# PROGRESS.md — pg_duckpipe Implementation Progress

## TODO

### Performance / Scalability
- [ ] Snapshot producers block WAL consumer — snapshot CSV producers (`run_csv_producer`) do sync file I/O (`fs::File::write_all`) and byte-by-byte quote tracking on the single-threaded tokio runtime, blocking the WAL consumer and all other async tasks during those windows; move producers to `spawn_blocking` or a dedicated thread so snapshots never interfere with WAL streaming
- [ ] Byte-based flush threshold — replace row-count `flush_batch_threshold` with `flush_buffer_size_mb`; estimate change byte size during `append_to_buffer()` (sum of `Value` sizes), track cumulative bytes per table, trigger flush when `buffered_bytes >= threshold`; uses `avg_row_bytes` from metrics for capacity planning
- [ ] Batch compaction tuning — reduce Parquet file proliferation under sustained small-batch writes
- [ ] Inline data flush
- [ ] Parquet-over-PG write throughput — ~10k rows/sec cap bottleneck for large catch-up
- [ ] Fan-in Parquet min/max pruning — leverage `_duckpipe_source` column statistics so DELETE scans skip Parquet files belonging to other sources
- [ ] Time-bounded DELETE — user-provided timestamp column + immutability horizon; skip Parquet files older than the horizon during DELETE
- [ ] DELETE phase dominates mixed DML flush — consider DuckLake-native delete-by-PK
- [ ] DuckLake commit contention — jumps from 1.7ms (1T) to 10.9ms (4T); metadata lock issue
- [ ] Mixed DML catch-up shows 0 rows/s — need `changes_applied` counter separate from `rows_synced`
- [ ] Mixed DML replication lag 50-100x append — WAL amplification + Parquet-scan DELETE phase

### Features
- [ ] Per-table config JSONB column on `table_mappings` — consolidate `routing_enabled` and future per-table settings into a single `config JSONB` column (like `sync_groups.config`), avoid schema sprawl
- [ ] Schema DDL sync — ALTER TABLE ADD/DROP COLUMN propagation
- [ ] Staged storage — durable delta layer between WAL and DuckLake to decouple CDC from file proliferation
- [ ] Explicit `Value` variants for more PG types (date, timestamp, uuid, numeric, interval, json)
- [ ] Sync tables with no PK (ensure e2e EOS)
- [ ] Column and row filtering — selective column sync and WHERE predicates on `add_table()`

### Monitoring / Observability
- [ ] Replication lag in `status()` — compute `pg_current_wal_lsn() - applied_lsn` as `lag_bytes`; needs special handling for remote groups (lag is relative to remote WAL tip, not local)
- [ ] Prometheus text rendering — expose metrics as Prometheus-compatible text format via external tools (postgres_exporter, JSON exporter) or native endpoint
- [ ] Per-table `avg_row_bytes` metric — track cumulative bytes and row count during `append_to_buffer()`, expose `avg_row_bytes` in `status()` / `metrics()` for capacity planning and byte-based flush threshold

### Robustness
- [ ] Query routing permission check — verify the current user has SELECT on both source and target tables before routing; currently inherits permissions from the user who ran `add_table()`
- [ ] Auto-mode: detect point lookups on any btree index, not just primary key — equality on a unique btree index column should also skip routing
- [ ] Query routing + fan-in: currently skips routing for fan-in targets entirely — could auto-inject `WHERE _duckpipe_source = 'group/schema.table'` filter instead
- [ ] Graceful handling of DuckLake schema drift (target table altered outside duckpipe)
- [ ] Connection pooling for flush thread PG metadata updates
- [ ] Regression tests for crash / error cases

### Infrastructure
- [ ] CI: `cargo chef` pattern for cached Rust dependency compilation
- [ ] CI: run `cargo test` for unit tests — currently only `make installcheck` in CI

### Future Vision
- [ ] Incremental materialized views — CDC-driven delta maintenance of aggregate/join views
- [ ] Schema evolution timeline — audit log of DDL changes from Relation messages in the WAL stream
- [ ] Stream rules — per-table predicates on the CDC stream with configurable actions (log, skip, pause, pg_notify, webhook)
- [ ] Source adapter trait — unified `Source` abstraction; pgoutput decoder becomes one implementation
- [ ] Kafka source — consume topics (Avro/JSON/Protobuf) with consumer group offset tracking
- [ ] S3 file source — poll or event-driven ingest of CSV/Parquet/JSON via DuckDB native readers

## Done

- Pure-Rust pgoutput decoder (RELATION, INSERT, UPDATE, DELETE, TRUNCATE, BEGIN, COMMIT)
- Typed `Value` enum with OID-based parsing (Bool, Int16, Int32, Int64, Float32, Float64, Text)
- Per-table state machine (PENDING, SNAPSHOT, CATCHUP, STREAMING, ERRORED)
- DuckDB embedded flush (buffer → compact → DELETE+INSERT via Appender API)
- Persistent per-table FlushWorker sessions (reuse DuckDB connection + cached schema)
- Streaming replication via `START_REPLICATION` (pgwire-replication crate)
- Reconnect-per-cycle design (~1ms Unix socket reconnect)
- Crash-safe slot advancement (`confirmed_lsn = min(applied_lsn)`)
- OID-based WAL routing (handles ALTER TABLE RENAME)
- TOAST unchanged column preservation on UPDATE
- TRUNCATE propagation (per-table drain + DELETE FROM)
- Per-table error isolation with exponential backoff auto-retry (30s × 2^n, max ~30min)
- Fully decoupled WAL/flush — self-triggered flush, backpressure via AtomicI64, flush threads own tokio runtime
- `REPLICA IDENTITY FULL` enforced — `add_table()` auto-sets identity
- Chunked COPY snapshot pipeline with parallel tasks and direct DuckDB connections
- Decoupled snapshots from sync cycle — WAL changes buffered in paused flush queues during snapshot
- CATCHUP skip logic (skip WAL changes ≤ snapshot_lsn)
- WAL consumer inline hot path — bookkeeping via `run_heartbeat` every 10k commits or 500ms
- Skip-delete optimization for pure-INSERT batches (`may_have_conflicts` flag)
- Flush-thread drain capped at `batch_threshold` for incremental progress visibility
- Mixed DML correctness — PK-only WHERE for DELETE phase
- Ticket-based FIFO `FlushGate` semaphore limiting concurrent flushes per group
- Multi-table streaming lag fix — `prune_removed_tables()` in coordinator
- LISTEN/NOTIFY wakeup — `add_table()`, `resync_table()`, `enable_group()` fire immediate wakeup
- `status()`, `worker_status()`, `metrics()` SRFs with SHM-backed per-table and per-group metrics
- In-memory metrics via PG shared memory (eliminates 3 PG round-trips per sync cycle)
- Per-table `avg_row_bytes` metric for capacity planning
- Flush runtime stats (`flush_count`, `flush_duration_ms`) in SHM
- Standardized logging via `tracing` macros
- Benchmark suite (`bench_suite.sh`) — 4 scenarios with automated analysis
- PostgreSQL extension (pgrx): SQL API, GUCs, bgworker, bootstrap DDL
- Standalone daemon (duckpipe-daemon) over TCP with REST API (`/status`, `/health`, `/metrics`, `/groups`, `/tables`)
- Dedicated bgworker per group for full isolation
- Per-group config — `global_config` table + `sync_groups.config` JSONB with `set_config`/`get_config` API
- Remote PG sync via `conninfo` — slot+publication on remote, catalog introspection, remote cleanup
- Regression tests all passing
- Dockerfile for self-contained playground env
- Spillable DuckDB buffer for snapshot WAL buffering — spill-to-disk + backpressure exclusion for snapshot-queued changes
- Fan-in streaming — multiple PG sources to single DuckLake target (`_duckpipe_source` column, source-scoped DELETE/INSERT, schema validation guard, cross-group and same-group fan-in, FlushCoordinator keyed by mapping_id)
- Transparent analytical query routing — planner hook to auto-reroute analytical queries to DuckLake
- Snapshot WAL buffering memory — spillable DuckDB buffer with spill-to-disk + snapshot-aware backpressure
- Multi-table streaming lag fix — `prune_removed_tables()` in coordinator + benchmark cleanup
- Snapshot detection delay fix — LISTEN/NOTIFY wakeup for immediate bgworker wake
- Daemon HTTP `GET /metrics` endpoint — JSON metrics merging FlushCoordinator + PG persisted data
- `applied_lsn` NULL during SNAPSHOT/CATCHUP — COALESCE with `snapshot_lsn` as virtual floor
- `worker_state` updated during snapshot — observability metrics moved to SHM
- Flush runtime stats (`flush_count`, `flush_duration_ms`) in SHM
- Benchmark suite cleanup fix — `prepare_env` queries all existing mappings
- Bug fixes: `confirmed_lsn` reset on re-add, `lag_bytes` flat during catch-up, benchmark cleanup, index on `group_id` FK, snapshot retry backoff, bounded DuckDB memory

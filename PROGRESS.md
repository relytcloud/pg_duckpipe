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
- [x] Snapshot via SQL functions (`pg_create_logical_replication_slot` + `pg_export_snapshot`)
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
- [x] 19 regression tests all passing
- [x] Observability: `status()` SRF exposes `consecutive_failures`, `retry_at`, `applied_lsn`, `queued_changes` per table
- [x] Observability: `worker_status()` SRF exposes `total_queued_changes`, `is_backpressured`
- [x] Standardized logging: shared `init_subscriber`, all `eprintln!` replaced with `tracing` macros
- [x] `rows_synced` credited during snapshot
- [x] `confirmed_lsn` resets to 0 on table re-add — fixed via `COALESCE(applied_lsn, snapshot_lsn)` floor
- [x] Large catch-up batch stall (pure-insert path) — fixed via `may_have_conflicts` flag skipping DELETE scan
- [x] `lag_bytes` flat during catch-up — fixed: `StandbyStatusUpdate` sent each cycle even when no new WAL
- [x] Flush-thread drain capped at `batch_threshold` for incremental progress visibility

## TODO

### Performance / Scalability
- [ ] Flush thread pool — 1 OS thread + 1 tokio runtime + 1 DuckDB connection per table; fixed-size pool needed for 50+ tables
- [ ] Batch compaction tuning — reduce Parquet file proliferation under sustained small-batch writes
- [ ] Inline data flush
- [ ] Parquet-over-PG write throughput — ~10k rows/sec cap; bottleneck for large catch-up batches

### Features
- [ ] `source_uri` column for pg_mooncake compatibility
- [ ] `conninfo` column in sync_groups for remote PG support
- [ ] Schema DDL sync (ALTER TABLE ADD/DROP COLUMN propagation)

### Monitoring / Observability
- [ ] `applied_lsn` stays NULL during SNAPSHOT/CATCHUP — should be set to `snapshot_lsn` after snapshot completes
- [ ] `worker_state` not updated during snapshot processing — stale metrics while snapshots run

### Robustness
- [ ] Snapshot failures have no retry backoff — risk of thrash on repeated failures
- [ ] Graceful handling of DuckLake schema drift (target table altered outside duckpipe)
- [ ] Connection pooling for flush thread PG metadata updates (currently short-lived connections per flush)
- [ ] Regression tests for crash / error cases

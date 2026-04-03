# pg_duckpipe Usage

## Prerequisites

- PostgreSQL 14+ with `wal_level = logical`
- `pg_duckdb` and `pg_duckpipe` in `shared_preload_libraries`
- Source tables must have a PRIMARY KEY for upsert mode (default). Tables without a PRIMARY KEY are supported in `append` sync mode.

## Quick Start

```sql
-- Add a table for sync (auto-creates target, auto-starts worker)
SELECT duckpipe.add_table('public.orders');

-- Check status
SELECT * FROM duckpipe.status();

-- Query the synced columnar table
SELECT count(*) FROM public.orders_ducklake;
```

## SQL API

### Table Management

```sql
-- Add a table to sync (default group, with initial snapshot)
SELECT duckpipe.add_table('public.orders');

-- Add with custom target name
SELECT duckpipe.add_table('public.orders', 'analytics.orders_col');

-- Add to a specific sync group, skip initial snapshot
SELECT duckpipe.add_table('public.orders', NULL, 'my_group', false);

-- Remove a table from sync
SELECT duckpipe.remove_table('public.orders');

-- Remove and drop the target table
SELECT duckpipe.remove_table('public.orders', true);

-- Move a table to a different sync group
SELECT duckpipe.move_table('public.orders', 'other_group');

-- Re-snapshot a table (truncates target, re-copies from source)
SELECT duckpipe.resync_table('public.orders');
```

### Sync Groups

Sync groups map to a PostgreSQL publication + replication slot pair. Multiple tables share one group. A `default` group is created automatically.

```sql
-- Create a new sync group
SELECT duckpipe.create_group('analytics');

-- Create with explicit publication/slot names
SELECT duckpipe.create_group('analytics', 'my_pub', 'my_slot');

-- Enable/disable a group
SELECT duckpipe.enable_group('analytics');
SELECT duckpipe.disable_group('analytics');

-- Drop a group (also drops slot and publication)
SELECT duckpipe.drop_group('analytics');
```

### Fan-In Streaming

Multiple source tables can sync into a single DuckLake target, with automatic source tagging and full DML isolation:

```sql
-- First source — adds normally
SELECT duckpipe.add_table('public.orders_us', 'public.orders_ducklake');

-- Second source — requires fan_in => true
SELECT duckpipe.add_table('public.orders_eu', 'public.orders_ducklake', 'default', true, true);

-- Query with source provenance
SELECT id, product, _duckpipe_source FROM orders_ducklake;
```

See **[Fan-In Streaming](FAN_IN.md)** for use cases, cross-group fan-in, monitoring, and operational details.

### Remote Sync

Sync tables from a remote PostgreSQL instance by providing a `conninfo` connection string:

```sql
SELECT duckpipe.create_group('remote_oltp',
    conninfo => 'host=prod-db.example.com port=5432 dbname=myapp user=replicator password=secret');

SELECT duckpipe.add_table('public.orders', sync_group => 'remote_oltp');
```

See **[Remote Sync](REMOTE_SYNC.md)** for connection string formats, TLS configuration, and details.

### Worker Control

The background worker starts automatically when `add_table()` is called. Manual control:

```sql
SELECT duckpipe.start_worker();
SELECT duckpipe.stop_worker();
```

## Monitoring

### Per-Table Status

```sql
SELECT source_table, state, rows_synced, last_sync,
       applied_lsn, consecutive_failures, retry_at, error_message,
       queued_changes
FROM duckpipe.status();
```

| Column | Description |
|--------|-------------|
| `state` | Current state: `SNAPSHOT`, `CATCHUP`, `STREAMING`, or `ERRORED` |
| `rows_synced` | Total rows flushed to the target table |
| `last_sync` | Timestamp of last successful flush |
| `applied_lsn` | WAL position of last durably flushed data |
| `consecutive_failures` | Number of flush failures since last success (ERRORED triggers at 3) |
| `retry_at` | Scheduled auto-retry time when in ERRORED state |
| `error_message` | Last error message (empty when healthy) |
| `snapshot_duration_ms` | Time taken by the initial snapshot (NULL before snapshot completes) |
| `snapshot_rows` | Number of rows copied during the initial snapshot |
| `queued_changes` | In-flight changes in this table's flush queue (from shared memory) |

### Group Overview

```sql
SELECT name, enabled, table_count, last_sync
FROM duckpipe.groups();
```

| Column | Description |
|--------|-------------|
| `table_count` | Number of tables in the group |
| `last_sync` | Timestamp of last confirmed LSN advancement |

### Worker Pipeline Status

```sql
SELECT total_queued_bytes, is_backpressured
FROM duckpipe.worker_status();
```

| Column | Description |
|--------|-------------|
| `total_queued_bytes` | Total bytes queued across all per-table flush queues (from shared memory) |
| `is_backpressured` | `true` when WAL consumption is paused because queues are full (from shared memory) |

### JSON Metrics

Returns a complete metrics snapshot as JSON, merging shared memory metrics with persisted PG state:

```sql
SELECT duckpipe.metrics();
```

Output structure:
```json
{
  "tables": [{
    "group": "default",
    "source_table": "public.orders",
    "state": "STREAMING",
    "rows_synced": 15000,
    "queued_changes": 42,
    "duckdb_memory_bytes": 1048576,
    "consecutive_failures": 0,
    "flush_count": 150,
    "flush_duration_ms": 23,
    "snapshot_duration_ms": 1234,
    "snapshot_rows": 1000,
    "applied_lsn": "0/1A3B4C0"
  }],
  "groups": [{
    "name": "default",
    "total_queued_bytes": 42,
    "is_backpressured": false
  }]
}
```

The same JSON structure is available from the daemon via `GET /metrics` (see [Daemon REST API](#daemon-rest-api) below).

### Table Listing

```sql
SELECT source_table, target_table, sync_group, enabled, rows_synced
FROM duckpipe.tables();
```

## Configuration

### GUCs (PostgreSQL parameters)

These parameters require `ALTER SYSTEM SET` + `SELECT pg_reload_conf()` (SIGHUP-level), except `data_inlining_row_limit` which is session-level.

| Parameter | Default | Range | Description |
|-----------|---------|-------|-------------|
| `duckpipe.enabled` | `on` | — | Enable/disable the background worker |
| `duckpipe.poll_interval` | `1000` | 100–3600000 ms | Interval between WAL poll cycles |
| `duckpipe.batch_size_per_group` | `100000` | 100–10000000 | Max WAL messages per group per cycle |
| `duckpipe.debug_log` | `off` | — | Emit critical-path timing logs |
| `duckpipe.data_inlining_row_limit` | `0` | 0–1000000 | DuckLake data inlining row limit |

### Per-Group Config (config table)

DuckDB resource limits and flush tuning are managed via the `duckpipe.global_config` table and per-group JSONB overrides on `sync_groups.config`. Resolution order: **hardcoded defaults ← global_config rows ← per-group JSONB**.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `duckdb_buffer_memory_mb` | int | `16` | DuckDB memory limit (MB) during buffer accumulation (low, allows spill to disk) |
| `duckdb_flush_memory_mb` | int | `512` | DuckDB memory limit (MB) during flush/compaction (high, for DuckLake writes) |
| `duckdb_threads` | int | `1` | DuckDB `SET threads` per FlushWorker |
| `flush_interval_ms` | int | `5000` | Time-based flush trigger (ms) |
| `flush_batch_threshold` | int | `50000` | Queue-size flush trigger |
| `max_concurrent_flushes` | int | `4` | Max concurrent flush operations per group |
| `max_queued_bytes` | int | `256000000` | Backpressure threshold (bytes; 256 MB default) |

#### Config API

```sql
-- Read/write global config
SELECT duckpipe.get_config();                                   -- all keys as JSON
SELECT duckpipe.get_config('duckdb_buffer_memory_mb');          -- single key
SELECT duckpipe.set_config('duckdb_buffer_memory_mb', '32');

-- Read/write per-group config (resolved: defaults ← global ← group)
SELECT duckpipe.get_group_config('default');               -- resolved JSON
SELECT duckpipe.get_group_config('default', 'duckdb_threads');
SELECT duckpipe.set_group_config('default', 'duckdb_threads', '4');
```

### Tuning Examples

```sql
-- Lower latency: flush more frequently (global)
SELECT duckpipe.set_config('flush_interval_ms', '200');
SELECT duckpipe.set_config('flush_batch_threshold', '1000');

-- Higher throughput for a specific group
SELECT duckpipe.set_group_config('analytics', 'flush_interval_ms', '5000');
SELECT duckpipe.set_group_config('analytics', 'flush_batch_threshold', '100000');

-- Give a heavy group more DuckDB resources
SELECT duckpipe.set_group_config('analytics', 'duckdb_flush_memory_mb', '2048');
SELECT duckpipe.set_group_config('analytics', 'duckdb_threads', '4');

-- Limit flush parallelism (reduces peak memory with many tables)
ALTER SYSTEM SET duckpipe.max_concurrent_flushes = 2;
SELECT pg_reload_conf();

-- Pause sync without stopping the worker
ALTER SYSTEM SET duckpipe.enabled = off;
SELECT pg_reload_conf();
```

## Table States

Each table transitions independently through these states:

```
SNAPSHOT → CATCHUP → STREAMING
                         ↓
                      ERRORED (auto-retry with exponential backoff)
```

| State | Meaning |
|-------|---------|
| `SNAPSHOT` | Initial data copy in progress |
| `CATCHUP` | Replaying WAL changes that arrived during snapshot |
| `STREAMING` | Normal operation — applying WAL changes in real time |
| `ERRORED` | Flush failures exceeded threshold; auto-retries with 30s × 2^n backoff (max ~30 min) |

## Target Table Naming

By default, `add_table('public.orders')` creates target `public.orders_ducklake`. Override with the second argument:

```sql
SELECT duckpipe.add_table('public.orders', 'analytics.orders_columnar');
```

## Partitioned Tables

pg_duckpipe supports partitioned source tables. When you call `add_table()` on a partitioned table, it:

1. **Auto-detects** the table is partitioned (`relkind = 'p'`)
2. **Sets `publish_via_partition_root = true`** on the publication so all partition changes appear under the parent table's identity in the WAL stream
3. **Sets `REPLICA IDENTITY FULL`** on all existing child partitions (recursive, handles multi-level partitioning)

All partition data flows to a **single DuckLake target** table.

```sql
-- Works with any partitioned table
CREATE TABLE logs (
    id serial, log_date date NOT NULL, message text,
    PRIMARY KEY (id, log_date)
) PARTITION BY RANGE (log_date);

CREATE TABLE logs_2024 PARTITION OF logs FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE logs_2025 PARTITION OF logs FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

SELECT duckpipe.add_table('public.logs');
-- All data from all partitions syncs to logs_ducklake
```

### New partitions

New partitions auto-inherit publication membership (PostgreSQL handles this). However, you must set REPLICA IDENTITY on new partitions manually:

```sql
CREATE TABLE logs_2026 PARTITION OF logs FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
ALTER TABLE logs_2026 REPLICA IDENTITY FULL;
```

### Dropping old partitions

When you drop an old partition, no WAL event is generated — DuckLake data is preserved. This makes partitioned tables ideal for the log-sink pattern: ingest into PG, sync to DuckLake for analytics, drop old partitions to reclaim PG storage.

### Recommended: append mode

For log-style partitioned tables, use `sync_mode => 'append'` for an immutable changelog. In append mode, TRUNCATE on partitions is ignored, preserving DuckLake data:

```sql
SELECT duckpipe.add_table('public.logs', sync_mode => 'append');
```

> **Note**: In upsert mode, truncating a single partition is reported as a parent-level TRUNCATE (due to `publish_via_partition_root`), which deletes **all** data from the DuckLake target — not just the truncated partition's data.

## Daemon REST API

The standalone daemon (`duckpipe`) exposes an HTTP REST API on `--api-port` (default 8080). Key endpoints:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check (uptime, group binding, lock status) |
| `/status` | GET | Per-table status + worker state + group info |
| `/metrics` | GET | Full metrics snapshot (same JSON shape as `duckpipe.metrics()`) |
| `/groups` | POST | Bind daemon to a sync group |
| `/groups` | DELETE | Unbind daemon from current group |
| `/tables` | GET | List table mappings for bound group |
| `/tables` | POST | Add table to bound group |
| `/tables/{source_table}` | DELETE | Remove table from bound group |
| `/tables/{source_table}/resync` | POST | Re-snapshot a table |

### Daemon Metrics

```bash
curl http://localhost:8080/metrics
```

Returns the same JSON structure as the PG `duckpipe.metrics()` function, merging in-process FlushCoordinator metrics with PG persisted data.

## Operational Safety

### WAL Retention and `max_slot_wal_keep_size`

pg_duckpipe uses a logical replication slot to track its position in the WAL stream. PostgreSQL **cannot recycle WAL segments** past a slot's `restart_lsn`. If the duckpipe worker crashes or is stopped without calling `drop_group()`, the slot remains and WAL files accumulate indefinitely — potentially filling the disk and blocking all writes on the cluster.

**Recommended**: set `max_slot_wal_keep_size` (PostgreSQL 13+) to cap WAL retention per slot:

```sql
-- Cap WAL retained per slot to 10 GB (adjust to your disk capacity)
ALTER SYSTEM SET max_slot_wal_keep_size = '10GB';
SELECT pg_reload_conf();
```

When a slot exceeds this limit, PostgreSQL invalidates it. An invalidated slot no longer holds WAL. If duckpipe restarts and finds its slot invalidated, it must re-snapshot — but the database stays healthy.

### Monitoring for WAL Lag

Check replication slot lag regularly:

```sql
SELECT slot_name,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag
FROM pg_replication_slots
WHERE slot_name LIKE 'duckpipe_%';
```

### Cleaning Up Orphaned Slots

If duckpipe is permanently removed without cleanup:

```sql
-- Drop the replication slot
SELECT pg_drop_replication_slot('duckpipe_slot_default');

-- Drop the publication
DROP PUBLICATION IF EXISTS duckpipe_pub_default;
```

### Empty Groups

When the last table is removed from a sync group via `remove_table()`, a WARNING is emitted reminding you to drop the group. An empty group's replication slot still holds WAL — run `drop_group()` to release it:

```sql
SELECT duckpipe.drop_group('my_group');
```

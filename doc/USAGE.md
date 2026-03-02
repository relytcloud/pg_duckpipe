# pg_duckpipe Usage

## Prerequisites

- PostgreSQL 14+ with `wal_level = logical`
- `pg_duckdb` and `pg_duckpipe` in `shared_preload_libraries`
- Source tables must have a PRIMARY KEY

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

-- Drop group but keep the replication slot
SELECT duckpipe.drop_group('analytics', false);
```

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
       applied_lsn, consecutive_failures, retry_at, error_message
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
SELECT total_queued_changes, is_backpressured, updated_at
FROM duckpipe.worker_status();
```

| Column | Description |
|--------|-------------|
| `total_queued_changes` | In-flight changes across all per-table flush queues |
| `is_backpressured` | `true` when WAL consumption is paused because queues are full |
| `updated_at` | Timestamp of last worker state update |

### Table Listing

```sql
SELECT source_table, target_table, sync_group, enabled, rows_synced
FROM duckpipe.tables();
```

## Configuration (GUCs)

All parameters require `ALTER SYSTEM SET` + `SELECT pg_reload_conf()` (SIGHUP-level), except `data_inlining_row_limit` which is session-level.

| Parameter | Default | Range | Description |
|-----------|---------|-------|-------------|
| `duckpipe.enabled` | `on` | — | Enable/disable the background worker |
| `duckpipe.poll_interval` | `1000` | 100–3600000 ms | Interval between WAL poll cycles |
| `duckpipe.batch_size_per_group` | `100000` | 100–10000000 | Max WAL messages per group per cycle |
| `duckpipe.flush_interval` | `1000` | 100–60000 ms | Time-based flush trigger interval |
| `duckpipe.flush_batch_threshold` | `10000` | 100–1000000 | Queue size that triggers immediate flush |
| `duckpipe.max_queued_changes` | `500000` | 1000–10000000 | Backpressure threshold (pauses WAL consumer) |
| `duckpipe.debug_log` | `off` | — | Emit critical-path timing logs |
| `duckpipe.data_inlining_row_limit` | `0` | 0–1000000 | DuckLake data inlining row limit |

### Tuning Examples

```sql
-- Lower latency: flush more frequently
ALTER SYSTEM SET duckpipe.flush_interval = 200;
ALTER SYSTEM SET duckpipe.flush_batch_threshold = 1000;
SELECT pg_reload_conf();

-- Higher throughput: batch more before flushing
ALTER SYSTEM SET duckpipe.flush_interval = 5000;
ALTER SYSTEM SET duckpipe.flush_batch_threshold = 100000;
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

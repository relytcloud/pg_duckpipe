# pg_duckpipe

PostgreSQL extension and daemon for CDC synchronization from heap tables (row store) to DuckLake tables (column store), enabling HTAP in one system.

## Overview

`pg_duckpipe` tails PostgreSQL WAL via logical replication (`pgoutput`), decodes row changes, and applies them to DuckLake targets managed by `pg_duckdb`.

```
Heap table writes -> WAL -> replication slot (pgoutput) -> duckpipe worker -> DuckLake table
```

The project is a Rust workspace:

- `duckpipe-pg`: `pgrx` extension (`CREATE EXTENSION pg_duckpipe`)
- `duckpipe-core`: shared CDC engine
- `duckpipe-daemon`: standalone binary using the same core engine

## Key Features

- Streaming replication path (`START_REPLICATION`) with crash-safe slot advancement
- Per-table state machine: `SNAPSHOT -> CATCHUP -> STREAMING` (+ `ERRORED`)
- Sync groups: multiple tables share one publication + one replication slot
- Automatic target creation (`USING ducklake`) via `duckpipe.add_table(...)`
- TOAST-unchanged UPDATE preservation (no NULL corruption on unchanged TOAST columns)
- Per-table error isolation with exponential backoff auto-retry
- Backpressure when queued changes exceed a configured threshold
- Rename safety via source-table OID tracking
- Dynamic background worker auto-start on `add_table()`

## Quick Start (Extension Mode)

```sql
-- Install extension (requires pg_duckdb)
CREATE EXTENSION pg_duckpipe CASCADE;

-- Source table must have a primary key
CREATE TABLE public.orders (
  id BIGSERIAL PRIMARY KEY,
  customer_id BIGINT NOT NULL,
  amount NUMERIC NOT NULL
);

-- Add table to sync (auto-creates public.orders_ducklake, starts worker if needed)
SELECT duckpipe.add_table('public.orders');

-- Write OLTP data
INSERT INTO public.orders(customer_id, amount) VALUES (101, 42.50);

-- Observe sync state
SELECT source_table, state, rows_synced, applied_lsn
FROM duckpipe.status();
```

## SQL API

### Sync groups

```sql
duckpipe.create_group(name, [publication], [slot_name]) -> text
duckpipe.drop_group(name, [drop_slot])
duckpipe.enable_group(name)
duckpipe.disable_group(name)
```

### Table management

```sql
duckpipe.add_table(source_table, [target_table], [sync_group], [copy_data])
duckpipe.remove_table(source_table, [drop_target])
duckpipe.move_table(source_table, new_group)
duckpipe.resync_table(source_table)
```

### Worker control

```sql
duckpipe.start_worker()
duckpipe.stop_worker()
```

### Monitoring

```sql
duckpipe.groups()
duckpipe.tables()
duckpipe.status()
duckpipe.worker_status()
```

`duckpipe.status()` includes per-table `state`, `queued_changes`, `error_message`,
`consecutive_failures`, `retry_at`, and `applied_lsn`.

## Configuration (GUCs)

| GUC | Default | Context | Notes |
|-----|---------|---------|-------|
| `duckpipe.poll_interval` | `1000` | `SIGHUP` | Poll interval in ms (100..3600000) |
| `duckpipe.batch_size_per_group` | `100000` | `SIGHUP` | Max WAL messages per group per cycle |
| `duckpipe.enabled` | `on` | `SIGHUP` | Enable/disable worker loop |
| `duckpipe.debug_log` | `off` | `SIGHUP` | Emit critical-path timing logs |
| `duckpipe.flush_interval` | `1000` | `SIGHUP` | Flush interval in ms (100..60000) |
| `duckpipe.flush_batch_threshold` | `10000` | `SIGHUP` | Queue size that triggers immediate flush |
| `duckpipe.max_queued_changes` | `500000` | `SIGHUP` | Backpressure threshold |
| `duckpipe.data_inlining_row_limit` | `0` | `USERSET` | DuckLake data inlining row limit |

Example:

```sql
ALTER SYSTEM SET duckpipe.flush_interval = 200;
ALTER SYSTEM SET duckpipe.flush_batch_threshold = 1000;
SELECT pg_reload_conf();
```

## Requirements

- PostgreSQL 14+
- `pg_duckdb` extension available
- Source tables require a `PRIMARY KEY`
- Logical replication enabled (`wal_level=logical`, slots/senders configured)

## Build

```bash
make
make install
```

## Test

```bash
make installcheck
make check-regression TEST=api
```

Current regression schedule contains 20 tests under `test/regression/`.

## Documentation

- [doc/USAGE.md](doc/USAGE.md): SQL usage and operations guide
- [doc/CODE_WALKTHROUGH.md](doc/CODE_WALKTHROUGH.md): source-level architecture walkthrough
- [doc/DESIGN_V2.md](doc/DESIGN_V2.md): historical v2 design notes
- [benchmark/README.md](benchmark/README.md): benchmark harness

## License

PostgreSQL License.

# pg_duckpipe

PostgreSQL extension for HTAP (Hybrid Transactional/Analytical Processing) synchronization from heap tables to pg_ducklake columnar tables.

## Overview

```
┌─────────────────────────────────────────────────────────────┐
│  PostgreSQL                                                  │
│                                                              │
│  ┌─────────────┐     automatic      ┌──────────────────┐   │
│  │ Heap Tables │  ─────sync─────►   │ DuckLake Tables  │   │
│  │ (OLTP)      │     (CDC)          │ (OLAP)           │   │
│  └─────────────┘                    └──────────────────┘   │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

**Write to heap tables (row store), query from DuckLake tables (column store).**

## Key Features

- **Production-ready CDC**: Uses pgoutput (PostgreSQL's production plugin)
- **Resource efficient**: Multiple tables share one publication/slot
- **Built-in parsing**: Reuses PostgreSQL's `logicalrep_read_*` functions
- **Flexible grouping**: Organize tables into sync groups as needed
- **TRUNCATE support**: Propagates TRUNCATE to target tables
- **Snapshot consistency**: CATCHUP state prevents duplicates during initial copy
- **Auto-restart**: Worker recovers automatically from transient errors
- **Low OLTP overhead**: No triggers, async processing

## Quick Start

```sql
-- Install extension (CASCADE pulls in pg_duckdb)
CREATE EXTENSION pg_duckpipe CASCADE;

-- Create source table
CREATE TABLE orders (id SERIAL PRIMARY KEY, customer_id INT, amount NUMERIC);

-- Add table to sync (auto-creates orders_ducklake, copies existing data + streams changes)
-- The background worker starts automatically if not already running
SELECT duckpipe.add_table('public.orders');

-- OLTP operations work normally
INSERT INTO orders (customer_id, amount) VALUES (1, 99.99);

-- Analytics on columnar storage (after ~1s sync delay)
SELECT customer_id, sum(amount), count(*)
FROM orders_ducklake
GROUP BY customer_id;
```

## Resource Efficiency

Multiple tables share a single publication and replication slot:

| Tables | Publications | Slots |
|--------|--------------|-------|
| 10 | 1 | 1 |
| 50 | 1 | 1 |
| 100 (2 groups) | 2 | 2 |

Compare to naive approach: 100 tables = 100 publications + 100 slots (hits `max_replication_slots` limit).

## Sync Groups

Group tables to manage resources and isolation:

```sql
-- Default: all tables in one group
SELECT duckpipe.add_table('public.orders');
SELECT duckpipe.add_table('public.customers');

-- Create separate group for high-volume tables
SELECT duckpipe.create_group('analytics');
SELECT duckpipe.add_table('public.events', sync_group := 'analytics');
SELECT duckpipe.add_table('public.logs', sync_group := 'analytics');
```

## API Reference

```sql
-- Sync groups
duckpipe.create_group(name, [publication], [slot_name]) → TEXT
duckpipe.drop_group(name, [drop_slot])
duckpipe.enable_group(name)
duckpipe.disable_group(name)

-- Table management
duckpipe.add_table(source_table, [target_table], [sync_group], [copy_data])
duckpipe.remove_table(source_table, [drop_target])
duckpipe.move_table(source_table, new_group)
duckpipe.resync_table(source_table)

-- Worker management
duckpipe.start_worker()
duckpipe.stop_worker()

-- Monitoring
duckpipe.groups() → TABLE(name, publication, slot_name, enabled, table_count, lag_bytes, last_sync)
duckpipe.tables() → TABLE(source_table, target_table, sync_group, enabled, rows_synced, last_sync)
duckpipe.status() → TABLE(sync_group, source_table, target_table, state, enabled, rows_synced, last_sync)
```

## Configuration

```sql
SET duckpipe.poll_interval = 1000;          -- ms between polls
SET duckpipe.batch_size_per_table = 1000;   -- fairness between tables
SET duckpipe.batch_size_per_group = 10000;  -- fairness between groups
SET duckpipe.enabled = on;                  -- enable/disable worker
SET duckpipe.debug_log = off;               -- emit critical-path timing logs
```

## Requirements

- PostgreSQL 14+
- pg_duckdb extension
- Source tables must have PRIMARY KEY

## Building

```bash
make
make install
```

## Running Tests

```bash
make installcheck               # Run all 12 regression tests
make check-regression TEST=api  # Run a single test
```

## Documentation

See [doc/DESIGN.md](doc/DESIGN.md) for technical architecture and design decisions.

## ETL Comparison Findings (2026-02-10)

Cross-check against sibling project `../etl` identified these improvement opportunities for `pg_duckpipe`:

1. **CATCHUP handoff should be LSN-gated (correctness)**
- Current behavior promotes all `CATCHUP` tables to `STREAMING` at end of each poll round, even when only a partial WAL window was consumed due to `duckpipe.batch_size_per_group`.
- Risk: rows already copied by snapshot can be replayed from leftover WAL and inserted again.
- **Proposed fix:** transition `CATCHUP -> STREAMING` only when slot consumption in the current round has advanced to/past each table's `snapshot_lsn` (or when no WAL remains in the round).

2. **Per-table error lifecycle and retry policy**
- ETL keeps explicit errored state and retry strategy (`manual`, `timed`, `none`) so one table can fail without stalling the entire pipeline.
- `pg_duckpipe` currently has only normal sync states and mostly hard-fails on apply-path errors.

3. **Richer lag observability**
- ETL exposes restart/flush lag and safe WAL headroom from replication slots.
- `pg_duckpipe` currently exposes only `pg_current_wal_lsn() - confirmed_lsn`.

4. **Stricter protocol/state invariants**
- ETL validates transaction invariants (e.g., begin/commit LSN consistency) and treats schema drift as explicit table error state.
- `pg_duckpipe` should add equivalent guards to fail fast and isolate bad tables.

5. **Failure-mode regression coverage**
- ETL has targeted retry/failpoint tests for consistency during failure and recovery.
- `pg_duckpipe` should expand regression coverage around handoff, retries, and partial-batch edge cases.

## Known Correctness Issues

### 1. ~~Snapshot Race Window~~ — FIXED

**Severity: ~~High~~ FIXED**

**Fix:** Replaced `GetXLogInsertRecPtr()` with a temporary replication slot created via `CREATE_REPLICATION_SLOT ... TEMPORARY LOGICAL pgoutput` over a libpq replication connection. The slot's `consistent_point` LSN is atomically tied to an exported snapshot, which is then imported (`SET TRANSACTION SNAPSHOT`) for the `INSERT INTO target SELECT * FROM source` copy. This eliminates the race window entirely — the snapshot and LSN are guaranteed consistent by PostgreSQL's replication protocol.

**Original problem (for historical context):** In the previous implementation, `INSERT INTO target SELECT * FROM source` used a transaction snapshot (S1) established at the start of the poll round, but `snapshot_lsn` was set to `pg_current_wal_lsn()` which reflects the latest WAL at call time. Any transaction that committed between S1 and the `pg_current_wal_lsn()` call would be invisible to the copy but skipped during CATCHUP, causing permanent silent data loss.

Regression test: `test/regression/sql/snapshot_race.sql` (confirms no data loss under concurrent writes).

### 2. Premature CATCHUP → STREAMING Transition — `worker.c:430`

**Severity: High**

After each poll round, ALL `CATCHUP` tables are unconditionally transitioned to `STREAMING`. If the slot contains more messages than `batch_size_per_group`, only a partial batch is consumed per round. CATCHUP tables that haven't consumed past their `snapshot_lsn` will be promoted to STREAMING prematurely, causing duplicate rows from WAL messages that should have been skipped.

### 3. Snapshot Copy Blocks All Streaming Tables — `worker.c:process_sync_group()`

**Severity: Medium**

`process_snapshot()` runs synchronously at the start of each `process_sync_group()` call, before any WAL is fetched. While copying a large table via `INSERT INTO target SELECT * FROM source`, all other tables in the group receive zero WAL consumption. The main slot holds back WAL, lag grows for every table, and a multi-minute copy stalls the entire group. Fix: move the snapshot copy to `add_table()` (user's session) so the worker only handles WAL streaming, or run it in a separate background worker.

### 4. TOAST Unchanged Columns Become NULL — `decoder.c:30`

**Severity: Medium**

When an `UPDATE` doesn't modify a TOASTed column (large text, jsonb, bytea > ~2KB), pgoutput sends status `'u'` (unchanged). The code converts this to NULL. Since UPDATEs are decomposed into `DELETE + INSERT`, the INSERT writes NULL for the unchanged TOAST column, silently corrupting data. Workaround: set `REPLICA IDENTITY FULL` on source tables with TOAST columns.

## Performance TODOs

- [ ] **Batch DELETEs in `apply_batch`** (`apply.c`): DELETEs are executed one-at-a-time via SPI. For UPDATE-heavy workloads (DELETE+INSERT pairs), this means 2N SPI calls per batch instead of 2. Batch DELETEs into a single `DELETE FROM t WHERE (pk) IN (VALUES ...)` statement.
- [x] **Zero-copy WAL decode** (`worker.c`): The decode loop allocates/copies a StringInfo per message. Point StringInfo directly at the pre-copied `wal_messages[i].data` buffer instead of alloc+copy+free per message.
- [ ] **Defer `update_table_metrics` to end-of-round** (`batch.c`): Each batch flush does an SPI UPDATE to `table_mappings`. Accumulate deltas and write once at end-of-round to reduce mid-pipeline SPI calls.
- [x] **Skip poll wait when there is work** (`worker.c`): The worker now loops immediately without sleeping when any group processed changes, instead of always waiting 10ms.
- [x] **Atomic snapshot via temp replication slot** (`worker.c`): Snapshot copy now uses a temporary replication slot (`CREATE_REPLICATION_SLOT ... TEMPORARY LOGICAL pgoutput`) over a libpq replication connection, atomically tying the exported snapshot to `consistent_point` LSN. Eliminates the snapshot race window.
- [ ] **Per-group workers** (`worker.c`, `api.c`): Currently one background worker processes all sync groups sequentially. Launch one worker per sync group so groups can fetch/decode/apply in parallel, preventing one slow group from blocking others.

## License

Same as PostgreSQL (PostgreSQL License).

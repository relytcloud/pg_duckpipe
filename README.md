# pg_duckpipe

PostgreSQL extension for HTAP (Hybrid Transactional/Analytical Processing) synchronization from heap tables to pg_ducklake columnar tables.

## Overview

```
┌─────────────────────────────────────────────────────────────┐
│  PostgreSQL                                                 │
│                                                             │
│  ┌─────────────┐     automatic      ┌──────────────────┐    │
│  │ Heap Tables │  ─────sync─────►   │ DuckLake Tables  │    │
│  │ (OLTP)      │     (CDC)          │ (OLAP)           │    │
│  └─────────────┘                    └──────────────────┘    │
│                                                             │
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

All parameters are `PGC_SIGHUP` — change via `ALTER SYSTEM SET` + `SELECT pg_reload_conf()`:

```sql
ALTER SYSTEM SET duckpipe.poll_interval = 1000;          -- ms between polls (min 100)
ALTER SYSTEM SET duckpipe.batch_size_per_table = 1000;   -- fairness between tables
ALTER SYSTEM SET duckpipe.batch_size_per_group = 10000;  -- fairness between groups (min 100)
ALTER SYSTEM SET duckpipe.enabled = on;                  -- enable/disable worker
ALTER SYSTEM SET duckpipe.debug_log = off;               -- emit critical-path timing logs
SELECT pg_reload_conf();
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
make installcheck               # Run all 13 regression tests
make check-regression TEST=api  # Run a single test
```

## Documentation

- [doc/DESIGN.md](doc/DESIGN.md) — Technical architecture, protocol details, and remote sync design (Section 16)
- [doc/STANDALONE_DESIGN.md](doc/STANDALONE_DESIGN.md) — Planned Rust-based standalone CDC engine

## ETL Comparison Findings (2026-02-10)

Cross-check against sibling project `../etl` identified these improvement opportunities for `pg_duckpipe`:

1. **~~CATCHUP handoff should be LSN-gated (correctness)~~ — FIXED**
- Promotion is now LSN-gated: `CATCHUP -> STREAMING` only when `snapshot_lsn <= pending_lsn`.

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

### 2. ~~Premature CATCHUP → STREAMING Transition~~ — FIXED

**Severity: ~~High~~ FIXED**

**Fix:** CATCHUP → STREAMING promotion is now LSN-gated. A CATCHUP table is promoted only when `snapshot_lsn <= pending_lsn` (the group's WAL consumption has advanced past the table's snapshot point). If no WAL was consumed in the round (`pending_lsn == 0`), no promotion happens. During CATCHUP, changes with LSN > snapshot_lsn are still applied normally — only changes at or below snapshot_lsn are skipped.

**Original problem (for historical context):** After each poll round, ALL CATCHUP tables were unconditionally transitioned to STREAMING. If the slot contained more messages than `batch_size_per_group`, only a partial batch was consumed per round. CATCHUP tables that hadn't consumed past their `snapshot_lsn` were promoted to STREAMING prematurely, causing duplicate rows from WAL messages that should have been skipped.

Regression test: `test/regression/sql/premature_catchup.sql` (200 separate transactions with `batch_size_per_group=100`, confirms no duplicates).

### 3. Snapshot Copy Blocks All Streaming Tables — `worker.c:process_sync_group()`

**Severity: Medium**

`process_snapshot()` runs synchronously at the start of each `process_sync_group()` call, before any WAL is fetched. While copying a large table via `INSERT INTO target SELECT * FROM source`, all other tables in the group receive zero WAL consumption. The main slot holds back WAL, lag grows for every table, and a multi-minute copy stalls the entire group. Fix: move the snapshot copy to `add_table()` (user's session) so the worker only handles WAL streaming, or run it in a separate background worker.

### 4. TOAST Unchanged Columns Become NULL — `decoder.c:30`

**Severity: Medium — Planned fix: native batched UPDATE SQL**

When an `UPDATE` doesn't modify a TOASTed column (large text, jsonb, bytea > ~2KB), pgoutput sends status `'u'` (unchanged). The current code converts this to NULL. Since UPDATEs are decomposed into `DELETE + INSERT`, the INSERT writes NULL for the unchanged TOAST column, silently corrupting data. Workaround: set `REPLICA IDENTITY FULL` on source tables with TOAST columns.

**Planned fix:** Replace the DELETE + INSERT decomposition with native `UPDATE ... FROM (VALUES ...)` SQL. Unchanged columns (status `'u'`) are omitted from the SET clause — they keep their current value in the target. UPDATEs are batched by their `col_unchanged` bitmask pattern, producing efficient single-statement updates per batch. See `doc/DESIGN.md` Section 17 for full design.

## Performance TODOs

- [ ] **Batch DELETEs in `apply_batch`** (`apply.c`): DELETEs are executed one-at-a-time via SPI. Batch DELETEs into a single `DELETE FROM t WHERE (pk) IN (VALUES ...)` statement.
- [x] **Zero-copy WAL decode** (`worker.c`): The decode loop allocates/copies a StringInfo per message. Point StringInfo directly at the pre-copied `wal_messages[i].data` buffer instead of alloc+copy+free per message.
- [ ] **Defer `update_table_metrics` to end-of-round** (`batch.c`): Each batch flush does an SPI UPDATE to `table_mappings`. Accumulate deltas and write once at end-of-round to reduce mid-pipeline SPI calls.
- [x] **Skip poll wait when there is work** (`worker.c`): The worker now loops immediately without sleeping when any group processed changes, instead of always waiting 10ms.
- [x] **Atomic snapshot via temp replication slot** (`worker.c`): Snapshot copy now uses a temporary replication slot (`CREATE_REPLICATION_SLOT ... TEMPORARY LOGICAL pgoutput`) over a libpq replication connection, atomically tying the exported snapshot to `consistent_point` LSN. Eliminates the snapshot race window.
- [ ] **Per-group workers** (`worker.c`, `api.c`): Currently one background worker processes all sync groups sequentially. Launch one worker per sync group so groups can fetch/decode/apply in parallel, preventing one slow group from blocking others.

## License

Same as PostgreSQL (PostgreSQL License).

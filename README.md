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
make installcheck               # Run all 9 regression tests
make check-regression TEST=api  # Run a single test
```

## Documentation

See [doc/DESIGN.md](doc/DESIGN.md) for technical architecture and design decisions.

## License

Same as PostgreSQL (PostgreSQL License).

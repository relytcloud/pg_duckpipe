# Quick Start: Hands-On Guide

This guide walks you through common pg_duckpipe workflows step by step.

## 0. Setup

The fastest way to get started is with Docker — everything is preconfigured:

```bash
docker run -d --name duckpipe \
  -p 15432:5432 \
  -e POSTGRES_PASSWORD=duckdb \
  pgducklake/pgduckpipe:18-main

PGPASSWORD=duckdb psql -h localhost -p 15432 -U postgres
```

If you're installing from source, make sure these are in place before continuing:

1. Set `wal_level = logical` in `postgresql.conf`
2. Add both extensions to `shared_preload_libraries`:
   ```
   shared_preload_libraries = 'pg_duckdb, pg_duckpipe'
   ```
3. Restart PostgreSQL
4. Create the extensions in your database:
   ```sql
   CREATE EXTENSION pg_duckdb;
   CREATE EXTENSION pg_duckpipe;
   ```

## 1. Add Your First Table

Source tables **must** have a primary key.

```sql
-- Create a source table
CREATE TABLE orders (
    id    BIGSERIAL PRIMARY KEY,
    customer TEXT,
    total    INT
);

-- Insert some initial data
INSERT INTO orders(customer, total) VALUES
    ('alice', 100),
    ('bob',   250),
    ('carol', 75);

-- Start syncing — this one call does everything:
--   creates orders_ducklake, snapshots existing rows, starts the worker
SELECT duckpipe.add_table('public.orders');
```

Wait a few seconds for the snapshot to complete, then verify:

```sql
-- Check sync state
SELECT source_table, target_table, state, rows_synced
FROM duckpipe.status();

--  source_table  |      target_table      |  state  | rows_synced
-- ---------------+------------------------+---------+-------------
--  public.orders | public.orders_ducklake | CATCHUP |           3

-- Query the columnar table
SELECT * FROM orders_ducklake ORDER BY id;

--  id | customer | total
-- ----+----------+-------
--   1 | alice    |   100
--   2 | bob      |   250
--   3 | carol    |    75
```

> State progresses `SNAPSHOT` → `CATCHUP` → `STREAMING`. You may see `CATCHUP` briefly after snapshot — it transitions to `STREAMING` automatically.

From now on, any DML on `orders` is automatically replicated:

```sql
INSERT INTO orders(customer, total) VALUES ('dave', 300);
UPDATE orders SET total = 500 WHERE customer = 'alice';
DELETE FROM orders WHERE customer = 'bob';

-- After a short delay, changes appear in the columnar table
SELECT * FROM orders_ducklake ORDER BY id;

--  id | customer | total
-- ----+----------+-------
--   1 | alice    |   500
--   3 | carol    |    75
--   4 | dave     |   300
```

## 2. Remove a Table

```sql
-- Stop syncing but keep the target table and its data
SELECT duckpipe.remove_table('public.orders');

-- Or remove and drop the target table entirely
SELECT duckpipe.remove_table('public.orders', drop_target => true);
```

After removal, new changes to `orders` are no longer captured.

## 3. Re-Add a Table

You can re-add a previously removed table at any time. It goes through a fresh snapshot:

```sql
-- Re-add — a new snapshot copies current source data to target
SELECT duckpipe.add_table('public.orders');

-- Check progress
SELECT source_table, state, rows_synced FROM duckpipe.status();
```

If you removed the table **without** `drop_target => true`, the existing target is reused and its old data is replaced by the fresh snapshot.

## 4. Add Multiple Tables

Add several tables to the same sync group — they share one replication slot and publication:

```sql
CREATE TABLE customers (
    id   BIGSERIAL PRIMARY KEY,
    name TEXT,
    email TEXT
);

CREATE TABLE products (
    id    BIGSERIAL PRIMARY KEY,
    name  TEXT,
    price INT
);

-- Add all three tables
SELECT duckpipe.add_table('public.orders');
SELECT duckpipe.add_table('public.customers');
SELECT duckpipe.add_table('public.products');

-- Each gets its own _ducklake target
SELECT source_table, target_table, sync_group, state
FROM duckpipe.status();

--    source_table   |       target_table        | sync_group |  state
-- ------------------+---------------------------+------------+---------
--  public.customers | public.customers_ducklake | default    | CATCHUP
--  public.orders    | public.orders_ducklake    | default    | CATCHUP
--  public.products  | public.products_ducklake  | default    | CATCHUP
```

## 5. Resync a Table

If a table gets out of sync or you want to force a fresh snapshot:

```sql
-- Resync truncates the target and re-copies all source data
SELECT duckpipe.resync_table('public.orders');

-- Monitor progress — state goes SNAPSHOT → CATCHUP → STREAMING
SELECT source_table, state, rows_synced FROM duckpipe.status();
```

No data is lost in the source table. The target is rebuilt from scratch.

## 6. Monitor Sync Status

### Per-Table Status

```sql
SELECT source_table, state, rows_synced, queued_changes,
       error_message, consecutive_failures
FROM duckpipe.status();
```

| Column | Meaning |
|--------|---------|
| `state` | `SNAPSHOT` → `CATCHUP` → `STREAMING` (or `ERRORED`) |
| `rows_synced` | Total rows flushed to the target |
| `queued_changes` | In-flight changes waiting to be flushed |
| `error_message` | Last error (empty when healthy) |
| `consecutive_failures` | Flush failures since last success |

### Group Overview

```sql
SELECT name, enabled, table_count, lag_bytes
FROM duckpipe.groups();
```

`lag_bytes` shows how far behind the group is from the current WAL tip — 0 means fully caught up.

### Worker Health

```sql
SELECT total_queued_changes, is_backpressured
FROM duckpipe.worker_status();
```

- `is_backpressured = true` means flush threads can't keep up and WAL consumption is paused.
- This is a safety mechanism — it resolves automatically once flushes catch up.

### What to Look For

- **Healthy**: all tables in `STREAMING` state, `lag_bytes` near 0
- **Catching up**: tables in `SNAPSHOT` or `CATCHUP` — normal after `add_table` or `resync_table`
- **Trouble**: tables in `ERRORED` state — check `error_message` and `consecutive_failures`

Errored tables auto-retry with exponential backoff. To force an immediate retry:

```sql
SELECT duckpipe.resync_table('public.orders');
```

## Common Pitfalls

| Symptom | Cause | Fix |
|---------|-------|-----|
| `add_table()` fails with "no primary key" | Source table has no PK | Add a primary key: `ALTER TABLE t ADD PRIMARY KEY (id)` |
| Extension not found | Not in `shared_preload_libraries` | Add to `postgresql.conf` and **restart** PostgreSQL |
| No data appearing in target | Worker not running | Check `SELECT * FROM duckpipe.worker_status()` — if empty, call `SELECT duckpipe.start_worker()` |
| Table stuck in `ERRORED` | Flush failure (check `error_message`) | Fix the underlying issue, then `SELECT duckpipe.resync_table(...)` |

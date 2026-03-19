# Fan-In Streaming

Fan-in streaming lets **multiple PostgreSQL source tables sync into a single DuckLake target table**. Each source's data is tagged with a `_duckpipe_source` column, so all DML operations (INSERT, UPDATE, DELETE, TRUNCATE) are fully isolated per source.

## Use Cases

### Shard Consolidation from Remote Instances

Multiple sharded databases each have an `orders` table. Fan-in merges them into one `orders_ducklake` table for unified analytics — no ETL pipeline or UNION ALL views required.

```
shard_us   orders ─┐
shard_eu   orders ─┼──▶  orders_ducklake  (single columnar table)
shard_apac orders ─┘
```

```sql
-- Each shard is a remote group with its own connection string
SELECT duckpipe.create_group('shard_us',
    conninfo => 'host=us-db.example.com dbname=myapp user=replicator');
SELECT duckpipe.create_group('shard_eu',
    conninfo => 'host=eu-db.example.com dbname=myapp user=replicator');

-- First shard
SELECT duckpipe.add_table('public.orders', 'public.orders_ducklake', 'shard_us');

-- Second shard — requires fan_in => true
SELECT duckpipe.add_table('public.orders', 'public.orders_ducklake', 'shard_eu', true, true);
```

### Multiple Tables on the Same Instance

Year-partitioned or region-split tables on the same database consolidate into one analytical table:

```sql
CREATE TABLE orders_2024 (id int PRIMARY KEY, product text, qty int);
CREATE TABLE orders_2025 (id int PRIMARY KEY, product text, qty int);

SELECT duckpipe.add_table('public.orders_2024', 'public.orders_ducklake');
SELECT duckpipe.add_table('public.orders_2025', 'public.orders_ducklake', 'default', true, true);
```

Both tables share a single replication slot and flush coordinator.

## Quick Start

```sql
-- Source tables with identical schemas (PRIMARY KEY required)
CREATE TABLE orders_us (id int PRIMARY KEY, product text, qty int);
CREATE TABLE orders_eu (id int PRIMARY KEY, product text, qty int);

-- First source — adds normally
SELECT duckpipe.add_table('public.orders_us', 'public.orders_ducklake');

-- Second source — requires fan_in => true
SELECT duckpipe.add_table(
    'public.orders_eu',
    'public.orders_ducklake',
    'default',    -- sync_group
    true,         -- copy_data (initial snapshot)
    true          -- fan_in
);
```

That's it. Both tables now stream changes into `orders_ducklake`.

## How It Works

### Source Tagging

Every row in the target table carries a `_duckpipe_source` column with the format `{group}/{schema}.{table}`:

```sql
SELECT id, product, qty, _duckpipe_source FROM orders_ducklake ORDER BY id;

 id | product  | qty | _duckpipe_source
----+----------+-----+---------------------------
  1 | widget   |  10 | default/public.orders_us
  2 | gadget   |   5 | default/public.orders_us
101 | sprocket |  20 | default/public.orders_eu
102 | bolt     |  50 | default/public.orders_eu
```

### DML Isolation

All operations are scoped to the originating source:

- **UPDATE** on `orders_us` only modifies rows tagged `default/public.orders_us`
- **DELETE** on `orders_eu` only removes rows tagged `default/public.orders_eu`
- **TRUNCATE** on `orders_us` translates to `DELETE ... WHERE _duckpipe_source = 'default/public.orders_us'` — other sources are untouched

**Performance:** fan-in does not degrade write performance compared to single-source sync. The heaviest operations (dedup, delete, update) are scoped to each source independently, so adding more sources does not increase the work per flush.

## Monitoring and Operations

### Listing Sources

```sql
SELECT source_table, target_table, sync_group, source_label, source_count
FROM duckpipe.tables()
WHERE target_table = 'public.orders_ducklake'
ORDER BY source_table;

   source_table   |      target_table       | sync_group |        source_label         | source_count
------------------+-------------------------+------------+-----------------------------+--------------
 public.orders_us | public.orders_ducklake  | default    | default/public.orders_us    |            2
 public.orders_eu | public.orders_ducklake  | default    | default/public.orders_eu    |            2
```

- **source_label**: identifies which source contributed which rows
- **source_count**: number of sources feeding this target (>1 indicates fan-in)

### Per-Source Status

Each source has its own state machine (SNAPSHOT, CATCHUP, STREAMING, ERRORED) — one source can be in SNAPSHOT while another is already STREAMING.

```sql
SELECT source_table, sync_group, source_label, state
FROM duckpipe.status()
WHERE target_table = 'public.orders_ducklake'
ORDER BY source_table;
```

### Querying by Source

Filter the target table using `_duckpipe_source`:

```sql
-- All rows from one source
SELECT * FROM orders_ducklake
WHERE _duckpipe_source = 'default/public.orders_us';

-- Row count per source
SELECT _duckpipe_source, count(*) FROM orders_ducklake GROUP BY 1;
```

DuckLake stores Parquet min/max statistics on `_duckpipe_source`, so these filters benefit from file-level pruning.

### Resync a Single Source

Re-snapshot one source without affecting others:

```sql
SELECT duckpipe.resync_table('public.orders_us', 'default');
```

This deletes only `default/public.orders_us` rows from the target and re-copies from `orders_us`.

### Remove a Single Source

Stop replication for one source while others continue:

```sql
SELECT duckpipe.remove_table('public.orders_us', false, 'default');
```

The `false` parameter means the target table is not dropped (other sources still use it). Previously synced rows from the removed source remain in the target — query or delete them manually if needed.

### Add a New Source Later

Fan-in is incremental. Add new sources at any time:

```sql
SELECT duckpipe.add_table('public.orders_apac', 'public.orders_ducklake', 'default', true, true);
```

The new source starts with a snapshot and catches up to live streaming while existing sources continue uninterrupted.

## Requirements

- All source tables must have a **PRIMARY KEY**
- All source tables must have **identical schemas** (same column names, same types, same order) — validated at `add_table()` time

## Limitations

- **No DDL support**: schema changes (ALTER TABLE) on fan-in source tables are not supported. All sources sharing a target must maintain identical schemas.
- Primary key values should be globally unique across sources for best results. Overlapping PKs across sources will not cause errors (each source's data is isolated), but analytics queries should be aware that the same PK can appear from different sources.

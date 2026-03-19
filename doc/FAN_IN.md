# Fan-In Streaming

Fan-in streaming lets **multiple PostgreSQL source tables sync into a single DuckLake target table**. Each source's data is tagged with a `_duckpipe_source` column, so all DML operations (INSERT, UPDATE, DELETE, TRUNCATE) are fully isolated per source.

## Use Cases

### Shard Consolidation

Multiple application shards write to separate tables (`orders_us`, `orders_eu`, `orders_apac`). Fan-in merges them into one `orders_ducklake` table for unified analytics — no ETL pipeline or UNION ALL views required.

```
orders_us   ─┐
orders_eu   ─┼──▶  orders_ducklake  (single columnar table)
orders_apac ─┘
```

### Temporal Table Merge

Year-partitioned tables (`events_2024`, `events_2025`) consolidate into one analytical table without manual data migration. New year tables are added incrementally.

### Multi-Tenant Aggregation

Per-tenant tables replicate into a shared analytics table with automatic source tagging, enabling cross-tenant analytics while preserving data provenance.

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

### Schema Validation

When adding a second source to a target, pg_duckpipe validates that both source tables have **identical column names and types** (excluding `_duckpipe_source`). A schema mismatch is rejected at `add_table()` time with a clear error message.

### Safety Guard

Adding a second source to an existing target without `fan_in => true` raises an error:

```sql
SELECT duckpipe.add_table('public.orders_eu', 'public.orders_ducklake');
-- ERROR: target public.orders_ducklake is already managed by group "default" ...
--        pass fan_in => true to confirm
```

This prevents accidental data merging.

## Cross-Group Fan-In

Different sync groups can feed the same target. This is useful when sources come from separate databases or need independent replication tuning.

```sql
-- Create dedicated groups
SELECT duckpipe.create_group('shard_a');
SELECT duckpipe.create_group('shard_b');

-- Each group manages its own source
SELECT duckpipe.add_table('public.orders_a', 'public.orders_ducklake', 'shard_a');
SELECT duckpipe.add_table('public.orders_b', 'public.orders_ducklake', 'shard_b', true, true);
```

Source labels reflect the group: `shard_a/public.orders_a`, `shard_b/public.orders_b`.

Cross-group fan-in allows independent configuration per group (flush intervals, memory limits, etc.) via [per-group config](USAGE.md#per-group-config-config-table).

## Same-Group Fan-In

Multiple sources within the same group also work:

```sql
SELECT duckpipe.add_table('public.events_2024', 'public.events_ducklake');
SELECT duckpipe.add_table('public.events_2025', 'public.events_ducklake', 'default', false, true);
```

Both tables share a single replication slot and flush coordinator.

## Monitoring

### tables()

```sql
SELECT source_table, target_table, sync_group, source_label, source_count
FROM duckpipe.tables()
WHERE target_table = 'public.orders_ducklake'
ORDER BY source_table;

   source_table   |      target_table       | sync_group |       source_label        | source_count
------------------+-------------------------+------------+---------------------------+--------------
 public.orders_a  | public.orders_ducklake  | shard_a    | shard_a/public.orders_a   |            2
 public.orders_b  | public.orders_ducklake  | shard_b    | shard_b/public.orders_b   |            2
```

- **source_label**: identifies which source contributed which rows
- **source_count**: number of sources feeding this target (>1 indicates fan-in)

### status()

```sql
SELECT source_table, sync_group, source_label, state
FROM duckpipe.status()
WHERE source_table IN ('public.orders_a', 'public.orders_b')
ORDER BY source_table;
```

Each source has its own state machine (SNAPSHOT, CATCHUP, STREAMING, ERRORED) — one source can be in SNAPSHOT while another is already STREAMING.

### Querying by Source

Filter the target table using `_duckpipe_source`:

```sql
-- All rows from shard_a
SELECT * FROM orders_ducklake WHERE _duckpipe_source = 'shard_a/public.orders_a';

-- Row count per source
SELECT _duckpipe_source, count(*) FROM orders_ducklake GROUP BY 1;
```

DuckLake stores Parquet min/max statistics on `_duckpipe_source`, so these filters benefit from file-level pruning.

## Operations

### Resync a Single Source

Re-snapshot one source without affecting others:

```sql
-- Specify the sync_group to disambiguate
SELECT duckpipe.resync_table('public.orders_a', 'shard_a');
```

This deletes only `shard_a/public.orders_a` rows from the target and re-copies from `orders_a`.

### Remove a Single Source

Stop replication for one source while others continue:

```sql
-- Specify sync_group to target the correct mapping
SELECT duckpipe.remove_table('public.orders_a', false, 'shard_a');
```

The `false` parameter means the target table is not dropped (other sources still use it). Previously synced rows from the removed source remain in the target — query or delete them manually if needed.

### Add a New Source Later

Fan-in is incremental. Add new sources at any time:

```sql
SELECT duckpipe.add_table('public.orders_c', 'public.orders_ducklake', 'shard_c', true, true);
```

The new source starts with a snapshot and catches up to live streaming while existing sources continue uninterrupted.

## Requirements

- All source tables must have a **PRIMARY KEY**
- All source tables must have **identical schemas** (same column names, same types, same order) — validated at `add_table()` time
- The `_duckpipe_source` column is automatically added to the target; do not include it in source schemas

## Limitations

- Primary key values should be globally unique across sources for best results. Overlapping PKs across sources will not cause errors (each source's data is isolated), but analytics queries should be aware that the same PK can appear from different sources.
- Schema changes (DDL) to one source require the same change to all sources sharing the target.

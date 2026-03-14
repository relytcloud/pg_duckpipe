# Data Types

pg_duckpipe syncs PostgreSQL heap tables to DuckLake columnar tables. This document describes how PostgreSQL data types are mapped to DuckDB types during CDC replication.

## Type Mapping Table

| PostgreSQL Type | DuckDB Type | Notes |
|---|---|---|
| `integer` | `INTEGER` | Pass-through |
| `bigint` | `BIGINT` | Pass-through |
| `smallint` | `SMALLINT` | Pass-through |
| `real` | `REAL` | PG `float` / `float4` |
| `double precision` | `DOUBLE` | Pass-through |
| `boolean` | `BOOLEAN` | Pass-through |
| `text` | `TEXT` | Pass-through |
| `character varying(n)` | `VARCHAR(n)` | Pass-through |
| `character(n)` | `VARCHAR(n)` | Pass-through; DuckDB treats `CHAR(n)` as `VARCHAR` |
| `date` | `DATE` | Pass-through |
| `timestamp without time zone` | `TIMESTAMP` | Pass-through |
| `timestamp with time zone` | `TIMESTAMP WITH TIME ZONE` | Pass-through |
| `time without time zone` | `TIME` | Pass-through |
| `numeric(p,s)` | `DECIMAL(p,s)` | Pass-through |
| `uuid` | `UUID` | Pass-through |
| `interval` | `INTERVAL` | Pass-through |
| `jsonb` | `JSON` | Explicit mapping in `map_pg_type_for_duckdb()` |
| `json` | `JSON` | Explicit mapping in `map_pg_type_for_duckdb()` |
| `integer[]`, `text[]`, etc. | `INTEGER[]`, `VARCHAR[]`, etc. | DuckDB supports array types |
| `serial` | `INTEGER` | `format_type()` outputs `integer` |
| `bigserial` | `BIGINT` | `format_type()` outputs `bigint` |
| `smallserial` | `SMALLINT` | `format_type()` outputs `smallint` |

## Type Conversion Architecture

Data flows through three layers when types are converted from PostgreSQL to DuckDB:

```
PG WAL (text values + OIDs)
        │
        ▼
Layer 1: parse_text_value()          ── OID-based parsing into Value enum
        │
        ▼
Layer 2: map_pg_type_for_duckdb()    ── PG type name → DuckDB type name (DDL)
        │
        ▼
Layer 3: DuckDB INSERT auto-cast     ── Text → target column type
```

### Layer 1: WAL Decoder (`parse_text_value`)

The pgoutput logical decoding plugin sends column values as text strings with their type OIDs. The decoder (`duckpipe-core/src/decoder.rs`) parses these into typed `Value` variants:

| OID | PG Type | Value Variant |
|-----|---------|---------------|
| 16 | `bool` | `Value::Bool` |
| 21 | `int2` | `Value::Int16` |
| 23, 26 | `int4`, `oid` | `Value::Int32` |
| 20 | `int8` | `Value::Int64` |
| 700 | `float4` | `Value::Float32` |
| 701 | `float8` | `Value::Float64` |
| *other* | *everything else* | `Value::Text` (fallback) |

Types not explicitly matched (text, varchar, timestamp, date, uuid, json, jsonb, numeric, etc.) are kept as `Value::Text` strings. DuckDB auto-casts these at insert time (Layer 3).

### Layer 2: DDL Type Mapping (`map_pg_type_for_duckdb`)

When `add_table()` creates the target DuckLake table, it introspects source columns via `pg_catalog.format_type()` and maps PG type names to DuckDB equivalents (`duckpipe-pg/src/api.rs`):

```rust
fn map_pg_type_for_duckdb(pg_type: &str) -> String {
    match pg_type {
        "jsonb" | "json" => "JSON".to_string(),
        other => other.to_string(),  // pass-through
    }
}
```

Most PG type names are directly compatible with DuckDB (e.g. `integer`, `text`, `boolean`, `timestamp without time zone`). Only types with incompatible names need explicit mapping — currently just `jsonb`/`json`.

### Layer 3: DuckDB Auto-Cast

When `Value::Text` strings are inserted into the DuckDB buffer table, DuckDB automatically casts the text representation to the target column's declared type. This handles uuid, numeric, timestamp, date, time, interval, and all other text-representable types without needing explicit conversion code.

## Snapshot Path

During initial table sync (`SNAPSHOT` state), pg_duckpipe uses `COPY ... TO STDOUT (FORMAT csv)` to bulk-export the source table. Key details:

- **NULL sentinel**: The custom string `__DUCKPIPE_NULL__` is used as the CSV NULL marker to distinguish SQL NULL from empty strings (`''`)
- **CSV escaping**: Commas, quotes, newlines, and special characters in text values are handled by PG's standard CSV escaping
- **Type handling**: All values arrive as CSV text; DuckDB auto-casts them to the target column types when loading

## Limitations

- **No schema evolution**: `ALTER TABLE` (add/drop/rename column, change type) is not propagated. Use `resync_table()` to rebuild the target after schema changes.
- **No bytea**: Binary data (`bytea`) is not supported. The text-based transport cannot represent arbitrary binary.
- **No composite/range types**: PostgreSQL composite types and range types are not supported.
- **No enum types**: PostgreSQL `CREATE TYPE ... AS ENUM` types are not mapped. The text representation is stored but the DuckDB column type will be `TEXT` via pass-through.

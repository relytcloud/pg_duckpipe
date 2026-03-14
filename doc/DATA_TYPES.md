# Data Types

Supported PostgreSQL data types and how they map to DuckDB in the target DuckLake table.

## Supported Types

| PostgreSQL | DuckDB | Notes |
|---|---|---|
| `integer` / `serial` | `INTEGER` | |
| `bigint` / `bigserial` | `BIGINT` | |
| `smallint` / `smallserial` | `SMALLINT` | |
| `real` | `REAL` | PG `float4` |
| `double precision` | `DOUBLE` | PG `float8` |
| `boolean` | `BOOLEAN` | |
| `text` | `TEXT` | |
| `varchar(n)` | `VARCHAR(n)` | |
| `char(n)` | `VARCHAR(n)` | |
| `date` | `DATE` | |
| `timestamp` | `TIMESTAMP` | |
| `timestamptz` | `TIMESTAMPTZ` | |
| `time` | `TIME` | |
| `numeric(p,s)` | `DECIMAL(p,s)` | |
| `uuid` | `UUID` | |
| `interval` | `INTERVAL` | |
| `jsonb` | `JSON` | |
| `json` | `JSON` | |
| `integer[]`, `text[]`, etc. | `INTEGER[]`, `VARCHAR[]`, etc. | Array types |

## Unsupported Types

- **`bytea`** — binary data cannot be transported via the text-based CDC pipeline
- **Composite types** — `CREATE TYPE ... AS (...)` are not mapped
- **Range types** — `int4range`, `tsrange`, etc. are not mapped
- **Enum types** — `CREATE TYPE ... AS ENUM` values are stored as `TEXT`

## Schema Changes

`ALTER TABLE` (add/drop/rename columns, change types) is **not** propagated automatically. After modifying a source table's schema, rebuild the target with:

```sql
SELECT duckpipe.resync_table('public.my_table');
```

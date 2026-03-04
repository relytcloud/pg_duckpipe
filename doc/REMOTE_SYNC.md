# Remote Sync

Sync tables from a **remote** PostgreSQL instance into local DuckLake columnar tables. The remote PG provides WAL replication and snapshots; the local PG holds metadata and DuckLake targets.

```
Remote PG (source)                Local PG (target + metadata)
+-----------------+               +---------------------------+
| heap tables     | ---WAL------> | duckpipe.sync_groups      |
| publication     | ---COPY-----> | duckpipe.table_mappings   |
| repl slot       |               | DuckLake columnar tables  |
+-----------------+               +---------------------------+
```

## Quick Start

```sql
-- Create a group connected to a remote PG
SELECT duckpipe.create_group('remote_oltp',
    conninfo => 'host=prod-db.example.com port=5432 dbname=myapp user=replicator password=secret');

-- Add tables (introspects remote catalog, creates local DuckLake target)
SELECT duckpipe.add_table('public.orders', sync_group => 'remote_oltp');
SELECT duckpipe.add_table('public.customers', sync_group => 'remote_oltp');

-- Query synced data locally
SELECT count(*) FROM public.orders_ducklake;

-- Check remote groups
SELECT name, conninfo IS NOT NULL AS is_remote FROM duckpipe.groups();
```

## Connection String

Both standard libpq formats are supported:

```sql
-- Key=value (recommended)
conninfo => 'host=db.example.com port=5432 dbname=myapp user=replicator password=secret'

-- PostgreSQL URI
conninfo => 'postgresql://replicator:secret@db.example.com:5432/myapp'
```

Supported parameters: `host`, `port`, `user`, `password`, `dbname`, `sslmode`.

Values containing spaces, quotes, or backslashes must be single-quoted per the libpq spec:

```sql
conninfo => 'host=db.example.com password=''my password'' dbname=myapp'
```

## TLS

Set `sslmode` to enable encrypted connections to the remote PG:

| sslmode | Behavior |
|---------|----------|
| `require` | TLS required, no certificate verification |
| `prefer` | Treated as `require` (no plaintext fallback) |
| `disable` / omitted | Plaintext connection |

```sql
SELECT duckpipe.create_group('secure_remote',
    conninfo => 'host=db.example.com port=5432 dbname=myapp user=replicator sslmode=require');
```

TLS uses Mozilla's root certificate store (`webpki-roots`). Only `disable`, `prefer`, and `require` modes are supported (`verify-ca` and `verify-full` are not available).

## How It Works

When a group has a `conninfo`, all source-side operations route to the remote PG:

| Operation | Local group | Remote group |
|---|---|---|
| WAL replication | local PG | remote PG |
| Snapshot COPY | local PG | remote PG |
| PK catalog query | local PG | remote PG |
| Publication/slot DDL | local SPI | remote PG via tokio-postgres |
| Target table CREATE | `LIKE source USING ducklake` | explicit DDL from remote catalog introspection |

Metadata updates (`sync_groups`, `table_mappings`) and DuckDB flush always happen locally.

### What `create_group(conninfo => ...)` does

1. Connects to the remote PG via `tokio-postgres`
2. Creates a logical replication slot (`duckpipe_slot_{name}`)
3. Creates an empty publication (`duckpipe_pub_{name}`)
4. Stores the `conninfo` in the local `sync_groups` table

### What `add_table()` does for remote groups

1. Connects to the remote PG
2. Adds the table to the remote publication (`ALTER PUBLICATION ... ADD TABLE`)
3. Sets `REPLICA IDENTITY FULL` on the remote source table
4. Queries remote `pg_catalog` for column definitions and primary key
5. Creates the local DuckLake target table using explicit DDL (not `LIKE`, since the source isn't local)
6. Inserts a mapping with the remote source OID

### Cleanup

`remove_table()` drops the table from the remote publication. `drop_group()` drops the remote slot and publication.

## Requirements

- Remote PG must have `wal_level = logical`
- Source tables must have a PRIMARY KEY
- The `conninfo` user must have replication privileges (`REPLICATION` role attribute or sufficient `pg_hba.conf` entry)
- All tables in a group come from the same PG instance (conninfo is per-group, not per-table)
- Network connectivity from local PG to remote PG on the specified port

## Security

- Passwords in `conninfo` are stored in the `sync_groups` table. The `groups()` SRF redacts the password field in its output.
- Use `sslmode=require` or stronger for production deployments over untrusted networks.
- The `conninfo` user needs only replication and SELECT privileges on source tables — avoid superuser credentials.

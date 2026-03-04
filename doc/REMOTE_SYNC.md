# Remote Sync

Sync tables from a **remote** PostgreSQL instance into local DuckLake columnar tables.

```
Remote PG (source)                    Local PG (target)
+-------------------+                 +---------------------------+
| heap tables       |---WAL stream--->| duckpipe bgworker         |
| publication       |---COPY--------->|   -> flush to DuckLake    |
| replication slot  |                 |                           |
+-------------------+                 | DuckLake columnar tables  |
                                      | (orders_ducklake, ...)    |
                                      +---------------------------+
```

## Quick Start

```sql
-- Create a group connected to a remote PG
SELECT duckpipe.create_group('remote_oltp',
    conninfo => 'host=prod-db.example.com port=5432 dbname=myapp user=replicator password=secret');

-- Add tables
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

## TLS

Set `sslmode` to enable encrypted connections:

| sslmode | Behavior |
|---------|----------|
| `require` | TLS required |
| `prefer` | Treated as `require` (no plaintext fallback) |
| `disable` / omitted | Plaintext connection |

```sql
SELECT duckpipe.create_group('secure_remote',
    conninfo => 'host=db.example.com port=5432 dbname=myapp user=replicator sslmode=require');
```

## Cleanup

```sql
-- Remove a single table from the remote publication
SELECT duckpipe.remove_table('public.orders', sync_group => 'remote_oltp');

-- Drop the entire group (removes remote slot + publication)
SELECT duckpipe.drop_group('remote_oltp');
```

## Requirements

- Remote PG must have `wal_level = logical`
- Source tables must have a PRIMARY KEY
- The `conninfo` user needs `REPLICATION` privilege and `SELECT` on source tables
- All tables in a group come from the same PG instance
- Network connectivity from local PG to remote PG on the specified port

## Security

- Passwords in `conninfo` are stored in the `sync_groups` table; `groups()` redacts them in output.
- Use `sslmode=require` for production deployments over untrusted networks.
- Grant only replication and SELECT privileges to the `conninfo` user — avoid superuser credentials.

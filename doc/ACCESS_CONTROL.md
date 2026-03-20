# Access Control

pg_duckpipe's permission model follows PostgreSQL conventions: management requires superuser access, target tables get read-only grants to source owners, and monitoring is available to any user with schema access.

## Extension Installation

`CREATE EXTENSION pg_duckpipe` requires superuser. The extension creates:

- **`duckpipe` schema** — owned by the installing superuser, no USAGE grant to PUBLIC
- **`duckpipe.sync_groups`** — publication/slot/conninfo metadata
- **`duckpipe.table_mappings`** — per-table sync state
- **`duckpipe.global_config`** — key-value configuration
- **`duckpipe.worker_state`** — runtime state (largely replaced by shared memory)

All internal tables are only accessible via the SQL API functions.

## Management Functions

All management functions are **REVOKE'd from PUBLIC**. Only superusers (or roles explicitly granted EXECUTE) can call them:

| Function | Purpose |
|----------|---------|
| `duckpipe.create_group()` | Create a sync group (publication + replication slot) |
| `duckpipe.drop_group()` | Drop a sync group and clean up resources |
| `duckpipe.enable_group()` | Enable a sync group |
| `duckpipe.disable_group()` | Disable a sync group |
| `duckpipe.add_table()` | Add a source table for CDC sync |
| `duckpipe.remove_table()` | Remove a table from sync |
| `duckpipe.move_table()` | Move a table between sync groups |
| `duckpipe.resync_table()` | Trigger a full resync |
| `duckpipe.set_routing()` | Enable/disable transparent query routing |
| `duckpipe.start_worker()` | Start the background worker |
| `duckpipe.stop_worker()` | Stop the background worker |
| `duckpipe.set_config()` | Modify global configuration |
| `duckpipe.set_group_config()` | Modify per-group configuration |

### Why superuser-only?

Management operations involve creating replication slots, publications, and background workers — all of which require elevated privileges in PostgreSQL. Keeping the entire management API superuser-gated is simple, safe, and consistent.

## Monitoring Functions

Monitoring functions are **not revoked from PUBLIC**. Any role with USAGE on the `duckpipe` schema can call them:

| Function | Returns |
|----------|---------|
| `duckpipe.groups()` | Sync group overview (conninfo passwords are redacted) |
| `duckpipe.tables()` | All table mappings across groups |
| `duckpipe.status()` | Per-table state, LSN, error info |
| `duckpipe.worker_status()` | Per-group queue depth and backpressure state |
| `duckpipe.metrics()` | JSON snapshot of runtime metrics |
| `duckpipe.get_config()` | Global configuration values |
| `duckpipe.get_group_config()` | Per-group configuration values |

### Granting monitoring access

By default the `duckpipe` schema has no USAGE grant to PUBLIC. To let non-superuser roles monitor sync status:

```sql
GRANT USAGE ON SCHEMA duckpipe TO monitoring_role;
```

## Target Table Permissions

### Local sync groups

When `add_table()` creates a target DuckLake table, it automatically:

1. **Creates the target table** — owned by the superuser who called `add_table()`
2. **Grants SELECT to the source table owner** — so the role that owns the source heap table can query the columnar copy

This means the source table owner gets read-only access to the target without manual intervention. No INSERT, UPDATE, or DELETE is granted — the target is append-only via the CDC pipeline.

```
Source: public.orders (owned by app_user)
  |
  add_table('public.orders')
  |
Target: public.orders_ducklake (owned by superuser, SELECT granted to app_user)
```

### Remote sync groups

When syncing from a remote PostgreSQL instance (`conninfo` is set), **no automatic grants** are issued on the target table. The source table owner exists on a different server and may not correspond to any local role.

Grant access manually after setup:

```sql
SELECT duckpipe.add_table('public.orders', NULL, 'remote_group');
GRANT SELECT ON public.orders_ducklake TO analyst_role;
```

### Additional grants

To grant access to additional roles beyond the source owner:

```sql
-- Read-only access for analysts
GRANT SELECT ON public.orders_ducklake TO analyst_role;

-- Access to all current and future tables in a schema
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT ON TABLES TO analyst_role;
```

## Background Worker

The background worker connects to PostgreSQL as the OS user (typically `postgres`). It:

- Reads from replication slots via the streaming replication protocol
- Writes to target DuckLake tables via standard SQL (INSERT/DELETE)
- Updates metadata in `duckpipe.table_mappings`

All bgworker operations run with the database owner's privileges. This is standard for PostgreSQL background workers and necessary for replication slot consumption.

## Replication Requirements

### Local sync

The installing superuser creates publications and replication slots automatically. No additional setup needed.

### Remote sync

The `conninfo` user on the remote PostgreSQL instance needs:

- **REPLICATION** privilege (for logical replication slots)
- **SELECT** on source tables (for initial snapshot COPY)

```sql
-- On the remote PostgreSQL instance:
CREATE ROLE replicator LOGIN REPLICATION PASSWORD 'secret';
GRANT SELECT ON public.orders TO replicator;
```

## Credential Storage

Connection strings for remote sync groups are stored in `duckpipe.sync_groups.conninfo`. Passwords are stored as-is in the database but **redacted in monitoring output** (`duckpipe.groups()` replaces password values with `********`).

For production deployments, consider:

- Using `.pgpass` files instead of inline passwords
- Using certificate-based authentication (`sslmode=verify-full`)
- Restricting SELECT on `duckpipe.sync_groups` to superusers (default — no public schema USAGE)

## Summary

| Object | Owner | PUBLIC access | Notes |
|--------|-------|---------------|-------|
| `duckpipe` schema | Extension installer | No USAGE | Grant USAGE for monitoring access |
| Internal metadata tables | Schema owner | No | Access only via API functions |
| Management functions | Schema owner | REVOKE'd | Superuser or explicit GRANT required |
| Monitoring functions | Schema owner | Allowed (if schema USAGE granted) | Read-only, passwords redacted |
| Target DuckLake tables (local) | `add_table()` caller | No | SELECT auto-granted to source owner |
| Target DuckLake tables (remote) | `add_table()` caller | No | Manual GRANT required |
| GUC parameters | N/A | Readable by all | Most require superuser to change globally |

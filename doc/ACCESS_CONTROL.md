# Access Control

## Who Can Do What

| Action | Required privilege |
|--------|-------------------|
| Install extension | Superuser |
| Manage sync (add/remove tables, create/drop groups, start/stop workers) | Superuser |
| Monitor sync (`status()`, `groups()`, `tables()`, `worker_status()`, `metrics()`) | Any user |
| Read config (`get_config()`, `get_group_config()`) | Any user |
| Change config (`set_config()`, `set_group_config()`) | Superuser |
| Query target DuckLake tables | Source table owner (auto-granted), or manually granted roles |

## Target Table Permissions

**Local sync**: `add_table()` automatically grants `SELECT` on the target DuckLake table to the owner of the source table. No INSERT/UPDATE/DELETE — the target is write-only via the CDC pipeline.

```
public.orders (owned by app_user)
  → add_table('public.orders')
  → public.orders_ducklake (SELECT granted to app_user)
```

**Remote sync**: No automatic grants. The source owner is on a different server. Grant access manually:

```sql
GRANT SELECT ON public.orders_ducklake TO analyst_role;
```

**Additional grants**: Use standard PostgreSQL GRANT to give more roles access:

```sql
GRANT SELECT ON public.orders_ducklake TO analyst_role;

-- Or for all future tables in a schema:
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT ON TABLES TO analyst_role;
```

## Monitoring Access

Monitoring functions use `SECURITY DEFINER` so any user can call them without needing direct access to internal tables. Passwords in connection strings are redacted in output.

Internal metadata tables (`sync_groups`, `table_mappings`, `global_config`) are not directly accessible — use the monitoring functions instead.

## Remote Sync Credentials

The remote `conninfo` user needs:

```sql
-- On the remote PostgreSQL instance:
CREATE ROLE replicator LOGIN REPLICATION PASSWORD '...';
GRANT SELECT ON public.orders TO replicator;
```

Connection strings are stored in `duckpipe.sync_groups.conninfo`. For production, prefer `.pgpass` or certificate-based authentication over inline passwords.

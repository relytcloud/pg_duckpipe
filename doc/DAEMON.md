# DuckPipe Daemon — REST API Guide

The `duckpipe` daemon is a standalone CDC sync engine that runs outside PostgreSQL. It connects to a PG instance with `pg_duckpipe` + `pg_ducklake` installed and streams WAL changes to DuckLake columnar tables.

The daemon exposes an HTTP REST API for managing sync groups, tables, and monitoring status — no `psql` required.

## Quick Start

### Local Sync

Source and target tables on the same PostgreSQL instance:

```bash
# Start daemon (unbound — no sync group yet)
duckpipe --connstr "host=localhost dbname=mydb user=rep" --api-port 8080

# Create a sync group (binds the daemon)
curl -s -X POST http://localhost:8080/groups \
  -H 'Content-Type: application/json' \
  -d '{"name":"default"}'

# Add a table to sync
curl -s -X POST http://localhost:8080/tables \
  -H 'Content-Type: application/json' \
  -d '{"source_table":"public.orders"}'

# Check sync status
curl -s http://localhost:8080/status | jq .
```

### Remote Sync

Source tables on a different PG, DuckLake target on the local PG:

```bash
# Start daemon (--connstr = local PG with DuckLake)
duckpipe --connstr "host=local_pg dbname=analytics user=rep" --api-port 8080

# Create remote group (conninfo = remote source PG)
curl -s -X POST http://localhost:8080/groups \
  -H 'Content-Type: application/json' \
  -d '{"name":"remote_sync","conninfo":"host=remote_pg dbname=prod user=replicator password=secret"}'

# Add tables from the remote PG
curl -s -X POST http://localhost:8080/tables \
  -H 'Content-Type: application/json' \
  -d '{"source_table":"public.orders"}'
```

### Pre-bound Start

Skip `POST /groups` by specifying the group at startup:

```bash
# Auto-creates "default" group if it doesn't exist
duckpipe --connstr "host=localhost dbname=mydb user=rep" --group default --api-port 8080

# Immediately add tables
curl -s -X POST http://localhost:8080/tables \
  -H 'Content-Type: application/json' \
  -d '{"source_table":"public.orders"}'
```

## CLI Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--connstr` | (required) | PG connection string (local PG with duckpipe extension) |
| `--group` | (none) | Pre-bind to this sync group at startup |
| `--api-port` | 8080 | HTTP API port (0 to disable) |
| `--poll-interval` | 1000 | WAL poll interval in ms |
| `--flush-interval` | 1000 | Flush interval in ms |
| `--flush-batch-threshold` | 10000 | Queued changes to trigger immediate flush |
| `--max-queued-changes` | 500000 | Backpressure threshold |
| `--ducklake-schema` | ducklake | DuckLake metadata schema |
| `--debug` | false | Enable debug timing logs |

## API Endpoints

### GET /health

Daemon health check. Always works regardless of group binding state.

```bash
curl -s http://localhost:8080/health | jq .
```

Response:
```json
{
  "status": "ok",
  "uptime_secs": 42,
  "group": "default",
  "locked": true
}
```

- `group`: bound group name, or `null` if unbound
- `locked`: whether the daemon holds the advisory lock

### GET /status

Per-table sync status and worker health for the bound group.

```bash
curl -s http://localhost:8080/status | jq .
```

Response:
```json
{
  "group": "default",
  "group_info": {
    "enabled": true,
    "confirmed_lsn": "0/1A3B4C0"
  },
  "tables": [
    {
      "source_table": "public.orders",
      "target_table": "public.orders_ducklake",
      "state": "STREAMING",
      "enabled": true,
      "rows_synced": 15000,
      "queued_changes": 0,
      "applied_lsn": "0/1A3B4C0",
      "error_message": null
    }
  ],
  "worker": {
    "total_queued_changes": 0,
    "is_backpressured": false
  }
}
```

### GET /tables

List table mappings for the bound group.

```bash
curl -s http://localhost:8080/tables | jq .
```

### POST /groups

Create a sync group and bind the daemon to it. Only works if the daemon is unbound.

```bash
# Local sync group
curl -s -X POST http://localhost:8080/groups \
  -H 'Content-Type: application/json' \
  -d '{"name":"default"}'

# Remote sync group (with conninfo)
curl -s -X POST http://localhost:8080/groups \
  -H 'Content-Type: application/json' \
  -d '{"name":"remote","conninfo":"host=remote_pg dbname=prod user=rep password=secret"}'
```

Returns `409 Conflict` if:
- Daemon is already bound to a group
- Another daemon holds the advisory lock for this group

### DELETE /groups

Drop the bound group. The daemon becomes unbound.

```bash
curl -s -X DELETE 'http://localhost:8080/groups'
```

### POST /groups/enable

Enable the bound group (resume sync).

```bash
curl -s -X POST http://localhost:8080/groups/enable
```

### POST /groups/disable

Disable the bound group (pause sync).

```bash
curl -s -X POST http://localhost:8080/groups/disable
```

### POST /tables

Add a table to the bound group.

```bash
# Basic — target auto-created as public.orders_ducklake
curl -s -X POST http://localhost:8080/tables \
  -H 'Content-Type: application/json' \
  -d '{"source_table":"public.orders"}'

# With options
curl -s -X POST http://localhost:8080/tables \
  -H 'Content-Type: application/json' \
  -d '{"source_table":"public.orders","target_table":"analytics.orders_col","copy_data":true}'
```

Parameters:
- `source_table` (required): fully qualified source table name
- `target_table` (optional): custom target table name
- `copy_data` (optional, default `true`): snapshot existing data

### DELETE /tables/{source_table}

Remove a table from sync.

```bash
# Remove table, keep the DuckLake target (default)
curl -s -X DELETE 'http://localhost:8080/tables/public.orders'

# Remove table and drop the target
curl -s -X DELETE 'http://localhost:8080/tables/public.orders?drop_target=true'
```

### POST /tables/{source_table}/resync

Trigger a full resync (re-snapshot) of a table.

```bash
curl -s -X POST http://localhost:8080/tables/public.orders/resync
```

## Group Binding Model

A daemon handles exactly **one** sync group. Two modes:

1. **Pre-bound**: `--group default` at startup. Group auto-created if needed. Sync starts immediately.
2. **Unbound**: No `--group`. Daemon starts with API only. `POST /groups` creates and binds a group. Sync starts after binding.

### Advisory Lock

When binding to a group, the daemon acquires a PostgreSQL advisory lock (`pg_try_advisory_lock(hashtext('duckpipe_daemon:GROUP_NAME'))`). This prevents multiple daemons from processing the same group. The lock is held for the lifetime of a persistent PG connection and released automatically if the daemon crashes.

## Error Handling

All error responses use the format:

```json
{
  "error": "error message here"
}
```

| HTTP Status | Meaning |
|-------------|---------|
| 200 | Success |
| 400 | Bad request (no group bound, invalid parameters, PG user errors) |
| 404 | Resource not found |
| 409 | Conflict (already bound, advisory lock held) |
| 500 | Internal server error (PG connection failure, etc.) |

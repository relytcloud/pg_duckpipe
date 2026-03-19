---
name: setup
description: Set up a running PostgreSQL instance with pg_duckdb, pg_ducklake, and pg_duckpipe extensions loaded
user_invocable: true
allowed_tools: Bash, Read, Glob, Grep, AskUserQuestion
---

# /setup — PostgreSQL Development Instance

Set up a running PostgreSQL instance with pg_duckdb, pg_ducklake, and pg_duckpipe fully loaded and ready to use. Intended to run after `/build` (or with a pre-existing PG installation).

Execute each phase below **in order**. If a step fails, stop and show the error — do not continue to the next phase.

**IMPORTANT**: All paths below are relative to the **repo root** (the directory containing `Cargo.toml`, `Makefile`, and `duckpipe-pg/`). Determine the repo root at the start — if working inside a worktree, use the worktree root.

**Defaults**:
- Data directory: `deps/pgdata` (gitignored via existing `deps/` entry)
- Port: `5588` (avoids conflicts with test ports 5555, 5556, 5566)

---

## Phase 0 — Locate PG Installation

Find `pg_config` using this priority:

1. `deps/postgres/work/app/bin/pg_config` (from `/build`)
2. `PG_CONFIG` environment variable
3. `pg_config` on `$PATH`

If none found, stop and tell the user to run `/build` first or set `PG_CONFIG`.

Once found, set these for all subsequent commands:

```bash
export PG_CONFIG="<resolved path>"
export PATH="$(dirname $PG_CONFIG):$PATH"
export PG_LIB="$($PG_CONFIG --pkglibdir)"
```

On **macOS**, set `DYLD_LIBRARY_PATH` so PG binaries can find `libduckdb.dylib`:

```bash
export DYLD_LIBRARY_PATH="$PG_LIB:${DYLD_LIBRARY_PATH:-}"
```

On **Linux**, set `LD_LIBRARY_PATH`:

```bash
export LD_LIBRARY_PATH="$PG_LIB:${LD_LIBRARY_PATH:-}"
```

Verify `pg_duckpipe` is installed by checking for `pg_duckpipe.so` or `pg_duckpipe.dylib` in `$PG_LIB`. If missing, tell the user to run `/build` first.

---

## Phase 1 — Initialize Data Directory

**Default data dir**: `deps/pgdata`

If `deps/pgdata` already exists:
- Use `AskUserQuestion` to ask the user whether to **reuse** the existing data directory or **reinitialize** it (delete and recreate).
- If reinitialize: `rm -rf deps/pgdata` then proceed with `initdb`.
- If reuse: skip `initdb` and go to Phase 3 (Start).

If it doesn't exist, run:

```bash
initdb -D deps/pgdata --no-locale --encoding=UTF8
```

---

## Phase 2 — Configure postgresql.conf

Append the following GUCs to `deps/pgdata/postgresql.conf`. First check if `# pg_duckpipe setup` marker already exists — if so, skip this phase (already configured).

```bash
cat >> deps/pgdata/postgresql.conf << 'EOF'

# pg_duckpipe setup
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
shared_preload_libraries = 'pg_duckdb,pg_ducklake,pg_duckpipe'
duckdb.unsafe_allow_mixed_transactions = on
port = 5588
listen_addresses = 'localhost'
unix_socket_directories = '/tmp'
logging_collector = on
log_directory = 'log'
log_filename = 'postgresql.log'
log_min_messages = warning
EOF
```

---

## Phase 3 — Start PostgreSQL

First check if PostgreSQL is already running on port 5588:

```bash
pg_isready -p 5588
```

If already running, print "PostgreSQL is already running on port 5588 — skipping start" and go to Phase 4.

If not running, start it:

```bash
pg_ctl -D deps/pgdata -l deps/pgdata/logfile start
```

Then poll until ready (timeout 15 seconds):

```bash
for i in $(seq 1 15); do
  pg_isready -p 5588 && break
  sleep 1
done
```

If not ready after 15 seconds, print the last 30 lines of `deps/pgdata/logfile` and stop.

---

## Phase 4 — Create Extensions

Connect via `psql` and create extensions:

```bash
psql -p 5588 -U $USER -d postgres -c "CREATE EXTENSION IF NOT EXISTS pg_duckdb;"
psql -p 5588 -U $USER -d postgres -c "CREATE EXTENSION IF NOT EXISTS pg_ducklake;"
psql -p 5588 -U $USER -d postgres -c "CREATE EXTENSION IF NOT EXISTS pg_duckpipe;"
```

If any `CREATE EXTENSION` fails, print the error and the last 30 lines of `deps/pgdata/logfile`, then stop.

---

## Phase 5 — Verify & Print Summary

Query `pg_extension` to confirm all three extensions are loaded:

```bash
psql -p 5588 -U $USER -d postgres -c "SELECT extname, extversion FROM pg_extension WHERE extname IN ('pg_duckdb', 'pg_ducklake', 'pg_duckpipe');"
```

If any extension is missing from the result, report the error and stop.

Print this summary:

```
PostgreSQL is running!
  Port:      5588
  Data dir:  deps/pgdata
  Log file:  deps/pgdata/logfile
  Connect:   psql -p 5588 -U $USER -d postgres

To stop:
  pg_ctl -D deps/pgdata stop
```

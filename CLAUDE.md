# CLAUDE.md

## Project

pg_duckpipe: PostgreSQL CDC extension — syncs heap tables to pg_ducklake columnar tables for HTAP.

## Build & Test

```bash
make installcheck               # Build + install + run all 25 regression tests
make check-regression TEST=api  # Run a single test
```

Tests use a temporary PG instance on port 5555 (wal_level=logical). See `test/regression/schedule`.

## Workspace

```
duckpipe-core/     # Shared engine: decoder, DuckDB flush, streaming replication, metadata, connstr
duckpipe-pg/       # PG extension (pgrx): GUCs, SQL API, bgworker, remote group DDL
duckpipe-daemon/   # Standalone daemon (TCP, clap CLI)
test/regression/   # SQL regression tests (25 tests)
```

## Architecture

```
Heap Tables → WAL → Replication Slot (pgoutput) → Decoder → FlushCoordinator → DuckLake
```

- **WAL consumer** (main thread): streaming replication via `START_REPLICATION`, pushes decoded changes to per-table shared queues
- **Flush threads** (per-table OS threads): self-trigger on queue size or time interval, own DuckDB connection + tokio runtime, handle PG metadata updates independently
- **Backpressure**: AtomicI64 counter pauses WAL consumer when queues exceed `max_queued_changes`
- **Crash safety**: `confirmed_lsn = min(applied_lsn)` — slot never advances past durably flushed data
- **State machine** (per table): PENDING → SNAPSHOT → CATCHUP → STREAMING (or ERRORED with auto-retry)

## Key Rules

- Source tables must have PRIMARY KEY
- Target auto-created as `{table}_ducklake` via `LIKE source USING ducklake` (local groups) or explicit DDL from remote introspection (remote groups)
- `add_table()` auto-starts the bgworker
- TRUNCATE uses per-table drain before DELETE (DuckLake ignores TRUNCATE)

## Documentation

| Doc | Purpose |
|-----|---------|
| `doc/USAGE.md` | User-facing: SQL API, monitoring, GUCs, tuning |
| `doc/REMOTE_SYNC.md` | Remote sync: connection strings, TLS, architecture |
| `doc/CODE_WALKTHROUGH.md` | Developer-facing: detailed code walkthrough |
| `doc/DESIGN_V2.md` | Historical: original v2 architecture design |
| `PROGRESS.md` | Implementation progress: done/todo checklist + detailed phase history |

## Dev Guidelines

- **TDD**: failing test first → fix → `make installcheck` (all must pass)
- **Format before commit**: always run `cargo fmt` before committing (CI enforces `cargo fmt --check`)
- **Docs**: update `CLAUDE.md`, `doc/CODE_WALKTHROUGH.md`, `PROGRESS.md` after major changes

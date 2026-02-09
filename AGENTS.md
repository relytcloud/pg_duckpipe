# AGENTS.md

This file provides guidance for coding agents working in this repository.

## Project Overview

`pg_duckpipe` is a PostgreSQL extension for CDC synchronization from heap tables (row store) to `pg_ducklake` tables (column store), enabling HTAP in one PostgreSQL instance.

Core design choice:
- Use PostgreSQL production logical decoding (`pgoutput`).
- Reuse built-in `logicalrep_read_*` parsers instead of custom wire parsing.

## Repository Layout

- `src/`: extension C source code.
- `sql/`: extension install SQL (`pg_duckpipe--1.0.sql`).
- `doc/`: architecture and design docs (`doc/DESIGN.md`).
- `test/regression/`: pg_regress tests (`sql/`, `expected/`, `schedule`, `regression.conf`).
- `benchmark/`: standalone benchmark harness and DB helpers.

## Build and Test Commands

- `make`: build extension with PGXS.
- `make install`: install extension into configured PostgreSQL.
- `make installcheck`: build + install + full regression run.
- `make check-regression`: regression tests only.
- `make check-regression TEST=api`: run one regression test.
- `make clean-regression`: remove regression artifacts.
- `make format`: format C files with `clang-format`.

## Architecture Map

Data flow:

`Heap tables -> WAL -> logical slot (pgoutput) -> pg_duckpipe worker -> DuckLake tables`

Key source files:
- `src/pg_duckpipe.h`: shared types, enums, function declarations.
- `src/pg_duckpipe.c`: `_PG_init`, GUC registration.
- `src/worker.c`: worker lifecycle, snapshot pass, WAL polling/apply loop.
- `src/decoder.c`: logical message decoding (`logicalrep_read_*`), per-message routing.
- `src/batch.c`: table-level change batching and flush.
- `src/apply.c`: SQL generation/execution against DuckLake targets.
- `src/api.c`: SQL API functions and monitoring SRFs.

## Sync Semantics

Table state machine:
- `PENDING -> SNAPSHOT -> CATCHUP -> STREAMING` (when `copy_data=true`)
- `PENDING -> STREAMING` (when `copy_data=false`)

Target table behavior:
- `duckpipe.add_table()` auto-creates target as:
  - `CREATE TABLE IF NOT EXISTS ... (LIKE source) USING ducklake`
- Default target name is `{source_table}_ducklake` in same schema unless overridden.

Update handling:
- `UPDATE` events are applied as `DELETE + INSERT`.

## GUCs

- `duckpipe.poll_interval` (ms)
- `duckpipe.batch_size_per_table`
- `duckpipe.batch_size_per_group`
- `duckpipe.enabled`

## Coding Conventions

- Language/style: PostgreSQL extension C style.
- Indentation: tabs + K&R braces; keep consistent with nearby code.
- Naming:
  - functions/variables: `snake_case`
  - constants/enums/macros: `ALL_CAPS` (e.g., `SYNC_STATE_STREAMING`)
- Keep changes minimal and scoped to the root cause.
- Run `make format` when C code changes.

## Testing Conventions

- Regression SQL lives in `test/regression/sql/`.
- Expected output lives in `test/regression/expected/`.
- SQL and expected file names should match (`foo.sql` <-> `foo.out`).
- Prefer targeted test runs while iterating, then broader run before finishing.

Typical targeted runs:
- `make check-regression TEST=streaming`
- `make check-regression TEST=api`

## Environment Requirements

- PostgreSQL 14+.
- `pg_duckdb` must be available (dependency of `pg_duckpipe`).
- Logical replication settings must be enabled (`wal_level=logical`, slots/senders configured).
- Regression config is in `test/regression/regression.conf` (default temp port `5555`).

## Commit and PR Guidance

- Commit messages: short, lowercase, descriptive.
- PRs should include:
  - clear summary of behavior change
  - tests run (commands)
  - doc updates if SQL/API behavior changed
  - mention which regression cases cover the change


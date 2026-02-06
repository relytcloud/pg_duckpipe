# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

pg_duckpipe is a PostgreSQL extension that provides automatic CDC (Change Data Capture) synchronization from heap tables (row store) to pg_ducklake columnar tables (column store). It enables HTAP (Hybrid Transactional/Analytical Processing) within a single PostgreSQL instance.

**Key design principle**: Reuse PostgreSQL's production `pgoutput` plugin and built-in `logicalrep_read_*` parsing functions rather than implementing custom decoding.

## Build Commands

```bash
make                            # Build the extension
make install                    # Install to PostgreSQL
make installcheck               # Build + install + run regression tests
make check-regression           # Run regression tests only
make check-regression TEST=api  # Run a single test
make clean-regression           # Remove test artifacts
make format                     # Format C code with clang-format
```

Tests run on a temporary PostgreSQL instance (port 5555) with special config (wal_level=logical, shared_preload_libraries). Test infrastructure lives in `test/regression/` with its own Makefile.

## Architecture

### Data Flow
```
Heap Tables → WAL → Logical Decoding Slot (pgoutput) → Background Worker → DuckLake Tables
```

### Source Files (src/)

- **pg_duckpipe.h**: Header with SyncState enum, all data structures, function declarations
- **pg_duckpipe.c**: Extension entry point (_PG_init), GUC parameter definitions
- **worker.c**: Background worker main loop, snapshot processing, WAL consumption, checkpointing
- **decoder.c**: Message dispatcher - calls PostgreSQL's `logicalrep_read_*` functions to parse binary pgoutput messages, handles CATCHUP skip logic, caches table mappings
- **batch.c**: Accumulates changes per table into batches, proper memory cleanup of nested SyncChange lists
- **apply.c**: Generates and executes SQL to apply batched changes to DuckLake tables (DELETE then INSERT for UPDATEs)
- **api.c**: SQL function implementations (add_table, remove_table, create_group, start_worker, etc.) and SRF monitoring views (groups, tables, status)

### Key Data Structures (pg_duckpipe.h)

- **SyncState** (enum): SYNC_STATE_PENDING, SYNC_STATE_SNAPSHOT, SYNC_STATE_CATCHUP, SYNC_STATE_STREAMING
- **SyncGroup**: Represents a publication + replication slot pair (multiple tables share one group)
- **TableMapping**: Maps source table to target DuckLake table with SyncState and snapshot_lsn
- **SyncBatch**: Accumulated changes for one target table (hash key: schema.table)
- **RelationCacheEntry**: Caches schema info from RELATION messages + cached TableMapping pointer

### Table State Machine

When adding a new table with copy_data=true: SNAPSHOT (copy data, record WAL LSN) → CATCHUP (skip WAL changes ≤ snapshot_lsn) → STREAMING (normal operation)

When adding with copy_data=false: directly STREAMING

## GUC Parameters

```sql
duckpipe.poll_interval          -- ms between polls (default 1000)
duckpipe.batch_size_per_table   -- changes per table per flush (default 1000)
duckpipe.batch_size_per_group   -- total changes per group per round (default 10000)
duckpipe.enabled                -- enable/disable worker (default on)
```

## Test Files

Tests are in `test/regression/sql/` with expected output in `test/regression/expected/`:
- **api.sql**: Group/table management API tests
- **monitoring.sql**: groups()/tables()/status() SRF tests
- **streaming.sql**: INSERT/UPDATE/DELETE CDC synchronization
- **snapshot_updates.sql**: Initial copy and concurrent updates
- **multiple_tables.sql**: Multiple tables in same sync group
- **data_types.sql**: Various PostgreSQL data types
- **resync.sql**: resync_table() functionality
- **truncate.sql**: TRUNCATE propagation

## PostgreSQL Internals Used

This extension heavily uses PostgreSQL logical replication internals from `replication/logicalproto.h`:
- `logicalrep_read_begin()`, `logicalrep_read_commit()`
- `logicalrep_read_rel()`, `logicalrep_read_insert()`, `logicalrep_read_update()`, `logicalrep_read_delete()`
- `logicalrep_read_truncate()`
- `LogicalRepRelation`, `LogicalRepTupleData`, `LogicalRepBeginData`, `LogicalRepCommitData`

Changes are fetched via SPI calling `pg_logical_slot_get_binary_changes()`.

## Requirements

- PostgreSQL 14+ (uses `logicalrep_read_*` API)
- pg_duckdb extension must be installed (pg_duckpipe depends on it via control file)
- Source tables must have PRIMARY KEY
- PostgreSQL config: `wal_level=logical`, extension in `shared_preload_libraries`

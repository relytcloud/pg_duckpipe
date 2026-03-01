<div align="center">

# pg_duckpipe

PostgreSQL extension for real-time CDC to pg_ducklake

[![dockerhub](https://img.shields.io/docker/pulls/pgducklake/pgduckpipe?logo=docker)](https://hub.docker.com/r/pgducklake/pgduckpipe)
[![License](https://img.shields.io/badge/License-MIT-blue)](https://github.com/YuweiXIAO/pg_duckpipe/blob/main/LICENSE)

</div>

## Overview

`pg_duckpipe` brings real-time change data capture (CDC) to PostgreSQL, enabling HTAP by continuously syncing your row tables to [DuckLake](https://github.com/relytcloud/pg_ducklake/) columnar tables. Run transactional and analytical workloads in a single database.

![pg_duckpipe architecture](images/arch.png)

## Key Features

- **One-command setup**: `duckpipe.add_table()` creates the columnar target and starts syncing automatically
- **Real-time CDC**: streams WAL changes continuously with ~5 s default flush latency (tunable)
- **Sync groups**: group multiple tables to share a single publication and replication slot

## Quick Start

```bash
# Start PostgreSQL with pg_duckpipe (includes pg_duckdb + pg_ducklake)
docker run -d --name duckpipe \
  -p 15432:5432 \
  -e POSTGRES_PASSWORD=duckdb \
  pgducklake/pgduckpipe:18-main

# Connect
PGPASSWORD=duckdb psql -h localhost -p 15432 -U postgres
```

Create a table, add it to sync, insert some rows, and query the columnar copy:

```sql
-- Create a source table (must have a primary key)
CREATE TABLE orders (id BIGSERIAL PRIMARY KEY, customer_id BIGINT, total INT);

-- Insert some existing data
INSERT INTO orders(customer_id, total) VALUES (101, 4250), (102, 9900);

-- Start syncing to DuckLake (snapshots existing rows, then streams new changes)
SELECT duckpipe.add_table('public.orders');

-- New writes are captured in real time
INSERT INTO orders(customer_id, total) VALUES (103, 1575);

-- Query the columnar copy (wait a few seconds for CDC to flush)
SELECT * FROM orders_ducklake;

-- Monitor sync state
SELECT source_table, state, rows_synced FROM duckpipe.status();
```

## Requirements

> The Docker image ships everything preconfigured. The notes below apply when installing from source.

- **PostgreSQL 18** — currently the only tested version; older versions may work but are unsupported
- **pg_ducklake** (which bundles pg_duckdb and libduckdb) must be installed
- `wal_level = logical` with sufficient `max_replication_slots` and `max_wal_senders`
- Both extensions must be preloaded: `shared_preload_libraries = 'pg_duckdb, pg_duckpipe'`
- Source tables must have a **PRIMARY KEY** (required by logical replication to identify rows)

## Benchmark

Sysbench results on Apple M1 Pro / 100k rows per table / 30s OLTP phase.
Mixed DML uses `oltp_read_write` (2 UPDATEs + 1 DELETE + 1 INSERT per txn on 100k-row base tables):

| Scenario | Tables | Workload | Snapshot | OLTP TPS | Avg Lag | Catch-up | Consistency |
|----------|--------|----------|----------|----------|---------|----------|-------------|
| Single-table append | 1 | `oltp_insert` | 41,668 rows/s | 9,495 | 3.3 MB | 2.2 s | PASS |
| Multi-table append | 4 | `oltp_insert` | 7,155 rows/s | 9,328 | 64.0 MB | 2.4 s | PASS |
| Single-table mixed DML | 1 | `oltp_read_write` | 8,160 rows/s | 667 | 170.5 MB | 65.0 s | PASS |
| Multi-table mixed DML | 4 | `oltp_read_write` | 7,544 rows/s | 477 | 377.3 MB | 68.4 s | PASS |

<details>
<summary>Flush performance breakdown</summary>

| Metric | 1 table insert | 4 tables insert | 1 table mixed | 4 tables mixed |
|--------|----------------|-----------------|----------------|----------------|
| Flush count | 30 | 121 | 30 | 122 |
| Avg latency (ms) | 34.4 | 19.8 | 31.2 | 26.9 |
| P99 latency (ms) | 71.6 | 144.1 | 43.8 | 127.8 |
| Avg rows/flush | 9,496 | 2,313 | 4,002 | 704 |

Phase breakdown (avg ms):

| Phase | 1 table insert | 4 tables insert | 1 table mixed | 4 tables mixed |
|-------|----------------|-----------------|----------------|----------------|
| load | 16.6 | 4.6 | 7.2 | 1.8 |
| compact | 5.1 | 3.5 | 3.8 | 3.3 |
| delete | 0.3 | 0.3 | 15.5 | 8.4 |
| insert | 8.5 | 4.4 | 2.0 | 1.6 |
| commit | 2.2 | 5.5 | 1.7 | 10.9 |

</details>

Run benchmarks yourself:

```bash
./benchmark/bench_suite.sh              # All 4 scenarios (30s each)
./benchmark/bench_suite.sh --duration 10  # Quick smoke test
cat benchmark/results/report.md         # View the generated report
```

## Build & Test

```bash
make && make install             # Build and install the extension
make installcheck                # Run all regression tests
make check-regression TEST=api   # Run a single test
```

## Documentation

| Doc | Description |
|-----|-------------|
| [doc/QUICKSTART.md](doc/QUICKSTART.md) | Hands-on walkthrough: add, remove, re-add, resync, multi-table, monitoring |
| [doc/USAGE.md](doc/USAGE.md) | SQL usage, monitoring, **configuration (GUCs)**, and tuning |
| [doc/DESIGN_V2.md](doc/DESIGN_V2.md) | Historical v2 design notes |
| [benchmark/README.md](benchmark/README.md) | Benchmark harness |

## License

[MIT](LICENSE)

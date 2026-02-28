# Benchmark

Measures pg_duckpipe CDC throughput using [sysbench](https://github.com/akopytov/sysbench) workloads.

## Quick Start

```bash
# Prerequisites: sysbench, PostgreSQL 18 with pg_duckpipe installed
brew install sysbench   # or: apt install sysbench

# Run the default benchmark (1 thread, 30 s, 1 table x 100k rows, append-only)
./benchmark/bench.sh

# Customize
./benchmark/bench.sh --threads 4 --duration 60 --table-size 100000
```

`bench.sh` starts a temporary PostgreSQL instance (port 5556), runs the benchmark, and prints results. No cleanup needed.

## Workloads

| Workload | Description | Flag |
|----------|-------------|------|
| `oltp_insert` | Append-only inserts (default) | `--workload oltp_insert` |
| `oltp_read_write` | Mixed INSERT/UPDATE/DELETE | `--workload oltp_read_write` |

## What It Measures

1. **Snapshot throughput**: time to sync the initial table data to DuckLake
2. **Streaming throughput**: OLTP TPS during continuous CDC
3. **Replication lag**: average and peak WAL lag during the OLTP phase
4. **Catch-up time**: how long after OLTP stops until DuckLake is fully caught up
5. **Consistency check**: verifies source and target data match after sync

## Manual Steps

If you prefer to control each step:

```bash
# Start a temporary PostgreSQL instance on port 5556
./benchmark/start_db.sh

# Run benchmark (default: oltp_insert)
python3 benchmark/run_sysbench.py \
  --db-url "host=localhost port=5556 user=postgres dbname=postgres"

# Mixed workload with custom parameters
python3 benchmark/run_sysbench.py \
  --workload oltp_read_write \
  --tables 4 --table-size 50000 \
  --threads 8 --duration 60 \
  --db-url "host=localhost port=5556 user=postgres dbname=postgres"

# Stop
./benchmark/stop_db.sh
```

### Consistency modes

Control post-run data verification with `--consistency-mode`:

| Mode | Behavior |
|------|----------|
| `auto` (default) | `safe` for append-only, `full` for mixed |
| `safe` | Row count comparison only |
| `full` | Row-by-row value comparison |
| `off` | Skip verification |

## Sample Results

**Environment**: MacBook Pro M1, PostgreSQL 18.1

**Config**: `flush_batch_threshold=10000`, `data_inlining_row_limit=1000`, `poll_interval=10000`

**Workload**: `oltp_insert`, 1 thread, 30 s, 1 table x 100k rows

```
 Snapshot Throughput  : 41929 rows/sec
 OLTP Throughput      : 8827.55 TPS
 Avg Replication Lag  : 2.9 MB
 Peak Replication Lag : 4.2 MB
 Catch-up Time        : 2.2 sec
 Consistency          : PASS
```
# Benchmark

Measures pg_duckpipe CDC throughput using [sysbench](https://github.com/akopytov/sysbench) workloads.

## Quick Start

```bash
# Prerequisites: sysbench, PostgreSQL 18 with pg_duckpipe installed
brew install sysbench   # or: apt install sysbench

# Run the default benchmark (1 thread, 30 s, 1 table x 100k rows, append-only)
./benchmark/bench.sh

# Customize
./benchmark/bench.sh --threads 4 --duration 60 --table-size 100000 --workload oltp_read_write
```

`bench.sh` starts a temporary PostgreSQL instance (port 5556), runs the benchmark, and prints results. No cleanup needed.

## Benchmark Suite

Run all 4 benchmark scenarios with a single command and get an automated analysis report:

```bash
# Full suite (30s per scenario, ~5 min total)
./benchmark/bench_suite.sh

# Quick smoke test (15s per scenario)
./benchmark/bench_suite.sh --duration 15

# View the generated report
cat benchmark/results/report.md
```

### Scenarios

| # | Name | Tables | Workload | Evaluates |
|---|------|--------|----------|-----------|
| 1 | Single-table append | 1 x 100k | `oltp_insert` | Baseline throughput |
| 2 | Multi-table append | 4 x 100k | `oltp_insert` | Parallel flush scaling |
| 3 | Single-table mixed DML | 1 x 100k | `oltp_read_write` | UPDATE/DELETE correctness |
| 4 | Multi-table mixed DML | 4 x 100k | `oltp_read_write` | Combined stress |

### Analysis Report

The suite automatically generates `benchmark/results/report.md` containing:

1. **Summary Table** — all scenarios side-by-side (snapshot rate, TPS, lag, catch-up, consistency)
2. **Flush Performance** — per-scenario flush stats (count, avg/p50/p99 latency, rows/flush, phase breakdown)
3. **Snapshot Timings** — per-table snapshot durations from PG logs
4. **WAL Processing** — WAL cycle time statistics
5. **Issues & Observations** — errors, warnings, backpressure events, anomalies

You can also run the analysis standalone on existing logs:

```bash
python3 benchmark/analyze_results.py --results-dir benchmark/results
```

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

## Duration

Always use `--duration 30` (the default) or longer for results committed to `benchmark/results/report.md`.
Shorter durations (e.g. 15s) are fine for smoke tests but produce unstable numbers —
fewer flush cycles mean higher variance in latency percentiles and throughput measurements.

## Sample Results

See `benchmark/results/report.md` for the latest full suite results.
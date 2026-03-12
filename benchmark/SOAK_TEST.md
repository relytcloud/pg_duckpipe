# pg_duckpipe Soak Test Framework

## What Is This?

A long-running test harness that continuously hammers pg_duckpipe with database writes, monitors the CDC pipeline health, and validates that every row makes it from PostgreSQL to DuckLake intact.

Unlike the existing `bench_suite.sh` (30-second runs measuring peak throughput), soak tests run for **hours** to catch problems that only surface over time: memory leaks, WAL slot growth, throughput degradation, and crash recovery bugs.

## Quick Start

```bash
# One command — starts PG + soak test, results land in benchmark/soak/soak_results/
docker compose -f benchmark/soak/docker-compose.soak.yml up

# Short smoke test (2 minutes)
DURATION=120 docker compose -f benchmark/soak/docker-compose.soak.yml up --build

# Pick a scenario
SCENARIO=error-recovery DURATION=300 docker compose -f benchmark/soak/docker-compose.soak.yml up

# Teardown
docker compose -f benchmark/soak/docker-compose.soak.yml down -v
```

## How It Works

```
┌─────────────────────────────────────────────────────────┐
│                    soak_test.py                          │
│                                                         │
│  ┌──────────────┐   sysbench subprocess                 │
│  │ Main Thread   │──► generates INSERT/UPDATE/DELETE     │
│  └──────────────┘    load against PG source tables      │
│                                                         │
│  ┌──────────────┐   every 5s: query duckpipe.status(),  │
│  │ Monitor       │──► worker_status(), WAL lag           │
│  │ Thread        │   writes row to metrics.csv           │
│  └──────────────┘                                       │
│                                                         │
│  ┌──────────────┐   every 5min: count(*) source table   │
│  │ Consistency   │──► vs count(*) ducklake target        │
│  │ Thread        │   records PASS/FAIL                   │
│  └──────────────┘                                       │
│                                                         │
│  ┌──────────────┐   every 2min (if enabled):            │
│  │ Chaos         │──► kill worker, remove tables, etc.   │
│  │ Thread        │                                       │
│  └──────────────┘                                       │
│                                                         │
│  ┌──────────────┐   every 1s: refresh terminal with     │
│  │ Display       │──► TPS, lag, table states, events     │
│  │ Thread        │                                       │
│  └──────────────┘                                       │
└─────────────────────────────────────────────────────────┘
```

**Lifecycle:**

1. **Prepare** — create extensions, run `sysbench prepare` (seed tables), add tables to duckpipe, wait for initial snapshot to complete (all tables STREAMING)
2. **Run** — start sysbench + all monitoring threads, display live dashboard
3. **Shutdown** — stop sysbench, wait for queued changes to drain to 0, run final consistency check
4. **Report** — generate `report.md` with aggregates, stability analysis, and PASS/FAIL verdict

## Test Scenarios

### 1. `sustained-insert` (default)

| | |
|---|---|
| **Workload** | INSERT only, 4 tables, 4 threads |
| **Chaos** | None |
| **Tests for** | Baseline stability under steady append load |

The simplest scenario. Catches: WAL slot leaks (retained WAL keeps growing), throughput degradation as tables get larger, memory leaks in flush queues.

---

### 2. `sustained-mixed`

| | |
|---|---|
| **Workload** | INSERT + UPDATE + DELETE, 4 tables, 4 threads |
| **Chaos** | None |
| **Tests for** | Mixed DML correctness over time |

Adds UPDATEs and DELETEs to the mix. The decoder must correctly handle all three DML types. The final consistency check is harder here — DELETEs reduce row counts and UPDATEs change data in-place. A bug in DELETE handling (e.g., TRUNCATE drain not working) would show up here but not in `sustained-insert`.

---

### 3. `multi-table-insert`

| | |
|---|---|
| **Workload** | INSERT only, 8 tables, 8 threads |
| **Chaos** | None |
| **Tests for** | Flush scaling and commit contention |

Doubles the table/thread count. 8 flush threads compete for DuckDB connections, PG metadata locks, and shared WAL consumer queues. Surfaces: deadlocks between flush threads, `confirmed_lsn` not advancing because one slow table holds back the minimum, backpressure triggering too aggressively.

---

### 4. `multi-table-mixed`

| | |
|---|---|
| **Workload** | INSERT + UPDATE + DELETE, 8 tables, 8 threads |
| **Chaos** | None |
| **Tests for** | Combined stress — high concurrency + mixed DML |

The hardest non-chaos scenario. Worst-case realistic workload for finding edge cases in the flush coordinator under contention.

---

### 5. `table-lifecycle`

| | |
|---|---|
| **Workload** | INSERT only, 4 tables, 4 threads |
| **Chaos** | Every 2 min: remove a random table, wait 10s, re-add it |
| **Tests for** | Dynamic table add/remove during active load |

What the chaos does:
```
Pick random table (e.g., sbtest2)
  → duckpipe.remove_table('public.sbtest2', true)   -- drops mapping + target
  → wait 10s (sysbench still inserting into sbtest2)
  → duckpipe.add_table('public.sbtest2')             -- re-add, fresh snapshot
  → wait for sbtest2 → STREAMING
```

Tests the table state machine re-initialization: new snapshot taken, old target dropped, WAL consumer handles changes for a table mid-snapshot, `confirmed_lsn` calculated correctly across mixed table states. A bug could leave orphaned slots, leak WAL, or produce duplicated/missing rows.

---

### 6. `error-recovery`

| | |
|---|---|
| **Workload** | INSERT only, 4 tables, 4 threads |
| **Chaos** | Every 2 min: stop worker, wait 5s, restart worker |
| **Tests for** | Worker crash recovery and WAL resumption |

What the chaos does:
```
duckpipe.stop_worker()      -- kills the bgworker process
  → wait 5s (WAL accumulates, sysbench keeps inserting)
duckpipe.start_worker()     -- restarts bgworker
  → wait for all tables → STREAMING
```

Simulates OOM/crash. After restart, the worker must resume from `confirmed_lsn` (not replay already-flushed data), reinitialize all flush threads, consume accumulated WAL without data loss, and the replication slot must still be valid. If `confirmed_lsn` is advanced too far → missing rows. Not far enough → duplicated rows.

## What Gets Measured

Every 5 seconds, the monitor thread records:

| Metric | What it means |
|--------|---------------|
| `sysbench_tps` | Transactions/sec hitting PG |
| `sync_rate_rows_s` | Rows/sec flowing through to DuckLake |
| `wal_lag_bytes` | How far behind the pipeline is (`current_wal_lsn - confirmed_lsn`) |
| `queued_changes` | Changes buffered in flush coordinator queues |
| `is_backpressured` | Whether the WAL consumer is paused (queues full) |
| `slot_retained_wal_bytes` | Total WAL retained by duckpipe replication slots |
| `tables_streaming` | How many tables are in STREAMING state |
| `tables_errored` | How many tables are in ERRORED state |

## Pass/Fail Criteria

The report generator (`soak_report.py`) applies these rules:

| Criterion | Threshold | Why |
|-----------|-----------|-----|
| Throughput CV | < 30% | Sync rate should be stable, not wildly fluctuating |
| Permanent errors | None | No tables stuck in ERRORED at end of test |
| Final consistency | PASS | Source and target row counts must match after catch-up |
| Slot growth | < 1 MB/hr (for tests > 1hr) | Retained WAL shouldn't grow unboundedly |

## Output Files

Results land in `benchmark/soak/soak_results/<scenario>_<timestamp>/`:

```
metrics.csv   — one row per poll interval (every 5s), all metrics above
events.log    — timestamped log of starts, stops, chaos events, consistency checks
config.txt    — scenario parameters used for this run
report.md     — generated summary with aggregates, stability analysis, verdict
```

## File Map

```
benchmark/
  lib.py                          — Shared utilities (SQL execution, monitoring queries, consistency checks)
  soak/
    soak_test.py                  — Main orchestrator (threads, scenarios, CLI, shutdown)
    soak_report.py                — Post-mortem report generator (reads CSV → markdown)
    soak_monitor.sh               — Standalone terminal dashboard (pure shell, no Python)
    Dockerfile.bench              — Container image for the bench client
    docker-compose.soak.yml       — One-command launch: DB + bench containers
```

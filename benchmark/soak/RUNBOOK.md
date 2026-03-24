# Soak Test Runbook (for AI agents)

Step-by-step instructions for running pg_duckpipe soak tests locally or on a remote server.

There are **two modes**: bgworker (PG extension) and daemon (standalone process). Both share the same scenarios and result format.

## Prerequisites

- Docker and Docker Compose installed
- For remote: SSH access (`ssh user@<host>`)
- Both modes build images from source (no pre-built image dependency)

## Configuration

### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SCENARIO` | `sustained-insert` | See scenarios below |
| `DURATION` | `3600` | Test duration in seconds |
| `PORT` | `5432` | PostgreSQL port on host |
| `PASSWORD` | `duckdb` | PostgreSQL password |
| `API_PORT` | `8080` | Daemon REST API port (daemon mode only) |

### Scenarios

| Name | What it tests |
|------|---------------|
| `sustained-insert` | Baseline: INSERT-only, 4 tables |
| `sustained-mixed` | 90% INSERT + 10% UPDATE, 4 tables |
| `multi-table-insert` | INSERT-only, 8 tables — flush scaling |
| `multi-table-mixed` | Mixed DML, 8 tables — combined stress |
| `table-lifecycle` | INSERT + chaos: remove/re-add tables every 2 min |
| `error-recovery` | INSERT + chaos: stop/start worker every 2 min |

### Recommended durations

| Purpose | Duration |
|---------|----------|
| Smoke test | `DURATION=120` (2 min) |
| Standard | `DURATION=3600` (1 hr) |
| Full soak | `DURATION=14400` (4 hr) |

---

## Mode 1: BGWorker (PG Extension)

Uses `docker-compose.soak.yml`. The sync engine runs as a PG background worker inside the database.

### Compose file

`benchmark/soak/docker-compose.soak.yml`

### Containers

| Container | Image | Role |
|-----------|-------|------|
| `db` | `pgducklake/pgduckpipe:18-main` | PostgreSQL + pg_duckdb + pg_ducklake + pg_duckpipe |
| `bench` | Built from `Dockerfile.bench` | Sysbench load generator + monitor |

### Run

```bash
SCENARIO=sustained-insert DURATION=3600 \
  docker compose -f benchmark/soak/docker-compose.soak.yml up --build
```

### Teardown

```bash
docker compose -f benchmark/soak/docker-compose.soak.yml down -v
```

---

## Mode 2: Daemon (Standalone Process)

Uses `docker-compose.soak-daemon.yml`. The sync engine runs as a standalone daemon process connecting to PG over TCP.

### Compose file

`benchmark/soak/docker-compose.soak-daemon.yml`

### Containers

| Container | Image | Role |
|-----------|-------|------|
| `db` | `pgducklake/pgduckpipe:18-main` | PostgreSQL + pg_duckdb + pg_ducklake + pg_duckpipe |
| `daemon` | Built from `docker/Dockerfile.daemon` | Standalone duckpipe sync daemon |
| `bench` | Built from `Dockerfile.bench` | Sysbench load generator + monitor |

### How it works

1. `db` starts; init script (`Z01-install-pg_duckpipe.sql`) creates the pg_duckpipe extension which auto-creates a "default" sync group
2. `daemon` starts with `--group default`, pre-binds to the existing group, sets it to daemon mode
3. `bench` starts, creates sysbench tables, adds them to duckpipe using a **split-transaction method** (see Troubleshooting), waits for STREAMING state, then runs the load test
4. `bench` monitors via daemon REST API (`GET /status`, `GET /metrics`) plus direct SQL for WAL lag

### Run

```bash
SCENARIO=sustained-mixed DURATION=3600 \
  docker compose -f benchmark/soak/docker-compose.soak-daemon.yml up --build
```

### Run detached (background)

```bash
SCENARIO=sustained-mixed DURATION=3600 \
  docker compose -f benchmark/soak/docker-compose.soak-daemon.yml up -d --build

# Follow bench output
docker compose -f benchmark/soak/docker-compose.soak-daemon.yml logs -f bench
```

### Teardown

```bash
docker compose -f benchmark/soak/docker-compose.soak-daemon.yml down -v
```

---

## Remote Server Run

Both modes work the same way on a remote server. The entire repo must be copied because images are built from source.

### 1. Copy repo to server

```bash
# Create tarball (excludes build artifacts)
tar czf /tmp/pg_duckpipe.tar.gz --exclude=target --exclude=.git --exclude='*.o' --exclude='*.so' .

# Copy and extract
scp /tmp/pg_duckpipe.tar.gz user@host:/tmp/
ssh user@host 'mkdir -p /tmp/pg_duckpipe && cd /tmp/pg_duckpipe && tar xzf /tmp/pg_duckpipe.tar.gz'
```

### 2. Run (daemon mode example)

```bash
ssh user@host 'cd /tmp/pg_duckpipe && \
  SCENARIO=sustained-mixed DURATION=3600 \
  docker compose -f benchmark/soak/docker-compose.soak-daemon.yml up -d --build'
```

Note: First build takes 5-15 minutes (compiles Rust). Subsequent rebuilds use Docker cache.

### 3. Monitor

```bash
# Bench output (live dashboard)
ssh user@host 'cd /tmp/pg_duckpipe && \
  docker compose -f benchmark/soak/docker-compose.soak-daemon.yml logs --tail=30 bench'

# Daemon flush logs
ssh user@host 'cd /tmp/pg_duckpipe && \
  docker compose -f benchmark/soak/docker-compose.soak-daemon.yml logs --tail=10 daemon'

# Container CPU/memory
ssh user@host 'docker stats --no-stream'

# Host memory and disk
ssh user@host 'free -h && df -h /'
```

### 4. Collect results

```bash
scp -r user@host:/tmp/pg_duckpipe/benchmark/soak/soak_results/ ./soak_results/
```

### 5. Teardown

```bash
ssh user@host 'cd /tmp/pg_duckpipe && \
  docker compose -f benchmark/soak/docker-compose.soak-daemon.yml down -v'
```

---

## Monitoring During Test

The bench container outputs a live dashboard every 5 seconds:

```
================================================================
  pg_duckpipe Soak Test  --  00:10:25 / 01:00:00
  Scenario: sustained-mixed   Sysbench TPS: ~480
================================================================
-- Pipeline --
  WAL Lag: 10.8 MB   Queued: 0   Backpressure: No
  Slot Retained WAL: 13.0 MB

-- Tables --
  public.sbtest1       STREAMING    rows=   931,394  queued=     0  errs=0

-- Throughput --
  Sync: 6,140 rows/s (5s)   Avg: 6,006 rows/s
  Peak lag: 74.7 MB   Avg lag: 12.5 MB
  DuckDB mem: 37.2 MB   Flushes: 468 (0.8/s)   Avg flush: 286 ms
```

### Key metrics to track over time

| Metric | What to watch | Warning sign |
|--------|---------------|--------------|
| `DuckDB mem` | Should stay bounded | Growing past `duckdb_flush_memory_mb` (default 512 MB) |
| `Avg flush` | Flush duration | Steadily increasing (DuckLake file proliferation from updates) |
| `WAL Lag` | Should stay bounded | Unbounded growth means pipeline can't keep up |
| `Slot Retained WAL` | Should stabilize | Growth means retained WAL is leaking |
| `Queued` | Pending changes | Large buildup means flush is slower than ingest |
| `Backpressure` | WAL consumer paused | If stuck on "Yes", flush threads are blocked |
| `errs` | Per-table error count | Non-zero outside chaos events |
| Container CPU % | Per-container CPU usage | Sustained 100%+ on single-core hosts |
| Container RSS | Per-container memory | Daemon RSS exceeding `duckdb_flush_memory_mb` + overhead |
| Host `available` memory | Free memory on host | Approaching zero means OOM risk |

### Reporting to the user

While the test is running, **collect and report an aggregated metrics table to the user every 5 minutes**. Use these commands to gather data:

```bash
# Pipeline metrics (from bench dashboard)
docker compose -f <compose-file> logs --tail=30 bench

# System resources (per-container CPU% and RSS)
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}"

# Host memory
free -h
```

Present a cumulative table like this each time:

| Metric | 5 min | 10 min | 15 min | ... |
|--------|-------|--------|--------|-----|
| DuckDB memory | | | | |
| Avg flush duration | | | | |
| Flush rate (/s) | | | | |
| WAL lag | | | | |
| Peak lag | | | | |
| Total rows synced | | | | |
| Sysbench TPS | | | | |
| Daemon CPU % | | | | |
| Daemon RSS | | | | |
| DB CPU % | | | | |
| DB RSS | | | | |
| Host available mem | | | | |
| Errors | | | | |

This lets the user spot trends (memory growth, flush degradation, CPU saturation) without waiting for the final report.

---

## Output Files

Each run produces `soak_results/<prefix>_<timestamp>/` containing:

| File | Contents |
|------|----------|
| `report.md` | Summary with aggregates, stability analysis, and PASS/FAIL verdict |
| `metrics.csv` | Time series (every 5s): TPS, sync rate, WAL lag, memory, flush stats |
| `events.log` | Timestamped log of starts, stops, chaos events, consistency checks |
| `config.txt` | Scenario parameters |

## Interpreting Results

Read `report.md`. The verdict is **PASS** if all criteria are met:

| Criterion | Threshold |
|-----------|-----------|
| Throughput CV | < 30% (sync rate should be stable) |
| Permanent errors | None (no tables stuck in ERRORED) |
| Final consistency | PASS (source and target row counts match after drain) |
| Slot growth | < 1 MB/hr (for tests > 1hr) |

Note: In-flight consistency checks during active workload will show FAIL (row counts differ due to lag). This is expected. Only the **final** consistency check (after workload stops and queues drain) determines the verdict.

---

## Troubleshooting

| Error | Fix |
|-------|-----|
| `add_table` fails with "Writing to DuckDB and Postgres tables in the same transaction" | Daemon mode uses `add_table_manual()` which splits DuckDB and PG writes into separate transactions |
| `drop_group` fails with "replication slot does not exist" | Don't call `drop_group()` on the auto-created default group; the daemon pre-binds to it |
| Daemon can't write to DuckLake data directory | Daemon container runs as root and shares the pgdata volume with db |

---

## Architecture Reference

### BGWorker mode
```
[bench] --sysbench--> [db: PostgreSQL + pg_duckpipe bgworker]
                           |
                           WAL --> bgworker --> DuckLake (local)
```

### Daemon mode
```
[bench] --sysbench--> [db: PostgreSQL]
                           |
                           WAL --> [daemon: duckpipe] --> DuckLake (shared volume)
                           |
[bench] --REST API--> [daemon: /status, /metrics]
```

## Notes

- Always use `-v` in teardown to remove the database volume between runs.
- The docker-compose files set `platform: linux/amd64`. Remove this line if running on an arm64 host with arm64 base images available.
- First build on a clean machine takes 5-15 minutes due to Rust compilation. Docker cache speeds up subsequent builds.
- The daemon compose requires the full repo (not just `benchmark/`) because it builds from `docker/Dockerfile.daemon`.

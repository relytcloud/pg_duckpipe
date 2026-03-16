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
| `db` | Built from `Dockerfile.db` | PostgreSQL + pg_duckdb + pg_ducklake + pg_duckpipe |
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
| `db` | Built from `Dockerfile.db` | PostgreSQL + pg_duckdb + pg_ducklake + pg_duckpipe |
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

### initdb fails with "access method ducklake does not exist"

**Cause**: `pg_ducklake` in `shared_preload_libraries` during `initdb` tries to register the "ducklake" access method before `pg_catalog` is ready.

**Fix**: `Dockerfile.db` does NOT set `shared_preload_libraries` in `postgresql.conf.sample`. Instead, docker-compose passes it via `command` override which only affects the server process, not `initdb`.

### add_table fails with "Writing to DuckDB and Postgres tables in the same transaction"

**Cause**: `duckpipe.add_table()` does `CREATE TABLE USING ducklake` (DuckDB write) + `INSERT INTO duckpipe.table_mappings` (PG write) in the same SPI transaction. Newer pg_duckdb versions reject cross-engine writes.

**Fix**: `soak_test_daemon.py` uses `add_table_manual()` which splits these into separate transactions:
1. Create replication slot + publication (PG)
2. Set REPLICA IDENTITY FULL (PG)
3. CREATE TABLE USING ducklake (DuckDB) — separate transaction
4. INSERT INTO table_mappings (PG) — separate transaction

### drop_group fails with "replication slot does not exist"

**Cause**: `CREATE EXTENSION pg_duckpipe` auto-creates a "default" group. Calling `drop_group('default')` tries to drop the replication slot which doesn't exist yet.

**Fix**: Don't call `drop_group()`. The daemon pre-binds to the auto-created group via `--group default`. If you need to remove it, use `DELETE FROM duckpipe.sync_groups WHERE name = 'default';` directly.

### Daemon can't write to DuckLake data directory

**Cause**: The daemon container runs DuckDB which needs to write Parquet files to the PG data directory owned by the `postgres` user.

**Fix**: The daemon container runs as `user: root` and shares the `daemon_pgdata` volume with the db container.

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
- The Docker images are `linux/amd64` only. On Apple Silicon, local runs use Rosetta emulation (slower but functional).
- First build on a clean machine takes 5-15 minutes due to Rust compilation. Docker cache speeds up subsequent builds.
- The daemon compose requires the full repo (not just `benchmark/`) because it builds from `docker/Dockerfile.daemon` and `benchmark/soak/Dockerfile.db`.

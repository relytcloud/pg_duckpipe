# Soak Test Runbook (for AI agents)

Step-by-step instructions for running pg_duckpipe soak tests locally or on a remote server.

## Prerequisites

- Docker and Docker Compose installed
- For remote: SSH access (`ssh user@<host>`)
- Network access to Docker Hub (`pgducklake/pgduckpipe:18-main`)

## Configuration

### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SCENARIO` | `sustained-insert` | See scenarios below |
| `DURATION` | `3600` | Test duration in seconds |
| `DB_IMAGE` | `pgducklake/pgduckpipe:18-main` | PostgreSQL Docker image |

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

## Option A: Local Run

Run directly from the repo root. Results write to `benchmark/soak/soak_results/`.

### 1. Run

```bash
SCENARIO=sustained-insert DURATION=3600 \
  docker compose -f benchmark/soak/docker-compose.soak.yml up --build
```

### 2. Monitor (from another terminal)

```bash
# Container logs
docker compose -f benchmark/soak/docker-compose.soak.yml logs -f bench

# System resource usage
docker stats --no-stream
```

### 3. Collect results

Results are already at `benchmark/soak/soak_results/<scenario>_<timestamp>/`.

### 4. Teardown

```bash
docker compose -f benchmark/soak/docker-compose.soak.yml down -v
```

---

## Option B: Remote Server Run

Use when you need a dedicated Linux machine (e.g., for long runs or amd64 platform).

### 1. Copy files to server

```bash
scp -r benchmark/ user@host:/tmp/pg_duckpipe_bench/
```

Only the `benchmark/` directory is needed. The Docker image contains pg_duckpipe.

### 2. Run

```bash
ssh user@host "cd /tmp/pg_duckpipe_bench && \
  SCENARIO=sustained-insert DURATION=3600 \
  docker compose -f benchmark/soak/docker-compose.soak.yml up --build 2>&1"
```

### 3. Monitor (from another terminal)

```bash
# Container logs
ssh user@host "docker compose -f /tmp/pg_duckpipe_bench/benchmark/soak/docker-compose.soak.yml logs -f bench"

# System resource usage
ssh user@host "docker stats --no-stream"
```

### 4. Collect results

```bash
scp -r user@host:/tmp/pg_duckpipe_bench/benchmark/soak/soak_results/ ./soak_results/
```

### 5. Teardown

```bash
ssh user@host "cd /tmp/pg_duckpipe_bench && \
  docker compose -f benchmark/soak/docker-compose.soak.yml down -v"
```

---

## Output Files

Each run produces `soak_results/<scenario>_<timestamp>/` containing:

| File | Contents |
|------|----------|
| `report.md` | Summary with aggregates, stability analysis, and PASS/FAIL verdict |
| `metrics.csv` | Time series (every 5s): TPS, sync rate, WAL lag, queued changes, slot WAL |
| `events.log` | Timestamped log of starts, stops, chaos events, consistency checks |
| `config.txt` | Scenario parameters |

## Interpreting Results

Read `report.md`. The verdict is PASS if all criteria are met:

| Criterion | Threshold |
|-----------|-----------|
| Throughput CV | < 30% (sync rate should be stable) |
| Permanent errors | None (no tables stuck in ERRORED) |
| Final consistency | PASS (source and target row counts match) |
| Slot growth | < 1 MB/hr (for tests > 1hr) |

### Key metrics to watch

- **sync_rate_rows_s**: Should remain stable. A downward trend suggests throughput degradation as tables grow.
- **wal_lag_bytes**: Should stay bounded. Unbounded growth means the pipeline can't keep up.
- **slot_retained_wal_bytes**: Should stabilize. Growth means retained WAL is leaking.
- **tables_errored**: Should be 0 except briefly during chaos events.

## Notes

- Always use `-v` in teardown to remove the database volume between runs.
- The Docker image is `linux/amd64` only. On Apple Silicon, local runs use Rosetta emulation (slower but functional).

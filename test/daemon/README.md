# Daemon E2E Tests

End-to-end tests for `duckpipe-daemon`, the standalone CDC sync binary. These tests exercise the daemon's full pipeline over TCP: WAL streaming, snapshot, flush to DuckLake.

## Quick Start

```bash
# From repo root — builds daemon binary + runs all tests
make check-daemon

# Run a single test (filename substring match)
make check-daemon TEST=snapshot

# Run both regression and daemon tests
make installcheck-all
```

## How It Works

The test runner (`run_tests.sh`) spins up a **fresh PostgreSQL instance** on port 5566 (isolated from regression tests on 5555), installs extensions, and sets the default sync group to `mode = 'daemon'` so that `add_table()` skips bgworker auto-start. It then iterates over `tests/*.sh` scripts. Each test:

1. Creates source table(s) and registers them via `duckpipe.add_table()`
2. Starts the daemon binary against the test PG instance
3. Performs DML and polls `*_ducklake` target tables for expected results
4. Cleans up via `trap EXIT` (daemon stop + table drop)

The daemon runs with fast intervals (`--poll-interval 200 --flush-interval 200 --flush-batch-threshold 100`) so tests complete in seconds.

## Tests

| Test | What it verifies |
|------|-----------------|
| `01_basic_streaming` | INSERT streaming, row values, STREAMING state |
| `02_snapshot_sync` | `copy_data=true` snapshot + streaming transition |
| `03_mixed_dml` | INSERT/UPDATE/DELETE including mixed transactions |
| `04_multiple_tables` | 3 tables syncing simultaneously, cross-table DML |
| `05_daemon_restart` | Catch-up from `confirmed_lsn` after restart |

## Writing New Tests

Create `tests/NN_descriptive_name.sh`:

```bash
#!/bin/bash
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib/helpers.sh"

TABLE="my_test_table"
test_init "$TABLE"                    # sets up trap-based cleanup

run_sql "CREATE TABLE ${TABLE} (id INT PRIMARY KEY, val TEXT);"
add_table "$TABLE"                    # registers for CDC (copy_data=false)
# add_table "$TABLE" true             # use this for snapshot mode
daemon_start                          # start daemon in background

run_sql "INSERT INTO ${TABLE} VALUES (1, 'hello');"
poll_sync "$TABLE" 1                  # wait for 1 row in ${TABLE}_ducklake
poll_query "SELECT val FROM ${TABLE}_ducklake WHERE id=1;" "hello"

echo "  my test: OK"
```

### Available Helpers (`lib/helpers.sh`)

| Function | Description |
|----------|-------------|
| `test_init TABLE...` | Set up trap-based cleanup for given tables |
| `add_table TABLE [COPY_DATA]` | Register table for CDC sync (default: `copy_data=false`) |
| `daemon_start [EXTRA_ARGS...]` | Start daemon in background with fast intervals |
| `daemon_stop` | SIGTERM + wait + SIGKILL if needed |
| `run_sql "SQL"` | Execute SQL via psql, returns unaligned output |
| `poll_sync TABLE COUNT [TIMEOUT]` | Poll until `count(*)` from `TABLE_ducklake` equals COUNT |
| `poll_query "SQL" EXPECTED [TIMEOUT]` | Poll until SQL output matches EXPECTED |
| `assert_eq ACTUAL EXPECTED [MSG]` | Assert equality (trims leading/trailing whitespace) |
| `cleanup_table TABLE` | Remove from duckpipe + drop source/target tables |

## Directory Layout

```
test/daemon/
├── README.md           # This file
├── Makefile            # check-daemon, clean-daemon targets
├── daemon.conf         # PostgreSQL config (port 5566, TCP enabled)
├── run_tests.sh        # Test runner: PG lifecycle, iterate tests, report
├── lib/
│   └── helpers.sh      # Shared helpers
├── tests/
│   ├── 01_basic_streaming.sh
│   ├── 02_snapshot_sync.sh
│   ├── 03_mixed_dml.sh
│   ├── 04_multiple_tables.sh
│   └── 05_daemon_restart.sh
├── tmp_check/          # (gitignored) temporary PG data directory
└── log/                # (gitignored) test + daemon logs
```

## Debugging Failures

On failure, the runner prints the last 30 lines of test output and 20 lines of daemon log. Full logs are in `test/daemon/log/`:

```bash
# Test output
cat test/daemon/log/01_basic_streaming.log

# Daemon stderr/stdout
cat test/daemon/log/01_basic_streaming_daemon.log

# PostgreSQL server log
cat /tmp/pg_duckpipe_daemon_test_log/postgresql.log
```

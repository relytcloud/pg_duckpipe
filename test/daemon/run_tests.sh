#!/bin/bash
# test/daemon/run_tests.sh — Daemon E2E test runner.
#
# Usage:
#   DUCKPIPE_BIN=path/to/duckpipe PG_CONFIG=path/to/pg_config bash run_tests.sh [test_filter]
#
# If test_filter is provided, only tests whose filename contains it are run.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/helpers.sh"

TEST_FILTER="${1:-}"
TESTS_PASSED=0
TESTS_FAILED=0
TESTS_TOTAL=0

echo "============================================"
echo "  pg_duckpipe daemon E2E tests"
echo "============================================"
echo "  PG_CONFIG:    $PG_CONFIG"
echo "  DUCKPIPE_BIN: $DUCKPIPE_BIN"
echo "  PORT:         $PORT"
echo "  DATA_DIR:     $DATA_DIR"
echo ""

# ---------- PG Instance Setup ----------

echo "--- Setting up PostgreSQL instance ---"
pg_init_instance
pg_start_instance
pg_install_extensions

# Ensure PG is stopped on exit
trap 'pg_stop_instance' EXIT

echo "--- PostgreSQL ready ---"
echo ""

# ---------- Discover & Run Tests ----------

test_files=()
for f in "$SCRIPT_DIR/tests/"*.sh; do
    [ -f "$f" ] || continue
    if [ -n "$TEST_FILTER" ]; then
        if [[ "$(basename "$f")" != *"$TEST_FILTER"* ]]; then
            continue
        fi
    fi
    test_files+=("$f")
done

if [ ${#test_files[@]} -eq 0 ]; then
    echo "No tests found${TEST_FILTER:+ matching '$TEST_FILTER'}"
    exit 1
fi

echo "Running ${#test_files[@]} test(s)..."
echo ""

for test_file in "${test_files[@]}"; do
    # Kill any leftover daemon from a prior failed test
    if [ -f "${LOG_DIR}/daemon.pid" ]; then
        kill "$(cat "${LOG_DIR}/daemon.pid")" 2>/dev/null || true
        rm -f "${LOG_DIR}/daemon.pid"
    fi

    test_name="$(basename "$test_file" .sh)"
    CURRENT_TEST_NAME="$test_name"
    export CURRENT_TEST_NAME
    TESTS_TOTAL=$((TESTS_TOTAL + 1))

    echo -n "  $test_name ... "
    test_log="${LOG_DIR}/${test_name}.log"
    start_time=$SECONDS

    if bash "$test_file" > "$test_log" 2>&1; then
        elapsed=$((SECONDS - start_time))
        echo "PASS (${elapsed}s)"
        TESTS_PASSED=$((TESTS_PASSED + 1))
    else
        elapsed=$((SECONDS - start_time))
        echo "FAIL (${elapsed}s)"
        TESTS_FAILED=$((TESTS_FAILED + 1))
        # Show last 30 lines of test output on failure
        echo "    --- test output (last 30 lines) ---"
        tail -30 "$test_log" | sed 's/^/    /'
        echo "    ---"
        # Show daemon log if it exists
        daemon_log="${LOG_DIR}/${test_name}_daemon.log"
        if [ -f "$daemon_log" ]; then
            echo "    --- daemon log (last 20 lines) ---"
            tail -20 "$daemon_log" | sed 's/^/    /'
            echo "    ---"
        fi
    fi
done

# ---------- Summary ----------

echo ""
echo "============================================"
echo "  Results: $TESTS_PASSED passed, $TESTS_FAILED failed, $TESTS_TOTAL total"
echo "============================================"

if [ "$TESTS_FAILED" -gt 0 ]; then
    exit 1
fi

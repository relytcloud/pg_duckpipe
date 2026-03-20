#!/bin/bash
# test/daemon/lib/helpers.sh — Shared helpers for daemon E2E tests.
#
# Provides:
#   PG lifecycle:     pg_init_instance, pg_start_instance, pg_stop_instance, pg_install_extensions
#   Daemon lifecycle: daemon_start, daemon_stop
#   SQL helpers:      run_sql
#   Polling:          poll_sync, poll_query
#   Assertions:       assert_eq
#   Cleanup:          cleanup_table

set -euo pipefail

# ---------- Configuration ----------

PORT=${DAEMON_TEST_PORT:-5566}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="${SCRIPT_DIR}/tmp_check"
LOG_DIR="${SCRIPT_DIR}/log"
PG_LOG_DIR="/tmp/pg_duckpipe_daemon_test_log"

# Resolve PG binaries from PG_CONFIG
PG_CONFIG="${PG_CONFIG:-pg_config}"
PG_BIN="$("$PG_CONFIG" --bindir)"
PG_LIB="$("$PG_CONFIG" --pkglibdir)"

export DYLD_LIBRARY_PATH="${PG_LIB}:${DYLD_LIBRARY_PATH:-}"
export LD_LIBRARY_PATH="${PG_LIB}:${LD_LIBRARY_PATH:-}"

# Daemon binary (must be set by caller / Makefile)
DUCKPIPE_BIN="${DUCKPIPE_BIN:?DUCKPIPE_BIN must point to the duckpipe binary}"

DAEMON_PID=""
DAEMON_LOG=""

# ---------- PG Lifecycle ----------

pg_init_instance() {
    rm -rf "$DATA_DIR"
    mkdir -p "$DATA_DIR" "$LOG_DIR" "$PG_LOG_DIR"

    "$PG_BIN/initdb" -D "$DATA_DIR" -E UTF8 --no-locale -U postgres >/dev/null

    # Append daemon test config
    cat "$SCRIPT_DIR/daemon.conf" >> "$DATA_DIR/postgresql.conf"

    # Allow TCP connections (trust) for replication and normal access
    cat >> "$DATA_DIR/pg_hba.conf" <<EOF
host all         all 127.0.0.1/32 trust
host replication all 127.0.0.1/32 trust
host all         all ::1/128      trust
host replication all ::1/128      trust
EOF
}

pg_start_instance() {
    : > "${LOG_DIR}/postgresql-startup.log"
    "$PG_BIN/pg_ctl" -D "$DATA_DIR" -l "${LOG_DIR}/postgresql-startup.log" start

    # Wait up to 15 seconds for readiness
    local i
    for i in $(seq 1 30); do
        if "$PG_BIN/pg_isready" -h 127.0.0.1 -p "$PORT" -U postgres >/dev/null 2>&1; then
            break
        fi
        sleep 0.5
    done

    if ! "$PG_BIN/pg_isready" -h 127.0.0.1 -p "$PORT" -U postgres >/dev/null 2>&1; then
        echo "FATAL: PostgreSQL did not start within 15s"
        cat "${LOG_DIR}/postgresql-startup.log" || true
        return 1
    fi

    # Create OS user role as superuser (needed for replication protocol)
    local os_user
    os_user="$(whoami)"
    if [ "$os_user" != "postgres" ]; then
        "$PG_BIN/psql" -h 127.0.0.1 -p "$PORT" -U postgres \
            -c "CREATE USER \"$os_user\" SUPERUSER;" 2>/dev/null || true
    fi
}

pg_install_extensions() {
    run_sql "
        CREATE EXTENSION IF NOT EXISTS pg_duckdb;
        CREATE EXTENSION IF NOT EXISTS pg_ducklake;
        CREATE EXTENSION IF NOT EXISTS pg_duckpipe;
    "
    # Switch the default group to daemon mode so add_table() won't auto-start
    # the PG bgworker (the daemon binary manages replication instead).
    run_sql "UPDATE duckpipe.sync_groups SET mode = 'daemon' WHERE name = 'default';"

    # Set fast flush config for E2E tests (defaults are tuned for production, not testing)
    run_sql "SELECT duckpipe.set_config('flush_interval_ms', '200');"
    run_sql "SELECT duckpipe.set_config('flush_batch_threshold', '100');"
}

pg_stop_instance() {
    if [ -d "$DATA_DIR" ]; then
        "$PG_BIN/pg_ctl" -D "$DATA_DIR" stop -m fast 2>/dev/null || true
    fi
}

# ---------- Daemon Lifecycle ----------

# _daemon_launch [EXTRA_ARGS...] — internal: launch daemon with shared defaults.
_daemon_launch() {
    local test_name="${CURRENT_TEST_NAME:-daemon}"
    DAEMON_LOG="${LOG_DIR}/${test_name}_daemon.log"

    local connstr="host=127.0.0.1 port=${PORT} dbname=postgres user=$(whoami)"

    "$DUCKPIPE_BIN" \
        --connstr "$connstr" \
        --poll-interval 200 \
        --duckdb-lib-dir "$PG_LIB" \
        "$@" \
        >"$DAEMON_LOG" 2>&1 &
    DAEMON_PID=$!
    echo "$DAEMON_PID" > "${LOG_DIR}/daemon.pid"
}

daemon_start() {
    _daemon_launch --group default "$@"

    # Give it a moment to start, then verify it's alive
    sleep 1
    if ! kill -0 "$DAEMON_PID" 2>/dev/null; then
        echo "FATAL: daemon failed to start (PID $DAEMON_PID)"
        cat "$DAEMON_LOG" || true
        DAEMON_PID=""
        rm -f "${LOG_DIR}/daemon.pid"
        return 1
    fi
    echo "  daemon started (PID $DAEMON_PID)"
}

daemon_stop() {
    if [ -n "$DAEMON_PID" ] && kill -0 "$DAEMON_PID" 2>/dev/null; then
        kill "$DAEMON_PID" 2>/dev/null || true

        # Wait up to 5 seconds for graceful shutdown
        local i
        for i in $(seq 1 10); do
            if ! kill -0 "$DAEMON_PID" 2>/dev/null; then
                break
            fi
            sleep 0.5
        done

        # Force kill if still alive
        if kill -0 "$DAEMON_PID" 2>/dev/null; then
            kill -9 "$DAEMON_PID" 2>/dev/null || true
            sleep 0.5
        fi
        echo "  daemon stopped (PID $DAEMON_PID)"
    fi
    DAEMON_PID=""
    rm -f "${LOG_DIR}/daemon.pid"
}

# ---------- SQL Helpers ----------

run_sql() {
    "$PG_BIN/psql" -X -A -t -h 127.0.0.1 -p "$PORT" -U postgres -d postgres -c "$1" 2>&1
}

# ---------- Polling ----------

# poll_sync TABLE COUNT [TIMEOUT_SEC]
# Polls SELECT count(*) FROM TABLE_ducklake until it equals COUNT.
poll_sync() {
    local table="$1"
    local expected="$2"
    local timeout="${3:-15}"
    local target="${table}_ducklake"

    local deadline=$((SECONDS + timeout))
    while [ $SECONDS -lt $deadline ]; do
        local actual
        actual="$(run_sql "SELECT count(*) FROM ${target};" 2>/dev/null || echo "")"
        actual="$(echo "$actual" | tr -d '[:space:]')"
        if [ "$actual" = "$expected" ]; then
            return 0
        fi
        sleep 0.5
    done

    echo "  poll_sync timeout: ${target} count=$(run_sql "SELECT count(*) FROM ${target};" 2>/dev/null || echo '?'), expected=$expected"
    return 1
}

# poll_query "SQL" EXPECTED [TIMEOUT_SEC]
# Generic poll: run SQL until its trimmed output matches EXPECTED.
poll_query() {
    local sql="$1"
    local expected="$2"
    local timeout="${3:-15}"

    local deadline=$((SECONDS + timeout))
    while [ $SECONDS -lt $deadline ]; do
        local actual
        actual="$(run_sql "$sql" 2>/dev/null || echo "")"
        actual="$(echo "$actual" | tr -d '[:space:]')"
        if [ "$actual" = "$expected" ]; then
            return 0
        fi
        sleep 0.5
    done

    echo "  poll_query timeout: got '$(run_sql "$sql" 2>/dev/null || echo '?')', expected '$expected'"
    return 1
}

# ---------- Assertions ----------

assert_eq() {
    local actual="$1"
    local expected="$2"
    local msg="${3:-}"

    # Trim leading/trailing whitespace for comparison
    actual="$(echo "$actual" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
    expected="$(echo "$expected" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"

    if [ "$actual" != "$expected" ]; then
        echo "  ASSERT FAILED${msg:+: $msg}"
        echo "    expected: '$expected'"
        echo "    actual:   '$actual'"
        return 1
    fi
}

# ---------- Test Setup ----------

# test_init TABLE... — set up trap-based cleanup for the given tables.
# Call at the top of each test script after sourcing helpers.sh.
test_init() {
    _TEST_TABLES=("$@")

    _test_cleanup() {
        daemon_stop
        for t in "${_TEST_TABLES[@]}"; do
            cleanup_table "$t"
        done
    }
    trap _test_cleanup EXIT

    # Pre-clean from any prior run
    for t in "${_TEST_TABLES[@]}"; do
        cleanup_table "$t"
    done
}

# add_table TABLE [COPY_DATA] — register a table for CDC sync.
add_table() {
    local table="$1"
    local copy_data="${2:-false}"
    run_sql "SELECT duckpipe.add_table('public.${table}', NULL, 'default', ${copy_data});"
}

# ---------- REST API Helpers ----------

API_PORT=${API_TEST_PORT:-9099}
HTTP_CODE=""
HTTP_BODY=""

# daemon_start_api [EXTRA_ARGS...] — start daemon with REST API enabled.
# Uses --api-port and no --group by default (unbound mode).
daemon_start_api() {
    _daemon_launch --api-port "$API_PORT" "$@"

    # Wait for API to be ready
    poll_api_ready 15
    echo "  daemon started with API (PID $DAEMON_PID, port $API_PORT)"
}

# poll_api_ready [TIMEOUT] — wait for the API server to accept connections.
poll_api_ready() {
    local timeout=${1:-15}
    local deadline=$((SECONDS + timeout))
    while [ $SECONDS -lt $deadline ]; do
        if curl -sf "http://localhost:${API_PORT}/health" > /dev/null 2>&1; then
            return 0
        fi
        # Check if daemon is still alive
        if ! kill -0 "$DAEMON_PID" 2>/dev/null; then
            echo "FATAL: daemon exited before API was ready (PID $DAEMON_PID)"
            cat "$DAEMON_LOG" || true
            DAEMON_PID=""
            rm -f "${LOG_DIR}/daemon.pid"
            return 1
        fi
        sleep 0.5
    done
    echo "FATAL: API not ready after ${timeout}s"
    cat "$DAEMON_LOG" || true
    return 1
}

# _http_request METHOD URL [EXTRA_CURL_ARGS...] — internal: sets HTTP_CODE and HTTP_BODY.
_http_request() {
    local method="$1"; shift
    local url="$1"; shift
    local tmp
    tmp=$(mktemp)
    HTTP_CODE=$(curl -s -o "$tmp" -w '%{http_code}' -X "$method" "$@" "$url" 2>/dev/null || echo "000")
    HTTP_BODY=$(cat "$tmp")
    rm -f "$tmp"
}

# http_get URL — GET request, sets HTTP_BODY and HTTP_CODE.
http_get() { _http_request GET "$1"; }

# http_post URL [JSON_BODY] — POST request with optional JSON body.
http_post() {
    local url="$1"
    local body="${2:-}"
    if [ -n "$body" ]; then
        _http_request POST "$url" -H 'Content-Type: application/json' -d "$body"
    else
        _http_request POST "$url"
    fi
}

# http_delete URL — DELETE request.
http_delete() { _http_request DELETE "$1"; }

# assert_http_code EXPECTED [MSG] — assert HTTP_CODE matches expected.
assert_http_code() {
    local expected="$1"
    local msg="${2:-HTTP status}"
    assert_eq "$HTTP_CODE" "$expected" "$msg: expected $expected, got $HTTP_CODE"
}

# jq_field FIELD — extract a field from HTTP_BODY via jq.
jq_field() {
    echo "$HTTP_BODY" | jq -r "$1"
}

# poll_api ENDPOINT JQ_EXPR EXPECTED [TIMEOUT]
# Poll a REST endpoint until jq expression matches expected value.
poll_api() {
    local endpoint="$1"
    local jq_expr="$2"
    local expected="$3"
    local timeout="${4:-30}"

    local url="http://localhost:${API_PORT}${endpoint}"
    local deadline=$((SECONDS + timeout))
    while [ $SECONDS -lt $deadline ]; do
        local actual
        actual=$(curl -sf "$url" 2>/dev/null | jq -r "$jq_expr" 2>/dev/null || echo "")
        actual="$(echo "$actual" | tr -d '[:space:]')"
        if [ "$actual" = "$expected" ]; then
            return 0
        fi
        sleep 0.5
    done

    echo "  poll_api timeout on $endpoint: jq '$jq_expr' got '$(curl -sf "$url" 2>/dev/null | jq -r "$jq_expr" 2>/dev/null || echo '?')', expected '$expected'"
    return 1
}

# ---------- Cleanup ----------

# cleanup_table TABLE — remove from duckpipe + drop source and target tables.
cleanup_table() {
    local table="$1"
    # remove_table may fail if not registered; errors are suppressed
    run_sql "SELECT duckpipe.remove_table('${table}', true);" 2>/dev/null || true
    run_sql "DROP TABLE IF EXISTS ${table} CASCADE;
             DROP TABLE IF EXISTS ${table}_ducklake CASCADE;" 2>/dev/null || true
}

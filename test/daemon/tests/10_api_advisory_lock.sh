#!/bin/bash
# 10_api_advisory_lock.sh — Duplicate daemon prevention via advisory lock.
#
# Steps:
#   1. Start daemon A with --group default --api-port 9099
#   2. Start daemon B with --group default --api-port 9098
#   3. Daemon B should fail to start (advisory lock conflict)
#   4. Verify daemon A still healthy

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib/helpers.sh"

DAEMON_B_PID=""
DAEMON_B_LOG="${LOG_DIR}/advisory_lock_daemon_b.log"
API_PORT_B=9098

_lock_test_cleanup() {
    # Stop daemon B if running
    if [ -n "$DAEMON_B_PID" ] && kill -0 "$DAEMON_B_PID" 2>/dev/null; then
        kill "$DAEMON_B_PID" 2>/dev/null || true
        sleep 0.5
        kill -9 "$DAEMON_B_PID" 2>/dev/null || true
    fi
    daemon_stop
}
trap _lock_test_cleanup EXIT

# --- Start daemon A ---
daemon_start_api --group default

# Verify daemon A is healthy
http_get "http://localhost:${API_PORT}/health"
assert_http_code "200" "daemon A health"
assert_eq "$(jq_field '.locked')" "true" "daemon A holds lock"

# --- Start daemon B with same group but different port ---
local_connstr="host=127.0.0.1 port=${PORT} dbname=postgres user=$(whoami)"

"$DUCKPIPE_BIN" \
    --connstr "$local_connstr" \
    --group default \
    --api-port "$API_PORT_B" \
    --poll-interval 200 \
    --flush-interval 200 \
    --flush-batch-threshold 100 \
    >"$DAEMON_B_LOG" 2>&1 &
DAEMON_B_PID=$!

# Wait for daemon B to exit (it should fail due to advisory lock conflict)
for i in $(seq 1 10); do
    if ! kill -0 "$DAEMON_B_PID" 2>/dev/null; then
        break
    fi
    sleep 0.5
done

# Daemon B should have exited
if kill -0 "$DAEMON_B_PID" 2>/dev/null; then
    echo "  FAIL: daemon B should have exited due to lock conflict"
    kill "$DAEMON_B_PID" 2>/dev/null || true
    DAEMON_B_PID=""
    exit 1
fi
echo "  daemon B exited as expected (advisory lock conflict)"
DAEMON_B_PID=""

# --- Verify daemon A is still healthy ---
http_get "http://localhost:${API_PORT}/health"
assert_http_code "200" "daemon A still healthy"
assert_eq "$(jq_field '.group')" "default" "daemon A still bound"

echo "  api advisory lock: OK"

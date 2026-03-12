#!/bin/bash
# 06_api_health.sh — Health endpoint + unbound/bound states.
#
# Steps:
#   1. Start daemon WITHOUT --group (unbound)
#   2. GET /health → status=ok, group=null
#   3. POST /groups {"name":"api_test"} → 200, daemon binds
#   4. GET /health → group="api_test", locked=true
#   5. Second POST /groups → 409 Conflict

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib/helpers.sh"

trap 'daemon_stop; run_sql "SELECT duckpipe.drop_group('"'"'api_test'"'"', true);" 2>/dev/null || true' EXIT

# --- Start daemon unbound (no --group) ---
daemon_start_api

# --- Test 1: Health endpoint when unbound ---
http_get "http://localhost:${API_PORT}/health"
assert_http_code "200" "health unbound"
assert_eq "$(jq_field '.status')" "ok" "health status"
assert_eq "$(jq_field '.group')" "null" "group should be null when unbound"

# --- Test 2: Bind daemon via POST /groups ---
http_post "http://localhost:${API_PORT}/groups" '{"name":"api_test"}'
assert_http_code "200" "create group"
assert_eq "$(jq_field '.group')" "api_test" "bound group name"

# --- Test 3: Health endpoint when bound ---
http_get "http://localhost:${API_PORT}/health"
assert_http_code "200" "health bound"
assert_eq "$(jq_field '.group')" "api_test" "group should be api_test"
assert_eq "$(jq_field '.locked')" "true" "should hold advisory lock"

# --- Test 4: Second POST /groups should fail (already bound) ---
http_post "http://localhost:${API_PORT}/groups" '{"name":"another"}'
assert_http_code "409" "duplicate bind should be 409"

# --- Cleanup: drop the group we created ---
http_delete "http://localhost:${API_PORT}/groups?drop_slot=true"

echo "  api health: OK"

#!/bin/bash
# 08_api_group_lifecycle.sh — Group enable/disable/drop via REST.
#
# Steps:
#   1. Start daemon unbound
#   2. POST /groups → bind
#   3. POST /tables → add table
#   4. POST /groups/disable → 200
#   5. GET /status → group disabled
#   6. POST /groups/enable → 200
#   7. DELETE /groups → 200 (drops group)
#   8. GET /health → group=null (unbound again)

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib/helpers.sh"

TABLE="api_group_test"
test_init "$TABLE"

# --- Setup ---
run_sql "CREATE TABLE ${TABLE} (id INT PRIMARY KEY, val TEXT);"

# Start daemon unbound
daemon_start_api

# --- Test 1: Create group ---
http_post "http://localhost:${API_PORT}/groups" '{"name":"lifecycle_test"}'
assert_http_code "200" "create group"

# --- Test 2: Add table ---
http_post "http://localhost:${API_PORT}/tables" "{\"source_table\":\"public.${TABLE}\",\"copy_data\":false}"
assert_http_code "200" "add table"

# --- Test 3: Disable group ---
http_post "http://localhost:${API_PORT}/groups/disable"
assert_http_code "200" "disable group"
assert_eq "$(jq_field '.enabled')" "false" "group disabled"

# Verify via /status
http_get "http://localhost:${API_PORT}/status"
assert_eq "$(jq_field '.group_info.enabled')" "false" "status shows disabled"

# --- Test 4: Enable group ---
http_post "http://localhost:${API_PORT}/groups/enable"
assert_http_code "200" "enable group"
assert_eq "$(jq_field '.enabled')" "true" "group enabled"

# --- Test 5: Drop group ---
http_delete "http://localhost:${API_PORT}/groups"
assert_http_code "200" "drop group"

# --- Test 6: Health shows unbound ---
http_get "http://localhost:${API_PORT}/health"
assert_eq "$(jq_field '.group')" "null" "group should be null after drop"

# --- Test 7: Endpoints that require a group should fail ---
http_get "http://localhost:${API_PORT}/status"
assert_http_code "400" "status without group should be 400"

echo "  api group lifecycle: OK"

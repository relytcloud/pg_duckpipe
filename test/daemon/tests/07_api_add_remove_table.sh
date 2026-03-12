#!/bin/bash
# 07_api_add_remove_table.sh — Full table lifecycle via REST API.
#
# Steps:
#   1. Start daemon with --group default
#   2. Create source table (CREATE TABLE ... PRIMARY KEY)
#   3. POST /tables {"source_table":"public.xxx"} → 200
#   4. GET /tables → shows table
#   5. Insert rows into source
#   6. GET /status → poll until state=STREAMING
#   7. DELETE /tables/public.xxx → 200
#   8. GET /tables → empty

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib/helpers.sh"

TABLE="api_table_test"
test_init "$TABLE"

# --- Setup ---
run_sql "CREATE TABLE ${TABLE} (id INT PRIMARY KEY, val TEXT);"

# Start daemon with --group default and API
daemon_start_api --group default

# --- Test 1: Add table via REST ---
http_post "http://localhost:${API_PORT}/tables" "{\"source_table\":\"public.${TABLE}\",\"copy_data\":false}"
echo "  POST /tables response: $HTTP_CODE $HTTP_BODY"
assert_http_code "200" "add table"
assert_eq "$(jq_field '.status')" "added" "add table status"

# --- Test 2: GET /tables shows the table ---
http_get "http://localhost:${API_PORT}/tables"
echo "  GET /tables response: $HTTP_CODE $HTTP_BODY"
assert_http_code "200" "list tables"
assert_eq "$(jq_field '.tables | length')" "1" "should have 1 table"
assert_eq "$(jq_field '.tables[0].source_table')" "public.${TABLE}" "source table name"

# --- Test 3: Insert rows and wait for sync ---
run_sql "INSERT INTO ${TABLE} VALUES (1, 'alpha'), (2, 'bravo'), (3, 'charlie');"

poll_api "/status" ".tables[0].state" "STREAMING" 30
poll_sync "$TABLE" 3

# --- Test 4: GET /status shows synced rows ---
http_get "http://localhost:${API_PORT}/status"
assert_http_code "200" "status"
assert_eq "$(jq_field '.tables[0].state')" "STREAMING" "should be STREAMING"

# --- Test 5: Remove table via REST ---
http_delete "http://localhost:${API_PORT}/tables/public.${TABLE}?drop_target=true"
assert_http_code "200" "remove table"

# --- Test 6: GET /tables → empty ---
http_get "http://localhost:${API_PORT}/tables"
assert_eq "$(jq_field '.tables | length')" "0" "should have 0 tables after removal"

echo "  api add/remove table: OK"

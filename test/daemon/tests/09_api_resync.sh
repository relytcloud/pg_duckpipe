#!/bin/bash
# 09_api_resync.sh — Resync a table via REST API.
#
# Steps:
#   1. Create table, add_table(copy_data=false), start daemon
#   2. Insert rows via streaming, verify via REST status
#   3. POST /tables/public.xxx/resync → 200
#   4. Resync snapshot re-copies all data, table returns to STREAMING

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib/helpers.sh"

TABLE="api_resync_test"
test_init "$TABLE"

# --- Setup: add table with copy_data=false before daemon ---
run_sql "CREATE TABLE ${TABLE} (id INT PRIMARY KEY, val TEXT);"
add_table "$TABLE" false

daemon_start_api --group default

# Wait for STREAMING state (copy_data=false → STREAMING directly)
poll_api "/status" ".tables[0].state" "STREAMING" 15

# --- Insert rows and verify streaming works ---
# With copy_data=false, only rows inserted after the slot was created are synced.
run_sql "INSERT INTO ${TABLE} VALUES (1, 'alpha'), (2, 'bravo'), (3, 'charlie');"
poll_sync "$TABLE" 3

# Verify via REST: table is tracked and streaming
http_get "http://localhost:${API_PORT}/tables"
assert_http_code "200" "list tables"
assert_eq "$(jq_field '.tables[0].source_table')" "public.${TABLE}" "table listed"

# --- Trigger resync via REST ---
http_post "http://localhost:${API_PORT}/tables/public.${TABLE}/resync"
assert_http_code "200" "resync"
assert_eq "$(jq_field '.status')" "resync_initiated" "resync status"

# --- Wait for re-sync snapshot to complete ---
# Wait for the state machine to leave SNAPSHOT (enters CATCHUP after snapshot completes).
# We must wait for this before inserting new rows, otherwise the snapshot
# could overwrite the new row's flush.
poll_query "SELECT state FROM duckpipe.table_mappings WHERE source_table='${TABLE}';" "CATCHUP" 30

# All 3 rows should be present after the resync snapshot
poll_sync "$TABLE" 3

# Insert a row to generate WAL — advances pending_lsn past snapshot_lsn,
# triggering the CATCHUP → STREAMING transition.
run_sql "INSERT INTO ${TABLE} VALUES (4, 'delta');"
poll_sync "$TABLE" 4

# Table should return to STREAMING
poll_api "/status" ".tables[0].state" "STREAMING" 30

echo "  api resync: OK"

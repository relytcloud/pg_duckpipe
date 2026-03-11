#!/bin/bash
# 02_snapshot_sync.sh — Verify snapshot (copy_data=true) followed by streaming.
#
# Steps:
#   1. Create table with 3 pre-existing rows
#   2. add_table(copy_data=true) → start daemon
#   3. Poll for snapshot completion (3 rows in target)
#   4. INSERT 1 more row → poll for 4 rows (streaming)
#   5. Verify state = STREAMING

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib/helpers.sh"

TABLE="snapshot_test"
test_init "$TABLE"

# --- Setup: pre-populate before sync ---
run_sql "CREATE TABLE ${TABLE} (id INT PRIMARY KEY, name TEXT);
         INSERT INTO ${TABLE} VALUES (1, 'one'), (2, 'two'), (3, 'three');"

add_table "$TABLE" true
daemon_start

# --- Test: snapshot should copy existing 3 rows ---
poll_sync "$TABLE" 3 20

# --- Test: streaming should pick up new INSERT ---
run_sql "INSERT INTO ${TABLE} VALUES (4, 'four');"
poll_sync "$TABLE" 4

# --- Verify state transitioned to STREAMING ---
poll_query "SELECT state FROM duckpipe.table_mappings WHERE source_table='${TABLE}';" "STREAMING"

echo "  snapshot sync: OK"

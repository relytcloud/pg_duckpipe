#!/bin/bash
# 01_basic_streaming.sh — Verify the daemon can stream INSERT changes to DuckLake.
#
# Steps:
#   1. Create table + add_table(copy_data=false)
#   2. Start daemon → INSERT 3 rows → poll_sync → verify data
#   3. Cleanup (via trap)

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib/helpers.sh"

TABLE="basic_stream_test"
test_init "$TABLE"

# --- Setup ---
run_sql "CREATE TABLE ${TABLE} (id INT PRIMARY KEY, val TEXT);"
add_table "$TABLE"
stop_bgworker
daemon_start

# --- Test ---
run_sql "INSERT INTO ${TABLE} VALUES (1, 'alpha'), (2, 'bravo'), (3, 'charlie');"

poll_sync "$TABLE" 3

actual_vals="$(run_sql "SELECT val FROM ${TABLE}_ducklake ORDER BY id;" | tr '\n' ',')"
assert_eq "$actual_vals" "alpha,bravo,charlie," "row values"

poll_query "SELECT state FROM duckpipe.table_mappings WHERE source_table='${TABLE}';" "STREAMING"

echo "  basic streaming: OK"

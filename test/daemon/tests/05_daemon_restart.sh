#!/bin/bash
# 05_daemon_restart.sh — Verify daemon catches up after restart.
#
# Steps:
#   1. Create table → add_table(copy_data=false) → daemon → INSERT → sync
#   2. Stop daemon → INSERT + UPDATE while daemon is down
#   3. Restart daemon → verify it catches up from confirmed_lsn

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib/helpers.sh"

TABLE="restart_test"
test_init "$TABLE"

# --- Setup ---
run_sql "CREATE TABLE ${TABLE} (id INT PRIMARY KEY, val TEXT);"
add_table "$TABLE"
stop_bgworker
daemon_start

# --- Phase 1: Initial sync ---
run_sql "INSERT INTO ${TABLE} VALUES (1, 'first'), (2, 'second');"
poll_sync "$TABLE" 2

# --- Phase 2: Stop daemon, make changes while it's down ---
daemon_stop

run_sql "INSERT INTO ${TABLE} VALUES (3, 'third'), (4, 'fourth');
         UPDATE ${TABLE} SET val = 'first_updated' WHERE id = 1;"

# Data should still show 2 rows in ducklake (daemon is down)
actual="$(run_sql "SELECT count(*) FROM ${TABLE}_ducklake;")"
assert_eq "$actual" "2" "count unchanged while daemon down"

# --- Phase 3: Restart daemon → should catch up ---
daemon_start

# Expect 4 rows total (2 original + 2 new inserts)
poll_sync "$TABLE" 4

# Verify the UPDATE was also applied
poll_query "SELECT val FROM ${TABLE}_ducklake WHERE id = 1;" "first_updated"

echo "  daemon restart: OK"

#!/bin/bash
# 03_mixed_dml.sh — Verify INSERT, UPDATE, DELETE all sync correctly.
#
# Steps:
#   1. Create table → add_table(copy_data=false) → daemon
#   2. INSERT 100 rows → verify count
#   3. UPDATE 50 rows → verify updated values
#   4. DELETE 30 rows → verify count = 70
#   5. Mixed txn: INSERT + UPDATE + DELETE → verify final state

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib/helpers.sh"

TABLE="mixed_dml_test"
test_init "$TABLE"

# --- Setup ---
run_sql "CREATE TABLE ${TABLE} (id INT PRIMARY KEY, val INT);"
add_table "$TABLE"
daemon_start

# --- Phase 1: INSERT 100 rows ---
run_sql "INSERT INTO ${TABLE} SELECT g, g FROM generate_series(1, 100) g;"
poll_sync "$TABLE" 100

# --- Phase 2: UPDATE 50 rows (set val = val + 1000 for id 1-50) ---
run_sql "UPDATE ${TABLE} SET val = val + 1000 WHERE id <= 50;"
poll_query "SELECT count(*) FROM ${TABLE}_ducklake WHERE val > 1000;" "50" 15

# Total count should still be 100
actual="$(run_sql "SELECT count(*) FROM ${TABLE}_ducklake;")"
assert_eq "$actual" "100" "count unchanged after UPDATE"

# --- Phase 3: DELETE 30 rows (id 71-100) ---
run_sql "DELETE FROM ${TABLE} WHERE id > 70;"
poll_sync "$TABLE" 70

# --- Phase 4: Mixed transaction ---
run_sql "BEGIN;
INSERT INTO ${TABLE} VALUES (101, 101), (102, 102), (103, 103);
UPDATE ${TABLE} SET val = 9999 WHERE id = 1;
DELETE FROM ${TABLE} WHERE id = 2;
COMMIT;"

# Expect: 70 - 1 (delete id=2) + 3 (inserts) = 72
poll_sync "$TABLE" 72

actual="$(run_sql "SELECT val FROM ${TABLE}_ducklake WHERE id = 1;")"
assert_eq "$actual" "9999" "mixed txn UPDATE check"

echo "  mixed DML: OK"

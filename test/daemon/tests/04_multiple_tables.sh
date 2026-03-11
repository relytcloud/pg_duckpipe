#!/bin/bash
# 04_multiple_tables.sh — Verify multiple tables sync simultaneously.
#
# Steps:
#   1. Create 3 tables → add_table(copy_data=false) for each → daemon
#   2. INSERT into each → poll all 3
#   3. Cross-table DML → verify independence

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib/helpers.sh"

TABLE_A="multi_a"
TABLE_B="multi_b"
TABLE_C="multi_c"
test_init "$TABLE_A" "$TABLE_B" "$TABLE_C"

# --- Setup ---
run_sql "CREATE TABLE ${TABLE_A} (id INT PRIMARY KEY, val TEXT);
         CREATE TABLE ${TABLE_B} (id INT PRIMARY KEY, val TEXT);
         CREATE TABLE ${TABLE_C} (id INT PRIMARY KEY, val TEXT);"

add_table "$TABLE_A"
add_table "$TABLE_B"
add_table "$TABLE_C"
daemon_start

# --- Phase 1: INSERT into each table ---
run_sql "INSERT INTO ${TABLE_A} VALUES (1, 'a1'), (2, 'a2');
         INSERT INTO ${TABLE_B} VALUES (1, 'b1'), (2, 'b2'), (3, 'b3');
         INSERT INTO ${TABLE_C} VALUES (1, 'c1');"

poll_sync "$TABLE_A" 2
poll_sync "$TABLE_B" 3
poll_sync "$TABLE_C" 1

# --- Phase 2: Cross-table DML (operations on different tables in same txn) ---
run_sql "BEGIN;
INSERT INTO ${TABLE_A} VALUES (3, 'a3');
DELETE FROM ${TABLE_B} WHERE id = 1;
UPDATE ${TABLE_C} SET val = 'c1_updated' WHERE id = 1;
COMMIT;"

poll_sync "$TABLE_A" 3
poll_sync "$TABLE_B" 2
poll_query "SELECT val FROM ${TABLE_C}_ducklake WHERE id = 1;" "c1_updated"

echo "  multiple tables: OK"

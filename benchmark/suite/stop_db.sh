#!/bin/bash
set -e

PG_BIN="/Users/xiaoyuwei/Desktop/workspace_ducklake/postgres/work/app/bin"
BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="$BENCH_DIR/bench_data"

if [ -d "$DATA_DIR" ]; then
    echo "[-] Stopping PostgreSQL..."
    "$PG_BIN/pg_ctl" -D "$DATA_DIR" stop -m fast || true
    echo "[+] Database stopped."
else
    echo "[!] No data directory found."
fi

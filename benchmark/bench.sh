#!/bin/bash
# bench.sh — start a fresh DB and run the pg_duckpipe sysbench benchmark end-to-end.
#
# Usage:
#   ./benchmark/bench.sh [options]
#
# Options (all optional, defaults shown):
#   --threads N        sysbench writer threads  (default: 1)
#   --duration N       OLTP phase seconds       (default: 30)
#   --table-size N     initial rows per table   (default: 100000)
#   --tables N         number of tables         (default: 1)
#   --catchup-timeout N  catch-up wait limit     (default: 600)
#   --skip-db-start    reuse the existing DB instead of a fresh one
#   --help

set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$BENCH_DIR/.." && pwd)"

# ── Defaults ────────────────────────────────────────────────────────────────
THREADS=1
DURATION=30
TABLE_SIZE=100000
TABLES=1
CATCHUP_TIMEOUT=600
SKIP_DB_START=0
PORT=5556

# ── Argument parsing ─────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --threads)        THREADS="$2";        shift 2 ;;
        --duration)       DURATION="$2";       shift 2 ;;
        --table-size)     TABLE_SIZE="$2";     shift 2 ;;
        --tables)         TABLES="$2";         shift 2 ;;
        --catchup-timeout) CATCHUP_TIMEOUT="$2"; shift 2 ;;
        --skip-db-start)  SKIP_DB_START=1;     shift ;;
        --help|-h)
            sed -n '2,/^set /p' "$0" | grep '^#' | sed 's/^# \?//'
            exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

DB_URL="host=localhost port=$PORT user=postgres dbname=postgres"

echo "==========================================================="
echo " pg_duckpipe End-to-End Benchmark"
echo "==========================================================="
echo " threads=$THREADS  duration=${DURATION}s  table_size=$TABLE_SIZE  tables=$TABLES"
echo " catchup_timeout=${CATCHUP_TIMEOUT}s"
echo "==========================================================="
echo ""

# ── 1. Start database ────────────────────────────────────────────────────────
if [[ $SKIP_DB_START -eq 0 ]]; then
    echo "[1/2] Starting fresh database..."
    bash "$BENCH_DIR/start_db.sh"
    echo ""
else
    echo "[1/2] Skipping DB start (--skip-db-start)."
    echo ""
fi

# ── 2. Run benchmark ─────────────────────────────────────────────────────────
echo "[2/2] Running benchmark..."
echo ""

python3 "$BENCH_DIR/run_sysbench.py" \
    --db-url "$DB_URL" \
    --workload oltp_insert \
    --tables "$TABLES" \
    --table-size "$TABLE_SIZE" \
    --threads "$THREADS" \
    --duration "$DURATION" \
    --catchup-timeout "$CATCHUP_TIMEOUT"

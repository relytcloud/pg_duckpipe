#!/bin/bash
# bench_suite.sh — run all 4 benchmark scenarios and produce an analysis report.
#
# Usage:
#   ./benchmark/suite/bench_suite.sh [options]
#
# Options:
#   --duration N   OLTP phase seconds per scenario (default: 30)
#   --help

set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="$BENCH_DIR/results"
PG_LOG="$BENCH_DIR/bench_pg.log"

# ���─ Defaults ──────────────────────────────────────────────────────────────────
# Use >=30s for reportable results; shorter durations produce unstable numbers.
DURATION=30

# ── Argument parsing ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --duration)  DURATION="$2"; shift 2 ;;
        --help|-h)
            sed -n '2,/^set /p' "$0" | grep '^#' | sed 's/^# \?//'
            exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# ── Scenario definitions ──────────────────────────────────────────────────────
# Each scenario: NAME TABLES WORKLOAD
SCENARIOS=(
    "single_table_insert:1:oltp_insert"
    "multi_table_insert:4:oltp_insert"
    "single_table_mixed:1:oltp_read_write"
    "multi_table_mixed:4:oltp_read_write"
)

SCENARIO_LABELS=(
    "Single-table append (1x100k, oltp_insert)"
    "Multi-table append (4x100k, oltp_insert)"
    "Single-table mixed DML (1x100k, oltp_read_write)"
    "Multi-table mixed DML (4x100k, oltp_read_write)"
)

echo "==========================================================="
echo " pg_duckpipe Benchmark Suite"
echo "==========================================================="
echo " scenarios: ${#SCENARIOS[@]}  duration: ${DURATION}s each"
echo "==========================================================="
echo ""

# ── Setup ─────────────────────────────────────────────────────────────────────
mkdir -p "$RESULTS_DIR"

echo "[1/${#SCENARIOS[@]}+1] Starting fresh database..."
bash "$BENCH_DIR/start_db.sh"
echo ""

# ── Run scenarios ─────────────────────────────────────────────────────────────
SCENARIO_IDX=0
for spec in "${SCENARIOS[@]}"; do
    IFS=':' read -r NAME TABLES WORKLOAD <<< "$spec"
    LABEL="${SCENARIO_LABELS[$SCENARIO_IDX]}"
    LOG_FILE="$RESULTS_DIR/${NAME}.log"
    SCENARIO_IDX=$((SCENARIO_IDX + 1))

    echo "==========================================================="
    echo " Scenario $SCENARIO_IDX/${#SCENARIOS[@]}: $LABEL"
    echo "==========================================================="

    # Truncate PG log to isolate per-scenario log analysis.
    # Save the current log segment for this scenario first.
    if [[ -f "$PG_LOG" ]]; then
        cp "$PG_LOG" "$RESULTS_DIR/${NAME}_pg.log"
        : > "$PG_LOG"
    fi

    bash "$BENCH_DIR/bench.sh" \
        --skip-db-start \
        --tables "$TABLES" \
        --workload "$WORKLOAD" \
        --duration "$DURATION" \
        --log-file "$LOG_FILE"

    # Save the PG log for this scenario (accumulated during this run).
    if [[ -f "$PG_LOG" ]]; then
        cp "$PG_LOG" "$RESULTS_DIR/${NAME}_pg.log"
    fi

    echo ""
done

# ── Analysis ──────────────────────────────────────────────────────────────────
echo "==========================================================="
echo " Generating Analysis Report"
echo "==========================================================="

python3 "$BENCH_DIR/analyze_results.py" \
    --results-dir "$RESULTS_DIR" \
    --output "$RESULTS_DIR/report.md"

echo ""
echo "[+] Suite complete. Report: $RESULTS_DIR/report.md"

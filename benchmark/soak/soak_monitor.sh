#!/bin/bash
# soak_monitor.sh — standalone live terminal dashboard for pg_duckpipe
#
# Polls duckpipe.status(), worker_status(), WAL lag, and slot size.
# Pure shell (psql + bc only) — works against any running pg_duckpipe instance.
#
# Usage:
#   ./soak_monitor.sh [--db-url CONNSTR] [--interval 2]
#   docker compose -f benchmark/soak/docker-compose.soak.yml run bench ./soak_monitor.sh

set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────
DB_URL="${DB_URL:-host=localhost port=5432 user=postgres dbname=postgres}"
INTERVAL=2

# ── Argument parsing ─────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --db-url)    DB_URL="$2";    shift 2 ;;
        --interval)  INTERVAL="$2";  shift 2 ;;
        --help|-h)
            echo "Usage: $0 [--db-url CONNSTR] [--interval SECONDS]"
            exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# ── Helpers ───────────────────────────────────────────────────────────────────
run_sql() {
    local sql="$1"
    local timeout="${2:-10}"
    PGPASSWORD="${PGPASSWORD:-}" psql "$DB_URL" -t -A -c "$sql" 2>/dev/null || echo ""
}

fmt_bytes() {
    local bytes="$1"
    if [[ -z "$bytes" || "$bytes" == "0" ]]; then
        echo "0 B"
        return
    fi
    if command -v bc >/dev/null 2>&1; then
        local mb
        mb=$(echo "scale=1; $bytes / 1048576" | bc 2>/dev/null || echo "0")
        echo "${mb} MB"
    else
        echo "${bytes} B"
    fi
}

fmt_number() {
    local n="$1"
    if [[ -z "$n" ]]; then
        echo "0"
        return
    fi
    # Use printf with locale-aware grouping if available, else plain
    printf "%'d" "$n" 2>/dev/null || echo "$n"
}

# ── Wait for DB ───────────────────────────────────────────────────────────────
echo "Connecting to database..."
for i in $(seq 1 30); do
    result=$(run_sql "SELECT 1" 5)
    if [[ "$result" == "1" ]]; then
        break
    fi
    if [[ $i -eq 30 ]]; then
        echo "ERROR: Could not connect to database after 60s"
        exit 1
    fi
    sleep 2
done

START_TIME=$(date +%s)

# ── Main loop ─────────────────────────────────────────────────────────────────
while true; do
    # Collect metrics
    NOW=$(date +%s)
    ELAPSED=$((NOW - START_TIME))
    ELAPSED_FMT=$(printf "%02d:%02d:%02d" $((ELAPSED/3600)) $(((ELAPSED%3600)/60)) $((ELAPSED%60)))

    # WAL lag
    WAL_LAG=$(run_sql "SELECT COALESCE(SUM((pg_current_wal_lsn() - confirmed_lsn)::int8), 0) FROM duckpipe.sync_groups WHERE enabled")
    WAL_LAG=${WAL_LAG:-0}
    WAL_LAG_FMT=$(fmt_bytes "$WAL_LAG")

    # Queued bytes + backpressure
    WORKER_DATA=$(run_sql "SELECT COALESCE(SUM(total_queued_bytes), 0), bool_or(is_backpressured) FROM duckpipe.worker_status()")
    QUEUED=$(echo "$WORKER_DATA" | cut -d'|' -f1 | tr -d ' ')
    QUEUED=${QUEUED:-0}
    BP=$(echo "$WORKER_DATA" | cut -d'|' -f2 | tr -d ' ')
    if [[ "$BP" == "t" ]]; then
        BP_STR="Yes"
    else
        BP_STR="No"
    fi

    # Slot retained WAL
    SLOT_WAL=$(run_sql "SELECT COALESCE(SUM(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)), 0)::bigint FROM pg_replication_slots WHERE slot_name LIKE 'duckpipe_%'")
    SLOT_WAL=${SLOT_WAL:-0}
    SLOT_WAL_FMT=$(fmt_bytes "$SLOT_WAL")

    # Total rows synced
    TOTAL_ROWS=$(run_sql "SELECT COALESCE(SUM(rows_synced), 0) FROM duckpipe.status()")
    TOTAL_ROWS=${TOTAL_ROWS:-0}
    TOTAL_ROWS_FMT=$(fmt_number "$TOTAL_ROWS")

    # Per-table status
    TABLE_STATUS=$(run_sql "SELECT source_table, state, rows_synced, queued_changes, consecutive_failures FROM duckpipe.status() ORDER BY source_table")

    # Count states
    STREAMING_COUNT=$(run_sql "SELECT count(*) FROM duckpipe.status() WHERE state = 'STREAMING'")
    ERRORED_COUNT=$(run_sql "SELECT count(*) FROM duckpipe.status() WHERE state = 'ERRORED'")
    SNAPSHOT_COUNT=$(run_sql "SELECT count(*) FROM duckpipe.status() WHERE state = 'SNAPSHOT'")
    CATCHUP_COUNT=$(run_sql "SELECT count(*) FROM duckpipe.status() WHERE state = 'CATCHUP'")
    STREAMING_COUNT=${STREAMING_COUNT:-0}
    ERRORED_COUNT=${ERRORED_COUNT:-0}
    SNAPSHOT_COUNT=${SNAPSHOT_COUNT:-0}
    CATCHUP_COUNT=${CATCHUP_COUNT:-0}

    # ── Render ────────────────────────────────────────────────────────────────
    clear
    W=64

    # Header
    printf "\033[1;36m"
    printf '%*s\n' "$W" '' | tr ' ' '='
    printf "  pg_duckpipe Monitor  --  %s elapsed\n" "$ELAPSED_FMT"
    printf '%*s\n' "$W" '' | tr ' ' '='
    printf "\033[0m"

    # Pipeline
    printf "\033[1;33m-- Pipeline --\033[0m\n"
    printf "  WAL Lag: %-12s  Queued: %-8s  Backpressure: %s\n" "$WAL_LAG_FMT" "$(fmt_number "$QUEUED")" "$BP_STR"
    printf "  Slot Retained WAL: %-12s  Total Rows Synced: %s\n" "$SLOT_WAL_FMT" "$TOTAL_ROWS_FMT"
    echo ""

    # Tables
    printf "\033[1;33m-- Tables --\033[0m\n"
    printf "  %-25s %-12s %12s %8s %5s\n" "TABLE" "STATE" "ROWS_SYNCED" "QUEUED" "ERRS"
    printf "  %-25s %-12s %12s %8s %5s\n" "-------------------------" "------------" "------------" "--------" "-----"

    if [[ -n "$TABLE_STATUS" ]]; then
        while IFS='|' read -r tbl state rows queued errs; do
            tbl=$(echo "$tbl" | tr -d ' ')
            state=$(echo "$state" | tr -d ' ')
            rows=$(echo "$rows" | tr -d ' ')
            queued=$(echo "$queued" | tr -d ' ')
            errs=$(echo "$errs" | tr -d ' ')

            # Color state
            case "$state" in
                STREAMING)  state_color="\033[1;32m" ;;
                ERRORED)    state_color="\033[1;31m" ;;
                SNAPSHOT)   state_color="\033[1;34m" ;;
                CATCHUP)    state_color="\033[1;33m" ;;
                *)          state_color="\033[0m" ;;
            esac

            printf "  %-25s ${state_color}%-12s\033[0m %12s %8s %5s\n" \
                "$tbl" "$state" "$(fmt_number "${rows:-0}")" "$(fmt_number "${queued:-0}")" "${errs:-0}"
        done <<< "$TABLE_STATUS"
    else
        printf "  (no tables registered)\n"
    fi
    echo ""

    # State summary
    printf "\033[1;33m-- Summary --\033[0m\n"
    printf "  STREAMING: %s  SNAPSHOT: %s  CATCHUP: %s  ERRORED: %s\n" \
        "$STREAMING_COUNT" "$SNAPSHOT_COUNT" "$CATCHUP_COUNT" "$ERRORED_COUNT"
    echo ""

    printf "\033[2m  Refreshing every %ss  |  Ctrl+C to exit\033[0m\n" "$INTERVAL"

    sleep "$INTERVAL"
done

#!/usr/bin/env python3
"""pg_duckpipe Soak Test Orchestrator.

Runs a long-duration workload against pg_duckpipe, continuously monitoring
pipeline health, collecting metrics to CSV, and optionally injecting chaos
events. A final consistency check runs after the workload stops.

Usage:
  python3 soak_test.py [options]
  # Or via Docker Compose (env vars configure defaults):
  docker compose -f benchmark/soak/docker-compose.soak.yml up
"""
import argparse
import collections
import csv
import datetime
import os
import random
import re
import signal
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lib import (
    run_sql,
    parse_db_url,
    get_sysbench_cmd,
    parse_int,
    get_total_lag_bytes,
    get_total_queued_bytes,
    get_full_status,
    get_worker_status,
    get_wal_slot_size_bytes,
    get_benchmark_rows_synced,
    get_metrics_json,
    wait_for_db_ready,
    verify_table_consistency_full,
)

# ==============================================================================
# Named Scenarios
# ==============================================================================

SCENARIOS = {
    "sustained-insert": {
        "workload": "oltp_insert",
        "tables": 4,
        "threads": 4,
        "table_size": 100000,
        "chaos": "none",
    },
    "sustained-mixed": {
        "workload": "/bench/oltp_insert_heavy.lua",
        "tables": 4,
        "threads": 4,
        "table_size": 100000,
        "chaos": "none",
    },
    "multi-table-insert": {
        "workload": "oltp_insert",
        "tables": 8,
        "threads": 8,
        "table_size": 100000,
        "chaos": "none",
    },
    "multi-table-mixed": {
        "workload": "oltp_read_write",
        "tables": 8,
        "threads": 8,
        "table_size": 100000,
        "chaos": "none",
    },
    "table-lifecycle": {
        "workload": "oltp_insert",
        "tables": 4,
        "threads": 4,
        "table_size": 100000,
        "chaos": "table-lifecycle",
    },
    "error-recovery": {
        "workload": "oltp_insert",
        "tables": 4,
        "threads": 4,
        "table_size": 100000,
        "chaos": "worker-restart",
    },
}

# CSV field names shared between bgworker and daemon soak tests
CSV_FIELDS = [
    'timestamp', 'elapsed_s', 'sysbench_tps', 'total_rows_synced',
    'rows_synced_delta', 'sync_rate_rows_s', 'wal_lag_bytes', 'wal_lag_mb',
    'queued_changes', 'is_backpressured', 'slot_retained_wal_bytes',
    'duckdb_memory_bytes', 'duckdb_memory_mb', 'flush_count',
    'flush_rate_s', 'avg_flush_duration_ms',
    'tables_streaming', 'tables_errored', 'tables_snapshot', 'tables_catchup',
    'max_consecutive_failures', 'consistency_check_result', 'event',
]


def aggregate_shm_metrics(metrics_data):
    """Extract totals from duckpipe.metrics() JSON. Returns (mem, flushes, durations)."""
    total_mem = 0
    total_flushes = 0
    flush_durations = []
    if metrics_data and 'tables' in metrics_data:
        for t in metrics_data['tables']:
            total_mem += t.get('duckdb_memory_bytes', 0)
            total_flushes += t.get('flush_count', 0)
            fd = t.get('flush_duration_ms', 0)
            if fd > 0:
                flush_durations.append(fd)
    return total_mem, total_flushes, flush_durations


def update_state_metrics(state, total_rows, total_mem, total_flushes, flush_durations,
                         wal_lag, queued, bp, slot_wal, table_statuses, poll_interval, elapsed):
    """Update SoakState with a monitor tick's collected data. Returns (delta, rate)."""
    with state.lock:
        delta = total_rows - state.prev_rows_synced
        rate = delta / poll_interval if poll_interval > 0 else 0
        state.sync_rate = rate
        state.prev_rows_synced = total_rows

        state.wal_lag_bytes = wal_lag
        state.queued_changes = queued
        state.is_backpressured = bp
        state.slot_retained_wal_bytes = slot_wal
        state.total_rows_synced = total_rows
        state.table_statuses = table_statuses

        state.total_duckdb_memory_bytes = total_mem
        flush_delta = total_flushes - state.prev_flush_count
        state.flush_rate = flush_delta / poll_interval if poll_interval > 0 else 0
        state.prev_flush_count = total_flushes
        state.total_flush_count = total_flushes
        state.avg_flush_duration_ms = (
            sum(flush_durations) / len(flush_durations) if flush_durations else 0
        )

        if wal_lag > state.peak_lag_bytes:
            state.peak_lag_bytes = wal_lag
        state.lag_sample_count += 1
        state.lag_sample_sum += wal_lag

        if elapsed > poll_interval:
            state.avg_sync_rate = total_rows / elapsed

    return delta, rate


def build_metrics_row(state, total_rows, delta, rate, wal_lag, queued, bp, slot_wal,
                      total_mem, total_flushes, table_statuses, elapsed):
    """Build a CSV row dict from the current state snapshot."""
    state_counts = collections.Counter(t['state'] for t in table_statuses)
    max_failures = max((t['consecutive_failures'] for t in table_statuses), default=0)

    with state.lock:
        event_str = state.events[-1][1] if state.events else ""

    return {
        'timestamp': datetime.datetime.now().isoformat(),
        'elapsed_s': f"{elapsed:.0f}",
        'sysbench_tps': f"{state.sysbench_tps:.1f}",
        'total_rows_synced': total_rows,
        'rows_synced_delta': delta,
        'sync_rate_rows_s': f"{rate:.0f}",
        'wal_lag_bytes': wal_lag,
        'wal_lag_mb': f"{wal_lag / 1048576:.2f}",
        'queued_changes': queued,
        'is_backpressured': bp,
        'slot_retained_wal_bytes': slot_wal,
        'duckdb_memory_bytes': total_mem,
        'duckdb_memory_mb': f"{total_mem / 1048576:.1f}",
        'flush_count': total_flushes,
        'flush_rate_s': f"{state.flush_rate:.1f}",
        'avg_flush_duration_ms': f"{state.avg_flush_duration_ms:.0f}",
        'tables_streaming': state_counts.get('STREAMING', 0),
        'tables_errored': state_counts.get('ERRORED', 0),
        'tables_snapshot': state_counts.get('SNAPSHOT', 0),
        'tables_catchup': state_counts.get('CATCHUP', 0),
        'max_consecutive_failures': max_failures,
        'consistency_check_result': "",
        'event': event_str,
    }


# ==============================================================================
# Shared State
# ==============================================================================

class SoakState:
    """Thread-safe shared state for the soak test."""
    def __init__(self):
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.start_time = time.time()

        # Sysbench
        self.sysbench_tps = 0.0
        self.sysbench_running = False

        # Pipeline metrics (updated by monitor thread)
        self.wal_lag_bytes = 0
        self.queued_changes = 0
        self.is_backpressured = False
        self.slot_retained_wal_bytes = 0
        self.total_rows_synced = 0
        self.prev_rows_synced = 0
        self.sync_rate = 0.0
        self.avg_sync_rate = 0.0
        self.peak_lag_bytes = 0
        self.lag_sample_count = 0
        self.lag_sample_sum = 0
        self.table_statuses = []

        # SHM metrics (from duckpipe.metrics())
        self.total_duckdb_memory_bytes = 0
        self.total_flush_count = 0
        self.prev_flush_count = 0
        self.flush_rate = 0.0  # flushes/s
        self.avg_flush_duration_ms = 0

        # Consistency (final check only)
        self.consistency_checks = []

        # Events log
        self.events = []

        # Metrics history for CSV
        self.metrics_rows = []

    def add_event(self, msg):
        with self.lock:
            ts = datetime.datetime.now().strftime("%H:%M:%S")
            self.events.append((ts, msg))
            # Keep last 50 events
            if len(self.events) > 50:
                self.events = self.events[-50:]


# ==============================================================================
# Sysbench Runner
# ==============================================================================

def run_sysbench_continuous(state, db_params, args):
    """Run sysbench as a subprocess, parse TPS from report-interval output."""
    sb_params = {
        'workload': args.workload,
        'tables': args.tables,
        'table_size': args.table_size,
        'threads': args.threads,
        'duration': args.duration if args.duration > 0 else 86400 * 7,  # 7 days for "infinite"
    }
    cmd = get_sysbench_cmd(sb_params, db_params, "run")

    state.add_event("Sysbench started")
    state.sysbench_running = True

    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, bufsize=1
    )

    # Parse report-interval lines: [ 5s ] thrd: 4 tps: 8234.56 ...
    tps_pattern = re.compile(r'\[\s*\d+s\s*\].*tps:\s*([\d.]+)')

    try:
        for line in proc.stdout:
            if state.stop_event.is_set():
                proc.terminate()
                break
            m = tps_pattern.search(line)
            if m:
                state.sysbench_tps = float(m.group(1))
        proc.wait(timeout=30)
    except Exception:
        proc.kill()

    state.sysbench_running = False
    state.sysbench_tps = 0.0
    state.add_event("Sysbench stopped")
    return proc


# ==============================================================================
# Monitor Thread
# ==============================================================================

def monitor_loop(state, db_params, args, csv_writer, csv_file):
    """Periodically collect metrics and write to CSV."""
    poll_interval = args.poll_interval

    # Initialize prev_rows to current count so the first delta doesn't
    # include rows from the prepare/snapshot phase.
    try:
        init_rows = get_benchmark_rows_synced(db_params, args.tables)
        state.prev_rows_synced = init_rows
    except Exception:
        pass

    while not state.stop_event.is_set():
        try:
            now = time.time()
            elapsed = now - state.start_time

            # Pipeline metrics
            wal_lag = get_total_lag_bytes(db_params)
            queued = get_total_queued_bytes(db_params)
            slot_wal = get_wal_slot_size_bytes(db_params)
            total_rows = get_benchmark_rows_synced(db_params, args.tables)

            # Worker status
            workers = get_worker_status(db_params)
            bp = any(w['is_backpressured'] for w in workers) if workers else False

            # Table statuses
            table_statuses = get_full_status(db_params)

            # SHM metrics via duckpipe.metrics() JSON
            total_mem, total_flushes, flush_durations = aggregate_shm_metrics(
                get_metrics_json(db_params)
            )

            # Update shared state
            delta, rate = update_state_metrics(
                state, total_rows, total_mem, total_flushes, flush_durations,
                wal_lag, queued, bp, slot_wal, table_statuses, poll_interval, elapsed,
            )

            # Write CSV row
            row = build_metrics_row(
                state, total_rows, delta, rate, wal_lag, queued, bp, slot_wal,
                total_mem, total_flushes, table_statuses, elapsed,
            )
            csv_writer.writerow(row)
            csv_file.flush()

            with state.lock:
                state.metrics_rows.append(row)

        except Exception as e:
            state.add_event(f"Monitor error: {e}")

        state.stop_event.wait(poll_interval)




# ==============================================================================
# Chaos Thread
# ==============================================================================

def chaos_loop(state, db_params, args):
    """Inject chaos events on a schedule."""
    if args.chaos == "none":
        return

    interval = args.chaos_interval

    # Wait before first chaos event
    state.stop_event.wait(interval)

    while not state.stop_event.is_set():
        if args.chaos == "worker-restart":
            chaos_worker_restart(state, db_params)
        elif args.chaos == "table-lifecycle":
            chaos_table_lifecycle(state, db_params, args)

        state.stop_event.wait(interval)


def chaos_worker_restart(state, db_params):
    """Stop worker, wait 5s, restart, wait for STREAMING."""
    state.add_event("CHAOS: Stopping worker...")
    run_sql(db_params, "SELECT duckpipe.stop_worker();")

    state.stop_event.wait(5)
    if state.stop_event.is_set():
        return

    state.add_event("CHAOS: Starting worker...")
    run_sql(db_params, "SELECT duckpipe.start_worker();")

    # Wait for tables to reach STREAMING (up to 120s)
    deadline = time.time() + 120
    while time.time() < deadline and not state.stop_event.is_set():
        statuses = get_full_status(db_params)
        if statuses and all(s['state'] == 'STREAMING' for s in statuses):
            state.add_event("CHAOS: Worker recovered, all tables STREAMING")
            return
        state.stop_event.wait(2)

    state.add_event("CHAOS: Worker recovery timeout")


def chaos_table_lifecycle(state, db_params, args):
    """Remove a random table, wait 10s, re-add it."""
    table_idx = random.randint(1, args.tables)
    table = f"public.sbtest{table_idx}"

    state.add_event(f"CHAOS: Removing table {table}...")
    run_sql(db_params, f"SELECT duckpipe.remove_table('{table}', true);")

    state.stop_event.wait(10)
    if state.stop_event.is_set():
        return

    state.add_event(f"CHAOS: Re-adding table {table}...")
    run_sql(db_params, f"SELECT duckpipe.add_table('{table}');")

    # Wait for table to reach STREAMING (up to 120s)
    deadline = time.time() + 120
    while time.time() < deadline and not state.stop_event.is_set():
        statuses = get_full_status(db_params)
        tbl_status = [s for s in statuses if s['source_table'] == table]
        if tbl_status and tbl_status[0]['state'] == 'STREAMING':
            state.add_event(f"CHAOS: {table} back to STREAMING")
            return
        state.stop_event.wait(2)

    state.add_event(f"CHAOS: {table} recovery timeout")


# ==============================================================================
# Terminal Display
# ==============================================================================

def display_loop(state, args):
    """Refresh the terminal display."""
    while not state.stop_event.is_set():
        render_display(state, args)
        state.stop_event.wait(1)


def render_display(state, args):
    """Render the status dashboard to the terminal."""
    now = time.time()
    elapsed = now - state.start_time
    elapsed_str = format_duration(elapsed)

    if args.duration > 0:
        duration_str = format_duration(args.duration)
        time_str = f"{elapsed_str} / {duration_str}"
    else:
        time_str = f"{elapsed_str} (no limit)"

    W = 64
    SEP = "=" * W

    lines = []
    lines.append(f"\033[2J\033[H")  # clear screen + cursor home

    # Header
    lines.append(f"\033[1;36m{SEP}\033[0m")
    lines.append(f"\033[1m  pg_duckpipe Soak Test  --  {time_str}\033[0m")
    lines.append(f"  Scenario: {args.scenario}   Sysbench TPS: ~{state.sysbench_tps:,.0f}")
    lines.append(f"\033[1;36m{SEP}\033[0m")

    # Pipeline
    lag_mb = state.wal_lag_bytes / 1048576
    slot_mb = state.slot_retained_wal_bytes / 1048576
    bp_str = "\033[1;31mYes\033[0m" if state.is_backpressured else "No"
    lines.append(f"\033[1;33m-- Pipeline --\033[0m")
    lines.append(f"  WAL Lag: {lag_mb:.1f} MB   Queued: {state.queued_changes:,}   Backpressure: {bp_str}")
    lines.append(f"  Slot Retained WAL: {slot_mb:.1f} MB")
    lines.append("")

    # Tables
    lines.append(f"\033[1;33m-- Tables --\033[0m")
    with state.lock:
        table_statuses = list(state.table_statuses)

    if table_statuses:
        for t in table_statuses:
            state_str = t['state']
            if state_str == 'STREAMING':
                state_color = "\033[1;32m"
            elif state_str == 'ERRORED':
                state_color = "\033[1;31m"
            elif state_str == 'SNAPSHOT':
                state_color = "\033[1;34m"
            else:
                state_color = "\033[1;33m"

            lines.append(
                f"  {t['source_table']:<20s} {state_color}{state_str:<12s}\033[0m "
                f"rows={t['rows_synced']:>10,}  queued={t['queued_changes']:>6,}  errs={t['consecutive_failures']}"
            )
    else:
        lines.append("  (no tables)")
    lines.append("")

    # Throughput
    lines.append(f"\033[1;33m-- Throughput --\033[0m")
    lines.append(
        f"  Sync: {state.sync_rate:,.0f} rows/s ({args.poll_interval}s)   "
        f"Avg: {state.avg_sync_rate:,.0f} rows/s"
    )
    peak_mb = state.peak_lag_bytes / 1048576
    avg_lag_mb = (state.lag_sample_sum / state.lag_sample_count / 1048576) if state.lag_sample_count else 0
    lines.append(f"  Peak lag: {peak_mb:.1f} MB   Avg lag: {avg_lag_mb:.1f} MB")
    mem_mb = state.total_duckdb_memory_bytes / 1048576
    lines.append(
        f"  DuckDB mem: {mem_mb:.1f} MB   "
        f"Flushes: {state.total_flush_count:,} ({state.flush_rate:.1f}/s)   "
        f"Avg flush: {state.avg_flush_duration_ms:.0f} ms"
    )
    lines.append("")

    # Consistency (final check only — mid-test mismatches are expected during active writes)
    lines.append(f"\033[1;33m-- Consistency --\033[0m")
    with state.lock:
        checks = list(state.consistency_checks)

    if checks:
        last_result = checks[-1][1]
        lines.append(f"  Final: {last_result}")
    else:
        lines.append(f"  Pending (checked after workload stops)")
    lines.append("")

    # Recent Events
    lines.append(f"\033[1;33m-- Recent Events --\033[0m")
    with state.lock:
        recent = list(state.events[-5:])
    for ts, msg in reversed(recent):
        lines.append(f"  {ts}  {msg}")
    if not recent:
        lines.append("  (none)")
    lines.append("")
    lines.append(f"\033[2m  Ctrl+C to stop gracefully\033[0m")

    sys.stdout.write("\n".join(lines) + "\n")
    sys.stdout.flush()


def format_duration(seconds):
    """Format seconds as HH:MM:SS."""
    s = int(seconds)
    return f"{s//3600:02d}:{(s%3600)//60:02d}:{s%60:02d}"


# ==============================================================================
# Prepare & Teardown
# ==============================================================================

def prepare(db_params, args):
    """Set up extensions, sysbench tables, and duckpipe table mappings."""
    print("[*] Waiting for database...")
    if not wait_for_db_ready(db_params, timeout=120):
        print("[!] Database not ready after 120s")
        sys.exit(1)

    print("[*] Setting up extensions...")
    run_sql(db_params, "CREATE EXTENSION IF NOT EXISTS pg_duckdb CASCADE;")
    run_sql(db_params, "CREATE EXTENSION IF NOT EXISTS pg_duckpipe CASCADE;")

    if not args.skip_prepare:
        print("[*] Running sysbench prepare...")
        sb_params = {
            'workload': args.workload,
            'tables': args.tables,
            'table_size': args.table_size,
            'threads': args.threads,
            'duration': 0,
        }
        # Cleanup first
        subprocess.run(
            get_sysbench_cmd(sb_params, db_params, "cleanup"),
            capture_output=True, timeout=120
        )
        # Prepare
        result = subprocess.run(
            get_sysbench_cmd(sb_params, db_params, "prepare"),
            capture_output=True, text=True, timeout=600
        )
        if result.returncode != 0:
            print(f"[!] Sysbench prepare failed:\n{result.stderr}")
            sys.exit(1)
        print(f"[*] Prepared {args.tables} tables with {args.table_size} rows each")

        # Clean stale mappings, then add tables
        print("[*] Configuring duckpipe table mappings...")
        for i in range(1, args.tables + 1):
            table = f"public.sbtest{i}"
            run_sql(db_params, f"SELECT duckpipe.remove_table('{table}', true);")
        for i in range(1, args.tables + 1):
            table = f"public.sbtest{i}"
            res = run_sql(db_params, f"SELECT duckpipe.add_table('{table}');")
            if res is None:
                print(f"[!] Failed to add table {table}")
                sys.exit(1)

        # Wait for all tables to reach STREAMING
        print("[*] Waiting for initial snapshot sync...")
        deadline = time.time() + 600
        nudge_sent = False
        while time.time() < deadline:
            statuses = get_full_status(db_params)
            if statuses and all(s['state'] == 'STREAMING' for s in statuses):
                print("[*] All tables STREAMING")
                break
            # If all tables finished snapshot (CATCHUP) but none are STREAMING,
            # emit a WAL message to nudge the CATCHUP->STREAMING transition.
            if (statuses and not nudge_sent
                    and all(s['state'] in ('CATCHUP', 'STREAMING') for s in statuses)
                    and any(s['state'] == 'CATCHUP' for s in statuses)):
                run_sql(db_params,
                        "SELECT pg_logical_emit_message(true, 'duckpipe_soak', 'catchup_trigger')",
                        timeout=5)
                nudge_sent = True
            time.sleep(2)
        else:
            print("[!] Timeout waiting for STREAMING state")
            sys.exit(1)


def final_consistency_check(state, db_params, args, get_queued=None):
    """Wait for catch-up and run a final consistency check.

    get_queued: callable returning current queued change count.
                Defaults to SQL-based get_total_queued_bytes(db_params).
    """
    if get_queued is None:
        get_queued = lambda: get_total_queued_bytes(db_params)

    state.add_event("Final: waiting for catch-up...")

    deadline = time.time() + 300
    while time.time() < deadline:
        if get_queued() == 0:
            break
        time.sleep(2)

    # Give a brief settle time
    time.sleep(5)

    state.add_event("Final: running consistency check...")
    mismatches = verify_table_consistency_full(db_params, args.tables)
    result = "PASS" if not mismatches else f"FAIL ({len(mismatches)} mismatches)"

    with state.lock:
        state.consistency_checks.append(("final", result, mismatches))

    state.add_event(f"Final consistency: {result}")
    return result, mismatches


# ==============================================================================
# Main
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="pg_duckpipe Soak Test Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--db-url",
        default=os.environ.get("DB_URL", "host=localhost port=5432 user=postgres dbname=postgres"))
    parser.add_argument("--scenario",
        default=os.environ.get("SOAK_SCENARIO", "sustained-insert"),
        choices=list(SCENARIOS.keys()))
    parser.add_argument("--duration", type=int,
        default=int(os.environ.get("SOAK_DURATION", "3600")),
        help="0=infinite (default: 3600)")
    parser.add_argument("--workload")
    parser.add_argument("--tables", type=int)
    parser.add_argument("--table-size", type=int, default=100000)
    parser.add_argument("--threads", type=int)
    parser.add_argument("--poll-interval", type=int, default=5)
    parser.add_argument("--consistency-interval", type=int, default=300)
    parser.add_argument("--output-dir")
    parser.add_argument("--skip-prepare", action="store_true")
    parser.add_argument("--chaos", choices=["none", "worker-restart", "table-lifecycle"])
    parser.add_argument("--chaos-interval", type=int, default=120)

    args = parser.parse_args()

    # Apply scenario defaults for fields not explicitly set
    scenario = SCENARIOS[args.scenario]
    if args.workload is None:
        args.workload = scenario['workload']
    if args.tables is None:
        args.tables = scenario['tables']
    if args.threads is None:
        args.threads = scenario['threads']
    if args.chaos is None:
        args.chaos = scenario['chaos']

    # Output directory
    if args.output_dir is None:
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        args.output_dir = os.path.join("soak_results", f"{args.scenario}_{ts}")
    os.makedirs(args.output_dir, exist_ok=True)

    db_params = parse_db_url(args.db_url)

    # Prepare
    prepare(db_params, args)

    # Open CSV
    csv_path = os.path.join(args.output_dir, "metrics.csv")
    csv_file = open(csv_path, 'w', newline='')
    csv_writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDS)
    csv_writer.writeheader()

    # Events log
    events_path = os.path.join(args.output_dir, "events.log")
    events_file = open(events_path, 'w')

    # Save config
    config_path = os.path.join(args.output_dir, "config.txt")
    with open(config_path, 'w') as f:
        for k, v in vars(args).items():
            f.write(f"{k}={v}\n")

    # Shared state
    state = SoakState()

    # Signal handler
    def handle_signal(signum, frame):
        state.add_event("Received shutdown signal")
        state.stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # Start threads
    threads = []

    # Monitor thread
    monitor_thread = threading.Thread(
        target=monitor_loop, args=(state, db_params, args, csv_writer, csv_file),
        daemon=True, name="monitor"
    )
    monitor_thread.start()
    threads.append(monitor_thread)

    # Chaos thread
    if args.chaos != "none":
        chaos_thread = threading.Thread(
            target=chaos_loop, args=(state, db_params, args),
            daemon=True, name="chaos"
        )
        chaos_thread.start()
        threads.append(chaos_thread)

    # Display thread
    display_thread = threading.Thread(
        target=display_loop, args=(state, args),
        daemon=True, name="display"
    )
    display_thread.start()
    threads.append(display_thread)

    # Run sysbench (blocks until done or stopped)
    state.add_event(f"Soak test started: {args.scenario}")
    sysbench_proc = run_sysbench_continuous(state, db_params, args)

    # If duration expired naturally (not Ctrl+C), trigger shutdown
    if not state.stop_event.is_set():
        state.add_event("Duration expired, shutting down...")
        state.stop_event.set()

    # Graceful shutdown
    print("\n\n[*] Shutting down...")

    # Final consistency check
    final_result, final_mismatches = final_consistency_check(state, db_params, args)

    # Write events log
    with state.lock:
        for ts, msg in state.events:
            events_file.write(f"{ts}  {msg}\n")
    events_file.close()
    csv_file.close()

    # Generate report
    try:
        from soak_report import generate_report
        report_path = os.path.join(args.output_dir, "report.md")
        generate_report(csv_path, events_path, config_path, report_path)
        print(f"[*] Report: {report_path}")
    except ImportError:
        print("[*] soak_report.py not available, skipping report generation")

    # Print summary
    elapsed = time.time() - state.start_time
    print(f"\n{'=' * 60}")
    print(f"  Soak Test Complete")
    print(f"{'=' * 60}")
    print(f"  Scenario     : {args.scenario}")
    print(f"  Duration     : {format_duration(elapsed)}")
    print(f"  Total Rows   : {state.total_rows_synced:,}")
    print(f"  Avg Sync Rate: {state.avg_sync_rate:,.0f} rows/s")
    print(f"  Peak Lag     : {state.peak_lag_bytes / 1048576:.1f} MB")
    print(f"  DuckDB Memory: {state.total_duckdb_memory_bytes / 1048576:.1f} MB")
    print(f"  Total Flushes: {state.total_flush_count:,}")
    print(f"  Consistency  : {final_result}")
    print(f"  Final Check  : {final_result}")
    print(f"  Results Dir  : {args.output_dir}")
    print(f"{'=' * 60}")

    # Exit with error if final check failed
    if "FAIL" in final_result:
        sys.exit(1)


if __name__ == "__main__":
    main()

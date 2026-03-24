#!/usr/bin/env python3
"""pg_duckpipe Daemon Soak Test Orchestrator.

Like soak_test.py but uses the daemon's REST API for group/table management
and monitoring instead of SQL functions.

Usage:
  python3 soak_test_daemon.py [options]
  # Or via Docker Compose:
  docker compose -f benchmark/soak/docker-compose.soak-daemon.yml up
"""
import argparse
import csv
import datetime
import json
import os
import signal
import subprocess
import sys
import threading
import time
import urllib.request
import urllib.error

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lib import (
    run_sql,
    parse_db_url,
    get_sysbench_cmd,
    get_total_lag_bytes,
    get_wal_slot_size_bytes,
    wait_for_db_ready,
)
from soak_test import (
    SoakState,
    SCENARIOS,
    CSV_FIELDS,
    aggregate_shm_metrics,
    update_state_metrics,
    build_metrics_row,
    run_sysbench_continuous,
    consistency_loop,
    display_loop,
    format_duration,
    final_consistency_check,
)

# ==============================================================================
# HTTP Helpers
# ==============================================================================

def http_get(url, timeout=30):
    """HTTP GET, return parsed JSON or None."""
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except Exception:
        return None


def http_post(url, data=None, timeout=30):
    """HTTP POST with JSON body, return (status_code, parsed JSON or None)."""
    try:
        body = json.dumps(data).encode() if data else None
        req = urllib.request.Request(url, data=body, method='POST')
        if body:
            req.add_header('Content-Type', 'application/json')
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        resp_body = e.read().decode() if e.fp else ''
        try:
            return e.code, json.loads(resp_body)
        except (json.JSONDecodeError, ValueError):
            return e.code, {'error': resp_body}
    except Exception as e:
        return 0, {'error': str(e)}


# ==============================================================================
# Daemon API Wrappers
# ==============================================================================

def wait_for_daemon(daemon_url, timeout=120):
    """Poll daemon /health until ready."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        data = http_get(f"{daemon_url}/health", timeout=5)
        if data and data.get('status') == 'ok':
            return True
        time.sleep(1)
    return False


def daemon_get_status(daemon_url):
    """GET /status, return parsed JSON."""
    return http_get(f"{daemon_url}/status")


def daemon_get_metrics(daemon_url):
    """GET /metrics, return parsed JSON."""
    return http_get(f"{daemon_url}/metrics")


# ==============================================================================
# Prepare (Daemon Mode)
# ==============================================================================

def add_table_manual(db_params, source_schema, source_table, group_name='default', copy_data=True):
    """Manually add a table to duckpipe, splitting DuckDB and PG writes into separate transactions.

    This works around the pg_duckdb restriction that blocks DuckDB + PG writes
    in the same transaction (which add_table() does internally via SPI).
    """
    target_table = f"{source_table}_ducklake"
    fqn = f"{source_schema}.{source_table}"

    # 1. Get group info
    group_info = run_sql(db_params,
        f"SELECT publication, slot_name FROM duckpipe.sync_groups WHERE name = '{group_name}'")
    if not group_info:
        print(f"[!] Group '{group_name}' not found")
        return False
    publication, slot_name = group_info.split('|')

    # 2. Create replication slot + publication (if needed), or alter publication
    pub_exists = run_sql(db_params,
        f"SELECT 1 FROM pg_publication WHERE pubname = '{publication}'")
    if not pub_exists:
        run_sql(db_params,
            f"SELECT pg_create_logical_replication_slot('{slot_name}', 'pgoutput')")
        run_sql(db_params,
            f"CREATE PUBLICATION {publication} FOR TABLE {fqn}")
    else:
        run_sql(db_params,
            f"ALTER PUBLICATION {publication} ADD TABLE {fqn}")

    # 3. REPLICA IDENTITY FULL
    run_sql(db_params, f"ALTER TABLE {fqn} REPLICA IDENTITY FULL")

    # 4. Get source OID
    source_oid = run_sql(db_params,
        f"SELECT c.oid FROM pg_class c "
        f"JOIN pg_namespace n ON n.oid = c.relnamespace "
        f"WHERE n.nspname = '{source_schema}' AND c.relname = '{source_table}'")

    # 5. Get column definitions
    cols = run_sql(db_params,
        f"SELECT a.attname || '|' || pg_catalog.format_type(a.atttypid, a.atttypmod) "
        f"FROM pg_class c "
        f"JOIN pg_namespace n ON n.oid = c.relnamespace "
        f"JOIN pg_attribute a ON a.attrelid = c.oid "
        f"WHERE n.nspname = '{source_schema}' AND c.relname = '{source_table}' "
        f"AND a.attnum > 0 AND NOT a.attisdropped "
        f"ORDER BY a.attnum")
    if not cols:
        print(f"[!] No columns found for {fqn}")
        return False

    col_clauses = []
    for line in cols.strip().split('\n'):
        name, type_str = line.split('|', 1)
        col_clauses.append(f"{name} {type_str}")

    # 6. CREATE ducklake target table (DuckDB write — separate transaction)
    create_sql = (
        f"CREATE TABLE IF NOT EXISTS {source_schema}.{target_table} "
        f"({', '.join(col_clauses)}) USING ducklake"
    )
    result = run_sql(db_params, create_sql)
    if result is None:
        print(f"[!] Failed to create ducklake target for {fqn}")
        return False

    # 7. INSERT table mapping (PG write — separate transaction)
    initial_state = 'SNAPSHOT' if copy_data else 'STREAMING'
    insert_sql = (
        f"INSERT INTO duckpipe.table_mappings "
        f"(group_id, source_schema, source_table, target_schema, target_table, state, source_oid) "
        f"SELECT sg.id, '{source_schema}', '{source_table}', "
        f"'{source_schema}', '{target_table}', '{initial_state}', {source_oid} "
        f"FROM duckpipe.sync_groups sg WHERE sg.name = '{group_name}'"
    )
    result = run_sql(db_params, insert_sql)
    if result is None:
        print(f"[!] Failed to insert table mapping for {fqn}")
        return False

    return True


def prepare_daemon(db_params, daemon_url, args):
    """Set up extensions, sysbench tables, and duckpipe via daemon REST API."""
    print("[*] Waiting for database...")
    if not wait_for_db_ready(db_params, timeout=120):
        print("[!] Database not ready after 120s")
        sys.exit(1)

    print("[*] Waiting for daemon API...")
    if not wait_for_daemon(daemon_url, timeout=120):
        print("[!] Daemon API not ready after 120s")
        sys.exit(1)
    health = http_get(f"{daemon_url}/health")
    print(f"[*] Daemon health: {health}")

    # Extensions are created by DB init scripts (Z01-install-pg_duckpipe.sql).
    # The daemon pre-binds to the "default" group via --group default.
    # Verify the group is bound.
    status = daemon_get_status(daemon_url)
    if not status or status.get('group') != 'default':
        print(f"[!] Daemon not bound to 'default' group. Status: {status}")
        sys.exit(1)
    print("[*] Daemon bound to 'default' group")

    if not args.skip_prepare:
        print("[*] Running sysbench prepare...")
        sb_params = {
            'workload': args.workload,
            'tables': args.tables,
            'table_size': args.table_size,
            'threads': args.threads,
            'duration': 0,
        }
        subprocess.run(
            get_sysbench_cmd(sb_params, db_params, "cleanup"),
            capture_output=True, timeout=120
        )
        result = subprocess.run(
            get_sysbench_cmd(sb_params, db_params, "prepare"),
            capture_output=True, text=True, timeout=600
        )
        if result.returncode != 0:
            print(f"[!] Sysbench prepare failed:\n{result.stderr}")
            sys.exit(1)
        print(f"[*] Prepared {args.tables} tables with {args.table_size} rows each")

        # Add tables using manual method (avoids pg_duckdb cross-write restriction)
        print("[*] Adding tables (manual split-transaction method)...")
        for i in range(1, args.tables + 1):
            if not add_table_manual(db_params, 'public', f'sbtest{i}'):
                sys.exit(1)
            print(f"  Added public.sbtest{i}")

        # Wait for all tables to reach STREAMING (poll via daemon API)
        print("[*] Waiting for initial snapshot sync...")
        deadline = time.time() + 600
        nudge_sent = False
        while time.time() < deadline:
            status = daemon_get_status(daemon_url)
            if status and 'tables' in status:
                tables = status['tables']
                states = [t.get('state', '') for t in tables]
                if tables and all(s == 'STREAMING' for s in states):
                    print("[*] All tables STREAMING")
                    break
                # Nudge CATCHUP->STREAMING transition
                if (not nudge_sent
                        and all(s in ('CATCHUP', 'STREAMING') for s in states)
                        and any(s == 'CATCHUP' for s in states)):
                    run_sql(db_params,
                            "SELECT pg_logical_emit_message(true, 'duckpipe_soak', 'catchup_trigger')",
                            timeout=5)
                    nudge_sent = True
            time.sleep(2)
        else:
            print("[!] Timeout waiting for STREAMING state")
            status = daemon_get_status(daemon_url)
            if status:
                for t in status.get('tables', []):
                    print(f"  {t.get('source_table')}: {t.get('state')} err={t.get('error_message')}")
            sys.exit(1)


# ==============================================================================
# Monitor Thread (Daemon Mode)
# ==============================================================================

def monitor_loop_daemon(state, db_params, daemon_url, args, csv_writer, csv_file):
    """Collect metrics via daemon REST API + WAL lag from SQL."""
    poll_interval = args.poll_interval

    # Initialize prev_rows from daemon status
    status = daemon_get_status(daemon_url)
    if status and 'tables' in status:
        init_rows = sum(t.get('rows_synced', 0) for t in status['tables'])
        state.prev_rows_synced = init_rows

    while not state.stop_event.is_set():
        try:
            now = time.time()
            elapsed = now - state.start_time

            # --- Daemon API: GET /status ---
            status = daemon_get_status(daemon_url)
            tables = status.get('tables', []) if status else []
            worker = status.get('worker') or {} if status else {}

            total_rows = sum(t.get('rows_synced', 0) for t in tables)
            queued = worker.get('total_queued_changes', 0) if worker else 0
            bp = worker.get('is_backpressured', False) if worker else False

            table_statuses = []
            for t in tables:
                table_statuses.append({
                    'source_table': t.get('source_table', ''),
                    'state': t.get('state', 'UNKNOWN'),
                    'rows_synced': t.get('rows_synced', 0),
                    'queued_changes': t.get('queued_changes', 0),
                    'consecutive_failures': t.get('consecutive_failures', 0),
                })

            # --- Daemon API: GET /metrics ---
            total_mem, total_flushes, flush_durations = aggregate_shm_metrics(
                daemon_get_metrics(daemon_url)
            )

            # --- SQL: WAL lag + slot size (only 2 SQL calls) ---
            wal_lag = get_total_lag_bytes(db_params)
            slot_wal = get_wal_slot_size_bytes(db_params)

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
# Final Consistency (Daemon Mode)
# ==============================================================================

def _daemon_get_queued(daemon_url):
    """Get queued change count via daemon REST API."""
    status = daemon_get_status(daemon_url)
    if status:
        worker = (status.get('worker') or {})
        return worker.get('total_queued_changes', 0)
    return 1  # non-zero to keep waiting


# ==============================================================================
# Main
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="pg_duckpipe Daemon Soak Test (REST API mode)",
    )
    parser.add_argument("--db-url",
        default=os.environ.get("DB_URL", "host=localhost port=5432 user=postgres dbname=postgres"))
    parser.add_argument("--daemon-url",
        default=os.environ.get("DAEMON_URL", "http://localhost:8080"))
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

    args = parser.parse_args()

    # Apply scenario defaults
    scenario = SCENARIOS[args.scenario]
    if args.workload is None:
        args.workload = scenario['workload']
    if args.tables is None:
        args.tables = scenario['tables']
    if args.threads is None:
        args.threads = scenario['threads']

    # Set chaos to none (daemon mode doesn't support chaos yet)
    args.chaos = "none"

    # Output directory
    if args.output_dir is None:
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        args.output_dir = os.path.join("soak_results", f"daemon-{args.scenario}_{ts}")
    os.makedirs(args.output_dir, exist_ok=True)

    db_params = parse_db_url(args.db_url)
    daemon_url = args.daemon_url.rstrip('/')

    # Prepare
    prepare_daemon(db_params, daemon_url, args)

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
        f.write("mode=daemon\n")
        f.write(f"daemon_url={daemon_url}\n")
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

    # Monitor thread (daemon mode — uses REST API)
    monitor_thread = threading.Thread(
        target=monitor_loop_daemon,
        args=(state, db_params, daemon_url, args, csv_writer, csv_file),
        daemon=True, name="monitor"
    )
    monitor_thread.start()
    threads.append(monitor_thread)

    # Consistency thread (uses psql for row counts)
    consistency_thread = threading.Thread(
        target=consistency_loop, args=(state, db_params, args),
        daemon=True, name="consistency"
    )
    consistency_thread.start()
    threads.append(consistency_thread)

    # Display thread
    display_thread = threading.Thread(
        target=display_loop, args=(state, args),
        daemon=True, name="display"
    )
    display_thread.start()
    threads.append(display_thread)

    # Run sysbench (blocks until done or stopped)
    state.add_event(f"Daemon soak test started: {args.scenario}")
    run_sysbench_continuous(state, db_params, args)

    # If duration expired naturally
    if not state.stop_event.is_set():
        state.add_event("Duration expired, shutting down...")
        state.stop_event.set()

    # Graceful shutdown
    print("\n\n[*] Shutting down...")

    # Final consistency check (via daemon API for queue drain)
    final_result, final_mismatches = final_consistency_check(
        state, db_params, args,
        get_queued=lambda: _daemon_get_queued(daemon_url),
    )

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
    print(f"  Daemon Soak Test Complete")
    print(f"{'=' * 60}")
    print(f"  Mode         : daemon (REST API)")
    print(f"  Scenario     : {args.scenario}")
    print(f"  Duration     : {format_duration(elapsed)}")
    print(f"  Total Rows   : {state.total_rows_synced:,}")
    print(f"  Avg Sync Rate: {state.avg_sync_rate:,.0f} rows/s")
    print(f"  Peak Lag     : {state.peak_lag_bytes / 1048576:.1f} MB")
    print(f"  DuckDB Memory: {state.total_duckdb_memory_bytes / 1048576:.1f} MB")
    print(f"  Total Flushes: {state.total_flush_count:,}")
    with state.lock:
        total_checks = len(state.consistency_checks)
        passed_checks = sum(1 for _, r, _ in state.consistency_checks if r == "PASS")
    print(f"  Consistency  : {passed_checks}/{total_checks} passed")
    print(f"  Final Check  : {final_result}")
    print(f"  Results Dir  : {args.output_dir}")
    print(f"{'=' * 60}")

    if "FAIL" in final_result:
        sys.exit(1)


if __name__ == "__main__":
    main()

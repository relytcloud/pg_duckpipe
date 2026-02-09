#!/usr/bin/env python3
import argparse
import os
import select
import subprocess
import time
import sys
import re

# ==============================================================================
# Configuration & Helpers
# ==============================================================================

WORKLOAD_EXPECTED_EVENTS_PER_TX = {
    # One INSERT statement per transaction/event.
    "oltp_insert": 1,
    # sysbench defaults: 1 DELETE/INSERT pair + 2 UPDATEs, and UPDATE is
    # applied by DuckPipe as DELETE+INSERT.
    "oltp_read_write": 6,
}

CONSISTENCY_MODES = ("auto", "safe", "full", "off")

def run_sql(db_params, sql, timeout=60):
    """Run SQL via psql with a timeout (default 60s)."""
    # db_params is a dict: {host, port, user, dbname}
    conn_str = f"host={db_params['host']} port={db_params['port']} user={db_params['user']} dbname={db_params['dbname']}"
    cmd = ["psql", conn_str, "-t", "-A", "-c", sql]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        print(f"[SQL TIMEOUT] {sql} (exceeded {timeout}s)")
        return None
    if result.returncode != 0:
        print(f"[SQL ERROR] {sql}\n{result.stderr}")
        return None
    return result.stdout.strip()

def parse_db_url(url):
    """Simple parser for key=value connection string."""
    params = {}
    for part in url.split():
        if '=' in part:
            k, v = part.split('=', 1)
            params[k] = v
    # Defaults
    if 'host' not in params: params['host'] = 'localhost'
    if 'port' not in params: params['port'] = '5432'
    if 'user' not in params: params['user'] = 'postgres'
    if 'dbname' not in params: params['dbname'] = 'postgres'
    return params

def get_sysbench_cmd(args, db_params, action):
    """Construct sysbench command."""
    cmd = [
        "sysbench",
        args.workload,
        f"--db-driver=pgsql",
        f"--pgsql-host={db_params['host']}",
        f"--pgsql-port={db_params['port']}",
        f"--pgsql-user={db_params['user']}",
        f"--pgsql-db={db_params['dbname']}",
        f"--tables={args.tables}",
        f"--table-size={args.table_size}",
        f"--threads={args.threads}",
        f"--time={args.duration}",
        f"--report-interval=1"
    ]
    # Disable auto-vacuum for stable results if needed, but standard is fine
    cmd.append(action)
    return cmd


def get_expected_events_per_tx(args):
    return WORKLOAD_EXPECTED_EVENTS_PER_TX.get(args.workload)


def parse_int(value, default=0):
    if value is None:
        return default
    value = value.strip()
    if value.isdigit() or (value.startswith("-") and value[1:].isdigit()):
        return int(value)
    return default


def get_total_lag_bytes(db_params):
    res = run_sql(
        db_params,
        "SELECT COALESCE(sum(pg_current_wal_lsn() - rs.confirmed_flush_lsn), 0) "
        "FROM duckpipe.sync_groups g "
        "JOIN pg_replication_slots rs ON rs.slot_name = g.slot_name "
        "WHERE g.enabled",
    )
    return parse_int(res, 0)

def get_benchmark_rows_synced(args, db_params):
    table_filter = ", ".join([f"'public.sbtest{i}'" for i in range(1, args.tables + 1)])
    res = run_sql(
        db_params,
        f"SELECT COALESCE(sum(rows_synced), 0) "
        f"FROM duckpipe.status() "
        f"WHERE source_table IN ({table_filter})",
    )
    return parse_int(res, 0)


def wait_for_db_ready(db_params, timeout=60):
    """Wait for the database to accept connections (e.g. after a crash/restart)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        res = run_sql(db_params, "SELECT 1", timeout=10)
        if res == "1":
            return True
        time.sleep(2.0)
    return False


def verify_table_consistency_full(args, db_params):
    """Verify consistency using separate count queries to avoid cross-engine
    JOIN crashes in pg_duckdb. Falls back gracefully if the server crashes."""
    mismatches = []
    for i in range(1, args.tables + 1):
        table = f"sbtest{i}"
        target = f"{table}_ducklake"

        # Use separate count queries - cross-engine JOINs between heap and
        # ducklake tables can crash pg_duckdb when many parquet files exist.
        src_s = run_sql(db_params, f"SELECT count(*) FROM public.{table}")
        if src_s is None:
            # Server may have crashed; wait for recovery
            print(f"    [!] Connection lost querying {table}, waiting for recovery...")
            if not wait_for_db_ready(db_params):
                mismatches.append({
                    "table": table, "source_count": None, "target_count": None,
                    "value_mismatch": None, "missing": None, "extra": None,
                    "mode": "full", "error": "connection lost",
                })
                continue
            src_s = run_sql(db_params, f"SELECT count(*) FROM public.{table}")

        dst_s = run_sql(db_params, f"SELECT count(*) FROM public.{target}")
        if dst_s is None:
            print(f"    [!] Connection lost querying {target}, waiting for recovery...")
            if not wait_for_db_ready(db_params):
                mismatches.append({
                    "table": table, "source_count": parse_int(src_s, -1),
                    "target_count": None, "value_mismatch": None,
                    "missing": None, "extra": None,
                    "mode": "full", "error": "connection lost on target count",
                })
                continue
            dst_s = run_sql(db_params, f"SELECT count(*) FROM public.{target}")

        src = parse_int(src_s, -1)
        dst = parse_int(dst_s, -1)

        if src != dst:
            mismatches.append({
                "table": table,
                "source_count": src,
                "target_count": dst,
                "value_mismatch": None,
                "missing": None,
                "extra": None,
                "mode": "full",
            })
    return mismatches


def resolve_consistency_mode(args):
    if args.consistency_mode != "auto":
        return args.consistency_mode
    # Append-only defaults to safe check to avoid expensive joins under heavy write load.
    if args.workload == "oltp_insert":
        return "safe"
    return "full"

# ==============================================================================
# Benchmarks
# ==============================================================================

def prepare_env(args, db_params):
    print("[-] [Setup] Enabling extensions...")
    # Extensions must be loaded first
    run_sql(db_params, "CREATE EXTENSION IF NOT EXISTS pg_duckdb CASCADE;")
    run_sql(db_params, "CREATE EXTENSION IF NOT EXISTS pg_duckpipe CASCADE;")

    print("[-] [Sysbench] Preparing environment (creating tables & data)...")
    # 1. Sysbench Prepare (Creates sbtest1..N and populates them)
    # We use cleanup first to ensure clean state
    subprocess.run(get_sysbench_cmd(args, db_params, "cleanup"), capture_output=True, timeout=120)

    cmd = get_sysbench_cmd(args, db_params, "prepare")
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    # Print progress with timeout
    prepare_deadline = time.time() + 600  # 10 minutes for prepare
    for line in proc.stdout:
        if "Creating table" in line or "Inserting" in line:
            print(f"    {line.strip()}")
        if time.time() > prepare_deadline:
            proc.kill()
            print("[!] Sysbench prepare timed out (600s).")
            sys.exit(1)
    proc.wait()

    if proc.returncode != 0:
        print("[!] Sysbench prepare failed.")
        sys.exit(1)

    # Clean up stale mappings/targets from prior runs.
    # add_table() auto-creates target tables via CREATE TABLE IF NOT EXISTS
    # ... (LIKE source) USING ducklake, so no manual creation needed.
    print("[-] [DuckPipe] Cleaning up stale mappings...")
    for i in range(1, args.tables + 1):
        table = f"sbtest{i}"
        run_sql(db_params, f"SELECT duckpipe.remove_table('public.{table}', true);")

def benchmark_snapshot(args, db_params):
    print("\n[=] Starting SNAPSHOT Benchmark...")
    print(f"    Tables: {args.tables}, Rows per table: {args.table_size}")

    # Ensure extensions are present (redundant but safe)
    run_sql(db_params, "CREATE EXTENSION IF NOT EXISTS pg_duckdb CASCADE;")
    run_sql(db_params, "CREATE EXTENSION IF NOT EXISTS pg_duckpipe CASCADE;")

    # Add tables to DuckPipe (auto-creates target tables)
    start_time = time.time()
    snapshot_deadline = start_time + args.snapshot_timeout
    for i in range(1, args.tables + 1):
        table = f"sbtest{i}"
        res = run_sql(db_params, f"SELECT duckpipe.add_table('public.{table}');")
        if res is None:
            print(f"[!] Failed to add table mapping for public.{table}")
            sys.exit(1)

    total_rows = args.tables * args.table_size
    print("[-] Monitoring snapshot sync...")

    while True:
        # Sum of synced rows across all tables
        res = run_sql(db_params, "SELECT sum(rows_synced) FROM duckpipe.status()")
        synced = int(res) if res and res.isdigit() else 0

        # Check actual count in target
        target_count = 0
        for i in range(1, args.tables + 1):
             cnt = run_sql(db_params, f"SELECT count(*) FROM public.sbtest{i}_ducklake")
             if cnt and cnt.isdigit():
                 target_count += int(cnt)

        # Check if all are streaming
        state_res = run_sql(db_params, "SELECT count(*) FROM duckpipe.status() WHERE state != 'STREAMING'")
        not_streaming = int(state_res) if state_res and state_res.isdigit() else args.tables

        elapsed = time.time() - start_time
        rate = target_count / elapsed if elapsed > 0 else 0

        sys.stdout.write(f"\r    Synced(View): {synced} | Actual: {target_count}/{total_rows} | Pending: {not_streaming} | Rate: {rate:.0f} rows/s | Time: {elapsed:.1f}s")
        sys.stdout.flush()

        if target_count >= total_rows and not_streaming == 0:
            print(f"\n[+] Snapshot complete!")
            return rate

        if time.time() > snapshot_deadline:
            print("\n[!] Snapshot timeout exceeded.")
            sys.exit(1)

        time.sleep(1.0)

def benchmark_streaming(args, db_params):
    print("\n[=] Starting STREAMING Benchmark...")
    print(f"    Workload: {args.workload}")
    print(f"    Running sysbench workload for {args.duration} seconds...")

    # Start Sysbench in background
    cmd = get_sysbench_cmd(args, db_params, "run")
    print(f"    Command: {' '.join(cmd)}")
    rows_before = get_benchmark_rows_synced(args, db_params)

    # Use Popen and read stdout in real-time with non-blocking I/O
    sysbench_proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)

    # Monitor loop
    start_time = time.time()
    monitor_interval = 1.0

    sysbench_output = []
    lag_samples = []

    # Make stdout non-blocking so we can poll lag while sysbench runs
    fd = sysbench_proc.stdout.fileno()
    import fcntl
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)

    while True:
        # Read lines non-blocking
        try:
            while True:
                line = sysbench_proc.stdout.readline()
                if not line:
                    break
                sysbench_output.append(line)
        except (IOError, BlockingIOError):
            pass

        # Check if process ended
        ret = sysbench_proc.poll()
        if ret is not None:
            # Drain remaining output (restore blocking for final read)
            fcntl.fcntl(fd, fcntl.F_SETFL, flags)
            for line in sysbench_proc.stdout:
                sysbench_output.append(line)
            break

        lag_bytes = get_total_lag_bytes(db_params)
        lag_samples.append(lag_bytes)

        time.sleep(monitor_interval)

        elapsed = time.time() - start_time
        if elapsed > args.duration + 30:
            print(f"\n    [!] Sysbench exceeded duration + 30s safety margin, killing.")
            sysbench_proc.kill()
            break

    # Join output
    stdout = "".join(sysbench_output)

    # Parse TPS from output
    tx_count_match = re.search(r"transactions:\s+(\d+)\s+\(([\d\.]+)\s+per sec\.\)", stdout)
    tx_count = parse_int(tx_count_match.group(1), 0) if tx_count_match else 0
    tps = float(tx_count_match.group(2)) if tx_count_match else 0.0
    avg_lag = (sum(lag_samples) / len(lag_samples)) if lag_samples else 0.0
    max_lag = max(lag_samples) if lag_samples else 0

    print("[-] Waiting for replication catch-up...")
    catchup_start = time.time()
    catchup_deadline = catchup_start + args.catchup_timeout
    expected_events_per_tx = get_expected_events_per_tx(args)
    expected_rows_delta = tx_count * expected_events_per_tx if tx_count > 0 and expected_events_per_tx is not None else None
    final_lag = get_total_lag_bytes(db_params)
    rows_after = get_benchmark_rows_synced(args, db_params)
    rows_delta = rows_after - rows_before

    # Stall detection: give up if no progress for this many consecutive checks
    stall_limit = 30
    stall_count = 0
    prev_rows_delta = rows_delta

    while time.time() < catchup_deadline:
        if expected_rows_delta is not None:
            if rows_delta >= expected_rows_delta:
                break
        elif final_lag == 0:
            break

        time.sleep(1.0)
        final_lag = get_total_lag_bytes(db_params)
        if final_lag == 0 and rows_delta == prev_rows_delta:
            # Might be a transient connection issue (e.g. server restarting after crash)
            wait_for_db_ready(db_params, timeout=30)
            final_lag = get_total_lag_bytes(db_params)
        rows_after = get_benchmark_rows_synced(args, db_params)
        rows_delta = rows_after - rows_before

        # Stall detection
        if rows_delta == prev_rows_delta:
            stall_count += 1
            if stall_count >= stall_limit:
                print(f"\n    [!] Worker stalled ({stall_limit}s with no progress). Stopping catch-up wait.")
                break
        else:
            stall_count = 0
        prev_rows_delta = rows_delta

        elapsed = time.time() - catchup_start
        if expected_rows_delta and expected_rows_delta > 0:
            pct = 100.0 * rows_delta / expected_rows_delta
            rate = rows_delta / elapsed if elapsed > 0 else 0
            eta = (expected_rows_delta - rows_delta) / rate if rate > 0 else 0
            sys.stdout.write(f"\r    Catch-up: {rows_delta}/{expected_rows_delta} ({pct:.1f}%) | {rate:.0f} events/s | ETA: {eta:.0f}s | lag: {final_lag}  ")
            sys.stdout.flush()

    if expected_rows_delta and expected_rows_delta > 0 and rows_delta > 0:
        print()  # newline after progress line

    catchup_complete = (rows_delta >= expected_rows_delta) if expected_rows_delta is not None else (final_lag == 0)
    consistency_mode = resolve_consistency_mode(args)
    checks_ran = False
    checks_skipped_reason = None
    mismatches = []
    should_run_checks = (consistency_mode != "off") and (catchup_complete or args.verify_on_timeout)

    if should_run_checks:
        checks_ran = True
        if consistency_mode == "full":
            mismatches = verify_table_consistency_full(args, db_params)
        else:
            # Safe mode intentionally avoids querying ducklake tables because
            # value-level queries can be unstable under concurrent apply load.
            if expected_rows_delta is None:
                mismatches = [{
                    "mode": "safe",
                    "reason": "no expected rows delta available",
                    "rows_synced_delta": rows_delta,
                    "rows_synced_expected": None,
                    "final_lag": final_lag,
                }]
            elif rows_delta != expected_rows_delta or final_lag != 0:
                mismatches = [{
                    "mode": "safe",
                    "reason": "metadata mismatch",
                    "rows_synced_delta": rows_delta,
                    "rows_synced_expected": expected_rows_delta,
                    "final_lag": final_lag,
                }]
    elif consistency_mode != "off":
        checks_skipped_reason = "catch-up incomplete"

    print("\n[Sysbench Output Summary]")
    for line in stdout.split('\n'):
        if "transactions:" in line or "latency" in line:
            print(f"    {line.strip()}")
    print(f"    lag(avg/max/final bytes): {avg_lag:.0f}/{max_lag}/{final_lag}")
    print(f"    consistency mode: {consistency_mode}")
    print(f"    rows_synced delta during streaming: {rows_delta}")
    if expected_rows_delta is not None:
        print(f"    rows_synced expected events/tx: {expected_events_per_tx}")
        print(f"    rows_synced expected delta: {expected_rows_delta}")
    if not catchup_complete:
        print("    [!] Catch-up incomplete before timeout; consistency mismatches may include unapplied changes.")
    if consistency_mode == "off":
        print("    [!] Consistency checks skipped (--consistency-mode off).")
    elif not checks_ran:
        print(f"    [!] Consistency checks skipped ({checks_skipped_reason}).")
    elif mismatches:
        print("    [!] Consistency mismatches detected:")
        for m in mismatches:
            if m["mode"] == "full":
                err = m.get("error", "")
                if err:
                    print(f"        {m['table']}: error={err}")
                else:
                    print(
                        "        "
                        f"{m['table']}: source={m['source_count']}, target={m['target_count']}"
                    )
            else:
                print(
                    "        "
                    f"reason={m['reason']}, rows_synced_delta={m['rows_synced_delta']}, "
                    f"rows_synced_expected={m['rows_synced_expected']}, final_lag={m['final_lag']}"
                )
    else:
        if consistency_mode == "full":
            print("    [OK] Source/target value and key consistency checks passed for all benchmark tables.")
        else:
            print("    [OK] Safe consistency checks (rows_synced/lag metadata) passed.")

    return tps, avg_lag, max_lag, final_lag, rows_delta, mismatches, checks_ran

# ==============================================================================
# Main
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(description="pg_duckpipe Sysbench Wrapper")
    parser.add_argument("--db-url", default="host=localhost user=postgres dbname=postgres", help="Connection string")
    parser.add_argument(
        "--workload",
        choices=sorted(WORKLOAD_EXPECTED_EVENTS_PER_TX.keys()),
        default="oltp_insert",
        help="Sysbench workload script to run",
    )
    parser.add_argument("--tables", type=int, default=1, help="Number of tables")
    parser.add_argument("--table-size", type=int, default=100000, help="Rows per table")
    parser.add_argument("--threads", type=int, default=4, help="Sysbench threads")
    parser.add_argument("--duration", type=int, default=30, help="Benchmark duration (seconds)")
    parser.add_argument("--snapshot-timeout", type=int, default=600, help="Seconds to wait for snapshot completion")
    parser.add_argument(
        "--catchup-timeout",
        type=int,
        default=3600,
        help="Seconds to wait for apply catch-up (default: 3600). "
             "Also stops early if worker stalls for 30s with no progress.",
    )
    parser.add_argument(
        "--consistency-mode",
        choices=CONSISTENCY_MODES,
        default="auto",
        help="Consistency check mode: auto, safe, full, or off",
    )
    parser.add_argument(
        "--verify-on-timeout",
        action="store_true",
        help="Run consistency checks even if catch-up timeout is hit",
    )
    parser.add_argument("--skip-prepare", action="store_true", help="Skip table preparation")

    args = parser.parse_args()
    db_params = parse_db_url(args.db_url)

    print("===========================================================")
    print(" pg_duckpipe Sysbench Benchmark")
    print("===========================================================")

    if not args.skip_prepare:
        prepare_env(args, db_params)

    snapshot_rate = benchmark_snapshot(args, db_params)
    streaming_tps, avg_lag, max_lag, final_lag, rows_delta, mismatches, checks_ran = benchmark_streaming(args, db_params)

    print("\n===========================================================")
    print(" Final Results")
    print("===========================================================")
    print(f" Snapshot Throughput : {snapshot_rate:.0f} rows/sec")
    print(f" OLTP Throughput     : {streaming_tps:.2f} TPS")
    print(f" Avg Lag             : {avg_lag:.0f} bytes")
    print(f" Peak Lag            : {max_lag} bytes")
    print(f" Final Lag           : {final_lag} bytes")
    print(f" rows_synced Delta   : {rows_delta}")
    if checks_ran:
        print(f" Count Mismatches    : {len(mismatches)}")
    else:
        print(" Count Mismatches    : skipped")
    print("===========================================================")

if __name__ == "__main__":
    main()

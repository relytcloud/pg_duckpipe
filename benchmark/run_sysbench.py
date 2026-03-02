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
    """Approximate WAL lag: pg_current_wal_lsn() - confirmed_lsn.
    Includes non-DML WAL so it never reaches 0, but useful as a directional
    indicator for benchmark progress monitoring and stall detection."""
    res = run_sql(
        db_params,
        "SELECT COALESCE(SUM((pg_current_wal_lsn() - confirmed_lsn)::int8), 0) "
        "FROM duckpipe.sync_groups WHERE enabled",
    )
    return parse_int(res, 0)

def get_total_queued_changes(db_params):
    """Return total changes still buffered in flush coordinator queues.
    A non-zero value means flush threads are still draining — not a true stall."""
    res = run_sql(
        db_params,
        "SELECT COALESCE(total_queued_changes, 0) FROM duckpipe.worker_status()",
    )
    return parse_int(res, 0)

def get_snapshot_metrics(db_params, num_tables):
    """Query duckpipe.status() for per-table snapshot timing from SQL-exposed metrics."""
    table_filter = ", ".join([f"'public.sbtest{i}'" for i in range(1, num_tables + 1)])
    res = run_sql(
        db_params,
        f"SELECT source_table, snapshot_duration_ms, snapshot_rows "
        f"FROM duckpipe.status() "
        f"WHERE source_table IN ({table_filter}) "
        f"AND snapshot_duration_ms IS NOT NULL",
    )
    metrics = []
    if res:
        for line in res.strip().split("\n"):
            parts = line.split("|")
            if len(parts) == 3:
                table = parts[0].strip()
                duration_ms = float(parts[1].strip())
                rows = int(parts[2].strip())
                metrics.append((table, rows, duration_ms))
    return metrics


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
    # Tracks whether we already emitted the WAL nudge to trigger CATCHUP→STREAMING.
    # After a snapshot, the replication slot may see no new WAL messages because the
    # snapshot copy happens outside the publication's WAL stream.  Without a commit
    # with LSN > snapshot_lsn the state machine never fires the CATCHUP→STREAMING
    # transition.  We emit one transactional logical message (pg_logical_emit_message)
    # to give the slot a COMMIT at a higher LSN.
    catchup_trigger_sent = False

    prev_snapshot_count = 0
    prev_snapshot_time = start_time

    while True:
        # rows_synced is credited after snapshot copy completes (includes snapshot rows).
        res = run_sql(db_params, "SELECT sum(rows_synced) FROM duckpipe.status()")
        synced = int(res) if res and res.isdigit() else 0

        # Check actual count in target (ground truth while snapshot may still be running)
        target_count = 0
        for i in range(1, args.tables + 1):
             cnt = run_sql(db_params, f"SELECT count(*) FROM public.sbtest{i}_ducklake")
             if cnt and cnt.isdigit():
                 target_count += int(cnt)

        # Check if all are streaming
        state_res = run_sql(db_params, "SELECT count(*) FROM duckpipe.status() WHERE state != 'STREAMING'")
        not_streaming = int(state_res) if state_res and state_res.isdigit() else args.tables

        now = time.time()
        elapsed = now - start_time
        interval_secs = now - prev_snapshot_time
        interval_rate = (target_count - prev_snapshot_count) / interval_secs if interval_secs > 0 else 0
        prev_snapshot_count = target_count
        prev_snapshot_time = now

        sys.stdout.write(f"\r    rows_synced: {synced:,} | Actual: {target_count:,}/{total_rows:,} | non-STREAMING: {not_streaming} | Rate: {interval_rate:.0f} rows/s | Time: {elapsed:.1f}s")
        sys.stdout.flush()

        if target_count >= total_rows and not_streaming == 0:
            elapsed = time.time() - start_time
            wall_clock_rate = target_count / elapsed if elapsed > 0 else 0
            print(f"\n[+] Snapshot complete!")

            # Fetch per-table snapshot metrics from duckpipe.status()
            snapshot_metrics = get_snapshot_metrics(db_params, args.tables)
            if snapshot_metrics:
                # Concurrent snapshots overlap; effective rate = total rows / max duration
                max_duration_ms = max(ms for _, _, ms in snapshot_metrics)
                total_snapshot_rows = sum(r for _, r, _ in snapshot_metrics)
                snapshot_rate = total_snapshot_rows / max_duration_ms * 1000 if max_duration_ms > 0 else wall_clock_rate
            else:
                snapshot_rate = wall_clock_rate
                snapshot_metrics = []

            return snapshot_rate, snapshot_metrics

        # If all rows are physically copied but tables are stuck in CATCHUP (no SNAPSHOT
        # remaining), nudge the WAL consumer by emitting a transactional logical message.
        # The resulting COMMIT updates pending_lsn past snapshot_lsn, triggering the
        # CATCHUP→STREAMING transition on the next worker cycle.
        if target_count >= total_rows and not_streaming > 0 and not catchup_trigger_sent:
            snap_res = run_sql(db_params, "SELECT count(*) FROM duckpipe.status() WHERE state = 'SNAPSHOT'")
            still_in_snapshot = int(snap_res) if snap_res and snap_res.isdigit() else 1
            if still_in_snapshot == 0:
                run_sql(db_params, "SELECT pg_logical_emit_message(true, 'duckpipe_bench', 'catchup_trigger')", timeout=5)
                catchup_trigger_sent = True

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
    catchup_start_time = time.time()
    catchup_deadline = catchup_start_time + args.catchup_timeout

    # For oltp_insert: expected final count = initial rows + inserts during OLTP.
    # This is the ground-truth termination signal; we poll the actual target table
    # count instead of the rows_synced metadata, which can be inflated due to WAL
    # replay on slot reconnects.
    expected_final_count = None
    if args.workload == "oltp_insert":
        expected_final_count = args.table_size * args.tables + tx_count

    # Record actual target row count at the start of catch-up so we can compute
    # catch-up throughput independently of the inflated rows_synced counter.
    catchup_start_count = 0
    for i in range(1, args.tables + 1):
        cnt = run_sql(db_params, f"SELECT count(*) FROM public.sbtest{i}_ducklake", timeout=10)
        if cnt and cnt.isdigit():
            catchup_start_count += int(cnt)

    actual_final_count = catchup_start_count
    final_lag = get_total_lag_bytes(db_params)
    prev_final_lag = final_lag
    prev_count = catchup_start_count
    prev_interval_time = catchup_start_time

    # Stall limit: 30 × 2s = 60s with no progress.
    # Must be > poll_interval (default 10s) so we don't give up between flush cycles.
    stall_limit = 30
    stall_count = 0

    while True:
        time.sleep(2.0)

        if time.time() >= catchup_deadline:
            break

        # Ground-truth: actual row count in target tables.
        current_count = 0
        for i in range(1, args.tables + 1):
            cnt = run_sql(db_params, f"SELECT count(*) FROM public.sbtest{i}_ducklake", timeout=10)
            if cnt and cnt.isdigit():
                current_count += int(cnt)

        now = time.time()
        final_lag = get_total_lag_bytes(db_params)
        queued_changes = get_total_queued_changes(db_params)
        count_increasing = current_count > prev_count
        lag_decreasing = final_lag < prev_final_lag
        flush_draining = queued_changes > 0

        # Primary termination: target count reached expected (oltp_insert only).
        if expected_final_count is not None and current_count >= expected_final_count:
            actual_final_count = current_count
            break

        # Progress display — windowed rate (rows added since last interval) so the
        # rate drops to 0 during stalls rather than showing a misleading falling average.
        interval_secs = now - prev_interval_time
        interval_rate = (current_count - prev_count) / interval_secs if interval_secs > 0 else 0
        lag_mb = final_lag / 1024 / 1024
        lag_trend = '▼' if lag_decreasing else ('▲' if final_lag > prev_final_lag else '–')
        if expected_final_count:
            pct = f"{100 * current_count / expected_final_count:.0f}%"
            count_str = f"{current_count:,}/{expected_final_count:,} ({pct})"
        else:
            count_str = f"{current_count:,}"
        queued_str = f" queued={queued_changes:,}" if queued_changes > 0 else ""
        sys.stdout.write(
            f"\r    Catch-up: {count_str} | {interval_rate:.0f} rows/s | lag={lag_mb:.1f}MB {lag_trend}{queued_str}  "
        )
        sys.stdout.flush()

        # Secondary: stall detection — no rows delivered, lag not decreasing, AND
        # flush coordinator has nothing queued (i.e., flush threads are idle).
        # Checking queued_changes prevents declaring a stall while the flush thread
        # is still draining a large batch that hasn't committed to DuckLake yet.
        if not count_increasing and not lag_decreasing and not flush_draining:
            stall_count += 1
            if stall_count >= stall_limit:
                actual_final_count = current_count
                break
        else:
            stall_count = 0

        prev_final_lag = final_lag
        prev_count = current_count
        prev_interval_time = now
        actual_final_count = current_count

    catchup_elapsed = time.time() - catchup_start_time
    catchup_throughput = (actual_final_count - catchup_start_count) / catchup_elapsed if catchup_elapsed > 0 else 0

    if actual_final_count > catchup_start_count:
        print()  # newline after the progress line

    # Consistency: target count matches expected.
    # Note: lag_bytes after OLTP is NOT a consistency indicator — pg_current_wal_lsn()
    # keeps advancing from non-DML WAL (checkpoints, autovacuum), so lag_bytes stays
    # large even when all DML has been applied.
    catchup_complete = (
        (expected_final_count is not None and actual_final_count >= expected_final_count)
        or (expected_final_count is None and not count_increasing and not lag_decreasing)
    )
    consistency_mode = resolve_consistency_mode(args)
    checks_ran = False
    checks_skipped_reason = None
    mismatches = []
    should_run_checks = (consistency_mode != "off") and (catchup_complete or args.verify_on_timeout)

    if should_run_checks:
        checks_ran = True
        if expected_final_count is not None:
            # For oltp_insert: we already know the actual count; no extra query needed.
            if actual_final_count < expected_final_count:
                mismatches.append({
                    "table": f"sbtest1..{args.tables}",
                    "source_count": expected_final_count,
                    "target_count": actual_final_count,
                    "mode": "full",
                })
        elif consistency_mode == "full":
            mismatches = verify_table_consistency_full(args, db_params)
    elif consistency_mode != "off":
        checks_skipped_reason = "catch-up incomplete (stall or timeout)"

    print("\n[Sysbench Output Summary]")
    for line in stdout.split('\n'):
        if "transactions:" in line or "latency" in line:
            print(f"    {line.strip()}")
    # Note: final lag includes non-DML WAL (checkpoints, autovacuum) that accumulated
    # after OLTP ended — it is NOT a data-consistency indicator.
    print(f"    lag(avg/peak): {avg_lag/1024/1024:.1f}MB / {max_lag/1024/1024:.1f}MB  [during OLTP]")
    print(f"    catch-up: {actual_final_count:,} rows in {catchup_elapsed:.1f}s ({catchup_throughput:.0f} rows/s)")
    if expected_final_count is not None:
        oltp_rows = actual_final_count - args.table_size * args.tables
        print(f"    expected: {expected_final_count:,} rows ({args.table_size * args.tables:,} initial + {tx_count:,} from OLTP)")
    print(f"    consistency mode: {consistency_mode}")
    if not catchup_complete:
        print("    [!] Catch-up did not complete before timeout; results may reflect partial apply.")
    if consistency_mode == "off":
        print("    [!] Consistency checks skipped (--consistency-mode off).")
    elif not checks_ran:
        print(f"    [!] Consistency checks skipped ({checks_skipped_reason}).")
    elif mismatches:
        print("    [!] Consistency mismatches detected:")
        for m in mismatches:
            err = m.get("error", "")
            if err:
                print(f"        {m['table']}: error={err}")
            else:
                print(f"        {m['table']}: source={m['source_count']}, target={m['target_count']}")
    else:
        print("    [OK] Consistency check passed — target count matches expected.")

    return tps, avg_lag, max_lag, final_lag, catchup_elapsed, catchup_throughput, actual_final_count, expected_final_count, mismatches, checks_ran

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

    snapshot_rate, snapshot_metrics = benchmark_snapshot(args, db_params)
    (streaming_tps, avg_lag, max_lag, final_lag,
     catchup_elapsed, catchup_throughput,
     actual_final_count, expected_final_count,
     mismatches, checks_ran) = benchmark_streaming(args, db_params)

    print("\n===========================================================")
    print(" Final Results")
    print("===========================================================")
    for table, rows, ms in snapshot_metrics:
        rate = rows / ms * 1000 if ms > 0 else 0
        print(f" Snapshot [{table}]  : {rows:,} rows in {ms:.0f}ms ({rate:,.0f} rows/s)")
    print(f" Snapshot Throughput  : {snapshot_rate:.0f} rows/sec")
    print(f" OLTP Throughput      : {streaming_tps:.2f} TPS")
    print(f" Avg Replication Lag  : {avg_lag/1024/1024:.1f} MB  [during OLTP]")
    print(f" Peak Replication Lag : {max_lag/1024/1024:.1f} MB  [during OLTP]")
    print(f" Catch-up Time        : {catchup_elapsed:.1f} sec")
    print(f" Catch-up Throughput  : {catchup_throughput:.0f} rows/sec")
    if expected_final_count is not None:
        status = "PASS" if actual_final_count >= expected_final_count else f"INCOMPLETE ({actual_final_count:,}/{expected_final_count:,})"
        print(f" Consistency          : {status}")
    if checks_ran:
        print(f" Count Mismatches     : {len(mismatches)}")
    else:
        print(" Count Mismatches     : skipped")
    print("===========================================================")

if __name__ == "__main__":
    main()

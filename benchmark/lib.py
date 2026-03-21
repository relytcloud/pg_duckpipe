#!/usr/bin/env python3
"""Shared utilities for pg_duckpipe benchmarking and soak testing.

Extracted from run_sysbench.py — reusable helpers for SQL execution,
sysbench command construction, monitoring queries, and consistency checks.
"""
import json
import os
import subprocess
import time
import sys

# ==============================================================================
# Constants
# ==============================================================================

WORKLOAD_EXPECTED_EVENTS_PER_TX = {
    "oltp_insert": 1,
    "oltp_read_write": 6,
    "oltp_insert_heavy": 10,  # 9 INSERTs + 1 UPDATE
    "/bench/oltp_insert_heavy.lua": 10,
}

CONSISTENCY_MODES = ("auto", "safe", "full", "off")

# ==============================================================================
# Core Utilities
# ==============================================================================

def run_sql(db_params, sql, timeout=60):
    """Run SQL via psql with a timeout (default 60s)."""
    conn_str = f"host={db_params['host']} port={db_params['port']} user={db_params['user']} dbname={db_params['dbname']}"
    if 'password' in db_params:
        env = {**os.environ, 'PGPASSWORD': db_params['password']}
    else:
        env = None
    cmd = ["psql", conn_str, "-t", "-A", "-c", sql]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=env)
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
    if 'host' not in params: params['host'] = 'localhost'
    if 'port' not in params: params['port'] = '5432'
    if 'user' not in params: params['user'] = 'postgres'
    if 'dbname' not in params: params['dbname'] = 'postgres'
    return params


def get_sysbench_cmd(args_or_params, db_params, action):
    """Construct sysbench command.

    args_or_params can be an argparse namespace with .workload, .tables,
    .table_size, .threads, .duration attributes, or a dict with the same keys.
    """
    if isinstance(args_or_params, dict):
        p = args_or_params
        workload = p['workload']
        tables = p['tables']
        table_size = p['table_size']
        threads = p['threads']
        duration = p['duration']
    else:
        workload = args_or_params.workload
        tables = args_or_params.tables
        table_size = args_or_params.table_size
        threads = args_or_params.threads
        duration = args_or_params.duration

    cmd = [
        "sysbench",
        workload,
        f"--db-driver=pgsql",
        f"--pgsql-host={db_params['host']}",
        f"--pgsql-port={db_params['port']}",
        f"--pgsql-user={db_params['user']}",
        f"--pgsql-db={db_params['dbname']}",
        f"--tables={tables}",
        f"--table-size={table_size}",
        f"--threads={threads}",
        f"--time={duration}",
        f"--report-interval=1",
    ]
    if 'password' in db_params:
        cmd.append(f"--pgsql-password={db_params['password']}")
    cmd.append(action)
    return cmd


def parse_int(value, default=0):
    if value is None:
        return default
    value = value.strip()
    if value.isdigit() or (value.startswith("-") and value[1:].isdigit()):
        return int(value)
    return default


def wait_for_db_ready(db_params, timeout=60):
    """Wait for the database to accept connections."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        res = run_sql(db_params, "SELECT 1", timeout=10)
        if res == "1":
            return True
        time.sleep(2.0)
    return False


# ==============================================================================
# Monitoring Queries
# ==============================================================================

def get_total_lag_bytes(db_params):
    """Approximate WAL lag: pg_current_wal_lsn() - confirmed_lsn."""
    res = run_sql(
        db_params,
        "SELECT COALESCE(SUM((pg_current_wal_lsn() - confirmed_lsn)::int8), 0) "
        "FROM duckpipe.sync_groups WHERE enabled",
    )
    return parse_int(res, 0)


def get_total_queued_changes(db_params):
    """Return total changes still buffered in flush coordinator queues."""
    res = run_sql(
        db_params,
        "SELECT COALESCE(SUM(total_queued_changes), 0) FROM duckpipe.worker_status()",
    )
    return parse_int(res, 0)


def get_snapshot_metrics(db_params, num_tables):
    """Query duckpipe.status() for per-table snapshot timing."""
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


def get_full_status(db_params):
    """Query duckpipe.status(), return list of dicts with all columns."""
    res = run_sql(
        db_params,
        "SELECT sync_group, source_table, target_table, state, enabled, "
        "rows_synced, queued_changes, error_message, consecutive_failures "
        "FROM duckpipe.status()",
    )
    rows = []
    if res:
        for line in res.strip().split("\n"):
            parts = line.split("|")
            if len(parts) >= 9:
                rows.append({
                    "sync_group": parts[0].strip(),
                    "source_table": parts[1].strip(),
                    "target_table": parts[2].strip(),
                    "state": parts[3].strip(),
                    "enabled": parts[4].strip() == "t",
                    "rows_synced": parse_int(parts[5].strip(), 0),
                    "queued_changes": parse_int(parts[6].strip(), 0),
                    "error_message": parts[7].strip() or None,
                    "consecutive_failures": parse_int(parts[8].strip(), 0),
                })
    return rows


def get_worker_status(db_params):
    """Query duckpipe.worker_status(), return list of dicts."""
    res = run_sql(
        db_params,
        "SELECT total_queued_changes, is_backpressured "
        "FROM duckpipe.worker_status()",
    )
    rows = []
    if res:
        for line in res.strip().split("\n"):
            parts = line.split("|")
            if len(parts) >= 2:
                rows.append({
                    "total_queued_changes": parse_int(parts[0].strip(), 0),
                    "is_backpressured": parts[1].strip() == "t",
                })
    return rows


def get_group_status(db_params):
    """Query duckpipe.groups(), return list of dicts."""
    res = run_sql(
        db_params,
        "SELECT name, enabled, slot_name, confirmed_lsn::text "
        "FROM duckpipe.groups()",
    )
    rows = []
    if res:
        for line in res.strip().split("\n"):
            parts = line.split("|")
            if len(parts) >= 4:
                rows.append({
                    "name": parts[0].strip(),
                    "enabled": parts[1].strip() == "t",
                    "slot_name": parts[2].strip(),
                    "confirmed_lsn": parts[3].strip() or None,
                })
    return rows


def get_wal_slot_size_bytes(db_params):
    """Get retained WAL size via pg_replication_slots (all duckpipe slots)."""
    res = run_sql(
        db_params,
        "SELECT COALESCE(SUM(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)), 0)::bigint "
        "FROM pg_replication_slots "
        "WHERE slot_name LIKE 'duckpipe_%'",
    )
    return parse_int(res, 0)


def get_metrics_json(db_params):
    """Query duckpipe.metrics() and return parsed JSON.

    Returns a dict with 'tables' and 'groups' keys containing SHM metrics
    (duckdb_memory_bytes, flush_count, flush_duration_ms, queued_changes)
    merged with PG-persisted data — all in a single SQL call.
    """
    res = run_sql(db_params, "SELECT duckpipe.metrics()")
    if res:
        try:
            return json.loads(res)
        except json.JSONDecodeError:
            return None
    return None


def get_benchmark_rows_synced(db_params, num_tables):
    """Get total rows_synced for sbtest tables."""
    table_filter = ", ".join([f"'public.sbtest{i}'" for i in range(1, num_tables + 1)])
    res = run_sql(
        db_params,
        f"SELECT COALESCE(sum(rows_synced), 0) "
        f"FROM duckpipe.status() "
        f"WHERE source_table IN ({table_filter})",
    )
    return parse_int(res, 0)


# ==============================================================================
# Consistency
# ==============================================================================

def verify_table_consistency_full(db_params, num_tables):
    """Verify consistency using separate count queries per table."""
    mismatches = []
    for i in range(1, num_tables + 1):
        table = f"sbtest{i}"
        target = f"{table}_ducklake"

        src_s = run_sql(db_params, f"SELECT count(*) FROM public.{table}")
        if src_s is None:
            print(f"    [!] Connection lost querying {table}, waiting for recovery...")
            if not wait_for_db_ready(db_params):
                mismatches.append({
                    "table": table, "source_count": None, "target_count": None,
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
                    "target_count": None, "mode": "full", "error": "connection lost on target count",
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
                "mode": "full",
            })
    return mismatches

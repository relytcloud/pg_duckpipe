#!/usr/bin/env python3
"""Analyze benchmark results and PG logs to produce a markdown report.

Inputs:
  - benchmark/results/<scenario>.log   (benchmark stdout per scenario)
  - benchmark/results/<scenario>_pg.log (postgres log per scenario)

Output:
  - benchmark/results/report.md
"""
import argparse
import glob
import os
import re
import statistics
import sys
from dataclasses import dataclass, field

# ==============================================================================
# Data structures
# ==============================================================================

@dataclass
class ScenarioResult:
    name: str
    label: str = ""
    # From benchmark output
    snapshot_rate: float = 0.0
    tps: float = 0.0
    avg_lag_mb: float = 0.0
    peak_lag_mb: float = 0.0
    catchup_time: float = 0.0
    catchup_throughput: float = 0.0
    consistency: str = "N/A"
    # From PG logs
    flush_count: int = 0
    flush_latencies_ms: list = field(default_factory=list)
    flush_rows: list = field(default_factory=list)
    flush_phases: list = field(default_factory=list)  # list of dicts
    snapshot_timings: list = field(default_factory=list)  # list of (table, rows, ms)
    wal_cycle_times_ms: list = field(default_factory=list)
    errors: list = field(default_factory=list)
    warnings: list = field(default_factory=list)
    backpressure_events: int = 0


SCENARIO_LABELS = {
    "single_table_insert": "Single-table append (1T, oltp_insert)",
    "multi_table_insert": "Multi-table append (4T, oltp_insert)",
    "single_table_mixed": "Single-table mixed (1T, oltp_read_write)",
    "multi_table_mixed": "Multi-table mixed (4T, oltp_read_write)",
}

# ==============================================================================
# Benchmark output parsing
# ==============================================================================

def parse_benchmark_log(path):
    """Parse a scenario's benchmark stdout log."""
    result = ScenarioResult(name=os.path.basename(path).replace(".log", ""))
    result.label = SCENARIO_LABELS.get(result.name, result.name)

    try:
        text = open(path).read()
    except FileNotFoundError:
        return result

    # Snapshot Throughput  : 41929 rows/sec
    m = re.search(r"Snapshot Throughput\s*:\s*([\d.]+)\s*rows/sec", text)
    if m:
        result.snapshot_rate = float(m.group(1))

    # OLTP Throughput      : 8827.55 TPS
    m = re.search(r"OLTP Throughput\s*:\s*([\d.]+)\s*TPS", text)
    if m:
        result.tps = float(m.group(1))

    # Avg Replication Lag  : 2.9 MB
    m = re.search(r"Avg Replication Lag\s*:\s*([\d.]+)\s*MB", text)
    if m:
        result.avg_lag_mb = float(m.group(1))

    # Peak Replication Lag : 4.2 MB
    m = re.search(r"Peak Replication Lag\s*:\s*([\d.]+)\s*MB", text)
    if m:
        result.peak_lag_mb = float(m.group(1))

    # Catch-up Time        : 2.2 sec
    m = re.search(r"Catch-up Time\s*:\s*([\d.]+)\s*sec", text)
    if m:
        result.catchup_time = float(m.group(1))

    # Catch-up Throughput  : 12345 rows/sec
    m = re.search(r"Catch-up Throughput\s*:\s*([\d.]+)\s*rows/sec", text)
    if m:
        result.catchup_throughput = float(m.group(1))

    # Consistency          : PASS  or  INCOMPLETE (...)
    m = re.search(r"Consistency\s*:\s*(.+)", text)
    if m:
        result.consistency = m.group(1).strip()
    elif "Consistency check passed" in text:
        result.consistency = "PASS"
    elif "Consistency mismatches" in text:
        result.consistency = "FAIL"

    # Per-table snapshot lines: Snapshot [public.sbtest1]  : 100,000 rows in 800ms (125,000 rows/s)
    for m in re.finditer(
        r"Snapshot \[(\S+)\]\s*:\s*([\d,]+)\s*rows in\s*([\d,]+)ms",
        text,
    ):
        table = m.group(1)
        rows = int(m.group(2).replace(",", ""))
        ms = float(m.group(3).replace(",", ""))
        result.snapshot_timings.append((table, rows, ms))

    return result


# ==============================================================================
# PG log parsing
# ==============================================================================

def parse_pg_log(result, path):
    """Parse a scenario's PG log and populate flush/snapshot/error stats."""
    try:
        text = open(path).read()
    except FileNotFoundError:
        return

    # DuckPipe timing: action=duckdb_flush target=... rows=N ... elapsed_ms=X
    for m in re.finditer(
        r"DuckPipe timing: action=duckdb_flush\s+target=\S+\s+rows=(\d+)\s+.*?elapsed_ms=([\d.]+)",
        text,
    ):
        rows = int(m.group(1))
        ms = float(m.group(2))
        result.flush_count += 1
        result.flush_latencies_ms.append(ms)
        result.flush_rows.append(rows)

    # DuckPipe perf: action=duckdb_flush target=... rows=N discover_ms=... ...
    for m in re.finditer(
        r"DuckPipe perf: action=duckdb_flush\s+target=\S+\s+rows=(\d+)\s+"
        r"discover_ms=([\d.]+)\s+buf_create_ms=([\d.]+)\s+load_ms=([\d.]+)\s+"
        r"compact_ms=([\d.]+)\s+begin_ms=([\d.]+)\s+delete_ms=([\d.]+)\s+"
        r"insert_ms=([\d.]+)\s+commit_ms=([\d.]+)\s+cleanup_ms=([\d.]+)\s+"
        r"total_ms=([\d.]+)",
        text,
    ):
        result.flush_phases.append({
            "rows": int(m.group(1)),
            "discover": float(m.group(2)),
            "buf_create": float(m.group(3)),
            "load": float(m.group(4)),
            "compact": float(m.group(5)),
            "begin": float(m.group(6)),
            "delete": float(m.group(7)),
            "insert": float(m.group(8)),
            "commit": float(m.group(9)),
            "cleanup": float(m.group(10)),
            "total": float(m.group(11)),
        })

    # DuckPipe timing: action=process_sync_group_streaming ... elapsed_ms=X
    for m in re.finditer(
        r"DuckPipe timing: action=process_sync_group_streaming\s+.*?elapsed_ms=([\d.]+)",
        text,
    ):
        result.wal_cycle_times_ms.append(float(m.group(1)))

    # Errors
    for m in re.finditer(r".*(?:flush error|worker error|worker caught error).*", text):
        result.errors.append(m.group(0).strip())

    # ERRORED state transitions
    for m in re.finditer(r".*ERRORED.*", text):
        result.warnings.append(m.group(0).strip())

    # Backpressure (look for paused/backpressure mentions)
    result.backpressure_events = len(re.findall(r"(?i)backpressure|paused.*queue", text))


# ==============================================================================
# Report generation
# ==============================================================================

def percentile(data, p):
    """Simple percentile (nearest-rank)."""
    if not data:
        return 0.0
    sorted_data = sorted(data)
    k = int(len(sorted_data) * p / 100)
    k = min(k, len(sorted_data) - 1)
    return sorted_data[k]


def fmt(val, suffix="", decimals=1):
    """Format a number with suffix, or '-' if zero."""
    if val == 0:
        return "-"
    if isinstance(val, int) or (isinstance(val, float) and val == int(val) and abs(val) > 100):
        return f"{int(val):,}{suffix}"
    return f"{val:,.{decimals}f}{suffix}"


def generate_report(scenarios):
    lines = []
    lines.append("# pg_duckpipe Benchmark Report\n")

    # ── Summary Table ─────────────────────────────────────────────────────
    lines.append("## Summary\n")
    lines.append("| Metric | " + " | ".join(s.label for s in scenarios) + " |")
    lines.append("|--------|" + "|".join("---" for _ in scenarios) + "|")

    rows = [
        ("Snapshot (rows/s)", [fmt(s.snapshot_rate) for s in scenarios]),
        ("OLTP TPS", [fmt(s.tps) for s in scenarios]),
        ("Avg Lag (MB)", [fmt(s.avg_lag_mb) for s in scenarios]),
        ("Peak Lag (MB)", [fmt(s.peak_lag_mb) for s in scenarios]),
        ("Catch-up (s)", [fmt(s.catchup_time) for s in scenarios]),
        ("Catch-up (rows/s)", [fmt(s.catchup_throughput) for s in scenarios]),
        ("Consistency", [s.consistency for s in scenarios]),
    ]
    for label, vals in rows:
        lines.append(f"| {label} | " + " | ".join(vals) + " |")

    # ── Flush Performance ─────────────────────────────────────────────────
    lines.append("\n## Flush Performance\n")
    lines.append("| Metric | " + " | ".join(s.label for s in scenarios) + " |")
    lines.append("|--------|" + "|".join("---" for _ in scenarios) + "|")

    flush_rows = []
    flush_rows.append(("Flush count", [fmt(s.flush_count) for s in scenarios]))
    flush_rows.append(("Avg latency (ms)", [
        fmt(statistics.mean(s.flush_latencies_ms)) if s.flush_latencies_ms else "-"
        for s in scenarios
    ]))
    flush_rows.append(("P50 latency (ms)", [
        fmt(percentile(s.flush_latencies_ms, 50)) if s.flush_latencies_ms else "-"
        for s in scenarios
    ]))
    flush_rows.append(("P99 latency (ms)", [
        fmt(percentile(s.flush_latencies_ms, 99)) if s.flush_latencies_ms else "-"
        for s in scenarios
    ]))
    flush_rows.append(("Avg rows/flush", [
        fmt(statistics.mean(s.flush_rows)) if s.flush_rows else "-"
        for s in scenarios
    ]))

    for label, vals in flush_rows:
        lines.append(f"| {label} | " + " | ".join(vals) + " |")

    # Phase breakdown (if available for any scenario)
    has_phases = any(s.flush_phases for s in scenarios)
    if has_phases:
        lines.append("\n### Flush Phase Breakdown (avg ms)\n")
        phase_keys = ["discover", "buf_create", "load", "compact", "begin", "delete", "insert", "commit", "cleanup"]
        lines.append("| Phase | " + " | ".join(s.label for s in scenarios) + " |")
        lines.append("|-------|" + "|".join("---" for _ in scenarios) + "|")
        for key in phase_keys:
            vals = []
            for s in scenarios:
                if s.flush_phases:
                    avg = statistics.mean(p[key] for p in s.flush_phases)
                    vals.append(fmt(avg))
                else:
                    vals.append("-")
            lines.append(f"| {key} | " + " | ".join(vals) + " |")

    # ── Snapshot Timings ──────────────────────────────────────────────────
    has_snapshots = any(s.snapshot_timings for s in scenarios)
    if has_snapshots:
        lines.append("\n## Snapshot Timings\n")
        for s in scenarios:
            if s.snapshot_timings:
                lines.append(f"**{s.label}**\n")
                for table, rows, ms in s.snapshot_timings:
                    lines.append(f"- `{table}`: {rows:,} rows in {ms:.1f}ms ({rows / ms * 1000:.0f} rows/s)")
                lines.append("")

    # ── WAL Processing ────────────────────────────────────────────────────
    has_wal = any(s.wal_cycle_times_ms for s in scenarios)
    if has_wal:
        lines.append("\n## WAL Processing Cycle Times\n")
        lines.append("| Metric | " + " | ".join(s.label for s in scenarios) + " |")
        lines.append("|--------|" + "|".join("---" for _ in scenarios) + "|")
        lines.append("| Cycles | " + " | ".join(
            fmt(len(s.wal_cycle_times_ms)) if s.wal_cycle_times_ms else "-"
            for s in scenarios
        ) + " |")
        lines.append("| Avg (ms) | " + " | ".join(
            fmt(statistics.mean(s.wal_cycle_times_ms)) if s.wal_cycle_times_ms else "-"
            for s in scenarios
        ) + " |")
        lines.append("| P99 (ms) | " + " | ".join(
            fmt(percentile(s.wal_cycle_times_ms, 99)) if s.wal_cycle_times_ms else "-"
            for s in scenarios
        ) + " |")

    # ── Issues & Observations ─────────────────────────────────────────────
    lines.append("\n## Issues & Observations\n")
    any_issues = False
    for s in scenarios:
        issues = []
        if s.errors:
            issues.append(f"**Errors ({len(s.errors)})**: " + "; ".join(s.errors[:5]))
            if len(s.errors) > 5:
                issues[-1] += f" ... and {len(s.errors) - 5} more"
        if s.warnings:
            issues.append(f"**Warnings ({len(s.warnings)})**: " + "; ".join(s.warnings[:3]))
            if len(s.warnings) > 3:
                issues[-1] += f" ... and {len(s.warnings) - 3} more"
        if s.backpressure_events:
            issues.append(f"**Backpressure events**: {s.backpressure_events}")
        if s.consistency not in ("PASS", "N/A", "-"):
            issues.append(f"**Consistency**: {s.consistency}")

        if issues:
            any_issues = True
            lines.append(f"### {s.label}\n")
            for issue in issues:
                lines.append(f"- {issue}")
            lines.append("")

    if not any_issues:
        lines.append("No errors, warnings, or anomalies detected.\n")

    return "\n".join(lines)


# ==============================================================================
# Main
# ==============================================================================

SCENARIO_ORDER = [
    "single_table_insert",
    "multi_table_insert",
    "single_table_mixed",
    "multi_table_mixed",
]


def main():
    parser = argparse.ArgumentParser(description="Analyze pg_duckpipe benchmark results")
    parser.add_argument("--results-dir", default="benchmark/results", help="Directory with result logs")
    parser.add_argument("--output", default=None, help="Output report path (default: <results-dir>/report.md)")
    args = parser.parse_args()

    results_dir = args.results_dir
    output_path = args.output or os.path.join(results_dir, "report.md")

    # Discover scenarios in canonical order
    scenarios = []
    for name in SCENARIO_ORDER:
        bench_log = os.path.join(results_dir, f"{name}.log")
        pg_log = os.path.join(results_dir, f"{name}_pg.log")
        if os.path.exists(bench_log):
            result = parse_benchmark_log(bench_log)
            parse_pg_log(result, pg_log)
            scenarios.append(result)

    # Also pick up any non-standard scenario logs
    for path in sorted(glob.glob(os.path.join(results_dir, "*.log"))):
        basename = os.path.basename(path).replace(".log", "")
        if basename.endswith("_pg") or basename == "report":
            continue
        if basename not in SCENARIO_ORDER:
            result = parse_benchmark_log(path)
            pg_log = os.path.join(results_dir, f"{basename}_pg.log")
            parse_pg_log(result, pg_log)
            scenarios.append(result)

    if not scenarios:
        print(f"No benchmark logs found in {results_dir}/")
        sys.exit(1)

    report = generate_report(scenarios)

    # Write to file
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        f.write(report)

    # Also print to stdout
    print(report)
    print(f"\nReport written to {output_path}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Post-mortem report generator for pg_duckpipe soak tests.

Reads metrics.csv + events.log + config.txt and generates a markdown report
with aggregates, stability analysis, consistency results, and pass/fail verdict.

Usage:
  python3 soak_report.py <output_dir>
  # or imported by soak_test.py:
  from soak_report import generate_report
"""
import csv
import math
import os
import sys


def load_metrics(csv_path):
    """Load metrics CSV, return list of dicts with numeric conversion."""
    rows = []
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            parsed = {}
            for k, v in row.items():
                # Try numeric conversion
                try:
                    if '.' in v:
                        parsed[k] = float(v)
                    else:
                        parsed[k] = int(v)
                except (ValueError, TypeError):
                    if v in ('True', 'true', 't'):
                        parsed[k] = True
                    elif v in ('False', 'false', 'f'):
                        parsed[k] = False
                    else:
                        parsed[k] = v
            rows.append(parsed)
    return rows


def load_events(events_path):
    """Load events log, return list of (timestamp, message)."""
    events = []
    if not os.path.exists(events_path):
        return events
    with open(events_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split("  ", 1)
            if len(parts) == 2:
                events.append((parts[0], parts[1]))
            else:
                events.append(("", line))
    return events


def load_config(config_path):
    """Load config file, return dict."""
    config = {}
    if not os.path.exists(config_path):
        return config
    with open(config_path, 'r') as f:
        for line in f:
            line = line.strip()
            if '=' in line:
                k, v = line.split('=', 1)
                config[k] = v
    return config


def compute_stats(values):
    """Compute basic statistics for a list of numbers."""
    if not values:
        return {'count': 0, 'mean': 0, 'min': 0, 'max': 0, 'std': 0, 'p95': 0, 'p99': 0}
    n = len(values)
    mean = sum(values) / n
    sorted_vals = sorted(values)
    variance = sum((x - mean) ** 2 for x in values) / n if n > 1 else 0
    std = math.sqrt(variance)

    p95_idx = min(int(n * 0.95), n - 1)
    p99_idx = min(int(n * 0.99), n - 1)

    return {
        'count': n,
        'mean': mean,
        'min': sorted_vals[0],
        'max': sorted_vals[-1],
        'std': std,
        'p95': sorted_vals[p95_idx],
        'p99': sorted_vals[p99_idx],
    }


def compute_cv(values):
    """Coefficient of variation (std / mean * 100)."""
    stats = compute_stats(values)
    if stats['mean'] == 0:
        return 0
    return (stats['std'] / stats['mean']) * 100


def compute_trend_slope(values):
    """Simple linear regression slope (value per sample)."""
    n = len(values)
    if n < 2:
        return 0
    x_mean = (n - 1) / 2
    y_mean = sum(values) / n
    numerator = sum((i - x_mean) * (values[i] - y_mean) for i in range(n))
    denominator = sum((i - x_mean) ** 2 for i in range(n))
    if denominator == 0:
        return 0
    return numerator / denominator


def generate_report(csv_path, events_path, config_path, output_path):
    """Generate a markdown report from soak test data."""
    metrics = load_metrics(csv_path)
    events = load_events(events_path)
    config = load_config(config_path)

    if not metrics:
        with open(output_path, 'w') as f:
            f.write("# Soak Test Report\n\nNo metrics data collected.\n")
        return

    # Extract time series
    tps_values = [r.get('sysbench_tps', 0) for r in metrics if isinstance(r.get('sysbench_tps'), (int, float))]
    # Skip the first data point (warmup/snapshot skew) for sync rate analysis
    sync_rates = [r.get('sync_rate_rows_s', 0) for r in metrics[1:] if isinstance(r.get('sync_rate_rows_s'), (int, float))]
    lag_bytes = [r.get('wal_lag_bytes', 0) for r in metrics if isinstance(r.get('wal_lag_bytes'), (int, float))]
    lag_mb = [b / 1048576 for b in lag_bytes]
    slot_bytes = [r.get('slot_retained_wal_bytes', 0) for r in metrics if isinstance(r.get('slot_retained_wal_bytes'), (int, float))]
    slot_mb = [b / 1048576 for b in slot_bytes]
    queued = [r.get('queued_changes', 0) for r in metrics if isinstance(r.get('queued_changes'), (int, float))]
    errored = [r.get('tables_errored', 0) for r in metrics if isinstance(r.get('tables_errored'), (int, float))]

    # Duration
    elapsed_values = [r.get('elapsed_s', 0) for r in metrics if isinstance(r.get('elapsed_s'), (int, float))]
    duration_s = max(elapsed_values) if elapsed_values else 0
    duration_str = f"{int(duration_s // 3600):02d}:{int((duration_s % 3600) // 60):02d}:{int(duration_s % 60):02d}"

    # Total rows
    total_rows = metrics[-1].get('total_rows_synced', 0) if metrics else 0

    # Stats
    tps_stats = compute_stats(tps_values)
    sync_stats = compute_stats(sync_rates)
    lag_stats = compute_stats(lag_mb)
    slot_stats = compute_stats(slot_mb)

    # Stability metrics
    sync_cv = compute_cv(sync_rates)
    lag_p95 = lag_stats['p95']
    lag_p99 = lag_stats['p99']

    # Slot growth trend (MB per hour)
    if slot_bytes and duration_s > 0:
        slot_slope = compute_trend_slope(slot_mb)
        poll_interval = int(config.get('poll_interval', 5))
        samples_per_hour = 3600 / poll_interval if poll_interval > 0 else 720
        slot_growth_mb_hr = slot_slope * samples_per_hour
    else:
        slot_growth_mb_hr = 0

    # Consistency checks from events
    consistency_events = [(ts, msg) for ts, msg in events if 'onsistency' in msg]
    consistency_passes = sum(1 for _, msg in consistency_events if 'PASS' in msg)
    consistency_fails = sum(1 for _, msg in consistency_events if 'FAIL' in msg)
    consistency_total = consistency_passes + consistency_fails

    # Final consistency
    final_events = [(ts, msg) for ts, msg in events if 'Final consistency' in msg]
    final_result = final_events[-1][1] if final_events else "N/A"

    # Permanent errors
    max_errored = max(errored) if errored else 0
    permanent_errors = any(e > 0 for e in errored[-10:]) if errored else False

    # Verdict
    verdict_reasons = []
    verdict = "PASS"

    if sync_cv > 30:
        verdict = "FAIL"
        verdict_reasons.append(f"Throughput CV {sync_cv:.1f}% > 30% threshold")
    if permanent_errors:
        verdict = "FAIL"
        verdict_reasons.append("Tables in ERRORED state at end of test")
    if "FAIL" in final_result:
        verdict = "FAIL"
        verdict_reasons.append(f"Final consistency check failed")
    if slot_growth_mb_hr > 1.0 and duration_s > 3600:
        verdict = "WARN"
        verdict_reasons.append(f"Slot growth {slot_growth_mb_hr:.1f} MB/hr > 1 MB/hr")

    if not verdict_reasons:
        verdict_reasons.append("All criteria met")

    # Generate markdown
    lines = []
    lines.append("# pg_duckpipe Soak Test Report\n")

    # Config
    lines.append("## Configuration\n")
    lines.append(f"| Parameter | Value |")
    lines.append(f"|-----------|-------|")
    for k in ['scenario', 'workload', 'tables', 'table_size', 'threads', 'duration', 'chaos', 'chaos_interval']:
        v = config.get(k, 'N/A')
        lines.append(f"| {k} | {v} |")
    lines.append(f"| actual duration | {duration_str} |")
    lines.append("")

    # Aggregates
    lines.append("## Aggregates\n")
    lines.append(f"| Metric | Value |")
    lines.append(f"|--------|-------|")
    lines.append(f"| Total Rows Synced | {total_rows:,} |")
    lines.append(f"| Avg Sysbench TPS | {tps_stats['mean']:,.0f} |")
    lines.append(f"| Avg Sync Rate | {sync_stats['mean']:,.0f} rows/s |")
    lines.append(f"| Peak Sync Rate | {sync_stats['max']:,.0f} rows/s |")
    lines.append(f"| Avg WAL Lag | {lag_stats['mean']:.1f} MB |")
    lines.append(f"| Peak WAL Lag | {lag_stats['max']:.1f} MB |")
    lines.append(f"| Max Tables Errored | {max_errored} |")
    lines.append("")

    # Stability
    lines.append("## Stability\n")
    lines.append(f"| Metric | Value | Threshold |")
    lines.append(f"|--------|-------|-----------|")
    lines.append(f"| Throughput CV | {sync_cv:.1f}% | < 30% |")
    lines.append(f"| Lag P95 | {lag_p95:.1f} MB | - |")
    lines.append(f"| Lag P99 | {lag_p99:.1f} MB | - |")
    lines.append(f"| Slot Growth | {slot_growth_mb_hr:.1f} MB/hr | < 1 MB/hr |")
    lines.append(f"| Permanent Errors | {'Yes' if permanent_errors else 'No'} | No |")
    lines.append("")

    # Consistency
    lines.append("## Consistency\n")
    lines.append(f"| Check | Result |")
    lines.append(f"|-------|--------|")
    for ts, msg in consistency_events:
        lines.append(f"| {ts} | {msg} |")
    if not consistency_events:
        lines.append(f"| - | No checks ran |")
    lines.append("")

    # Events
    lines.append("## Notable Events\n")
    chaos_events = [(ts, msg) for ts, msg in events if 'CHAOS' in msg]
    if chaos_events:
        for ts, msg in chaos_events:
            lines.append(f"- `{ts}` {msg}")
    else:
        lines.append("No chaos events.")
    lines.append("")

    # Verdict
    verdict_emoji = {"PASS": "PASS", "FAIL": "FAIL", "WARN": "WARN"}[verdict]
    lines.append(f"## Verdict: **{verdict_emoji}**\n")
    for reason in verdict_reasons:
        lines.append(f"- {reason}")
    lines.append("")

    report_text = "\n".join(lines)
    with open(output_path, 'w') as f:
        f.write(report_text)

    return verdict


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 soak_report.py <output_dir>")
        print("  output_dir should contain metrics.csv, events.log, config.txt")
        sys.exit(1)

    output_dir = sys.argv[1]
    csv_path = os.path.join(output_dir, "metrics.csv")
    events_path = os.path.join(output_dir, "events.log")
    config_path = os.path.join(output_dir, "config.txt")
    report_path = os.path.join(output_dir, "report.md")

    if not os.path.exists(csv_path):
        print(f"[!] No metrics.csv found in {output_dir}")
        sys.exit(1)

    verdict = generate_report(csv_path, events_path, config_path, report_path)
    print(f"Report generated: {report_path}")
    print(f"Verdict: {verdict}")


if __name__ == "__main__":
    main()

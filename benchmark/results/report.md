# pg_duckpipe Benchmark Report

## Summary

| Metric | Single-table append (1T, oltp_insert) | Multi-table append (4T, oltp_insert) | Single-table mixed (1T, oltp_read_write) | Multi-table mixed (4T, oltp_read_write) |
|--------|---|---|---|---|
| Snapshot (rows/s) | 136,240 | 163,599 | 132,450 | 150,830 |
| OLTP TPS | 10,102.3 | 9,412.7 | 626.6 | 450.3 |
| Avg Lag (MB) | 3.3 | 64.6 | 170.8 | 369.3 |
| Peak Lag (MB) | 5.3 | 125.5 | 189.2 | 390.9 |
| Catch-up (s) | 2.2 | 2.4 | 65.6 | 69.0 |
| Catch-up (rows/s) | - | - | - | - |
| Consistency | PASS | PASS | PASS | PASS |

## Flush Performance

| Metric | Single-table append (1T, oltp_insert) | Multi-table append (4T, oltp_insert) | Single-table mixed (1T, oltp_read_write) | Multi-table mixed (4T, oltp_read_write) |
|--------|---|---|---|---|
| Flush count | 31 | 121 | 30 | 122 |
| Avg latency (ms) | 35.1 | 17.6 | 31.0 | 25.6 |
| P50 latency (ms) | 33.6 | 15.2 | 30.3 | 22.8 |
| P99 latency (ms) | 63.6 | 107 | 48.4 | 122.1 |
| Avg rows/flush | 9,776.6 | 2,333.8 | 3,760 | 664.4 |

### Flush Phase Breakdown (avg ms)

| Phase | Single-table append (1T, oltp_insert) | Multi-table append (4T, oltp_insert) | Single-table mixed (1T, oltp_read_write) | Multi-table mixed (4T, oltp_read_write) |
|-------|---|---|---|---|
| discover | 0.5 | 0.5 | - | - |
| buf_create | 0.4 | 0.3 | 0.3 | 0.3 |
| load | 16.9 | 4.7 | 6.8 | 1.6 |
| compact | 4.9 | 3.5 | 3.8 | 3.2 |
| begin | 0.1 | 0.1 | 0.1 | 0.1 |
| delete | 0.3 | 0.3 | 14.8 | 8.3 |
| insert | 9.3 | 4.0 | 2.0 | 1.5 |
| commit | 2.0 | 3.6 | 2.7 | 10.0 |
| cleanup | 0.4 | 0.4 | 0.4 | 0.4 |

## Snapshot Timings

**Single-table append (1T, oltp_insert)**

- `public.sbtest1`: 100,000 rows in 734.0ms (136240 rows/s)

**Multi-table append (4T, oltp_insert)**

- `public.sbtest1`: 100,000 rows in 2423.0ms (41271 rows/s)
- `public.sbtest2`: 100,000 rows in 2405.0ms (41580 rows/s)
- `public.sbtest3`: 100,000 rows in 2235.0ms (44743 rows/s)
- `public.sbtest4`: 100,000 rows in 2445.0ms (40900 rows/s)

**Single-table mixed (1T, oltp_read_write)**

- `public.sbtest1`: 100,000 rows in 755.0ms (132450 rows/s)

**Multi-table mixed (4T, oltp_read_write)**

- `public.sbtest1`: 100,000 rows in 2545.0ms (39293 rows/s)
- `public.sbtest2`: 100,000 rows in 2627.0ms (38066 rows/s)
- `public.sbtest3`: 100,000 rows in 2642.0ms (37850 rows/s)
- `public.sbtest4`: 100,000 rows in 2652.0ms (37707 rows/s)


## WAL Processing Cycle Times

| Metric | Single-table append (1T, oltp_insert) | Multi-table append (4T, oltp_insert) | Single-table mixed (1T, oltp_read_write) | Multi-table mixed (4T, oltp_read_write) |
|--------|---|---|---|---|
| Cycles | 35 | 51 | 2,671 | 3,184 |
| Avg (ms) | 1,049.3 | 804.7 | 19.1 | 13.9 |
| P99 (ms) | 5,003.3 | 5,005.2 | 18.9 | 9.3 |

## Issues & Observations

No errors, warnings, or anomalies detected.

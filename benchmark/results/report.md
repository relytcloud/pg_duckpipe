# pg_duckpipe Benchmark Report

## Summary

| Metric | Single-table append (1T, oltp_insert) | Multi-table append (4T, oltp_insert) | Single-table mixed (1T, oltp_read_write) | Multi-table mixed (4T, oltp_read_write) |
|--------|---|---|---|---|
| Snapshot (rows/s) | 115,607 | 241,984 | 130,719 | 230,814 |
| OLTP TPS | 7,051.5 | 6,176.4 | 448.6 | 329.0 |
| Avg Lag (MB) | 1.7 | 2.6 | 0.9 | 4.3 |
| Peak Lag (MB) | 4.5 | 5.1 | 1.9 | 47.7 |
| Catch-up (s) | 2.2 | 2.4 | 66.2 | 68.8 |
| Catch-up (rows/s) | 141 | 525 | - | - |
| Consistency | PASS | PASS | PASS | PASS |

## Flush Performance

| Metric | Single-table append (1T, oltp_insert) | Multi-table append (4T, oltp_insert) | Single-table mixed (1T, oltp_read_write) | Multi-table mixed (4T, oltp_read_write) |
|--------|---|---|---|---|
| Flush count | 29 | 115 | 27 | 112 |
| Avg latency (ms) | 52.9 | 48.1 | 109.6 | 80.1 |
| P50 latency (ms) | 46.5 | 28.3 | 105.2 | 60.7 |
| P99 latency (ms) | 183.1 | 324.2 | 226.4 | 488.8 |
| Avg rows/flush | 7,295.1 | 1,611.3 | 2,990.9 | 529.0 |

### Flush Phase Breakdown (avg ms)

| Phase | Single-table append (1T, oltp_insert) | Multi-table append (4T, oltp_insert) | Single-table mixed (1T, oltp_read_write) | Multi-table mixed (4T, oltp_read_write) |
|-------|---|---|---|---|
| discover | 2.6 | 2.6 | 1.5 | 3.4 |
| buf_create | 0.4 | 0.5 | 0.4 | 0.5 |
| load | 14.4 | 4.0 | 6.3 | 2.0 |
| compact | 7.3 | 4.5 | 5.2 | 4.3 |
| begin | 0.2 | 0.1 | 0.2 | 0.2 |
| delete | 1.1 | 1.4 | 25.0 | 17.7 |
| insert | 9.7 | 5.1 | 3.3 | 1.7 |
| commit | 16.3 | 29.3 | 67.2 | 49.7 |
| cleanup | 0.7 | 0.5 | 0.5 | 0.5 |

## Snapshot Timings

**Single-table append (1T, oltp_insert)**

- `public.sbtest1`: 100,000 rows in 865.0ms (115607 rows/s)

**Multi-table append (4T, oltp_insert)**

- `public.sbtest1`: 100,000 rows in 808.0ms (123762 rows/s)
- `public.sbtest2`: 100,000 rows in 1630.0ms (61350 rows/s)
- `public.sbtest3`: 100,000 rows in 1653.0ms (60496 rows/s)
- `public.sbtest4`: 100,000 rows in 1549.0ms (64558 rows/s)

**Single-table mixed (1T, oltp_read_write)**

- `public.sbtest1`: 100,000 rows in 765.0ms (130719 rows/s)

**Multi-table mixed (4T, oltp_read_write)**

- `public.sbtest1`: 100,000 rows in 872.0ms (114679 rows/s)
- `public.sbtest2`: 100,000 rows in 1719.0ms (58173 rows/s)
- `public.sbtest3`: 100,000 rows in 1733.0ms (57703 rows/s)
- `public.sbtest4`: 100,000 rows in 1606.0ms (62267 rows/s)


## WAL Processing Cycle Times

| Metric | Single-table append (1T, oltp_insert) | Multi-table append (4T, oltp_insert) | Single-table mixed (1T, oltp_read_write) | Multi-table mixed (4T, oltp_read_write) |
|--------|---|---|---|---|
| Cycles | 147 | 170 | 2,332 | 2,264 |
| Avg (ms) | 231.8 | 209.7 | 12.1 | 12.5 |
| P99 (ms) | 2,003.0 | 2,003.8 | 15.5 | 15.3 |

## Issues & Observations

No errors, warnings, or anomalies detected.

# pg_duckpipe Benchmark Report

## Summary

| Metric | Single-table append (1T, oltp_insert) | Multi-table append (4T, oltp_insert) | Single-table mixed (1T, oltp_read_write) | Multi-table mixed (4T, oltp_read_write) |
|--------|---|---|---|---|
| Snapshot (rows/s) | 14,569 | 26,353 | 6,020 | 38,858 |
| OLTP TPS | 9,261.0 | 8,075.7 | 613.6 | 444.4 |
| Avg Lag (MB) | 2.7 | 55.0 | 154 | 332.1 |
| Peak Lag (MB) | 4.0 | 126 | 171.7 | 357.3 |
| Catch-up (s) | 2.2 | 2.5 | 65.4 | 68.5 |
| Catch-up (rows/s) | - | 109 | - | - |
| Consistency | PASS | PASS | PASS | PASS |

## Flush Performance

| Metric | Single-table append (1T, oltp_insert) | Multi-table append (4T, oltp_insert) | Single-table mixed (1T, oltp_read_write) | Multi-table mixed (4T, oltp_read_write) |
|--------|---|---|---|---|
| Flush count | 29 | 119 | 30 | 120 |
| Avg latency (ms) | 41.3 | 24.9 | 35.0 | 33.9 |
| P50 latency (ms) | 33.8 | 16.8 | 33.2 | 23.9 |
| P99 latency (ms) | 229.2 | 240.9 | 109.5 | 169.4 |
| Avg rows/flush | 9,580.6 | 2,036.0 | 3,682.2 | 666.6 |

### Flush Phase Breakdown (avg ms)

| Phase | Single-table append (1T, oltp_insert) | Multi-table append (4T, oltp_insert) | Single-table mixed (1T, oltp_read_write) | Multi-table mixed (4T, oltp_read_write) |
|-------|---|---|---|---|
| discover | 0.6 | 0.5 | - | - |
| buf_create | 0.5 | 0.3 | 0.3 | 0.4 |
| load | 18.2 | 4.4 | 6.9 | 1.7 |
| compact | 6.6 | 3.8 | 3.9 | 3.5 |
| begin | 0.1 | 0.1 | 0.1 | 0.1 |
| delete | 2.1 | 0.2 | 14.8 | 9.2 |
| insert | 9.9 | 5.9 | 2.8 | 1.6 |
| commit | 2.0 | 9.0 | 5.5 | 16.9 |
| cleanup | 0.5 | 0.4 | 0.4 | 0.4 |

## Snapshot Timings

**Single-table append (1T, oltp_insert)**

- `public.sbtest1`: 100,000 rows in 799.7ms (125046 rows/s)

**Multi-table append (4T, oltp_insert)**

- `public.sbtest1`: 100,000 rows in 2903.2ms (34445 rows/s)
- `public.sbtest2`: 100,000 rows in 3150.2ms (31744 rows/s)
- `public.sbtest3`: 100,000 rows in 3162.5ms (31621 rows/s)
- `public.sbtest4`: 100,000 rows in 3187.5ms (31373 rows/s)

**Single-table mixed (1T, oltp_read_write)**

- `public.sbtest1`: 100,000 rows in 1037.5ms (96383 rows/s)

**Multi-table mixed (4T, oltp_read_write)**

- `public.sbtest1`: 100,000 rows in 4152.2ms (24084 rows/s)
- `public.sbtest3`: 100,000 rows in 4231.0ms (23635 rows/s)
- `public.sbtest4`: 100,000 rows in 4369.0ms (22889 rows/s)
- `public.sbtest2`: 100,000 rows in 4550.1ms (21978 rows/s)


## WAL Processing Cycle Times

| Metric | Single-table append (1T, oltp_insert) | Multi-table append (4T, oltp_insert) | Single-table mixed (1T, oltp_read_write) | Multi-table mixed (4T, oltp_read_write) |
|--------|---|---|---|---|
| Cycles | 61 | 70 | 2,631 | 3,059 |
| Avg (ms) | 596.8 | 581.6 | 19.4 | 14.6 |
| P99 (ms) | 5,004.0 | 5,004.6 | 17.0 | 10.5 |

## Issues & Observations

No errors, warnings, or anomalies detected.

# pg_duckpipe Benchmark Report

## Summary

| Metric | Single-table append (1T, oltp_insert) | Multi-table append (4T, oltp_insert) | Single-table mixed (1T, oltp_read_write) | Multi-table mixed (4T, oltp_read_write) |
|--------|---|---|---|---|
| Snapshot (rows/s) | 114,416 | 177,936 | 124,533 | 252,366 |
| OLTP TPS | 6,025.1 | 6,275.9 | 491.2 | 343.2 |
| Avg Lag (MB) | 1.7 | 2.5 | 0.8 | 1.4 |
| Peak Lag (MB) | 2.8 | 4.6 | 2.1 | 2.3 |
| Catch-up (s) | 2.2 | 2.5 | 65.2 | 68.5 |
| Catch-up (rows/s) | - | - | - | - |
| Consistency | PASS | PASS | PASS | PASS |

## Flush Performance

| Metric | Single-table append (1T, oltp_insert) | Multi-table append (4T, oltp_insert) | Single-table mixed (1T, oltp_read_write) | Multi-table mixed (4T, oltp_read_write) |
|--------|---|---|---|---|
| Flush count | 14 | 56 | 14 | 56 |
| Avg latency (ms) | 62.2 | 63.9 | 83.5 | 86.9 |
| P50 latency (ms) | 51.2 | 28.7 | 94.8 | 57.2 |
| P99 latency (ms) | 141.6 | 405.9 | 129.4 | 686.2 |
| Avg rows/flush | 6,456.1 | 1,681.1 | 3,158.6 | 551.8 |

### Flush Phase Breakdown (avg ms)

| Phase | Single-table append (1T, oltp_insert) | Multi-table append (4T, oltp_insert) | Single-table mixed (1T, oltp_read_write) | Multi-table mixed (4T, oltp_read_write) |
|-------|---|---|---|---|
| discover | 3.4 | 3.9 | 2.8 | 5.6 |
| buf_create | 0.4 | 2.5 | 0.3 | 0.3 |
| load | 16.4 | 5.0 | 6.3 | 1.6 |
| compact | 11.2 | 9.1 | 4.4 | 4.0 |
| begin | 0.2 | 0.2 | 0.2 | 0.1 |
| delete | 0.8 | 3.3 | 17.1 | 16.0 |
| insert | 11.6 | 5.2 | 2.8 | 1.7 |
| commit | 17.3 | 34.0 | 48.9 | 57.0 |
| cleanup | 0.6 | 0.6 | 0.5 | 0.4 |

## Snapshot Timings

**Single-table append (1T, oltp_insert)**

- `public.sbtest1`: 100,000 rows in 874.0ms (114416 rows/s)

**Multi-table append (4T, oltp_insert)**

- `public.sbtest1`: 100,000 rows in 844.0ms (118483 rows/s)
- `public.sbtest2`: 100,000 rows in 2248.0ms (44484 rows/s)
- `public.sbtest3`: 100,000 rows in 2150.0ms (46512 rows/s)
- `public.sbtest4`: 100,000 rows in 2234.0ms (44763 rows/s)

**Single-table mixed (1T, oltp_read_write)**

- `public.sbtest1`: 100,000 rows in 803.0ms (124533 rows/s)

**Multi-table mixed (4T, oltp_read_write)**

- `public.sbtest1`: 100,000 rows in 1252.0ms (79872 rows/s)
- `public.sbtest2`: 100,000 rows in 1585.0ms (63091 rows/s)
- `public.sbtest3`: 100,000 rows in 1563.0ms (63980 rows/s)
- `public.sbtest4`: 100,000 rows in 1475.0ms (67797 rows/s)


## WAL Processing Cycle Times

| Metric | Single-table append (1T, oltp_insert) | Multi-table append (4T, oltp_insert) | Single-table mixed (1T, oltp_read_write) | Multi-table mixed (4T, oltp_read_write) |
|--------|---|---|---|---|
| Cycles | 95 | 87 | 1,233 | 1,130 |
| Avg (ms) | 202.7 | 274.1 | 18.0 | 21.5 |
| P99 (ms) | 2,003.5 | 2,023.1 | 16.6 | 19.7 |

## Issues & Observations

No errors, warnings, or anomalies detected.

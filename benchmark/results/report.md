# pg_duckpipe Benchmark Report

## Summary

| Metric | Single-table append (1T, oltp_insert) | Multi-table append (4T, oltp_insert) | Single-table mixed (1T, oltp_read_write) | Multi-table mixed (4T, oltp_read_write) |
|--------|---|---|---|---|
| Snapshot (rows/s) | 127,065 | 320,256 | 184,162 | 299,401 |
| OLTP TPS | 3,810.7 | 6,780.7 | 416.2 | 314.8 |
| Avg Lag (MB) | 1.5 | 2.6 | 0.9 | 2.4 |
| Peak Lag (MB) | 3.6 | 4.7 | 1.8 | 12.7 |
| Catch-up (s) | 2.3 | 2.4 | 65.8 | 69.2 |
| Catch-up (rows/s) | 44.0 | 1,175 | - | - |
| Consistency | PASS | PASS | PASS | PASS |

## Flush Performance

| Metric | Single-table append (1T, oltp_insert) | Multi-table append (4T, oltp_insert) | Single-table mixed (1T, oltp_read_write) | Multi-table mixed (4T, oltp_read_write) |
|--------|---|---|---|---|
| Flush count | 27 | 116 | 27 | 112 |
| Avg latency (ms) | 110.2 | 39.4 | 121.3 | 83.2 |
| P50 latency (ms) | 66.2 | 24.9 | 113.2 | 63.8 |
| P99 latency (ms) | 739.5 | 340.4 | 280.5 | 467.3 |
| Avg rows/flush | 4,245.8 | 1,753.8 | 2,775.3 | 505.9 |

### Flush Phase Breakdown (avg ms)

| Phase | Single-table append (1T, oltp_insert) | Multi-table append (4T, oltp_insert) | Single-table mixed (1T, oltp_read_write) | Multi-table mixed (4T, oltp_read_write) |
|-------|---|---|---|---|
| discover | 1.1 | 3.2 | 2.0 | 2.1 |
| buf_create | 1.4 | 0.4 | 0.5 | 0.6 |
| load | 15.2 | 3.1 | 3.5 | 1.2 |
| compact | 16.3 | 4.8 | 6.2 | 4.3 |
| begin | 0.2 | 0.1 | 0.2 | 0.1 |
| delete | 0.4 | 1.7 | 24.3 | 19.2 |
| insert | 14.8 | 4.9 | 3.8 | 1.7 |
| commit | 60.2 | 20.6 | 80.3 | 53.4 |
| cleanup | 0.6 | 0.5 | 0.5 | 0.5 |

## Snapshot Timings

**Single-table append (1T, oltp_insert)**

- `public.sbtest1`: 100,000 rows in 787.0ms (127065 rows/s)

**Multi-table append (4T, oltp_insert)**

- `public.sbtest1`: 100,000 rows in 742.0ms (134771 rows/s)
- `public.sbtest2`: 100,000 rows in 1146.0ms (87260 rows/s)
- `public.sbtest3`: 100,000 rows in 1249.0ms (80064 rows/s)
- `public.sbtest4`: 100,000 rows in 1234.0ms (81037 rows/s)

**Single-table mixed (1T, oltp_read_write)**

- `public.sbtest1`: 100,000 rows in 543.0ms (184162 rows/s)

**Multi-table mixed (4T, oltp_read_write)**

- `public.sbtest1`: 100,000 rows in 697.0ms (143472 rows/s)
- `public.sbtest2`: 100,000 rows in 1336.0ms (74850 rows/s)
- `public.sbtest3`: 100,000 rows in 1085.0ms (92166 rows/s)
- `public.sbtest4`: 100,000 rows in 1194.0ms (83752 rows/s)


## WAL Processing Cycle Times

| Metric | Single-table append (1T, oltp_insert) | Multi-table append (4T, oltp_insert) | Single-table mixed (1T, oltp_read_write) | Multi-table mixed (4T, oltp_read_write) |
|--------|---|---|---|---|
| Cycles | 402 | 140 | 2,594 | 2,526 |
| Avg (ms) | 66.4 | 254.9 | 10.7 | 11.5 |
| P99 (ms) | 928.5 | 2,004.1 | 14.8 | 13.3 |

## Issues & Observations

No errors, warnings, or anomalies detected.

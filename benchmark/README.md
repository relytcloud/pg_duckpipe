# pg_duckpipe Performance Benchmark

This directory contains a benchmark suite for `pg_duckpipe` utilizing `sysbench` for workload generation.

## Files

-   `BENCHMARK_DESIGN.md`: Design document detailing objectives and methodology.
-   `run_sysbench.py`: Main Python script to run the benchmark using `sysbench`.
-   `start_db.sh`: Helper to start a temporary PostgreSQL instance with correct configuration.
-   `stop_db.sh`: Helper to stop the temporary instance.

## Prerequisites

-   `sysbench` installed (e.g., `brew install sysbench` or `apt install sysbench`).
-   PostgreSQL 14+ with `pg_duckdb` and `pg_duckpipe` extensions.

## Usage

1.  **Start the Database**:
    ```bash
    ./start_db.sh
    ```
    This creates a new data directory `bench_data` and starts Postgres on port 5556.
    The benchmark config also sets conservative DuckPipe batch sizes to improve
    apply visibility during correctness checks.

2.  **Run Benchmark**:
    ```bash
    # Run with default settings (append-only: oltp_insert)
    python3 run_sysbench.py --db-url "host=localhost port=5556 user=postgres dbname=postgres"
    
    # Run mixed OLTP workload (insert/update/delete)
    python3 run_sysbench.py \
        --workload oltp_read_write \
        --db-url "host=localhost port=5556 user=postgres dbname=postgres"

    # Customize parameters
    python3 run_sysbench.py \
        --workload oltp_read_write \
        --tables 4 \
        --table-size 50000 \
        --threads 8 \
        --duration 60 \
        --catchup-timeout 600 \
        --db-url "host=localhost port=5556 user=postgres dbname=postgres"

    # Force full row-by-row consistency check (more expensive)
    python3 run_sysbench.py \
        --consistency-mode full \
        --db-url "host=localhost port=5556 user=postgres dbname=postgres"

    # Run checks even when catch-up timeout is hit
    python3 run_sysbench.py \
        --verify-on-timeout \
        --db-url "host=localhost port=5556 user=postgres dbname=postgres"
    ```

3.  **Stop the Database**:
    ```bash
    ./stop_db.sh
    ```

## Methodology

1.  **Preparation**: Uses `sysbench <workload> prepare` to create and populate source tables (`sbtestN`).
2.  **Snapshot Benchmark**:
    -   Creates corresponding target tables in DuckDB (`ducklake.sbtestN`).
    -   Enables `pg_duckpipe` sync.
    -   Measures time to sync initial data.
3.  **Streaming Benchmark**:
    -   By default runs `sysbench oltp_insert run` for append-only traffic.
    -   Optionally runs `sysbench oltp_read_write run` for mixed INSERT/UPDATE/DELETE traffic.
    -   Consistency checks default to `safe` for append-only and `full` for mixed workload.
    -   Monitors replication status (TODO: lag measurement).

## Sysbench DML Behavior

This benchmark supports two workloads:

1.  `oltp_insert` (default, append-only)
    -   INSERT per transaction: `1`
    -   UPDATE per transaction: `0`
    -   DELETE per transaction: `0`
    -   Expected DuckPipe apply events per transaction: `1`

2.  `oltp_read_write` (mixed OLTP)
    -   sysbench defaults: `index_updates=1`, `non_index_updates=1`, `delete_inserts=1`
    -   INSERT per transaction: `1`
    -   UPDATE per transaction: `2`
    -   DELETE per transaction: `1`
    -   Expected DuckPipe apply events per transaction: `6`
      (UPDATE is decoded/applied as DELETE+INSERT in DuckPipe)

The wrapper uses these per-transaction event counts to estimate expected
`rows_synced` delta during catch-up.

## Consistency Modes

-   `auto` (default): `safe` for `oltp_insert`, `full` for `oltp_read_write`.
-   `safe`: checks only metadata (`rows_synced` delta vs expected and final lag).
-   `full`: checks value-level parity and missing/extra keys (heavier SQL).
-   `off`: skip consistency checks.

By default, consistency checks are skipped when catch-up does not complete
before timeout. Use `--verify-on-timeout` to force checks in that case.

## Results

The script reports:
-   **Snapshot Throughput**: Average rows per second synced during initial copy.
-   **OLTP Throughput**: Transactions per second on the source database (from `sysbench`).

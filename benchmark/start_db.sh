#!/bin/bash
set -e

# Configuration
# Resolving absolute path to postgres bin
PG_BIN="/Users/xiaoyuwei/Desktop/workspace_ducklake/postgres/work/app/bin"
BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="$BENCH_DIR/bench_data"
LOG_FILE="$BENCH_DIR/bench_pg.log"
PORT=5556

export PATH="$PG_BIN:$PATH"

# Cleanup previous run
if [ -d "$DATA_DIR" ]; then
    echo "[-] Removing existing data dir..."
    "$PG_BIN/pg_ctl" -D "$DATA_DIR" stop -m immediate 2>/dev/null || true
    rm -rf "$DATA_DIR"
fi

# Init DB
echo "[-] Initializing database..."
"$PG_BIN/initdb" -D "$DATA_DIR" -E UTF8 --no-locale -U postgres > /dev/null

# Configure
echo "[-] Configuring..."
cat >> "$DATA_DIR/postgresql.conf" <<EOF
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
shared_preload_libraries = 'pg_duckdb,pg_ducklake,pg_duckpipe'
port = $PORT
listen_addresses = 'localhost'
unix_socket_directories = '/tmp'
log_min_messages = warning
duckdb.unsafe_allow_mixed_transactions=on
duckpipe.debug_log=on
duckpipe.flush_batch_threshold=10000
duckpipe.data_inlining_row_limit=1000
# Longer poll cycle so WAL scan overhead doesn't crowd out data delivery.
# Each cycle gets poll_interval/2 ms to read WAL; with large backlogs the server
# needs several hundred ms just to scan past restart_lsn before it can stream new data.
duckpipe.poll_interval=10000
EOF

# Start each benchmark run with a fresh log file to avoid mixing old failures.
: > "$LOG_FILE"

# Start
echo "[-] Starting PostgreSQL on port $PORT..."
"$PG_BIN/pg_ctl" -D "$DATA_DIR" -l "$LOG_FILE" start

# Wait for startup
echo "[-] Waiting for socket..."
for i in {1..10}; do
    if "$PG_BIN/pg_isready" -h localhost -p $PORT -U postgres >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

# The bgworker connects back via the replication protocol using the OS user ($USER).
# Create that role as superuser so the WAL streaming connection succeeds.
OS_USER=$(whoami)
if [ "$OS_USER" != "postgres" ]; then
    echo "[-] Creating OS user role '$OS_USER'..."
    "$PG_BIN/psql" -h localhost -p $PORT -U postgres \
        -c "CREATE USER \"$OS_USER\" SUPERUSER;" 2>/dev/null || true
fi

echo "[+] Database started!"
echo "    Port: $PORT"
echo "    User: postgres (OS user '$OS_USER' also created)"
echo "    Log:  $LOG_FILE"
echo ""
echo "Run benchmark:"
echo "python3 $BENCH_DIR/run_sysbench.py --db-url 'host=localhost port=$PORT user=postgres dbname=postgres'"

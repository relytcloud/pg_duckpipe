#!/bin/bash
# Allow DuckDB's postgres_scanner to connect via TCP from localhost without a
# password.  DuckDB does not support unix-socket connections, so flush workers
# connect to 127.0.0.1 instead of the unix socket used by the bgworker itself.
set -e
echo "host all all 127.0.0.1/32 trust" >> "$PGDATA/pg_hba.conf"
echo "host all all ::1/128      trust" >> "$PGDATA/pg_hba.conf"

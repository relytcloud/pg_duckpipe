#!/usr/bin/env bash
# Download the ducklake DuckDB extension to the standard offline cache directory.
#
# Usage: fetch-ducklake-ext.sh [duckdb-version]
# Default version: v1.4.3  (must match libduckdb.so shipped by pg_ducklake)
#
# NOTE: postgres_scanner (a ducklake dependency) must be bundled by pg_ducklake.
# The version on extensions.duckdb.org lacks the -Bsymbolic fix
# (duckdb/duckdb-postgres#402) needed to run inside a PG backend.
#
# Installs to: $DUCKDB_HOME/.duckdb/extensions/<ver>/<arch>/ducklake.duckdb_extension
#
# Set DUCKDB_HOME to override the base directory (default: /var/lib/postgresql).

set -euo pipefail

DUCKDB_VER="${1:-v1.4.3}"
DUCKDB_HOME="${DUCKDB_HOME:-/var/lib/postgresql}"
ARCH=$(uname -m | sed 's/x86_64/linux_amd64/;s/aarch64/linux_arm64/')
EXT_DIR="${DUCKDB_HOME}/.duckdb/extensions/${DUCKDB_VER}/${ARCH}"

mkdir -p "${EXT_DIR}"
curl -fsSL \
    "https://extensions.duckdb.org/${DUCKDB_VER}/${ARCH}/ducklake.duckdb_extension.gz" \
    | gzip -d > "${EXT_DIR}/ducklake.duckdb_extension"

echo "ducklake ${DUCKDB_VER} (${ARCH}) installed to ${EXT_DIR}"

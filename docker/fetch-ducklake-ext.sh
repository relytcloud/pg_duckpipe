#!/usr/bin/env bash
# Download DuckDB extensions to the standard offline cache directory.
#
# Usage: fetch-ducklake-ext.sh [duckdb-version]
# Default version: v1.4.3  (must match libduckdb.so shipped by pg_ducklake)
#
# Extensions installed:
#   - ducklake              (always)
#   - postgres_scanner      (only when FETCH_POSTGRES_SCANNER=1)
#
# The upstream postgres_scanner from extensions.duckdb.org lacks the
# -Bsymbolic fix (duckdb/duckdb-postgres#402) needed inside PG backends,
# so the PG extension image must NOT use it.  The standalone daemon runs
# outside PG and can use the upstream version safely.
#
# Installs to: $DUCKDB_HOME/.duckdb/extensions/<ver>/<arch>/
#
# Set DUCKDB_HOME to override the base directory (default: /var/lib/postgresql).

set -euo pipefail

DUCKDB_VER="${1:-v1.4.3}"
DUCKDB_HOME="${DUCKDB_HOME:-/var/lib/postgresql}"
FETCH_POSTGRES_SCANNER="${FETCH_POSTGRES_SCANNER:-0}"
ARCH=$(uname -m | sed 's/x86_64/linux_amd64/;s/aarch64/linux_arm64/')
EXT_DIR="${DUCKDB_HOME}/.duckdb/extensions/${DUCKDB_VER}/${ARCH}"

mkdir -p "${EXT_DIR}"

curl -fsSL \
    "https://extensions.duckdb.org/${DUCKDB_VER}/${ARCH}/ducklake.duckdb_extension.gz" \
    | gzip -d > "${EXT_DIR}/ducklake.duckdb_extension"
echo "ducklake ${DUCKDB_VER} (${ARCH}) installed to ${EXT_DIR}"

if [ "${FETCH_POSTGRES_SCANNER}" = "1" ]; then
    curl -fsSL \
        "https://extensions.duckdb.org/${DUCKDB_VER}/${ARCH}/postgres_scanner.duckdb_extension.gz" \
        | gzip -d > "${EXT_DIR}/postgres_scanner.duckdb_extension"
    echo "postgres_scanner ${DUCKDB_VER} (${ARCH}) installed to ${EXT_DIR}"
fi

#!/usr/bin/env bash
# Download the ducklake (and its dependency postgres_scanner) DuckDB extensions
# to the standard offline cache directory.
#
# Usage: fetch-ducklake-ext.sh [duckdb-version]
# Default version: v1.4.3  (must match libduckdb.so shipped by pg_ducklake)
#
# Installs to: /var/lib/postgresql/.duckdb/extensions/<ver>/<arch>/

set -euo pipefail

DUCKDB_VER="${1:-v1.4.3}"
ARCH=$(uname -m | sed 's/x86_64/linux_amd64/;s/aarch64/linux_arm64/')
EXT_DIR="/var/lib/postgresql/.duckdb/extensions/${DUCKDB_VER}/${ARCH}"

mkdir -p "${EXT_DIR}"

for ext in ducklake postgres_scanner; do
    curl -fsSL \
        "https://extensions.duckdb.org/${DUCKDB_VER}/${ARCH}/${ext}.duckdb_extension.gz" \
        | gzip -d > "${EXT_DIR}/${ext}.duckdb_extension"
    echo "${ext} ${DUCKDB_VER} (${ARCH}) installed to ${EXT_DIR}"
done

#!/usr/bin/env bash
# Download DuckDB extensions to the standard offline cache directory.
#
# Usage: fetch-ducklake-ext.sh [duckdb-version]
# Default version: v1.4.3  (must match libduckdb.so shipped by pg_ducklake)
#
# Extensions installed:
#   - ducklake              (unless SKIP_DUCKLAKE=1)
#   - postgres_scanner      (only when FETCH_POSTGRES_SCANNER=1)
#
# The upstream postgres_scanner from extensions.duckdb.org lacks the
# -Bsymbolic fix (duckdb/duckdb-postgres#402) needed inside PG backends,
# so the PG extension image must NOT use it.  The standalone daemon runs
# outside PG and can use the upstream version safely.
#
# Installs to: $DUCKDB_HOME/.duckdb/extensions/<ver>/<arch>/
#
# When pg_config is on PATH, also installs ducklake.duckdb_extension to
# pkglibdir for direct LOAD at runtime (no INSTALL needed).
#
# Set DUCKDB_HOME to override the base directory (default: /var/lib/postgresql).

set -euo pipefail

DUCKDB_VER="${1:-v1.4.3}"
DUCKDB_HOME="${DUCKDB_HOME:-/var/lib/postgresql}"
SKIP_DUCKLAKE="${SKIP_DUCKLAKE:-0}"
FETCH_POSTGRES_SCANNER="${FETCH_POSTGRES_SCANNER:-0}"
ARCH=$(uname -m | sed 's/x86_64/linux_amd64/;s/aarch64/linux_arm64/')
EXT_DIR="${DUCKDB_HOME}/.duckdb/extensions/${DUCKDB_VER}/${ARCH}"

# Auto-detect pkglibdir for direct LOAD at runtime
PKGLIBDIR=""
if command -v pg_config >/dev/null 2>&1; then
    PKGLIBDIR="$(pg_config --pkglibdir)"
fi

mkdir -p "${EXT_DIR}"

if [ "${SKIP_DUCKLAKE}" != "1" ]; then
    curl -fsSL \
        "https://extensions.duckdb.org/${DUCKDB_VER}/${ARCH}/ducklake.duckdb_extension.gz" \
        | gzip -d > "${EXT_DIR}/ducklake.duckdb_extension"
    echo "ducklake ${DUCKDB_VER} (${ARCH}) installed to ${EXT_DIR}"

    if [ -n "${PKGLIBDIR}" ]; then
        install -m 644 "${EXT_DIR}/ducklake.duckdb_extension" "${PKGLIBDIR}/ducklake.duckdb_extension"
        echo "ducklake.duckdb_extension also installed to ${PKGLIBDIR}"
    fi
fi

if [ "${FETCH_POSTGRES_SCANNER}" = "1" ]; then
    curl -fsSL \
        "https://extensions.duckdb.org/${DUCKDB_VER}/${ARCH}/postgres_scanner.duckdb_extension.gz" \
        | gzip -d > "${EXT_DIR}/postgres_scanner.duckdb_extension"
    echo "postgres_scanner ${DUCKDB_VER} (${ARCH}) installed to ${EXT_DIR}"
fi

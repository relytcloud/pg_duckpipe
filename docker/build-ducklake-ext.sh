#!/usr/bin/env bash
# Build ducklake.duckdb_extension from a pg_ducklake source tree.
#
# Usage: build-ducklake-ext.sh [output-dir]
#
# If output-dir is omitted, auto-detects via `pg_config --pkglibdir`.
#
# Environment variables:
#   DUCKLAKE_REPO  — path to pg_ducklake checkout (default: ../pg_ducklake)
#
# Requires: cmake, ninja (or make), C++ compiler

set -euo pipefail

REPO="${DUCKLAKE_REPO:-../pg_ducklake}"

# Output directory: argument > pg_config auto-detect
OUTPUT_DIR="${1:-}"
if [ -z "${OUTPUT_DIR}" ] && command -v pg_config >/dev/null 2>&1; then
    OUTPUT_DIR="$(pg_config --pkglibdir)"
fi
if [ -z "${OUTPUT_DIR}" ]; then
    echo "Usage: build-ducklake-ext.sh <output-dir>"
    echo "Or ensure pg_config is on PATH for auto-detection."
    exit 1
fi

# Skip if already built (use FORCE=1 to rebuild)
if [ -f "${OUTPUT_DIR}/ducklake.duckdb_extension" ] && [ "${FORCE:-0}" != "1" ]; then
    echo "==> ducklake.duckdb_extension already exists at ${OUTPUT_DIR}. Skipping (set FORCE=1 to rebuild)."
    exit 0
fi

DUCKLAKE_SRC="${REPO}/third_party/ducklake"
if [ ! -d "${DUCKLAKE_SRC}" ]; then
    echo "ERROR: ${DUCKLAKE_SRC} not found. Is DUCKLAKE_REPO a pg_ducklake checkout?"
    exit 1
fi
if [ ! -f "${DUCKLAKE_SRC}/duckdb/CMakeLists.txt" ]; then
    echo "ERROR: ${DUCKLAKE_SRC}/duckdb/ is empty. Run 'git submodule update --init --recursive' in pg_ducklake."
    exit 1
fi
cd "${DUCKLAKE_SRC}"

# Use Ninja if available, else Unix Makefiles
if command -v ninja >/dev/null 2>&1; then
    GEN="Ninja"
else
    GEN="Unix Makefiles"
fi

# Use a dedicated build dir to avoid conflicts with pg_ducklake's static lib
# build (which may have used build/release/ with a different DuckDB source).
BUILD_DIR="build/release_loadable"

echo "==> Building ducklake loadable extension (generator=${GEN}) ..."
EXT_CONFIG="${PWD}/extension_config.cmake"
cmake -G "${GEN}" \
    -DCMAKE_BUILD_TYPE=Release \
    -DDUCKDB_EXTENSION_CONFIGS="${EXT_CONFIG}" \
    -DEXTENSION_STATIC_BUILD=0 \
    -DBUILD_SHELL=0 \
    -DBUILD_UNITTESTS=0 \
    -S ./duckdb/ \
    -B "${BUILD_DIR}"
cmake --build "${BUILD_DIR}" --config Release --target ducklake_loadable_extension

EXT_FILE="${BUILD_DIR}/extension/ducklake/ducklake.duckdb_extension"
if [ ! -f "${EXT_FILE}" ]; then
    echo "ERROR: ducklake.duckdb_extension not found at ${EXT_FILE}"
    echo "Build output:"
    find "${BUILD_DIR}" -name '*.duckdb_extension' 2>/dev/null || true
    exit 1
fi

mkdir -p "${OUTPUT_DIR}"
install -m 644 "${EXT_FILE}" "${OUTPUT_DIR}/ducklake.duckdb_extension"
echo "==> ducklake.duckdb_extension installed to ${OUTPUT_DIR}"

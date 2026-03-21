#!/usr/bin/env bash
# Build ducklake.duckdb_extension from pg_ducklake source.
#
# Usage: build-ducklake-ext.sh [output-dir]
#
# If output-dir is omitted, auto-detects via `pg_config --pkglibdir`.
#
# Environment variables:
#   DUCKLAKE_REPO    — git URL (default: https://github.com/relytcloud/pg_ducklake.git)
#   DUCKLAKE_COMMIT  — branch, tag, or commit (default: main)
#
# Requires: git, cmake, ninja (or make), C++ compiler

set -euo pipefail

REPO="${DUCKLAKE_REPO:-https://github.com/relytcloud/pg_ducklake.git}"
COMMIT="${DUCKLAKE_COMMIT:-main}"

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

WORKDIR=$(mktemp -d)
trap 'rm -rf "${WORKDIR}"' EXIT

echo "==> Cloning pg_ducklake @ ${COMMIT} ..."
git clone --depth 1 --branch "${COMMIT}" "${REPO}" "${WORKDIR}/pg_ducklake" || {
    # Fallback for commit hashes (--branch only works for branches/tags)
    rm -rf "${WORKDIR}/pg_ducklake"
    git clone --depth 50 "${REPO}" "${WORKDIR}/pg_ducklake"
    cd "${WORKDIR}/pg_ducklake"
    git checkout "${COMMIT}"
}

cd "${WORKDIR}/pg_ducklake"
git submodule update --init --recursive --depth 1
cd third_party/ducklake

# Use Ninja if available, else Unix Makefiles
if command -v ninja >/dev/null 2>&1; then
    GEN="Ninja"
else
    GEN="Unix Makefiles"
fi

echo "==> Building ducklake loadable extension (generator=${GEN}) ..."
EXT_CONFIG="${PWD}/extension_config.cmake"
mkdir -p build/release
cmake -G "${GEN}" \
    -DCMAKE_BUILD_TYPE=Release \
    -DDUCKDB_EXTENSION_CONFIGS="${EXT_CONFIG}" \
    -DEXTENSION_STATIC_BUILD=0 \
    -DBUILD_SHELL=0 \
    -DBUILD_UNITTESTS=0 \
    -S ./duckdb/ \
    -B build/release
cmake --build build/release --config Release --target ducklake_loadable_extension

EXT_FILE="build/release/extension/ducklake/ducklake.duckdb_extension"
if [ ! -f "${EXT_FILE}" ]; then
    echo "ERROR: ducklake.duckdb_extension not found at ${EXT_FILE}"
    echo "Build output:"
    find build/release -name '*.duckdb_extension' 2>/dev/null || true
    exit 1
fi

mkdir -p "${OUTPUT_DIR}"
install -m 644 "${EXT_FILE}" "${OUTPUT_DIR}/ducklake.duckdb_extension"
echo "==> ducklake.duckdb_extension installed to ${OUTPUT_DIR}"

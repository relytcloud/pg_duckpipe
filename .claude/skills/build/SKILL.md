---
name: build
description: Build pg_duckpipe and all dependencies (PostgreSQL, pg_ducklake) from source into deps/
user_invocable: true
allowed_tools: Bash, Read, Glob, Grep, AskUserQuestion
---

# /build — Full Bootstrap Build for pg_duckpipe

Build PostgreSQL → pg_ducklake → pg_duckpipe from source. All dependencies are cloned and built inside `deps/` within the repo root (gitignored, self-contained, disposable).

Execute each phase below **in order**. Skip steps whose outputs already exist (idempotent). If a step fails, stop and show the error — do not continue to the next phase.

**IMPORTANT**: All paths below are relative to the **repo root** (the directory containing `Cargo.toml`, `Makefile`, and `duckpipe-pg/`). Determine the repo root at the start — if working inside a worktree, use the worktree root.

---

## Phase 0 — Prerequisites Check

Verify these tools are installed and on `$PATH`. If any are missing, list **all** missing ones and tell the user how to install them before stopping.

| Tool | Check command | macOS install hint |
|------|--------------|-------------------|
| Rust/cargo | `cargo --version` | `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \| sh` |
| cmake | `cmake --version` | `brew install cmake` |
| lz4 | `pkg-config --exists liblz4` or check `/opt/homebrew/opt/lz4` | `brew install lz4` |
| libxml2 | `pkg-config --exists libxml-2.0` or `xml2-config --version` | `brew install libxml2` |
| perl | `perl --version` | (usually preinstalled) |
| python3 | `python3 --version` | `brew install python` |

On macOS, also verify Homebrew is available (`brew --version`).

---

## Phase 1 — Clone Dependencies

Create `deps/` in the repo root if it doesn't exist.

### PostgreSQL

```bash
# Skip if deps/postgres already exists
# Pin to REL_18_STABLE — pg_ducklake requires PG <= 18
git clone --depth 1 --branch REL_18_STABLE https://github.com/postgres/postgres.git deps/postgres
```

### pg_ducklake

```bash
# Skip if deps/pg_ducklake already exists
git clone --recurse-submodules https://github.com/relytcloud/pg_ducklake.git deps/pg_ducklake
```

If either directory already exists, print "Already cloned — skipping" and move on.

---

## Phase 2 — Build PostgreSQL

**Skip condition**: `deps/postgres/work/app/bin/pg_config` exists and is executable.

On **macOS**, pass Homebrew lz4 paths to `./configure` so they get baked into PGXS's `Makefile.global` (required for pg_ducklake's sub-builds to find lz4):

```bash
cd deps/postgres
mkdir -p work/app

# macOS: bake Homebrew lz4 paths into PGXS so sub-builds find -llz4
# IMPORTANT: Only add lz4-specific paths. Do NOT add broad -I/opt/homebrew/include
# as it can pull in system-installed DuckDB headers that conflict with submodule headers.
if [ "$(uname -s)" = "Darwin" ]; then
  export LDFLAGS="-L/opt/homebrew/opt/lz4/lib"
  export CPPFLAGS="-I/opt/homebrew/opt/lz4/include"
fi

./configure \
  --prefix="$(pwd)/work/app" \
  --with-perl \
  --with-python \
  --with-libxml \
  --enable-cassert \
  --enable-debug \
  --without-icu
make install -j$(nproc 2>/dev/null || sysctl -n hw.ncpu)
```

After build, verify `deps/postgres/work/app/bin/pg_config` exists.

Set `PG_CONFIG` for all subsequent phases:

```bash
export PG_CONFIG="$(pwd)/deps/postgres/work/app/bin/pg_config"
export PATH="$(dirname $PG_CONFIG):$PATH"
```

---

## Phase 3 — Build pg_ducklake

**Skip condition**: `pg_ducklake.so` or `pg_ducklake.dylib` exists in `$($PG_CONFIG --pkglibdir)`.

pg_ducklake links against `libduckdb` from PG's lib dir, so pg_duckdb must be installed first. Build in two steps:

```bash
cd deps/pg_ducklake

# Step 1: Build and install pg_duckdb (installs libduckdb + pg_duckdb into PG lib dir)
make -C third_party/pg_duckdb -j$(nproc 2>/dev/null || sysctl -n hw.ncpu)
make -C third_party/pg_duckdb install

# Step 2: Build and install pg_ducklake (can now link against libduckdb)
make -j$(nproc 2>/dev/null || sysctl -n hw.ncpu)
make install
```

Note: lz4 paths are already baked into PG's `Makefile.global` from Phase 2's configure step.

Verify that `libduckdb.dylib` (macOS) or `libduckdb.so` (Linux) exists in `$($PG_CONFIG --pkglibdir)`.

---

## Phase 4 — Install cargo-pgrx

**Skip condition**: `cargo pgrx --version` succeeds AND reports version `0.16.1`.

```bash
cargo install cargo-pgrx --version 0.16.1 --locked
```

Then initialize pgrx with our custom PG. **Important**: If `~/.pgrx/config.toml` has stale entries pointing to non-existent PG installs, `cargo pgrx install` will fail. Verify all entries in config.toml are valid after init.

```bash
cargo pgrx init --pg18="$PG_CONFIG"
cat ~/.pgrx/config.toml  # verify no stale pg_config paths
```

---

## Phase 5 — Build pg_duckpipe

Run from the **repo root**:

```bash
PG_CONFIG="$PG_CONFIG" make install
```

This invokes `cargo pgrx install` with the correct `DUCKDB_LIB_DIR` and library paths (handled by the Makefile).

---

## Phase 6 — Verify

Check that the extension was installed:

```bash
PG_LIB=$($PG_CONFIG --pkglibdir)

# Extension shared library
ls "$PG_LIB"/pg_duckpipe.{so,dylib} 2>/dev/null

# Extension SQL files
PG_SHARE=$($PG_CONFIG --sharedir)
ls "$PG_SHARE/extension"/pg_duckpipe* 2>/dev/null
```

Print a summary:

```
Build complete!
  PostgreSQL:   deps/postgres/work/app/bin/pg_config
  pg_ducklake:  $PG_LIB/pg_ducklake.{so,dylib}
  pg_duckpipe:  $PG_LIB/pg_duckpipe.{so,dylib}

To run tests:
  PG_CONFIG=deps/postgres/work/app/bin/pg_config make installcheck
```

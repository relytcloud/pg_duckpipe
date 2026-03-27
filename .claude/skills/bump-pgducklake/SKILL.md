---
name: bump-pgducklake
description: Bump pg_ducklake (and optionally DuckDB) to a new version. Updates all pinned commits and version constants across the repo.
user_invocable: true
allowed_tools: Bash, Read, Edit, Glob, Grep, WebFetch, AskUserQuestion
---

# /bump-pgducklake — Bump pg_ducklake / DuckDB Version

Bump the pg_ducklake commit pin (which transitively bumps DuckDB, pg_duckdb, and the ducklake extension) across all files in the repo.

**IMPORTANT**: All paths below are relative to the **repo root** (the directory containing `Cargo.toml`, `Makefile`, and `duckpipe-pg/`). Determine the repo root at the start — if working inside a worktree, use the worktree root.

---

## Step 0 — Determine the Target

Ask the user or infer from context:
- **pg_ducklake commit**: the target commit hash on `https://github.com/relytcloud/pg_ducklake.git` (default: latest `main`)
- **DuckDB version**: the DuckDB version that commit includes (check the pg_ducklake commit log for version bump commits)

If the user just says "bump to latest", fetch the latest commit:
```bash
git ls-remote https://github.com/relytcloud/pg_ducklake.git refs/heads/main
```

To find the DuckDB version, check the pg_ducklake repo's recent commits or `third_party/pg_duckdb/third_party/duckdb` submodule.

---

## Step 1 — Determine the duckdb-rs Crate Version

The `duckdb` Rust crate uses the version scheme `1.{10000*major + 100*minor + patch}.0`. For example:
- DuckDB v1.5.0 → `1.10500.0`
- DuckDB v1.5.1 → `1.10501.0`
- DuckDB v1.6.0 → `1.10600.0`

Verify the crate version exists on crates.io before proceeding:
```
https://crates.io/api/v1/crates/duckdb/versions
```

---

## Step 2 — Update All Version Pins

There are **7 files** with version constants to update. Update all of them:

### pg_ducklake commit (`PGDUCKLAKE_COMMIT`)

| File | Variable | Format |
|------|----------|--------|
| `Makefile` | `PGDUCKLAKE_COMMIT  ?= <hash>` | Make variable default |
| `docker/Dockerfile` | `ARG PGDUCKLAKE_COMMIT=<hash>` | Docker ARG default (line 2) |
| `docker/Dockerfile.daemon` | `ARG PGDUCKLAKE_COMMIT=<hash>` | Docker ARG default |
| `docker/docker-bake.hcl` | `default = "<hash>"` | HCL variable default (in the `PGDUCKLAKE_COMMIT` variable block) |

### DuckDB version (`DUCKDB_VERSION`)

| File | Variable | Format |
|------|----------|--------|
| `docker/.env` | `DUCKDB_VERSION=v<version>` | Env var (e.g. `v1.5.1`) |
| `docker/Dockerfile.daemon` | `ARG DUCKDB_VERSION=v<version>` | Docker ARG default |
| `docker/docker-bake.hcl` | `default = "v<version>"` | HCL variable default (in the `DUCKDB_VERSION` variable block) |

### duckdb-rs crate version

| File | Variable | Format |
|------|----------|--------|
| `Cargo.toml` | `duckdb = { version = "=<crate_version>" }` | Exact pin in workspace deps |

After editing `Cargo.toml`, update `Cargo.lock`:
```bash
cargo update -p duckdb
```

---

## Step 3 — Ask User What Else to Do

After updating all version pins, use AskUserQuestion to ask the user what additional steps to run:

- Question: "Version pins updated. What else should I do?"
- Options:
  1. **Done** — Stop here (just the version pin updates)
  2. **Build** — Also rebuild pg_duckpipe (`make build`)
  3. **Build + Test** — Rebuild and run regression tests (`make installcheck`)

If the user selects **Done**, skip to Step 6 (Summary).

---

## Step 4 — Build pg_duckpipe (if requested)

```bash
PG_CONFIG=<pg_config_path> make build
```

If there's a local pg_ducklake checkout that needs updating first, check if it's already at the target commit. If not, warn the user they may need to rebuild pg_ducklake first (via `/build` or manually).

---

## Step 5 — Run Regression Tests (if requested)

```bash
PG_CONFIG=<pg_config_path> make check-regression
```

All tests must pass before considering the bump complete.

---

## Step 6 — Summary

Print a summary of what was changed:

```
Bumped pg_ducklake to <commit> (DuckDB v<version>)

Files updated:
  Makefile                  PGDUCKLAKE_COMMIT → <short_hash>
  docker/Dockerfile         PGDUCKLAKE_COMMIT → <short_hash>
  docker/Dockerfile.daemon  PGDUCKLAKE_COMMIT → <short_hash>, DUCKDB_VERSION → v<version>
  docker/docker-bake.hcl    PGDUCKLAKE_COMMIT → <short_hash>, DUCKDB_VERSION → v<version>
  docker/.env               DUCKDB_VERSION → v<version>
  Cargo.toml                duckdb → =<crate_version>
  Cargo.lock                (auto-updated)
```

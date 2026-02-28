ARG PG_VERSION=18

###############################################################################
### BUILDER — compile pg_duckpipe against the pg_ducklake installation
###############################################################################
FROM pgducklake/pgducklake:${PG_VERSION}-main AS builder
ARG PG_VERSION

USER root

# Build tools + PostgreSQL dev headers (for pgrx bindgen against the installed PG)
RUN apt-get update -qq && \
    apt-get install -y \
        postgresql-server-dev-${PG_VERSION} \
        build-essential \
        clang \
        libclang-dev \
        pkg-config \
        cmake \
        ninja-build \
        curl \
        ca-certificates \
        libssl-dev && \
    rm -rf /var/lib/apt/lists/*

# Install Rust system-wide so it's on PATH for the build
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
    sh -s -- -y --no-modify-path --default-toolchain stable --profile minimal

# cargo-pgrx version must match pgrx in Cargo.toml (0.16.1)
RUN cargo install --locked cargo-pgrx --version 0.16.1

WORKDIR /build
COPY . .

# Register the installed PG with pgrx (writes $PGRX_HOME/config.toml).
# Uses the existing pg_config; does NOT download or build PG from source.
RUN cargo pgrx init --pg${PG_VERSION} /usr/lib/postgresql/${PG_VERSION}/bin/pg_config

# Point duckdb-rs (non-bundled) at the shared libduckdb.so that pg_ducklake
# already ships.  LD_LIBRARY_PATH lets the pgrx_embed SQL-generation binary
# find it at run-time during the build step.
ENV DUCKDB_LIB_DIR=/usr/lib/postgresql/${PG_VERSION}/lib \
    LD_LIBRARY_PATH=/usr/lib/postgresql/${PG_VERSION}/lib

# Build and install into the live postgres dirs.
# Cache mounts avoid re-downloading crates on rebuild.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo pgrx install \
        --package pg_duckpipe \
        --pg-config /usr/lib/postgresql/${PG_VERSION}/bin/pg_config \
        --release

###############################################################################
### OUTPUT — pg_ducklake + pg_duckpipe, ready to use
###############################################################################
FROM pgducklake/pgducklake:${PG_VERSION}-main
ARG PG_VERSION

USER root

# Copy pg_duckpipe shared library and extension files
COPY --from=builder \
    /usr/lib/postgresql/${PG_VERSION}/lib/pg_duckpipe.so \
    /usr/lib/postgresql/${PG_VERSION}/lib/

COPY --from=builder \
    /usr/share/postgresql/${PG_VERSION}/extension/pg_duckpipe.control \
    /usr/share/postgresql/${PG_VERSION}/extension/pg_duckpipe--1.0.0.sql \
    /usr/share/postgresql/${PG_VERSION}/extension/

# Extend pg_ducklake's postgresql.conf.sample:
#   - wal_level=logical    required for CDC streaming replication
#   - preload both pg_duckdb and pg_duckpipe bgworker
#   - unsafe_allow_mixed_transactions lets pg_duckpipe flush inside PG txns
# PostgreSQL uses the last occurrence of each GUC, so these override the
# shared_preload_libraries='pg_duckdb' line already written by pg_ducklake.
RUN printf '\n# pg_duckpipe CDC settings\n\
wal_level = logical\n\
max_replication_slots = 10\n\
max_wal_senders = 10\n\
shared_preload_libraries = '"'"'pg_duckdb,pg_duckpipe'"'"'\n\
duckdb.unsafe_allow_mixed_transactions = on\n' \
    >> /usr/share/postgresql/postgresql.conf.sample

# Make libduckdb.so (shipped by pg_ducklake) findable by the dynamic linker so
# pg_duckpipe.so (linked dynamically against it) loads correctly at runtime.
RUN echo "/usr/lib/postgresql/${PG_VERSION}/lib" > /etc/ld.so.conf.d/duckdb.conf && \
    /sbin/ldconfig

# Pre-install the ducklake DuckDB extension so flush workers never need internet
# access at runtime.  The postgres user's $HOME/.duckdb/ is outside PGDATA so it
# survives the Docker volume mount at /var/lib/postgresql/data.
#
# DuckDB version must match libduckdb.so shipped by pg_ducklake in this image.
RUN apt-get update -qq && apt-get install -y --no-install-recommends curl && \
    DUCKDB_VER="v1.4.3" && \
    ARCH="$(uname -m | sed 's/x86_64/linux_amd64/;s/aarch64/linux_arm64/')" && \
    EXT_DIR="/var/lib/postgresql/.duckdb/extensions/${DUCKDB_VER}/${ARCH}" && \
    mkdir -p "${EXT_DIR}" && \
    curl -fsSL \
        "https://extensions.duckdb.org/${DUCKDB_VER}/${ARCH}/ducklake.duckdb_extension.gz" \
        | gzip -d > "${EXT_DIR}/ducklake.duckdb_extension" && \
    chown -R postgres:postgres /var/lib/postgresql/.duckdb && \
    apt-get remove -y curl && apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

# Create pg_duckpipe extension after pg_duckdb (scripts run alphabetically).
# 0004-localhost-trust.sh adds pg_hba entries for TCP loopback trust auth so
# DuckDB flush workers can connect via 127.0.0.1 without a password.
COPY docker/init.d/ /docker-entrypoint-initdb.d/
RUN chmod +x /docker-entrypoint-initdb.d/Z02-localhost-trust.sh

USER postgres

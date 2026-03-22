ARG PG_VERSION=18
ARG DUCKLAKE_COMMIT=c6b2fd9408431cd86f720fba276774d2bcf23eff

###############################################################################
### DUCKLAKE-BUILDER — clone + build pg_duckdb + libduckdb + pg_ducklake
###############################################################################
FROM postgres:${PG_VERSION}-bookworm AS ducklake-builder
ARG PG_VERSION
ARG DUCKLAKE_COMMIT

# C++ build deps for DuckDB + pg_duckdb + pg_ducklake.
RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends \
        postgresql-server-dev-${PG_VERSION} \
        build-essential \
        cmake \
        ninja-build \
        git \
        curl \
        ca-certificates \
        pkg-config \
        libssl-dev \
        liblz4-dev \
        libstdc++-12-dev \
        libcurl4-openssl-dev \
        jq && \
    rm -rf /var/lib/apt/lists/*

# Clone pg_ducklake at the pinned commit with all submodules (pg_duckdb, duckdb, ducklake).
# Uses shallow fetch for speed — only the exact commit and one level of history.
WORKDIR /build/pg_ducklake
RUN git init . && \
    git remote add origin https://github.com/relytcloud/pg_ducklake.git && \
    git fetch --depth 1 origin ${DUCKLAKE_COMMIT} && \
    git checkout FETCH_HEAD && \
    git submodule update --init --recursive --depth 1

# Build and install pg_duckdb (+ libduckdb.so) and pg_ducklake.
RUN PG_CONFIG=/usr/lib/postgresql/${PG_VERSION}/bin/pg_config make install

# Build ducklake.duckdb_extension (cached together with the C++ layer above).
COPY docker/build-ducklake-ext.sh /tmp/build-ducklake-ext.sh
RUN DUCKLAKE_REPO=/build/pg_ducklake \
    /tmp/build-ducklake-ext.sh /usr/lib/postgresql/${PG_VERSION}/lib

###############################################################################
### BUILDER — compile pg_duckpipe
###############################################################################
FROM ducklake-builder AS builder
ARG PG_VERSION

# Additional build deps for pgrx (clang/libclang for bindgen)
RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends \
        clang \
        libclang-dev && \
    rm -rf /var/lib/apt/lists/*

# Install Rust system-wide
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
    sh -s -- -y --no-modify-path --default-toolchain stable --profile minimal

# cargo-pgrx version must match pgrx in Cargo.toml (0.16.1)
RUN cargo install --locked cargo-pgrx --version 0.16.1

WORKDIR /build/pg_duckpipe

# Copy workspace files and source dirs (selective to preserve layer cache
# when only Rust code changes — the C++ ducklake-builder layer stays cached).
COPY Cargo.toml Cargo.lock ./
COPY duckpipe-core/ duckpipe-core/
COPY duckpipe-pg/ duckpipe-pg/
COPY duckpipe-daemon/ duckpipe-daemon/

# Register the installed PG with pgrx (uses existing pg_config, does NOT
# download or build PG from source).
RUN cargo pgrx init --pg${PG_VERSION} /usr/lib/postgresql/${PG_VERSION}/bin/pg_config

# Point duckdb-rs (non-bundled) at libduckdb.so built by pg_ducklake.
ENV DUCKDB_LIB_DIR=/usr/lib/postgresql/${PG_VERSION}/lib \
    LD_LIBRARY_PATH=/usr/lib/postgresql/${PG_VERSION}/lib

# Build and install pg_duckpipe.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/pg_duckpipe/target \
    cargo pgrx install \
        --package pg_duckpipe \
        --pg-config /usr/lib/postgresql/${PG_VERSION}/bin/pg_config \
        --release

###############################################################################
### OUTPUT — pg_duckdb + pg_ducklake + pg_duckpipe, ready to use
###############################################################################
FROM postgres:${PG_VERSION}-bookworm AS output
ARG PG_VERSION

# Runtime deps for DuckDB (libcurl for httpfs, libssl for TLS)
RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        libcurl4 && \
    rm -rf /var/lib/apt/lists/*

# Copy shared libraries
COPY --from=builder \
    /usr/lib/postgresql/${PG_VERSION}/lib/libduckdb.so \
    /usr/lib/postgresql/${PG_VERSION}/lib/pg_duckdb.so \
    /usr/lib/postgresql/${PG_VERSION}/lib/pg_ducklake.so \
    /usr/lib/postgresql/${PG_VERSION}/lib/pg_duckpipe.so \
    /usr/lib/postgresql/${PG_VERSION}/lib/ducklake.duckdb_extension \
    /usr/lib/postgresql/${PG_VERSION}/lib/

# Copy extension control + SQL files
COPY --from=builder /usr/share/postgresql/${PG_VERSION}/extension/ /tmp/ext-staging/
RUN cp /tmp/ext-staging/pg_duckdb* \
       /tmp/ext-staging/pg_ducklake* \
       /tmp/ext-staging/pg_duckpipe* \
       /usr/share/postgresql/${PG_VERSION}/extension/ && \
    rm -rf /tmp/ext-staging

# Make libduckdb.so findable by the dynamic linker
RUN echo "/usr/lib/postgresql/${PG_VERSION}/lib" > /etc/ld.so.conf.d/duckdb.conf && \
    /sbin/ldconfig

# Write GUCs to postgresql.conf.sample (the template used by initdb).
# This ensures PG starts with the right settings on first boot — no restart
# needed.  PostgreSQL uses the last occurrence of each GUC.
RUN printf '\n# pg_duckpipe CDC settings\n\
wal_level = logical\n\
max_replication_slots = 10\n\
max_wal_senders = 10\n\
shared_preload_libraries = '"'"'pg_duckdb,pg_ducklake,pg_duckpipe'"'"'\n\
duckdb.unsafe_allow_mixed_transactions = on\n' \
    >> /usr/share/postgresql/postgresql.conf.sample

# Init scripts (run alphabetically by Docker entrypoint)
COPY docker/init.d/ /docker-entrypoint-initdb.d/
RUN chmod +x /docker-entrypoint-initdb.d/*.sh

USER postgres

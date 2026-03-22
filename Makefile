PG_CONFIG ?= pg_config
PG_LIB    := $(shell $(PG_CONFIG) --pkglibdir)
BUILD_TYPE ?= release

PGRX_PROFILE_FLAG := $(if $(filter release,$(BUILD_TYPE)),--release,--profile $(BUILD_TYPE))

.PHONY: all check-cargo-pgrx build install install-ducklake-ext check-regression clean-regression \
       installcheck build-daemon check-daemon clean-daemon installcheck-all format clean

all: build

check-cargo-pgrx:
	@if ! command -v cargo-pgrx >/dev/null 2>&1; then \
		echo "cargo-pgrx is not installed."; \
		echo "Hint: cargo install cargo-pgrx --version 0.16.1 --locked"; \
		echo "Hint: PG_CONFIG=$PG_CONFIG cargo pgrx init"; \
		exit 1; \
	fi

build: check-cargo-pgrx
	# Point duckdb-rs (non-bundled) at the shared libduckdb shipped by pg_ducklake.
	# macOS: make the lib findable at link-time for the pgrx_embed SQL-gen binary.
	cd duckpipe-pg && \
	DUCKDB_LIB_DIR="$(PG_LIB)" \
	DYLD_LIBRARY_PATH="$(PG_LIB):$(DYLD_LIBRARY_PATH)" \
	cargo pgrx install $(PGRX_PROFILE_FLAG) --pg-config=$(PG_CONFIG)

# Build ducklake.duckdb_extension and install to pkglibdir.
# Skips if already present. Clones pg_ducklake on demand if DUCKLAKE_REPO is not set.
DUCKLAKE_GIT_URL ?= https://github.com/relytcloud/pg_ducklake.git
DUCKLAKE_COMMIT  ?= 65cace70932f3f68b1a89251f971c903ab3b7781

install-ducklake-ext:
	@if [ -f "$(PG_LIB)/ducklake.duckdb_extension" ] && [ "$${FORCE:-0}" != "1" ]; then \
		echo "==> ducklake.duckdb_extension already in $(PG_LIB). Skipping (set FORCE=1 to rebuild)."; \
	else \
		repo="$(DUCKLAKE_REPO)"; \
		if [ -z "$$repo" ]; then \
			echo "==> Cloning pg_ducklake @ $(DUCKLAKE_COMMIT) ..."; \
			tmpdir=$$(mktemp -d); \
			trap 'rm -rf "$$tmpdir"' EXIT; \
			git init "$$tmpdir/pg_ducklake" && \
			git -C "$$tmpdir/pg_ducklake" remote add origin "$(DUCKLAKE_GIT_URL)" && \
			git -C "$$tmpdir/pg_ducklake" fetch --depth 1 origin "$(DUCKLAKE_COMMIT)" && \
			git -C "$$tmpdir/pg_ducklake" checkout FETCH_HEAD && \
			git -C "$$tmpdir/pg_ducklake" submodule update --init --recursive --depth 1 && \
			repo="$$tmpdir/pg_ducklake"; \
		fi; \
		DUCKLAKE_REPO="$$repo" FORCE=1 docker/build-ducklake-ext.sh "$(PG_LIB)"; \
	fi

install: build install-ducklake-ext

check-regression:
	$(MAKE) -C test/regression check-regression

clean-regression:
	$(MAKE) -C test/regression clean-regression

installcheck: install
	$(MAKE) check-regression

build-daemon:
	DUCKDB_LIB_DIR="$(PG_LIB)" \
	DYLD_LIBRARY_PATH="$(PG_LIB):$(DYLD_LIBRARY_PATH)" \
	LD_LIBRARY_PATH="$(PG_LIB):$(LD_LIBRARY_PATH)" \
	cargo build --release -p duckpipe-daemon

check-daemon:
	$(MAKE) -C test/daemon check-daemon PG_CONFIG=$(PG_CONFIG)

clean-daemon:
	$(MAKE) -C test/daemon clean-daemon

installcheck-all: installcheck check-daemon

format:
	cd duckpipe-pg && cargo fmt

clean:
	cd duckpipe-pg && cargo clean

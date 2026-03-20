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

# Build ducklake.duckdb_extension from pg_ducklake source and install to pkglibdir.
# Uses sibling ../pg_ducklake by default. Override with DUCKLAKE_REPO=<url>.
# Skips if no repo is available (no sibling dir and DUCKLAKE_REPO not set).
install-ducklake-ext:
	@repo="$(DUCKLAKE_REPO)"; \
	if [ -z "$$repo" ] && [ -d ../pg_ducklake ]; then \
		repo="$$(cd ../pg_ducklake && pwd)"; \
	fi; \
	if [ -n "$$repo" ]; then \
		DUCKLAKE_REPO="$$repo" docker/build-ducklake-ext.sh "$(PG_LIB)"; \
	else \
		echo "NOTICE: pg_ducklake not found at ../pg_ducklake and DUCKLAKE_REPO not set. Skipping ducklake extension install."; \
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

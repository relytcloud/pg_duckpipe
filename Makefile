PG_CONFIG ?= pg_config
PG_LIB    := $(shell $(PG_CONFIG) --pkglibdir)

.PHONY: all check-cargo-pgrx build install check-regression clean-regression installcheck format clean

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
	cargo pgrx install --pg-config=$(PG_CONFIG)

install: build

check-regression:
	$(MAKE) -C test/regression check-regression

clean-regression:
	$(MAKE) -C test/regression clean-regression

installcheck: install
	$(MAKE) check-regression

format:
	cd duckpipe-pg && cargo fmt

clean:
	cd duckpipe-pg && cargo clean

PG_CONFIG = /Users/xiaoyuwei/Desktop/workspace_ducklake/postgres/work/app/bin/pg_config
PG_LIB    = $(shell $(PG_CONFIG) --pkglibdir)

# Point duckdb-rs (non-bundled) at the shared libduckdb shipped by pg_ducklake.
export DUCKDB_LIB_DIR = $(PG_LIB)
# macOS: make the lib findable at link-time for the pgrx_embed SQL-gen binary.
export DYLD_LIBRARY_PATH := $(PG_LIB):$(DYLD_LIBRARY_PATH)

.PHONY: build install check-regression clean-regression installcheck format clean

build:
	cd duckpipe-pg && cargo pgrx install --pg-config=$(PG_CONFIG)

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

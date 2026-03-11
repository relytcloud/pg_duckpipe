PG_CONFIG ?= pg_config
PG_LIB   := $(shell $(PG_CONFIG) --pkglibdir)

# Root of the workspace (two levels up from test/daemon/)
WORKSPACE := $(shell cd ../.. && pwd)

.PHONY: check-daemon clean-daemon

check-daemon:
	# Step 1: Build daemon binary
	cd $(WORKSPACE) && DUCKDB_LIB_DIR="$(PG_LIB)" \
		DYLD_LIBRARY_PATH="$(PG_LIB)" LD_LIBRARY_PATH="$(PG_LIB)" \
		cargo build --release -p duckpipe-daemon
	# Step 2: Run tests
	PG_CONFIG=$(PG_CONFIG) \
		DUCKPIPE_BIN=$(WORKSPACE)/target/release/duckpipe \
		DYLD_LIBRARY_PATH="$(PG_LIB)" LD_LIBRARY_PATH="$(PG_LIB)" \
		bash run_tests.sh $(if $(TEST),$(TEST),)

clean-daemon:
	rm -rf tmp_check log results

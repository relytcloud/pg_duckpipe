MODULE_big = pg_ducklake_sync
OBJS = src/pg_ducklake_sync.o src/worker.o src/decoder.o src/batch.o src/apply.o src/api.o

EXTENSION = pg_ducklake_sync
DATA = sql/pg_ducklake_sync--1.0.sql
PGFILEDESC = "pg_ducklake_sync - PostgreSQL HTAP Sync Extension"

# Disable default installcheck - we define our own with temp instance
NO_INSTALLCHECK = 1

REGRESS = api streaming snapshot_updates
REGRESS_OPTS = --temp-config=$(srcdir)/test/ducklake_sync.conf --inputdir=test

PG_CONFIG = /Users/xiaoyuwei/Desktop/workspace_ducklake/postgres/work/app/bin/pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
override PG_CFLAGS += -Wno-declaration-after-statement
include $(PGXS)

# Custom installcheck using temp instance (required for wal_level=logical)
.PHONY: installcheck
installcheck: install
	rm -rf ./tmp_check
	$(pg_regress_installcheck) $(REGRESS_OPTS) \
		--temp-instance=./tmp_check \
		--port=5555 \
		$(REGRESS)

.PHONY: format
format:
	clang-format -i src/*.c src/*.h

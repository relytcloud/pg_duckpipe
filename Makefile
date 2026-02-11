MODULE_big = pg_duckpipe
OBJS = src/pg_duckpipe.o src/worker.o src/decoder.o src/batch.o src/apply.o src/api.o

EXTENSION = pg_duckpipe
DATA = sql/pg_duckpipe--1.0.sql
PGFILEDESC = "pg_duckpipe - PostgreSQL HTAP Sync Extension"

NO_INSTALLCHECK = 1

PG_CONFIG = /Users/xiaoyuwei/Desktop/workspace_ducklake/postgres/work/app/bin/pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
override PG_CFLAGS += -Wno-declaration-after-statement
PG_CPPFLAGS += -I$(shell $(PG_CONFIG) --includedir)
SHLIB_LINK += -L$(shell $(PG_CONFIG) --libdir) -lpq
include $(PGXS)

check-regression:
	$(MAKE) -C test/regression check-regression

clean-regression:
	$(MAKE) -C test/regression clean-regression

installcheck: install
	$(MAKE) check-regression

.PHONY: format
format:
	clang-format -i src/*.c src/*.h

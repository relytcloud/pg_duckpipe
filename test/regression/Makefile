# Makefile for pg_duckpipe regression tests

PG_CONFIG ?= /Users/xiaoyuwei/Desktop/workspace_ducklake/postgres/work/app/bin/pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

check-regression:
	$(pg_regress_installcheck) \
	--temp-instance=./tmp_check \
	--temp-config regression.conf \
	--port=5555 \
	--encoding=UTF8 \
	--load-extension=pg_duckdb \
	--load-extension=pg_ducklake \
	--load-extension=pg_duckpipe \
	$(if $(TEST),$(TEST),--schedule=schedule)

clean-regression:
	rm -rf $(CURDIR)/tmp_check
	rm -rf $(CURDIR)/log
	rm -rf $(CURDIR)/results
	rm -f  $(CURDIR)/regression.diffs
	rm -f  $(CURDIR)/regression.out

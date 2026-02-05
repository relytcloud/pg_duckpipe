#include "pg_ducklake_sync.h"

PG_MODULE_MAGIC;

void _PG_init(void);

int ducklake_sync_poll_interval = 1000;
int ducklake_sync_batch_size_per_table = 1000;
int ducklake_sync_batch_size_per_group = 10000;
bool ducklake_sync_enabled = true;

void
_PG_init(void) {
	DefineCustomIntVariable("ducklake_sync.poll_interval", "Interval in milliseconds between polls", NULL,
	                        &ducklake_sync_poll_interval, 1000, 100, 3600000, PGC_SIGHUP, GUC_UNIT_MS, NULL, NULL,
	                        NULL);

	DefineCustomIntVariable("ducklake_sync.batch_size_per_table", "Maximum number of changes per table per batch", NULL,
	                        &ducklake_sync_batch_size_per_table, 1000, 1, 1000000, PGC_SIGHUP, 0, NULL, NULL, NULL);

	DefineCustomIntVariable("ducklake_sync.batch_size_per_group", "Maximum number of changes per group per round", NULL,
	                        &ducklake_sync_batch_size_per_group, 10000, 100, 10000000, PGC_SIGHUP, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable("ducklake_sync.enabled", "Enable pg_ducklake_sync background worker", NULL,
	                         &ducklake_sync_enabled, true, PGC_SIGHUP, 0, NULL, NULL, NULL);
}

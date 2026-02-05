#include "pg_ducklake_sync.h"

#include "access/xlog.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "executor/spi.h"
#include "libpq/pqformat.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/snapmgr.h"
#include "utils/wait_classes.h"

static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

/* Memory context for sync operations */
static MemoryContext SyncMemoryContext = NULL;

static void
ducklake_sync_sighup(SIGNAL_ARGS) {
	int save_errno = errno;
	got_sighup = true;
	SetLatch(MyLatch);
	errno = save_errno;
}

static void
ducklake_sync_sigterm(SIGNAL_ARGS) {
	int save_errno = errno;
	got_sigterm = true;
	SetLatch(MyLatch);
	errno = save_errno;
}

/* Get all enabled sync groups from metadata */
List *
get_enabled_sync_groups(void) {
	List *groups = NIL;
	int ret;

	ret = SPI_execute("SELECT id, name, publication, slot_name FROM "
	                  "ducklake_sync.sync_groups WHERE enabled = true",
	                  true, 0);

	if (ret == SPI_OK_SELECT) {
		for (uint64 i = 0; i < SPI_processed; i++) {
			SyncGroup *group = palloc0(sizeof(SyncGroup));
			bool isnull;

			group->id = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull));
			group->name =
			    pstrdup(TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2, &isnull)));
			group->publication =
			    pstrdup(TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 3, &isnull)));
			group->slot_name =
			    pstrdup(TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 4, &isnull)));
			group->pending_lsn = 0;

			groups = lappend(groups, group);
		}
	}

	return groups;
}

/* Look up table mapping by source schema and table name */
TableMapping *
get_table_mapping(SyncGroup *group, char *schemaname, char *relname) {
	TableMapping *mapping = NULL;
	Datum values[3];
	Oid argtypes[3] = {INT4OID, TEXTOID, TEXTOID};
	bool isnull;
	int ret;

	Datum lsn_datum;

	values[0] = Int32GetDatum(group->id);
	values[1] = CStringGetTextDatum(schemaname);
	values[2] = CStringGetTextDatum(relname);

	ret = SPI_execute_with_args("SELECT id, group_id, source_schema, source_table, target_schema, "
	                            "target_table, "
	                            "       state, snapshot_lsn, enabled "
	                            "FROM ducklake_sync.table_mappings "
	                            "WHERE group_id = $1 AND source_schema = $2 AND source_table = $3",
	                            3, argtypes, values, NULL, true, 1);

	if (ret == SPI_OK_SELECT && SPI_processed > 0) {
		HeapTuple tuple = SPI_tuptable->vals[0];
		TupleDesc tupdesc = SPI_tuptable->tupdesc;

		mapping = palloc0(sizeof(TableMapping));
		mapping->id = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 1, &isnull));
		mapping->group_id = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 2, &isnull));
		mapping->source_schema = pstrdup(TextDatumGetCString(SPI_getbinval(tuple, tupdesc, 3, &isnull)));
		mapping->source_table = pstrdup(TextDatumGetCString(SPI_getbinval(tuple, tupdesc, 4, &isnull)));
		mapping->target_schema = pstrdup(TextDatumGetCString(SPI_getbinval(tuple, tupdesc, 5, &isnull)));
		mapping->target_table = pstrdup(TextDatumGetCString(SPI_getbinval(tuple, tupdesc, 6, &isnull)));
		mapping->state = pstrdup(TextDatumGetCString(SPI_getbinval(tuple, tupdesc, 7, &isnull)));

		lsn_datum = SPI_getbinval(tuple, tupdesc, 8, &isnull);
		mapping->snapshot_lsn = isnull ? InvalidXLogRecPtr : DatumGetLSN(lsn_datum);

		mapping->enabled = DatumGetBool(SPI_getbinval(tuple, tupdesc, 9, &isnull));
	}

	return mapping;
}

/* Create hash table for batches */
static HTAB *
create_batch_hash(void) {
	HASHCTL hashctl;

	memset(&hashctl, 0, sizeof(hashctl));
	hashctl.keysize = NAMEDATALEN * 2; /* schema.table */
	hashctl.entrysize = sizeof(SyncBatch);
	hashctl.hcxt = SyncMemoryContext;

	return hash_create("SyncBatches", 32, &hashctl, HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);
}

/* Create hash table for relation cache */
static HTAB *
create_rel_cache_hash(void) {
	HASHCTL hashctl;

	memset(&hashctl, 0, sizeof(hashctl));
	hashctl.keysize = sizeof(LogicalRepRelId);
	hashctl.entrysize = sizeof(RelationCacheEntry);
	hashctl.hcxt = SyncMemoryContext;

	return hash_create("RelationCache", 32, &hashctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

typedef struct SnapshotTask {
	int id;
	char *s_schema;
	char *s_table;
	char *t_schema;
	char *t_table;
} SnapshotTask;

/* Process snapshot for a table */
static void
process_snapshot(SyncGroup *group) {
	int ret;
	uint64 i;
	List *tasks = NIL;
	MemoryContext oldcxt;

	ret = SPI_execute_with_args("SELECT id, source_schema, source_table, target_schema, target_table "
	                            "FROM ducklake_sync.table_mappings "
	                            "WHERE group_id = $1 AND state = 'SNAPSHOT' AND enabled = true",
	                            1, (Oid[]) {INT4OID}, (Datum[]) {Int32GetDatum(group->id)}, NULL, true, 0);

	if (ret == SPI_OK_SELECT && SPI_processed > 0) {
		int num_tables = SPI_processed;
		SPITupleTable *tuptable = SPI_tuptable;

		/* Copy tasks to SyncMemoryContext to survive SPI_commit */
		oldcxt = MemoryContextSwitchTo(SyncMemoryContext);

		for (i = 0; i < num_tables; i++) {
			bool isnull;
			SnapshotTask *task = palloc(sizeof(SnapshotTask));
			task->id = DatumGetInt32(SPI_getbinval(tuptable->vals[i], tuptable->tupdesc, 1, &isnull));
			task->s_schema =
			    pstrdup(TextDatumGetCString(SPI_getbinval(tuptable->vals[i], tuptable->tupdesc, 2, &isnull)));
			task->s_table =
			    pstrdup(TextDatumGetCString(SPI_getbinval(tuptable->vals[i], tuptable->tupdesc, 3, &isnull)));
			task->t_schema =
			    pstrdup(TextDatumGetCString(SPI_getbinval(tuptable->vals[i], tuptable->tupdesc, 4, &isnull)));
			task->t_table =
			    pstrdup(TextDatumGetCString(SPI_getbinval(tuptable->vals[i], tuptable->tupdesc, 5, &isnull)));
			tasks = lappend(tasks, task);
		}
		MemoryContextSwitchTo(oldcxt);
	}

	/* Process tasks with transaction splitting */
	if (tasks != NIL) {
		ListCell *lc;
		foreach (lc, tasks) {
			SnapshotTask *task = (SnapshotTask *)lfirst(lc);
			StringInfoData buf;

			initStringInfo(&buf);
			appendStringInfo(&buf, "INSERT INTO %s.%s SELECT * FROM %s.%s", quote_identifier(task->t_schema),
			                 quote_identifier(task->t_table), quote_identifier(task->s_schema),
			                 quote_identifier(task->s_table));

			elog(LOG, "DuckLake Sync: Copying data for %s.%s", task->s_schema, task->s_table);

			/* DuckDB Write */
			if (SPI_execute(buf.data, false, 0) < 0) {
				elog(ERROR, "Failed to copy data for %s.%s", task->s_schema, task->s_table);
			}
			pfree(buf.data);

			/* Commit DuckDB write */
			PopActiveSnapshot();
			SPI_commit();
			SPI_start_transaction();
			PushActiveSnapshot(GetTransactionSnapshot());

			/* Postgres Write (Update state) */
			{
				char query[256];
				snprintf(query, sizeof(query),
				         "UPDATE ducklake_sync.table_mappings SET state = 'STREAMING' "
				         "WHERE id = %d",
				         task->id);
				SPI_execute(query, false, 0);
			}

			/* Commit Postgres write */
			PopActiveSnapshot();
			SPI_commit();
			SPI_start_transaction();
			PushActiveSnapshot(GetTransactionSnapshot());
		}
	}
}

/* Process one sync group - read changes from slot and apply */
int
process_sync_group(SyncGroup *group) {
	int total_processed = 0;
	int ret;
	HTAB *batches;
	HTAB *rel_cache;
	StringInfoData query;

	/* Handle snapshots first */
	process_snapshot(group);

	/* Create hash tables for this processing round */
	batches = create_batch_hash();
	rel_cache = create_rel_cache_hash();

	/* Check if replication slot exists */
	{
		Datum values[1] = {CStringGetTextDatum(group->slot_name)};
		Oid argtypes[1] = {TEXTOID};
		int ret = SPI_execute_with_args("SELECT 1 FROM pg_replication_slots WHERE slot_name = $1", 1, argtypes, values,
		                                NULL, true, 1);

		if (ret != SPI_OK_SELECT || SPI_processed == 0) {
			hash_destroy(batches);
			hash_destroy(rel_cache);
			return 0;
		}
	}

	/* Query the replication slot for binary changes */
	initStringInfo(&query);
	appendStringInfo(&query,
	                 "SELECT lsn, data FROM pg_logical_slot_get_binary_changes("
	                 "'%s', NULL, %d, 'proto_version', '1', 'publication_names', '%s')",
	                 group->slot_name, ducklake_sync_batch_size_per_group, group->publication);

	ret = SPI_execute(query.data, true, 0);
	pfree(query.data);

	if (ret != SPI_OK_SELECT) {
		elog(WARNING, "Failed to get changes from slot %s", group->slot_name);
		hash_destroy(batches);
		hash_destroy(rel_cache);
		return 0;
	}

	/* Process each change message */
	for (uint64 i = 0; i < SPI_processed; i++) {
		bool isnull;
		XLogRecPtr lsn;
		Datum lsn_datum = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull);
		Datum data_datum = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2, &isnull);

		if (isnull)
			continue;

		lsn = DatumGetLSN(lsn_datum);
		{
			bytea *data = DatumGetByteaP(data_datum);
			int len = VARSIZE(data) - VARHDRSZ;
			char *raw = VARDATA(data);

			/* Create StringInfo for parsing */
			StringInfoData buf;
			initStringInfo(&buf);
			appendBinaryStringInfo(&buf, raw, len);

			/* Decode and process the message */
			decode_message(&buf, lsn, group, batches, rel_cache);

			pfree(buf.data);
			total_processed++;
		}
	}

	/* Flush any remaining batches */
	flush_all_batches(batches);

	/* Clean up hash tables */
	hash_destroy(batches);
	hash_destroy(rel_cache);

	return total_processed;
}

PGDLLEXPORT void
ducklake_sync_worker_main(Datum main_arg) {
	Oid dboid = DatumGetObjectId(main_arg);
	char *dbname;

	pqsignal(SIGHUP, ducklake_sync_sighup);
	pqsignal(SIGTERM, ducklake_sync_sigterm);
	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnectionByOid(dboid, InvalidOid, 0);

	StartTransactionCommand();
	dbname = get_database_name(dboid);
	/* Copy dbname to a safe context before committing */
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(TopMemoryContext);
		dbname = pstrdup(dbname);
		MemoryContextSwitchTo(oldcxt);
	}
	CommitTransactionCommand();

	if (dbname == NULL)
		elog(FATAL, "database with OID %u does not exist", dboid);

	SyncMemoryContext = AllocSetContextCreate(TopMemoryContext, "pg_ducklake_sync", ALLOCSET_DEFAULT_SIZES);

	elog(LOG, "pg_ducklake_sync worker started for database '%s'", dbname);

	while (!got_sigterm) {
		List *groups;
		ListCell *lc;
		bool any_work = false;

		/* Reload config if signaled */
		if (got_sighup) {
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/* Skip if disabled */
		if (!ducklake_sync_enabled) {
			(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, ducklake_sync_poll_interval,
			                PG_WAIT_EXTENSION);
			ResetLatch(MyLatch);
			continue;
		}

		/* Process sync groups */
		StartTransactionCommand();
		/* Ensure we have a snapshot for SPI queries */
		PushActiveSnapshot(GetTransactionSnapshot());

		/* Use non-atomic SPI connection to allow transaction commit in
		 * process_snapshot */
		if (SPI_connect_ext(SPI_OPT_NONATOMIC) != SPI_OK_CONNECT)
			elog(ERROR, "SPI_connect failed");

		{
			MemoryContext old = MemoryContextSwitchTo(SyncMemoryContext);
			groups = get_enabled_sync_groups();
			MemoryContextSwitchTo(old);
		}

		foreach (lc, groups) {
			SyncGroup *group = (SyncGroup *)lfirst(lc);
			int processed = process_sync_group(group);
			if (processed > 0)
				any_work = true;
		}

		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();

		/* Reset memory context after each round */
		MemoryContextReset(SyncMemoryContext);

		/* Wait before next poll (shorter if there was work) */
		(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
		                any_work ? 10 : ducklake_sync_poll_interval, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
	}

	elog(LOG, "pg_ducklake_sync worker shutting down");
	proc_exit(0);
}

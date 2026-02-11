#include "pg_duckpipe.h"

#include "libpq-fe.h"

#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "executor/spi.h"
#include "libpq/pqformat.h"
#include "portability/instr_time.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/snapmgr.h"
#include "utils/wait_classes.h"

static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

/* Memory context for sync operations - non-static so other files can use it */
MemoryContext SyncMemoryContext = NULL;

static double
duckpipe_elapsed_ms(const instr_time *start) {
	instr_time end = *start;

	INSTR_TIME_SET_CURRENT(end);
	INSTR_TIME_SUBTRACT(end, *start);
	return INSTR_TIME_GET_MILLISEC(end);
}

static void
duckpipe_sighup(SIGNAL_ARGS) {
	int save_errno = errno;
	got_sighup = true;
	SetLatch(MyLatch);
	errno = save_errno;
}

static void
duckpipe_sigterm(SIGNAL_ARGS) {
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
	                  "duckpipe.sync_groups WHERE enabled = true",
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
	                            "FROM duckpipe.table_mappings "
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
		mapping->state = sync_state_from_string(TextDatumGetCString(SPI_getbinval(tuple, tupdesc, 7, &isnull)));

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
	hashctl.keysize = NAMEDATALEN * 2 + 2; /* schema.table.\0 */
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
	instr_time snapshot_timer;
	bool timing_enabled = duckpipe_debug_log;

	if (timing_enabled)
		INSTR_TIME_SET_CURRENT(snapshot_timer);

	ret = SPI_execute_with_args("SELECT id, source_schema, source_table, target_schema, target_table "
	                            "FROM duckpipe.table_mappings "
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

	/* Process tasks using temporary replication slot with exported snapshot.
	 *
	 * For each table in SNAPSHOT state we:
	 *  1. Open a libpq replication connection back to our own database
	 *  2. CREATE_REPLICATION_SLOT ... TEMPORARY LOGICAL pgoutput
	 *     (EXPORT_SNAPSHOT is the default — returns consistent_point + snapshot_name)
	 *  3. Import that snapshot via SET TRANSACTION SNAPSHOT in our SPI session
	 *  4. Copy data under the imported snapshot (atomically consistent with consistent_point)
	 *  5. Close the replication connection (drops temp slot, releases snapshot)
	 *  6. Record consistent_point as snapshot_lsn and transition to CATCHUP
	 *
	 * This guarantees zero race window: the snapshot and WAL position are
	 * determined atomically by the replication slot creation. */
	if (tasks != NIL) {
		ListCell *lc;
		char *dbname;
		const char *port_str;

		/* Capture connection parameters while we still have a valid catalog snapshot */
		dbname = pstrdup(get_database_name(MyDatabaseId));
		port_str = GetConfigOptionByName("port", NULL, false);

		foreach (lc, tasks) {
			SnapshotTask *task = (SnapshotTask *)lfirst(lc);
			StringInfoData buf;
			XLogRecPtr consistent_point;
			char *snapshot_name = NULL;
			PGconn *repl_conn = NULL;
			instr_time table_timer;

			if (timing_enabled)
				INSTR_TIME_SET_CURRENT(table_timer);

			/* --- Step 1: Open replication connection and create temp slot --- */
			{
				const char *keywords[] = {"dbname", "port", "replication", NULL};
				const char *conn_values[] = {dbname, port_str, "database", NULL};
				PGresult *res;
				const char *cp_str;
				const char *sn_str;
				uint32 hi, lo;

				repl_conn = PQconnectdbParams(keywords, conn_values, false);
				if (PQstatus(repl_conn) != CONNECTION_OK) {
					elog(WARNING, "DuckPipe: replication connection failed for %s.%s: %s",
					     task->s_schema, task->s_table, PQerrorMessage(repl_conn));
					PQfinish(repl_conn);
					continue; /* skip this table, retry next round */
				}

				res = PQexec(repl_conn,
				             "CREATE_REPLICATION_SLOT duckpipe_snap_tmp TEMPORARY LOGICAL pgoutput");
				if (PQresultStatus(res) != PGRES_TUPLES_OK) {
					elog(WARNING, "DuckPipe: CREATE_REPLICATION_SLOT failed for %s.%s: %s",
					     task->s_schema, task->s_table, PQresultErrorMessage(res));
					PQclear(res);
					PQfinish(repl_conn);
					continue;
				}

				/* Result columns: slot_name(0), consistent_point(1), snapshot_name(2), output_plugin(3) */
				cp_str = PQgetvalue(res, 0, 1);
				sn_str = PQgetvalue(res, 0, 2);

				if (sscanf(cp_str, "%X/%X", &hi, &lo) != 2) {
					elog(WARNING, "DuckPipe: failed to parse consistent_point '%s'", cp_str);
					PQclear(res);
					PQfinish(repl_conn);
					continue;
				}

				consistent_point = ((uint64)hi << 32) | lo;

				oldcxt = MemoryContextSwitchTo(SyncMemoryContext);
				snapshot_name = pstrdup(sn_str);
				MemoryContextSwitchTo(oldcxt);

				PQclear(res);
				/* Keep repl_conn open — it holds the exported snapshot alive */
			}

			/* --- Step 2: Import snapshot and copy data --- */
			PopActiveSnapshot();
			SPI_commit();
			SPI_start_transaction();

			/* Import the exported snapshot (requires REPEATABLE READ, must be
			 * the first data-access statement in the transaction) */
			if (SPI_execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", false, 0) < 0)
				elog(ERROR, "DuckPipe: failed to SET TRANSACTION ISOLATION LEVEL");

			initStringInfo(&buf);
			appendStringInfo(&buf, "SET TRANSACTION SNAPSHOT %s", quote_literal_cstr(snapshot_name));
			if (SPI_execute(buf.data, false, 0) < 0)
				elog(ERROR, "DuckPipe: failed to SET TRANSACTION SNAPSHOT %s", snapshot_name);
			pfree(buf.data);

			PushActiveSnapshot(GetTransactionSnapshot());

			/* Clear target before snapshot copy */
			initStringInfo(&buf);
			appendStringInfo(&buf, "DELETE FROM %s.%s", quote_identifier(task->t_schema),
			                 quote_identifier(task->t_table));
			if (SPI_execute(buf.data, false, 0) != SPI_OK_DELETE)
				elog(WARNING, "Failed to clear target table %s.%s before snapshot", task->t_schema, task->t_table);
			pfree(buf.data);

			initStringInfo(&buf);
			appendStringInfo(&buf, "INSERT INTO %s.%s SELECT * FROM %s.%s", quote_identifier(task->t_schema),
			                 quote_identifier(task->t_table), quote_identifier(task->s_schema),
			                 quote_identifier(task->s_table));

			elog(LOG, "DuckPipe: Copying data for %s.%s (snapshot=%s consistent_point=%X/%X)", task->s_schema,
			     task->s_table, snapshot_name, LSN_FORMAT_ARGS(consistent_point));

			if (SPI_execute(buf.data, false, 0) < 0)
				elog(ERROR, "Failed to copy data for %s.%s", task->s_schema, task->s_table);
			pfree(buf.data);

			/* Commit DuckDB write */
			PopActiveSnapshot();
			SPI_commit();

			/* --- Step 3: Close replication connection (drops temp slot + snapshot) --- */
			PQfinish(repl_conn);
			repl_conn = NULL;

			SPI_start_transaction();
			PushActiveSnapshot(GetTransactionSnapshot());

			/* Transition to CATCHUP with consistent_point as snapshot_lsn */
			{
				Datum values[2] = {LSNGetDatum(consistent_point), Int32GetDatum(task->id)};
				Oid argtypes[2] = {LSNOID, INT4OID};
				SPI_execute_with_args("UPDATE duckpipe.table_mappings SET state = 'CATCHUP', "
				                      "snapshot_lsn = $1 WHERE id = $2",
				                      2, argtypes, values, NULL, false, 0);
			}

			/* Commit metadata write */
			PopActiveSnapshot();
			SPI_commit();
			SPI_start_transaction();
			PushActiveSnapshot(GetTransactionSnapshot());

			if (timing_enabled) {
				elog(LOG, "DuckPipe timing: action=snapshot_table group=%s source=%s.%s target=%s.%s elapsed_ms=%.3f",
				     group->name ? group->name : "<unknown>", task->s_schema, task->s_table, task->t_schema,
				     task->t_table, duckpipe_elapsed_ms(&table_timer));
			}
		}
	}

	if (timing_enabled) {
		elog(LOG, "DuckPipe timing: action=snapshot_round group=%s tables=%d elapsed_ms=%.3f",
		     group->name ? group->name : "<unknown>", list_length(tasks), duckpipe_elapsed_ms(&snapshot_timer));
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
	WalMessage *wal_messages = NULL;
	uint64 num_messages = 0;
	instr_time group_timer;
	instr_time fetch_timer;
	instr_time copy_timer;
	instr_time decode_timer;
	instr_time flush_timer;
	bool timing_enabled = duckpipe_debug_log;

	if (timing_enabled)
		INSTR_TIME_SET_CURRENT(group_timer);

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

	/* Query the replication slot for binary changes
	 * Use quote_literal_cstr to properly escape slot_name and publication
	 */
	initStringInfo(&query);
	appendStringInfo(&query,
	                 "SELECT lsn, data FROM pg_logical_slot_get_binary_changes("
	                 "%s, NULL, %d, 'proto_version', '1', 'publication_names', %s)",
	                 quote_literal_cstr(group->slot_name), duckpipe_batch_size_per_group,
	                 quote_literal_cstr(group->publication));

	if (timing_enabled)
		INSTR_TIME_SET_CURRENT(fetch_timer);
	ret = SPI_execute(query.data, true, 0);
	pfree(query.data);

	if (ret != SPI_OK_SELECT) {
		elog(WARNING, "Failed to get changes from slot %s", group->slot_name);
		hash_destroy(batches);
		hash_destroy(rel_cache);
		return 0;
	}

	/* Pre-copy all WAL messages into SyncMemoryContext so they survive SPI_commit */
	num_messages = SPI_processed;
	if (timing_enabled) {
		elog(LOG, "DuckPipe timing: action=fetch_wal group=%s slot=%s fetched_messages=%llu elapsed_ms=%.3f",
		     group->name ? group->name : "<unknown>", group->slot_name ? group->slot_name : "<unknown>",
		     (unsigned long long)num_messages, duckpipe_elapsed_ms(&fetch_timer));
	}

	if (num_messages > 0) {
		if (timing_enabled)
			INSTR_TIME_SET_CURRENT(copy_timer);
		MemoryContext oldcxt = MemoryContextSwitchTo(SyncMemoryContext);
		wal_messages = palloc(sizeof(WalMessage) * num_messages);

		for (uint64 i = 0; i < num_messages; i++) {
			bool isnull;
			Datum lsn_datum = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull);
			Datum data_datum = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2, &isnull);

			if (isnull) {
				wal_messages[i].data = NULL;
				wal_messages[i].len = 0;
				wal_messages[i].lsn = InvalidXLogRecPtr;
				continue;
			}

			wal_messages[i].lsn = DatumGetLSN(lsn_datum);
			{
				bytea *data = DatumGetByteaP(data_datum);
				int len = VARSIZE(data) - VARHDRSZ;
				char *raw = VARDATA(data);

				wal_messages[i].data = palloc(len);
				memcpy(wal_messages[i].data, raw, len);
				wal_messages[i].len = len;
			}
		}
		MemoryContextSwitchTo(oldcxt);

		if (timing_enabled) {
			elog(LOG, "DuckPipe timing: action=copy_wal_messages group=%s copied_messages=%llu elapsed_ms=%.3f",
			     group->name ? group->name : "<unknown>", (unsigned long long)num_messages,
			     duckpipe_elapsed_ms(&copy_timer));
		}
	}

	/* Process each pre-copied WAL message.
	 * Zero-copy: point StringInfo directly at the pre-copied buffer
	 * instead of allocating and copying per message. */
	if (timing_enabled)
		INSTR_TIME_SET_CURRENT(decode_timer);
	for (uint64 i = 0; i < num_messages; i++) {
		StringInfoData buf;

		if (wal_messages[i].data == NULL)
			continue;

		/* Point directly at pre-copied data - no alloc, no copy */
		buf.data = wal_messages[i].data;
		buf.len = wal_messages[i].len;
		buf.maxlen = wal_messages[i].len;
		buf.cursor = 0;

		decode_message(&buf, wal_messages[i].lsn, group, batches, rel_cache);
		total_processed++;
	}
	if (timing_enabled) {
		elog(LOG, "DuckPipe timing: action=decode_messages group=%s decoded_messages=%d elapsed_ms=%.3f",
		     group->name ? group->name : "<unknown>", total_processed, duckpipe_elapsed_ms(&decode_timer));
	}

	/* Flush any remaining batches */
	if (timing_enabled)
		INSTR_TIME_SET_CURRENT(flush_timer);
	flush_all_batches(batches);
	if (timing_enabled) {
		elog(LOG, "DuckPipe timing: action=flush_batches group=%s elapsed_ms=%.3f",
		     group->name ? group->name : "<unknown>", duckpipe_elapsed_ms(&flush_timer));
	}

	/* Commit data changes before metadata updates */
	PopActiveSnapshot();
	SPI_commit();
	SPI_start_transaction();
	PushActiveSnapshot(GetTransactionSnapshot());

	/* Transition any CATCHUP tables to STREAMING.
	 * After consuming a round of WAL, any table still in CATCHUP has now
	 * consumed past its snapshot_lsn and can move to normal streaming. */
	{
		Datum values[1] = {Int32GetDatum(group->id)};
		Oid argtypes[1] = {INT4OID};
		SPI_execute_with_args("UPDATE duckpipe.table_mappings SET state = 'STREAMING' "
		                      "WHERE group_id = $1 AND state = 'CATCHUP'",
		                      1, argtypes, values, NULL, false, 0);
	}

	/* Update confirmed_lsn and last_sync_at after successful processing */
	if (total_processed > 0 && group->pending_lsn != 0) {
		Datum values[2] = {LSNGetDatum(group->pending_lsn), Int32GetDatum(group->id)};
		Oid argtypes[2] = {LSNOID, INT4OID};
		SPI_execute_with_args("UPDATE duckpipe.sync_groups SET confirmed_lsn = $1, "
		                      "last_sync_at = now() WHERE id = $2",
		                      2, argtypes, values, NULL, false, 0);

		elog(DEBUG1,
		     "DuckPipe: sync progress group=%s slot=%s processed_changes=%d fetched_messages=%llu "
		     "confirmed_lsn=%X/%X",
		     group->name ? group->name : "<unknown>", group->slot_name ? group->slot_name : "<unknown>",
		     total_processed, (unsigned long long)num_messages, LSN_FORMAT_ARGS(group->pending_lsn));
	}

	/* Clean up hash tables */
	hash_destroy(batches);
	hash_destroy(rel_cache);

	if (timing_enabled) {
		elog(LOG,
		     "DuckPipe timing: action=process_sync_group group=%s slot=%s processed_changes=%d fetched_messages=%llu "
		     "elapsed_ms=%.3f",
		     group->name ? group->name : "<unknown>", group->slot_name ? group->slot_name : "<unknown>",
		     total_processed, (unsigned long long)num_messages, duckpipe_elapsed_ms(&group_timer));
	}

	return total_processed;
}

PGDLLEXPORT void
duckpipe_worker_main(Datum main_arg) {
	Oid dboid = DatumGetObjectId(main_arg);
	char *dbname;

	pqsignal(SIGHUP, duckpipe_sighup);
	pqsignal(SIGTERM, duckpipe_sigterm);
	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnectionByOid(dboid, InvalidOid, 0);

	StartTransactionCommand();
	dbname = get_database_name(dboid);
	if (dbname == NULL) {
		CommitTransactionCommand();
		elog(FATAL, "database with OID %u does not exist", dboid);
	}
	/* Copy dbname to a safe context before committing */
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(TopMemoryContext);
		dbname = pstrdup(dbname);
		MemoryContextSwitchTo(oldcxt);
	}
	CommitTransactionCommand();

	SyncMemoryContext = AllocSetContextCreate(TopMemoryContext, "pg_duckpipe", ALLOCSET_DEFAULT_SIZES);

	elog(LOG, "pg_duckpipe worker started for database '%s'", dbname);

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
		if (!duckpipe_enabled) {
			(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, duckpipe_poll_interval,
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

		PG_TRY();
		{
			foreach (lc, groups) {
				SyncGroup *group = (SyncGroup *)lfirst(lc);
				int processed = process_sync_group(group);
				if (processed > 0)
					any_work = true;
			}

			SPI_finish();
			PopActiveSnapshot();
			CommitTransactionCommand();
		}
		PG_CATCH();
		{
			EmitErrorReport();
			FlushErrorState();
			AbortCurrentTransaction();
		}
		PG_END_TRY();

		/* Reset memory context after each round (both success and error paths) */
		MemoryContextReset(SyncMemoryContext);

		/* If there was work, loop immediately without sleeping -
		 * there may be more WAL to consume. Only wait when idle. */
		if (!any_work) {
			(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, duckpipe_poll_interval,
			                PG_WAIT_EXTENSION);
			ResetLatch(MyLatch);
		}
	}

	elog(LOG, "pg_duckpipe worker shutting down");
	proc_exit(0);
}

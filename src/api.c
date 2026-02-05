#include "pg_ducklake_sync.h"

#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "utils/builtins.h"

/* Track background worker handle for stop functionality */
/* static BackgroundWorkerHandle *worker_handle = NULL; */

PG_FUNCTION_INFO_V1(ducklake_sync_create_group);
Datum
ducklake_sync_create_group(PG_FUNCTION_ARGS) {
	char *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char *pub = PG_ARGISNULL(1) ? psprintf("ducklake_sync_pub_%s", name) : text_to_cstring(PG_GETARG_TEXT_PP(1));
	char *slot = PG_ARGISNULL(2) ? psprintf("ducklake_sync_slot_%s", name) : text_to_cstring(PG_GETARG_TEXT_PP(2));
	StringInfoData buf;
	int ret;
	Datum values[3];
	Oid argtypes[3] = {TEXTOID, TEXTOID, TEXTOID};
	char nulls[3] = {' ', ' ', ' '};

	if ((ret = SPI_connect()) != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed: %d", ret);

	/* 1. Create slot (Must be first to avoid transaction write conflict) */
	initStringInfo(&buf);
	appendStringInfo(&buf, "SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", quote_literal_cstr(slot));
	ret = SPI_execute(buf.data, false, 0);
	if (ret < 0)
		elog(ERROR, "Failed to create slot %s", slot);

	/* 2. Create publication */
	resetStringInfo(&buf);
	appendStringInfo(&buf, "CREATE PUBLICATION %s", quote_identifier(pub));
	ret = SPI_execute(buf.data, false, 0);
	if (ret < 0)
		elog(ERROR, "Failed to create publication %s", pub);

	/* 3. Insert into sync_groups */
	values[0] = CStringGetTextDatum(name);
	values[1] = CStringGetTextDatum(pub);
	values[2] = CStringGetTextDatum(slot);

	ret = SPI_execute_with_args("INSERT INTO ducklake_sync.sync_groups (name, "
	                            "publication, slot_name) VALUES ($1, $2, $3)",
	                            3, argtypes, values, nulls, false, 0);

	if (ret != SPI_OK_INSERT)
		elog(ERROR, "Failed to insert into sync_groups");

	SPI_finish();

	PG_RETURN_TEXT_P(cstring_to_text(name));
}

PG_FUNCTION_INFO_V1(ducklake_sync_drop_group);
Datum
ducklake_sync_drop_group(PG_FUNCTION_ARGS) {
	char *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	bool drop_slot = PG_GETARG_BOOL(1);
	char *pub;
	char *slot;
	int ret;
	bool isnull;

	if ((ret = SPI_connect()) != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	{
		Datum values[1] = {CStringGetTextDatum(name)};
		Oid argtypes[1] = {TEXTOID};
		if (SPI_execute_with_args("SELECT publication, slot_name FROM "
		                          "ducklake_sync.sync_groups WHERE name = $1",
		                          1, argtypes, values, NULL, true, 0) != SPI_OK_SELECT)
			elog(ERROR, "Failed to query sync_groups");

		if (SPI_processed == 0)
			elog(ERROR, "Sync group not found: %s", name);

		pub = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
		slot = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull));
	}

	if (drop_slot) {
		StringInfoData buf;
		initStringInfo(&buf);
		appendStringInfo(&buf, "SELECT pg_drop_replication_slot(%s)", quote_literal_cstr(slot));
		ret = SPI_execute(buf.data, false, 0);
		if (ret < 0)
			elog(WARNING, "Failed to drop slot %s", slot);
	}

	{
		StringInfoData buf;
		initStringInfo(&buf);
		appendStringInfo(&buf, "DROP PUBLICATION IF EXISTS %s", quote_identifier(pub));
		ret = SPI_execute(buf.data, false, 0);
		if (ret < 0)
			elog(WARNING, "Failed to drop publication %s", pub);
	}

	{
		Datum values[1] = {CStringGetTextDatum(name)};
		Oid argtypes[1] = {TEXTOID};
		SPI_execute_with_args("DELETE FROM ducklake_sync.sync_groups WHERE name = $1", 1, argtypes, values, NULL, false,
		                      0);
	}

	SPI_finish();

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(ducklake_sync_enable_group);
Datum
ducklake_sync_enable_group(PG_FUNCTION_ARGS) {
	char *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	int ret;

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	{
		Datum values[1] = {CStringGetTextDatum(name)};
		Oid argtypes[1] = {TEXTOID};

		ret = SPI_execute_with_args("UPDATE ducklake_sync.sync_groups SET enabled = true WHERE name = $1", 1, argtypes,
		                            values, NULL, false, 0);

		if (ret != SPI_OK_UPDATE)
			elog(ERROR, "Failed to enable sync group '%s'", name);

		if (SPI_processed == 0)
			elog(ERROR, "Sync group '%s' not found", name);
	}

	SPI_finish();
	elog(NOTICE, "Sync group '%s' enabled", name);
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(ducklake_sync_disable_group);
Datum
ducklake_sync_disable_group(PG_FUNCTION_ARGS) {
	char *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	int ret;

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	{
		Datum values[1] = {CStringGetTextDatum(name)};
		Oid argtypes[1] = {TEXTOID};

		ret = SPI_execute_with_args("UPDATE ducklake_sync.sync_groups SET enabled = false WHERE name = $1", 1, argtypes,
		                            values, NULL, false, 0);

		if (ret != SPI_OK_UPDATE)
			elog(ERROR, "Failed to disable sync group '%s'", name);

		if (SPI_processed == 0)
			elog(ERROR, "Sync group '%s' not found", name);
	}

	SPI_finish();
	elog(NOTICE, "Sync group '%s' disabled", name);
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(ducklake_sync_add_table);
Datum
ducklake_sync_add_table(PG_FUNCTION_ARGS) {
	char *source_table_arg = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char *target_table_arg = PG_ARGISNULL(1) ? NULL : text_to_cstring(PG_GETARG_TEXT_PP(1));
	char *group = PG_ARGISNULL(2) ? "default" : text_to_cstring(PG_GETARG_TEXT_PP(2));
	bool copy_data = PG_ARGISNULL(3) ? true : PG_GETARG_BOOL(3);

	char *source_table_copy = pstrdup(source_table_arg);
	char *schema = "public";
	char *table = source_table_copy;
	char *t_schema = "ducklake";
	char *t_table;
	char *dot = strchr(source_table_copy, '.');
	if (dot) {
		*dot = '\0';
		schema = source_table_copy;
		table = dot + 1;
	}
	if (!target_table_arg) {
		t_table = pstrdup(table);
	} else {
		char *target_copy = pstrdup(target_table_arg);
		char *t_dot = strchr(target_copy, '.');
		if (t_dot) {
			*t_dot = '\0';
			t_schema = target_copy;
			t_table = t_dot + 1;
		} else {
			t_table = target_copy;
		}
	}

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	{
		StringInfoData buf;
		char *publication = NULL;
		char *slot_name = NULL;
		bool pub_exists = false;
		int ret;

		/* 1. Get publication and slot name for this group */
		{
			Datum values[1] = {CStringGetTextDatum(group)};
			Oid argtypes[1] = {TEXTOID};
			bool isnull;

			ret = SPI_execute_with_args("SELECT publication, slot_name FROM "
			                            "ducklake_sync.sync_groups WHERE name = $1",
			                            1, argtypes, values, NULL, true, 1);

			if (ret != SPI_OK_SELECT || SPI_processed == 0)
				elog(ERROR, "Sync group '%s' not found", group);

			publication =
			    pstrdup(TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull)));
			slot_name =
			    pstrdup(TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull)));
		}

		/* 2. Check if publication exists */
		{
			Datum values[1] = {CStringGetTextDatum(publication)};
			Oid argtypes[1] = {TEXTOID};

			ret = SPI_execute_with_args("SELECT 1 FROM pg_publication WHERE pubname = $1", 1, argtypes, values, NULL,
			                            true, 1);

			pub_exists = (ret == SPI_OK_SELECT && SPI_processed > 0);
		}

		/* 3. Create or alter publication to add this table */
		initStringInfo(&buf);
		if (!pub_exists) {
			/* Create replication slot FIRST (must happen before any writes) */
			appendStringInfo(&buf, "SELECT pg_create_logical_replication_slot(%s, 'pgoutput')",
			                 quote_literal_cstr(slot_name));
			ret = SPI_execute(buf.data, false, 0);
			if (ret < 0)
				elog(ERROR, "Failed to create replication slot %s", slot_name);

			/* Create publication with this table */
			resetStringInfo(&buf);
			appendStringInfo(&buf, "CREATE PUBLICATION %s FOR TABLE %s.%s", quote_identifier(publication),
			                 quote_identifier(schema), quote_identifier(table));
			ret = SPI_execute(buf.data, false, 0);
			if (ret < 0)
				elog(ERROR, "Failed to create publication %s", publication);
		} else {
			/* Add table to existing publication */
			appendStringInfo(&buf, "ALTER PUBLICATION %s ADD TABLE %s.%s", quote_identifier(publication),
			                 quote_identifier(schema), quote_identifier(table));
			ret = SPI_execute(buf.data, false, 0);
			if (ret < 0)
				elog(ERROR, "Failed to add table to publication %s", publication);
		}

		if (!target_table_arg) {
			resetStringInfo(&buf);
			appendStringInfo(&buf, "CREATE SCHEMA IF NOT EXISTS %s", quote_identifier(t_schema));
			SPI_execute(buf.data, false, 0);

			resetStringInfo(&buf);
			appendStringInfo(&buf, "CREATE TABLE %s.%s (LIKE %s.%s INCLUDING ALL) USING ducklake",
			                 quote_identifier(t_schema), quote_identifier(t_table), quote_identifier(schema),
			                 quote_identifier(table));
			ret = SPI_execute(buf.data, false, 0);
			if (ret < 0)
				elog(ERROR, "Failed to create target table %s.%s", t_schema, t_table);
		}

		/* 7. Insert table mapping */
		{
			Datum values[6];
			Oid argtypes[6] = {TEXTOID, TEXTOID, TEXTOID, TEXTOID, TEXTOID, TEXTOID};
			char *initial_state = copy_data ? "SNAPSHOT" : "STREAMING";

			values[0] = CStringGetTextDatum(schema);
			values[1] = CStringGetTextDatum(table);
			values[2] = CStringGetTextDatum(t_schema);
			values[3] = CStringGetTextDatum(t_table);
			values[4] = CStringGetTextDatum(initial_state);
			values[5] = CStringGetTextDatum(group);

			ret = SPI_execute_with_args("INSERT INTO ducklake_sync.table_mappings (group_id, source_schema, "
			                            "source_table, target_schema, target_table, state) "
			                            "SELECT id, $1, $2, $3, $4, $5 FROM ducklake_sync.sync_groups WHERE "
			                            "name = $6",
			                            6, argtypes, values, NULL, false, 0);

			if (ret != SPI_OK_INSERT)
				elog(ERROR, "Failed to add table mapping");
		}

		pfree(buf.data);
		SPI_finish();
	}
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(ducklake_sync_remove_table);
Datum
ducklake_sync_remove_table(PG_FUNCTION_ARGS) {
	char *source_table_arg = text_to_cstring(PG_GETARG_TEXT_PP(0));
	bool drop_target = PG_ARGISNULL(1) ? false : PG_GETARG_BOOL(1);

	char *source_copy = pstrdup(source_table_arg);
	char *schema = "public";
	char *table = source_copy;
	char *dot = strchr(source_copy, '.');

	char *publication = NULL;
	char *t_schema = NULL;
	char *t_table = NULL;
	StringInfoData buf;
	int ret;

	if (dot) {
		*dot = '\0';
		schema = source_copy;
		table = dot + 1;
	}

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	{
		Datum values[2] = {CStringGetTextDatum(schema), CStringGetTextDatum(table)};
		Oid argtypes[2] = {TEXTOID, TEXTOID};
		bool isnull;

		ret = SPI_execute_with_args("SELECT g.publication, m.target_schema, m.target_table "
		                            "FROM ducklake_sync.table_mappings m "
		                            "JOIN ducklake_sync.sync_groups g ON m.group_id = g.id "
		                            "WHERE m.source_schema = $1 AND m.source_table = $2",
		                            2, argtypes, values, NULL, true, 1);

		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			publication =
			    pstrdup(TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull)));
			t_schema =
			    pstrdup(TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull)));
			t_table =
			    pstrdup(TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3, &isnull)));
		}
	}

	if (publication) {
		/* Check if table is in publication before trying to drop */
		Datum check_values[2] = {CStringGetTextDatum(publication), CStringGetTextDatum(table)};
		Oid check_types[2] = {TEXTOID, TEXTOID};

		ret = SPI_execute_with_args("SELECT 1 FROM pg_publication_tables WHERE "
		                            "pubname = $1 AND tablename = $2",
		                            2, check_types, check_values, NULL, true, 1);

		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			initStringInfo(&buf);
			appendStringInfo(&buf, "ALTER PUBLICATION %s DROP TABLE %s.%s", quote_identifier(publication),
			                 quote_identifier(schema), quote_identifier(table));
			SPI_execute(buf.data, false, 0);
			pfree(buf.data);
		}
	}

	if (drop_target && t_schema && t_table) {
		initStringInfo(&buf);
		appendStringInfo(&buf, "DROP TABLE IF EXISTS %s.%s", quote_identifier(t_schema), quote_identifier(t_table));
		SPI_execute(buf.data, false, 0);
		pfree(buf.data);
	}

	{
		Datum values[2] = {CStringGetTextDatum(schema), CStringGetTextDatum(table)};
		Oid argtypes[2] = {TEXTOID, TEXTOID};

		ret = SPI_execute_with_args("DELETE FROM ducklake_sync.table_mappings WHERE "
		                            "source_schema = $1 AND source_table = $2",
		                            2, argtypes, values, NULL, false, 0);

		if (ret != SPI_OK_DELETE)
			elog(WARNING, "Failed to remove table mapping");
	}

	SPI_finish();
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(ducklake_sync_move_table);
Datum
ducklake_sync_move_table(PG_FUNCTION_ARGS) {
	char *source_table_arg = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char *new_group = text_to_cstring(PG_GETARG_TEXT_PP(1));

	char *source_copy = pstrdup(source_table_arg);
	char *schema = "public";
	char *table = source_copy;
	char *dot = strchr(source_copy, '.');
	if (dot) {
		*dot = '\0';
		schema = source_copy;
		table = dot + 1;
	}

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	/* Use parameterized query to prevent SQL injection */
	{
		Datum values[3];
		Oid argtypes[3] = {TEXTOID, TEXTOID, TEXTOID};

		values[0] = CStringGetTextDatum(new_group);
		values[1] = CStringGetTextDatum(schema);
		values[2] = CStringGetTextDatum(table);

		{
			int ret = SPI_execute_with_args("UPDATE ducklake_sync.table_mappings SET group_id = (SELECT id FROM "
			                                "ducklake_sync.sync_groups WHERE name = $1) "
			                                "WHERE source_schema = $2 AND source_table = $3",
			                                3, argtypes, values, NULL, false, 0);

			if (ret != SPI_OK_UPDATE)
				elog(ERROR, "Failed to move table");
		}
	}

	SPI_finish();
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(ducklake_sync_resync_table);
Datum
ducklake_sync_resync_table(PG_FUNCTION_ARGS) {
	char *source_table_arg = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char *source_copy = pstrdup(source_table_arg);
	char *schema = "public";
	char *table = source_copy;
	char *dot = strchr(source_copy, '.');
	char *t_schema = NULL;
	char *t_table = NULL;
	int ret;

	if (dot) {
		*dot = '\0';
		schema = source_copy;
		table = dot + 1;
	}

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	/* Get target table info */
	{
		Datum values[2] = {CStringGetTextDatum(schema), CStringGetTextDatum(table)};
		Oid argtypes[2] = {TEXTOID, TEXTOID};
		bool isnull;

		ret = SPI_execute_with_args("SELECT target_schema, target_table FROM ducklake_sync.table_mappings "
		                            "WHERE source_schema = $1 AND source_table = $2",
		                            2, argtypes, values, NULL, true, 1);

		if (ret != SPI_OK_SELECT || SPI_processed == 0)
			elog(ERROR, "Table mapping for '%s.%s' not found", schema, table);

		t_schema =
		    pstrdup(TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull)));
		t_table = pstrdup(TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull)));
	}

	/* Truncate target table */
	{
		StringInfoData buf;
		initStringInfo(&buf);
		appendStringInfo(&buf, "TRUNCATE TABLE %s.%s", quote_identifier(t_schema), quote_identifier(t_table));
		ret = SPI_execute(buf.data, false, 0);
		if (ret != SPI_OK_UTILITY)
			elog(WARNING, "Failed to truncate target table %s.%s", t_schema, t_table);
		pfree(buf.data);
	}

	/* Reset state to SNAPSHOT to trigger re-copy */
	{
		Datum values[2] = {CStringGetTextDatum(schema), CStringGetTextDatum(table)};
		Oid argtypes[2] = {TEXTOID, TEXTOID};

		ret = SPI_execute_with_args("UPDATE ducklake_sync.table_mappings SET state = 'SNAPSHOT', "
		                            "rows_synced = 0, last_sync_at = NULL "
		                            "WHERE source_schema = $1 AND source_table = $2",
		                            2, argtypes, values, NULL, false, 0);

		if (ret != SPI_OK_UPDATE)
			elog(ERROR, "Failed to reset table state");
	}

	SPI_finish();
	elog(NOTICE, "Table '%s.%s' marked for resync", schema, table);
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(ducklake_sync_groups);
Datum
ducklake_sync_groups(PG_FUNCTION_ARGS) {
	FuncCallContext *funcctx;
	int call_cntr;
	int max_calls;
	TupleDesc tupdesc;
	AttInMetadata *attinmeta;

	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;
		int ret;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Build tuple descriptor */
		tupdesc = CreateTemplateTupleDesc(7);
		TupleDescInitEntry(tupdesc, 1, "name", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, 2, "publication", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, 3, "slot_name", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, 4, "enabled", BOOLOID, -1, 0);
		TupleDescInitEntry(tupdesc, 5, "table_count", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, 6, "lag_bytes", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, 7, "last_sync", TIMESTAMPTZOID, -1, 0);

		attinmeta = TupleDescGetAttInMetadata(tupdesc);
		funcctx->attinmeta = attinmeta;

		/* Query sync groups */
		if (SPI_connect() != SPI_OK_CONNECT)
			elog(ERROR, "SPI_connect failed");

		ret = SPI_execute("SELECT g.name, g.publication, g.slot_name, g.enabled, "
		                  "       (SELECT count(*) FROM ducklake_sync.table_mappings "
		                  "m WHERE m.group_id = g.id)::int4 as table_count, "
		                  "       COALESCE(pg_current_wal_lsn() - g.confirmed_lsn, "
		                  "0)::int8 as lag_bytes, "
		                  "       g.last_sync_at "
		                  "FROM ducklake_sync.sync_groups g "
		                  "ORDER BY g.name",
		                  true, 0);

		if (ret == SPI_OK_SELECT) {
			funcctx->max_calls = SPI_processed;
			funcctx->user_fctx = SPI_tuptable;
		} else {
			funcctx->max_calls = 0;
		}

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	call_cntr = funcctx->call_cntr;
	max_calls = funcctx->max_calls;
	attinmeta = funcctx->attinmeta;

	if (call_cntr < max_calls) {
		SPITupleTable *tuptable = (SPITupleTable *)funcctx->user_fctx;
		HeapTuple tuple;
		Datum result;
		Datum values[7];
		bool nulls[7];
		bool isnull;
		int i;

		for (i = 0; i < 7; i++) {
			values[i] = SPI_getbinval(tuptable->vals[call_cntr], tuptable->tupdesc, i + 1, &isnull);
			nulls[i] = isnull;
		}

		tuple = heap_form_tuple(attinmeta->tupdesc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	} else {
		SPI_finish();
		SRF_RETURN_DONE(funcctx);
	}
}

PG_FUNCTION_INFO_V1(ducklake_sync_tables);
Datum
ducklake_sync_tables(PG_FUNCTION_ARGS) {
	FuncCallContext *funcctx;
	int call_cntr;
	int max_calls;
	TupleDesc tupdesc;
	AttInMetadata *attinmeta;

	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;
		int ret;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Build tuple descriptor */
		tupdesc = CreateTemplateTupleDesc(6);
		TupleDescInitEntry(tupdesc, 1, "source_table", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, 2, "target_table", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, 3, "sync_group", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, 4, "enabled", BOOLOID, -1, 0);
		TupleDescInitEntry(tupdesc, 5, "rows_synced", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, 6, "last_sync", TIMESTAMPTZOID, -1, 0);

		attinmeta = TupleDescGetAttInMetadata(tupdesc);
		funcctx->attinmeta = attinmeta;

		/* Query table mappings */
		if (SPI_connect() != SPI_OK_CONNECT)
			elog(ERROR, "SPI_connect failed");

		ret = SPI_execute("SELECT m.source_schema || '.' || m.source_table as source_table, "
		                  "       m.target_schema || '.' || m.target_table as target_table, "
		                  "       g.name as sync_group, "
		                  "       m.enabled, m.rows_synced, m.last_sync_at "
		                  "FROM ducklake_sync.table_mappings m "
		                  "JOIN ducklake_sync.sync_groups g ON m.group_id = g.id "
		                  "ORDER BY g.name, m.source_schema, m.source_table",
		                  true, 0);

		if (ret == SPI_OK_SELECT) {
			funcctx->max_calls = SPI_processed;
			funcctx->user_fctx = SPI_tuptable;
		} else {
			funcctx->max_calls = 0;
		}

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	call_cntr = funcctx->call_cntr;
	max_calls = funcctx->max_calls;
	attinmeta = funcctx->attinmeta;

	if (call_cntr < max_calls) {
		SPITupleTable *tuptable = (SPITupleTable *)funcctx->user_fctx;
		HeapTuple tuple;
		Datum result;
		Datum values[6];
		bool nulls[6];
		bool isnull;
		int i;

		for (i = 0; i < 6; i++) {
			values[i] = SPI_getbinval(tuptable->vals[call_cntr], tuptable->tupdesc, i + 1, &isnull);
			nulls[i] = isnull;
		}

		tuple = heap_form_tuple(attinmeta->tupdesc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	} else {
		SPI_finish();
		SRF_RETURN_DONE(funcctx);
	}
}

PG_FUNCTION_INFO_V1(ducklake_sync_status);
Datum
ducklake_sync_status(PG_FUNCTION_ARGS) {
	FuncCallContext *funcctx;
	int call_cntr;
	int max_calls;
	TupleDesc tupdesc;
	AttInMetadata *attinmeta;

	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;
		int ret;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Build tuple descriptor */
		tupdesc = CreateTemplateTupleDesc(7);
		TupleDescInitEntry(tupdesc, 1, "sync_group", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, 2, "source_table", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, 3, "target_table", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, 4, "state", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, 5, "enabled", BOOLOID, -1, 0);
		TupleDescInitEntry(tupdesc, 6, "rows_synced", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, 7, "last_sync", TIMESTAMPTZOID, -1, 0);

		attinmeta = TupleDescGetAttInMetadata(tupdesc);
		funcctx->attinmeta = attinmeta;

		/* Query status */
		if (SPI_connect() != SPI_OK_CONNECT)
			elog(ERROR, "SPI_connect failed");

		ret = SPI_execute("SELECT g.name as sync_group, "
		                  "       m.source_schema || '.' || m.source_table as source_table, "
		                  "       m.target_schema || '.' || m.target_table as target_table, "
		                  "       m.state, m.enabled, m.rows_synced, m.last_sync_at "
		                  "FROM ducklake_sync.table_mappings m "
		                  "JOIN ducklake_sync.sync_groups g ON m.group_id = g.id "
		                  "ORDER BY g.name, m.source_schema, m.source_table",
		                  true, 0);

		if (ret == SPI_OK_SELECT) {
			funcctx->max_calls = SPI_processed;
			funcctx->user_fctx = SPI_tuptable;
		} else {
			funcctx->max_calls = 0;
		}

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	call_cntr = funcctx->call_cntr;
	max_calls = funcctx->max_calls;
	attinmeta = funcctx->attinmeta;

	if (call_cntr < max_calls) {
		SPITupleTable *tuptable = (SPITupleTable *)funcctx->user_fctx;
		HeapTuple tuple;
		Datum result;
		Datum values[7];
		bool nulls[7];
		bool isnull;
		int i;

		for (i = 0; i < 7; i++) {
			values[i] = SPI_getbinval(tuptable->vals[call_cntr], tuptable->tupdesc, i + 1, &isnull);
			nulls[i] = isnull;
		}

		tuple = heap_form_tuple(attinmeta->tupdesc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	} else {
		SPI_finish();
		SRF_RETURN_DONE(funcctx);
	}
}

PG_FUNCTION_INFO_V1(ducklake_sync_start_worker);
Datum
ducklake_sync_start_worker(PG_FUNCTION_ARGS) {
	BackgroundWorker worker;
	char *dbname;
	char *bgw_type = "pg_ducklake_sync";
	bool running = false;

	dbname = get_database_name(MyDatabaseId);
	if (dbname == NULL)
		elog(ERROR, "Could not get current database name");

	/* Check if worker is already running using pg_stat_activity */
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	{
		Datum values[1] = {CStringGetTextDatum(bgw_type)};
		Oid argtypes[1] = {TEXTOID};

		int ret = SPI_execute_with_args("SELECT 1 FROM pg_stat_activity WHERE backend_type = $1", 1, argtypes, values,
		                                NULL, true, 0);

		if (ret == SPI_OK_SELECT && SPI_processed > 0)
			running = true;
	}
	SPI_finish();

	if (running) {
		elog(NOTICE, "Worker already running for database %s", dbname);
		PG_RETURN_VOID();
	}

	/* Start new worker */
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	snprintf(worker.bgw_library_name, BGW_MAXLEN, "pg_ducklake_sync");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "ducklake_sync_worker_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "pg_ducklake_sync worker [%s]", dbname);
	snprintf(worker.bgw_type, BGW_MAXLEN, "%s", bgw_type);
	worker.bgw_main_arg = ObjectIdGetDatum(MyDatabaseId);
	worker.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&worker, NULL))
		elog(ERROR, "Failed to start background worker for database %s", dbname);

	elog(NOTICE, "Background worker started for database %s", dbname);
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(ducklake_sync_stop_worker);
Datum
ducklake_sync_stop_worker(PG_FUNCTION_ARGS) {
	char *bgw_type = "pg_ducklake_sync";
	int terminated_count = 0;

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	{
		Datum values[1] = {CStringGetTextDatum(bgw_type)};
		Oid argtypes[1] = {TEXTOID};

		/* Terminate all workers of our type */
		int ret = SPI_execute_with_args("SELECT pg_terminate_backend(pid) FROM "
		                                "pg_stat_activity WHERE backend_type = $1",
		                                1, argtypes, values, NULL, false, 0);

		if (ret == SPI_OK_SELECT)
			terminated_count = SPI_processed;
	}

	SPI_finish();

	if (terminated_count > 0)
		elog(NOTICE, "Terminated %d worker(s)", terminated_count);
	else
		elog(NOTICE, "No workers found to terminate");

	PG_RETURN_VOID();
}

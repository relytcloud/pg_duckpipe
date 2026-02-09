#include "pg_duckpipe.h"

#include "catalog/pg_type.h"
#include "nodes/bitmapset.h"
#include "utils/memutils.h"

/*
 * Batch management functions
 */

/* Update per-table sync metrics after a successful batch apply. */
static void
update_table_metrics(int mapping_id, int64 delta_rows) {
	Datum values[2];
	Oid argtypes[2] = {INT8OID, INT4OID};

	if (delta_rows <= 0)
		return;

	values[0] = Int64GetDatum(delta_rows);
	values[1] = Int32GetDatum(mapping_id);

	SPI_execute_with_args("UPDATE duckpipe.table_mappings "
	                      "SET rows_synced = rows_synced + $1, "
	                      "    last_sync_at = now() "
	                      "WHERE id = $2",
	                      2, argtypes, values, NULL, false, 0);
}

/* Build batch attnames/keyattrs using source table catalogs when RELATION
 * metadata is unavailable in this poll round. */
static void
load_batch_metadata_from_source(SyncBatch *batch, TableMapping *mapping) {
	Datum values[2];
	Oid argtypes[2] = {TEXTOID, TEXTOID};
	int ret;

	values[0] = CStringGetTextDatum(mapping->source_schema);
	values[1] = CStringGetTextDatum(mapping->source_table);

	ret = SPI_execute_with_args("SELECT a.attname "
	                            "FROM pg_class c "
	                            "JOIN pg_namespace n ON n.oid = c.relnamespace "
	                            "JOIN pg_attribute a ON a.attrelid = c.oid "
	                            "WHERE n.nspname = $1 AND c.relname = $2 "
	                            "  AND a.attnum > 0 AND NOT a.attisdropped "
	                            "ORDER BY a.attnum",
	                            2, argtypes, values, NULL, true, 0);

	if (ret != SPI_OK_SELECT || SPI_processed == 0)
		elog(ERROR, "Failed to load column metadata for %s.%s", mapping->source_schema, mapping->source_table);

	for (uint64 i = 0; i < SPI_processed; i++) {
		bool isnull;
		char *attname = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull));
		if (!isnull)
			batch->attnames = lappend(batch->attnames, pstrdup(attname));
	}

	ret = SPI_execute_with_args("SELECT k.attnum::int2 "
	                            "FROM pg_class c "
	                            "JOIN pg_namespace n ON n.oid = c.relnamespace "
	                            "JOIN pg_index i ON i.indrelid = c.oid AND i.indisprimary "
	                            "JOIN unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true "
	                            "WHERE n.nspname = $1 AND c.relname = $2 "
	                            "ORDER BY k.ord",
	                            2, argtypes, values, NULL, true, 0);

	if (ret == SPI_OK_SELECT && SPI_processed > 0) {
		batch->keyattrs = palloc(sizeof(int) * SPI_processed);
		batch->nkeyattrs = 0;

		for (uint64 i = 0; i < SPI_processed; i++) {
			bool isnull;
			int attnum = DatumGetInt16(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull));
			if (!isnull && attnum > 0)
				batch->keyattrs[batch->nkeyattrs++] = attnum - 1; /* 0-based index */
		}
	}
}

/*
 * Free a list of SyncChange structs, including their internal
 * col_values and key_values string lists.
 * list_free_deep only frees the top-level pointed-to objects,
 * but SyncChange contains nested Lists of palloc'd strings.
 */
static void
free_change_list(List *changes) {
	ListCell *lc;

	foreach (lc, changes) {
		SyncChange *change = (SyncChange *)lfirst(lc);

		/* Handle col_values - free each string then the list */
		if (change->col_values != NIL) {
			ListCell *vc;
			foreach (vc, change->col_values) {
				char *val = (char *)lfirst(vc);
				if (val != NULL)
					pfree(val);
			}
			list_free(change->col_values);
			change->col_values = NIL;
		}
		/* Handle key_values - free each string then the list */
		if (change->key_values != NIL) {
			ListCell *vc;
			foreach (vc, change->key_values) {
				char *val = (char *)lfirst(vc);
				if (val != NULL)
					pfree(val);
			}
			list_free(change->key_values);
			change->key_values = NIL;
		}
		pfree(change);
	}
	list_free(changes);
}

/*
 * Add a change to the batch for the target table.
 * If the batch is full, flush it immediately.
 */
void
batch_add_change(HTAB *batches, TableMapping *mapping, SyncChange *change, LogicalRepRelation *rel) {
	SyncBatch *batch;
	bool found;
	char key[NAMEDATALEN * 2 + 2];

	/* Build key as schema.table */
	snprintf(key, sizeof(key), "%s.%s", mapping->target_schema, mapping->target_table);

	/* Create or find batch */
	batch = (SyncBatch *)hash_search(batches, key, HASH_ENTER, &found);

	if (!found) {
		MemoryContext oldcxt;

		/* New batch - key is already copied into batch->target_table by hash_search
		 * since it's the first field and keysize matches */
		batch->mapping_id = mapping->id;
		batch->changes = NIL;
		batch->count = 0;
		batch->last_lsn = 0;
		batch->attnames = NIL;
		batch->nkeyattrs = 0;
		batch->keyattrs = NULL;

		/* Copy column names and key attrs in SyncMemoryContext so they
		 * survive SPI_commit during transaction splitting */
		oldcxt = MemoryContextSwitchTo(SyncMemoryContext);

		if (rel != NULL) {
			for (int i = 0; i < rel->natts; i++) {
				batch->attnames = lappend(batch->attnames, pstrdup(rel->attnames[i]));
			}

			/* Copy key attribute indices if available */
			if (rel->attkeys != NULL) {
				int nkeys = 0;
				/* Count keys - attkeys is a Bitmapset */
				nkeys = bms_num_members(rel->attkeys);
				if (nkeys > 0) {
					int x = -1;
					batch->keyattrs = palloc(sizeof(int) * nkeys);
					batch->nkeyattrs = 0;
					while ((x = bms_next_member(rel->attkeys, x)) >= 0) {
						batch->keyattrs[batch->nkeyattrs++] = x;
					}
				}
			}
		} else {
			load_batch_metadata_from_source(batch, mapping);
		}

		MemoryContextSwitchTo(oldcxt);
	} else if (batch->mapping_id != mapping->id) {
		elog(ERROR, "Conflicting mappings share target table key %s", key);
	}

	/* Add change in SyncMemoryContext so it survives SPI_commit */
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(SyncMemoryContext);
		batch->changes = lappend(batch->changes, change);
		MemoryContextSwitchTo(oldcxt);
	}
	batch->count++;
	batch->last_lsn = change->lsn;

	/* Flush if full */
	if (batch->count >= duckpipe_batch_size_per_table) {
		int applied_count = batch->count;
		apply_batch(batch);
		update_table_metrics(batch->mapping_id, applied_count);

		/* Reset batch - keep attnames/keyattrs but clear changes */
		free_change_list(batch->changes);
		batch->changes = NIL;
		batch->count = 0;
	}
}

/*
 * Flush all batches in the hash table.
 */
void
flush_all_batches(HTAB *batches) {
	HASH_SEQ_STATUS status;
	SyncBatch *batch;

	hash_seq_init(&status, batches);

	while ((batch = (SyncBatch *)hash_seq_search(&status)) != NULL) {
		if (batch->count > 0) {
			int applied_count = batch->count;
			apply_batch(batch);
			update_table_metrics(batch->mapping_id, applied_count);

			/* Reset batch */
			free_change_list(batch->changes);
			batch->changes = NIL;
			batch->count = 0;
		}
	}
}

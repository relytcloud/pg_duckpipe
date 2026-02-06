#include "pg_duckpipe.h"

#include "executor/spi.h"
#include "utils/builtins.h"

/*
 * Helper to convert LogicalRepTupleData to a List of string values.
 * We deep copy the strings because the buffer is transient.
 *
 * LogicalRepTupleData structure (this PostgreSQL version):
 *   StringInfoData *colvalues; -- array of StringInfoData, one per column
 *   char *colstatus;           -- status per column ('n'=null, 'u'=unchanged,
 * 't'=text, 'b'=binary) int ncols;                 -- number of columns
 */
static List *
tuple_to_list(LogicalRepTupleData *tuple, LogicalRepRelation *rel) {
	List *values = NIL;
	int i;

	for (i = 0; i < tuple->ncols; i++) {
		char status = tuple->colstatus[i];
		char *val_str = NULL;

		if (status == 'n') /* NULL */
		{
			val_str = NULL;
		} else if (status == 'u') /* Unchanged (TOAST) */
		{
			/* Treat as NULL for now - for INSERT this shouldn't happen */
			val_str = NULL;
		} else /* 't' (text) or 'b' (binary) */
		{
			/* colvalues is an array of StringInfoData */
			int len = tuple->colvalues[i].len;
			char *data = tuple->colvalues[i].data;

			val_str = palloc(len + 1);
			memcpy(val_str, data, len);
			val_str[len] = '\0';
		}

		values = lappend(values, val_str);
	}

	return values;
}

void
decode_message(StringInfo buf, XLogRecPtr lsn, SyncGroup *group, HTAB *batches, HTAB *rel_cache) {
	char msgtype = pq_getmsgbyte(buf);

	switch (msgtype) {
	case LOGICAL_REP_MSG_RELATION: {
		LogicalRepRelation *rel = logicalrep_read_rel(buf);
		RelationCacheEntry *entry;
		bool found;

		entry = (RelationCacheEntry *)hash_search(rel_cache, &rel->remoteid, HASH_ENTER, &found);
		/* If found, we might want to free old one? */
		entry->rel = rel;      /* Store the new relation definition */
		entry->mapping = NULL; /* Reset cached mapping on schema change */
		break;
	}
	case LOGICAL_REP_MSG_INSERT: {
		LogicalRepTupleData newtup;
		LogicalRepRelId relid;
		TableMapping *mapping;
		RelationCacheEntry *entry;
		SyncChange *change;

		relid = logicalrep_read_insert(buf, &newtup);

		/* Find Relation info */
		entry = (RelationCacheEntry *)hash_search(rel_cache, &relid, HASH_FIND, NULL);
		if (!entry)
			return; /* Should not happen if protocol flow is correct */

		/* Find Table Mapping (cached per relation per poll round) */
		if (entry->mapping == NULL)
			entry->mapping = get_table_mapping(group, entry->rel->nspname, entry->rel->relname);
		mapping = entry->mapping;
		if (!mapping || !mapping->enabled)
			return;

		/* During CATCHUP, skip changes already included in snapshot */
		if (strcmp(mapping->state, "CATCHUP") == 0 && mapping->snapshot_lsn != InvalidXLogRecPtr &&
		    lsn <= mapping->snapshot_lsn)
			return;

		/* Construct Change */
		change = palloc0(sizeof(SyncChange));
		change->type = SYNC_CHANGE_INSERT;
		change->lsn = lsn;
		change->col_values = tuple_to_list(&newtup, entry->rel);

		batch_add_change(batches, mapping, change, entry->rel);
		break;
	}
	case LOGICAL_REP_MSG_UPDATE: {
		LogicalRepTupleData oldtup, newtup;
		bool has_old;
		LogicalRepRelId relid;
		TableMapping *mapping;
		RelationCacheEntry *entry;
		SyncChange *change;

		relid = logicalrep_read_update(buf, &has_old, &oldtup, &newtup);

		entry = (RelationCacheEntry *)hash_search(rel_cache, &relid, HASH_FIND, NULL);
		if (!entry)
			return;
		if (entry->mapping == NULL)
			entry->mapping = get_table_mapping(group, entry->rel->nspname, entry->rel->relname);
		mapping = entry->mapping;
		if (!mapping || !mapping->enabled)
			return;

		/* During CATCHUP, skip changes already included in snapshot */
		if (strcmp(mapping->state, "CATCHUP") == 0 && mapping->snapshot_lsn != InvalidXLogRecPtr &&
		    lsn <= mapping->snapshot_lsn)
			return;

		/* Treat UPDATE as DELETE + INSERT for simplicity in V1 (easy for column
		 * store) */
		/* Always generate DELETE first.
		   If has_old is true, use oldtup for key.
		   If has_old is false, it means key didn't change (or REPLICA IDENTITY
		   DEFAULT), so use newtup for key.
		*/
		{
			SyncChange *del_change = palloc0(sizeof(SyncChange));
			del_change->type = SYNC_CHANGE_DELETE;
			del_change->lsn = lsn;

			if (has_old)
				del_change->key_values = tuple_to_list(&oldtup, entry->rel);
			else
				del_change->key_values = tuple_to_list(&newtup, entry->rel);

			batch_add_change(batches, mapping, del_change, entry->rel);
		}

		change = palloc0(sizeof(SyncChange));
		change->type = SYNC_CHANGE_INSERT;
		change->lsn = lsn;
		change->col_values = tuple_to_list(&newtup, entry->rel);
		batch_add_change(batches, mapping, change, entry->rel);
		break;
	}
	case LOGICAL_REP_MSG_DELETE: {
		LogicalRepTupleData oldtup;
		LogicalRepRelId relid;
		TableMapping *mapping;
		RelationCacheEntry *entry;
		SyncChange *change;

		relid = logicalrep_read_delete(buf, &oldtup);

		entry = (RelationCacheEntry *)hash_search(rel_cache, &relid, HASH_FIND, NULL);
		if (!entry)
			return;
		if (entry->mapping == NULL)
			entry->mapping = get_table_mapping(group, entry->rel->nspname, entry->rel->relname);
		mapping = entry->mapping;
		if (!mapping || !mapping->enabled)
			return;

		/* During CATCHUP, skip changes already included in snapshot */
		if (strcmp(mapping->state, "CATCHUP") == 0 && mapping->snapshot_lsn != InvalidXLogRecPtr &&
		    lsn <= mapping->snapshot_lsn)
			return;

		change = palloc0(sizeof(SyncChange));
		change->type = SYNC_CHANGE_DELETE;
		change->lsn = lsn;
		change->key_values = tuple_to_list(&oldtup, entry->rel);
		batch_add_change(batches, mapping, change, entry->rel);
		break;
	}
	case LOGICAL_REP_MSG_BEGIN: {
		LogicalRepBeginData begin_data;
		logicalrep_read_begin(buf, &begin_data);
		break;
	}
	case LOGICAL_REP_MSG_COMMIT: {
		LogicalRepCommitData commit_data;
		logicalrep_read_commit(buf, &commit_data);
		group->pending_lsn = commit_data.end_lsn;
		flush_all_batches(batches);
		break;
	}
	case LOGICAL_REP_MSG_TRUNCATE: {
		/*
		 * Handle TRUNCATE by truncating all target tables involved.
		 * logicalrep_read_truncate returns a List of LogicalRepRelId values.
		 */
		bool cascade;
		bool restart_seqs;
		List *relid_list;
		ListCell *lc;

		relid_list = logicalrep_read_truncate(buf, &cascade, &restart_seqs);

		/* Flush pending batches before TRUNCATE */
		flush_all_batches(batches);

		/* Execute TRUNCATEs within the caller's SPI session */
		foreach (lc, relid_list) {
			LogicalRepRelId relid = lfirst_oid(lc);
			RelationCacheEntry *entry = (RelationCacheEntry *)hash_search(rel_cache, &relid, HASH_FIND, NULL);
			if (entry) {
				TableMapping *mapping = get_table_mapping(group, entry->rel->nspname, entry->rel->relname);
				if (mapping && mapping->enabled) {
					StringInfoData truncate_buf;
					initStringInfo(&truncate_buf);
					appendStringInfo(&truncate_buf, "TRUNCATE TABLE %s.%s", quote_identifier(mapping->target_schema),
					                 quote_identifier(mapping->target_table));
					if (cascade)
						appendStringInfoString(&truncate_buf, " CASCADE");
					if (restart_seqs)
						appendStringInfoString(&truncate_buf, " RESTART IDENTITY");

					if (SPI_execute(truncate_buf.data, false, 0) != SPI_OK_UTILITY)
						elog(WARNING, "Failed to truncate target table %s.%s", mapping->target_schema,
						     mapping->target_table);
					else
						elog(LOG, "DuckPipe: Truncated %s.%s", mapping->target_schema, mapping->target_table);

					pfree(truncate_buf.data);
				}
			}
		}

		list_free(relid_list);
		break;
	}
	default:
		elog(DEBUG1, "pg_duckpipe: unknown message type %c", msgtype);
		break;
	}
}

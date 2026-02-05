#include "pg_ducklake_sync.h"

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
		entry->rel = rel; /* Store the new relation definition */
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

		/* Find Table Mapping */
		mapping = get_table_mapping(group, entry->rel->nspname, entry->rel->relname);
		if (!mapping || !mapping->enabled)
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
		mapping = get_table_mapping(group, entry->rel->nspname, entry->rel->relname);
		if (!mapping || !mapping->enabled)
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
		mapping = get_table_mapping(group, entry->rel->nspname, entry->rel->relname);
		if (!mapping || !mapping->enabled)
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
		/* Handle truncate if needed, or ignore for now */
		break;
	}
	}
}

#include "pg_duckpipe.h"

#include "nodes/bitmapset.h"
#include "utils/memutils.h"

/*
 * Batch management functions
 */

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

		if (change->col_values != NIL) {
			list_free_deep(change->col_values);
			change->col_values = NIL;
		}
		if (change->key_values != NIL) {
			list_free_deep(change->key_values);
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
		/* New batch - key is already copied into batch->target_table by hash_search
		 * since it's the first field and keysize matches */
		batch->changes = NIL;
		batch->count = 0;
		batch->last_lsn = 0;
		batch->attnames = NIL;
		batch->nkeyattrs = 0;
		batch->keyattrs = NULL;

		/* Copy column names from relation */
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
						/* attkeys seems to use 0-based attribute numbers in PG 18 or
						 * LogicalRepRelation? */
						/* If x=0 (first col), we want index 0. So no subtraction. */
						batch->keyattrs[batch->nkeyattrs++] = x;
					}
				}
			}
		}
	}

	/* Add change */
	batch->changes = lappend(batch->changes, change);
	batch->count++;
	batch->last_lsn = change->lsn;

	/* Flush if full */
	if (batch->count >= duckpipe_batch_size_per_table) {
		apply_batch(batch);

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
			apply_batch(batch);

			/* Reset batch */
			free_change_list(batch->changes);
			batch->changes = NIL;
			batch->count = 0;
		}
	}
}

#include "pg_ducklake_sync.h"

#include "executor/spi.h"
#include "utils/builtins.h"

/*
 * Quote literal value for SQL.
 * Returns palloc'd string.
 */
static char *
quote_val(char *val) {
	StringInfoData buf;
	initStringInfo(&buf);

	if (val == NULL)
		appendStringInfoString(&buf, "NULL");
	else {
		appendStringInfoChar(&buf, '\'');
		/* Escape quotes */
		for (char *p = val; *p; p++) {
			if (*p == '\'')
				appendStringInfoChar(&buf, '\'');
			appendStringInfoChar(&buf, *p);
		}
		appendStringInfoChar(&buf, '\'');
	}
	return buf.data;
}

void
apply_batch(SyncBatch *batch) {
	StringInfoData insert_buf;
	bool insert_started = false;
	ListCell *lc;
	int ret;

	if (!batch || batch->count == 0)
		return;

	if ((ret = SPI_connect()) != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed: %d", ret);

	initStringInfo(&insert_buf);

	foreach (lc, batch->changes) {
		SyncChange *change = (SyncChange *)lfirst(lc);

		if (change->type == SYNC_CHANGE_INSERT) {
			ListCell *vc;
			bool first_col = true;

			if (!insert_started) {
				appendStringInfo(&insert_buf, "INSERT INTO %s VALUES ", batch->target_table);
				insert_started = true;
			} else {
				appendStringInfoString(&insert_buf, ", ");
			}

			appendStringInfoChar(&insert_buf, '(');
			foreach (vc, change->col_values) {
				char *val = (char *)lfirst(vc);
				char *quoted = quote_val(val);

				if (!first_col)
					appendStringInfoString(&insert_buf, ", ");
				appendStringInfoString(&insert_buf, quoted);
				first_col = false;

				if (val && quoted != val)
					pfree(quoted);
			}
			appendStringInfoChar(&insert_buf, ')');
		} else if (change->type == SYNC_CHANGE_DELETE) {
			/* Flush inserts first if any */
			if (insert_started) {
				if (SPI_execute(insert_buf.data, false, 0) < 0)
					elog(ERROR, "Failed to execute INSERT batch for %s", batch->target_table);
				resetStringInfo(&insert_buf);
				insert_started = false;
			}

			/* Execute DELETE - need column names from batch */
			if (batch->attnames == NIL) {
				elog(WARNING, "DELETE skipped: no column names available for %s", batch->target_table);
				continue;
			}

			{
				StringInfoData del_buf;
				initStringInfo(&del_buf);
				appendStringInfo(&del_buf, "DELETE FROM %s WHERE ", batch->target_table);

				/* Build WHERE clause using key columns if known, otherwise use all
				 * columns */
				if (batch->nkeyattrs > 0 && batch->keyattrs != NULL) {
					/* Use only key columns */
					for (int i = 0; i < batch->nkeyattrs; i++) {
						int attidx = batch->keyattrs[i];

						if (attidx >= list_length(change->key_values)) {
							elog(WARNING,
							     "Key attribute index %d out of bounds for key_values list "
							     "(len %d). Skipping DELETE/UPDATE key check.",
							     attidx, list_length(change->key_values));
							continue;
						}

						char *colname = (char *)list_nth(batch->attnames, attidx);
						char *val = (char *)list_nth(change->key_values, attidx);
						char *quoted = quote_val(val);

						if (i > 0)
							appendStringInfoString(&del_buf, " AND ");
						appendStringInfo(&del_buf, "%s = %s", colname, quoted);

						if (quoted != val && val != NULL)
							pfree(quoted);
					}
				} else {
					/* No key info - use all columns from key_values */
					ListCell *kc, *nc;
					bool first_key = true;

					forboth(kc, change->key_values, nc, batch->attnames) {
						char *val = (char *)lfirst(kc);
						char *colname = (char *)lfirst(nc);

						/* Skip NULL values in WHERE clause (use IS NULL) */
						if (val == NULL) {
							if (!first_key)
								appendStringInfoString(&del_buf, " AND ");
							appendStringInfo(&del_buf, "%s IS NULL", colname);
						} else {
							char *quoted = quote_val(val);
							if (!first_key)
								appendStringInfoString(&del_buf, " AND ");
							appendStringInfo(&del_buf, "%s = %s", colname, quoted);
							pfree(quoted);
						}
						first_key = false;
					}
				}

				/* Execute the DELETE */
				if (SPI_execute(del_buf.data, false, 0) < 0)
					elog(WARNING, "Failed to execute DELETE for %s", batch->target_table);

				pfree(del_buf.data);
			}
		}
	}

	/* Flush remaining inserts */
	if (insert_started) {
		if (SPI_execute(insert_buf.data, false, 0) < 0)
			elog(ERROR, "Failed to execute INSERT batch for %s", batch->target_table);
	}

	SPI_finish();
}

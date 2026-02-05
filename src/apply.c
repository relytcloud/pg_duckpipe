#include "pg_ducklake_sync.h"

#include "executor/spi.h"
#include "utils/builtins.h"

/*
 * Quote literal value for SQL using PostgreSQL's quote_literal_cstr.
 * Returns palloc'd string. Handles NULL values properly.
 */
static char *
quote_val(char *val) {
	if (val == NULL)
		return pstrdup("NULL");
	return quote_literal_cstr(val);
}

/*
 * Build a properly quoted table reference (schema.table).
 * Returns palloc'd string.
 */
static char *
quote_qualified_table(const char *schema, const char *table) {
	StringInfoData buf;
	initStringInfo(&buf);
	appendStringInfo(&buf, "%s.%s", quote_identifier(schema), quote_identifier(table));
	return buf.data;
}

/*
 * Parse schema.table from batch->target_table into separate parts.
 * Returns palloc'd strings via out parameters.
 */
static void
parse_target_table(const char *target_table, char **schema, char **table) {
	char *copy = pstrdup(target_table);
	char *dot = strchr(copy, '.');

	if (dot) {
		*dot = '\0';
		*schema = pstrdup(copy);
		*table = pstrdup(dot + 1);
	} else {
		*schema = pstrdup("public");
		*table = pstrdup(copy);
	}
	pfree(copy);
}

void
apply_batch(SyncBatch *batch) {
	StringInfoData insert_buf;
	bool insert_started = false;
	ListCell *lc;
	int ret;
	char *target_schema;
	char *target_table;
	char *quoted_target;

	if (!batch || batch->count == 0)
		return;

	if ((ret = SPI_connect()) != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed: %d", ret);

	/* Parse and quote the target table name once */
	parse_target_table(batch->target_table, &target_schema, &target_table);
	quoted_target = quote_qualified_table(target_schema, target_table);

	initStringInfo(&insert_buf);

	foreach (lc, batch->changes) {
		SyncChange *change = (SyncChange *)lfirst(lc);

		if (change->type == SYNC_CHANGE_INSERT) {
			ListCell *vc;
			bool first_col = true;

			if (!insert_started) {
				appendStringInfo(&insert_buf, "INSERT INTO %s VALUES ", quoted_target);
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

				pfree(quoted);
			}
			appendStringInfoChar(&insert_buf, ')');
		} else if (change->type == SYNC_CHANGE_DELETE) {
			/* Flush inserts first if any */
			if (insert_started) {
				ret = SPI_execute(insert_buf.data, false, 0);
				if (ret != SPI_OK_INSERT)
					elog(ERROR, "Failed to execute INSERT batch for %s (ret=%d)", batch->target_table, ret);
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
				appendStringInfo(&del_buf, "DELETE FROM %s WHERE ", quoted_target);

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

						if (i > 0)
							appendStringInfoString(&del_buf, " AND ");

						if (val == NULL) {
							appendStringInfo(&del_buf, "%s IS NULL", quote_identifier(colname));
						} else {
							char *quoted = quote_val(val);
							appendStringInfo(&del_buf, "%s = %s", quote_identifier(colname), quoted);
							pfree(quoted);
						}
					}
				} else {
					/* No key info - use all columns from key_values */
					ListCell *kc, *nc;
					bool first_key = true;

					forboth(kc, change->key_values, nc, batch->attnames) {
						char *val = (char *)lfirst(kc);
						char *colname = (char *)lfirst(nc);

						if (!first_key)
							appendStringInfoString(&del_buf, " AND ");

						/* Handle NULL values in WHERE clause with IS NULL */
						if (val == NULL) {
							appendStringInfo(&del_buf, "%s IS NULL", quote_identifier(colname));
						} else {
							char *quoted = quote_val(val);
							appendStringInfo(&del_buf, "%s = %s", quote_identifier(colname), quoted);
							pfree(quoted);
						}
						first_key = false;
					}
				}

				/* Execute the DELETE */
				ret = SPI_execute(del_buf.data, false, 0);
				if (ret != SPI_OK_DELETE)
					elog(WARNING, "Failed to execute DELETE for %s (ret=%d)", batch->target_table, ret);

				pfree(del_buf.data);
			}
		}
	}

	/* Flush remaining inserts */
	if (insert_started) {
		ret = SPI_execute(insert_buf.data, false, 0);
		if (ret != SPI_OK_INSERT)
			elog(ERROR, "Failed to execute INSERT batch for %s (ret=%d)", batch->target_table, ret);
	}

	pfree(insert_buf.data);
	pfree(quoted_target);
	pfree(target_schema);
	pfree(target_table);

	SPI_finish();
}

#ifndef PG_DUCKPIPE_H
#define PG_DUCKPIPE_H

#include "postgres.h"

#include <string.h>

#include "access/xlogdefs.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "postmaster/bgworker.h"
#include "replication/logicalproto.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/wait_event.h"

/* GUC variables */
extern int duckpipe_poll_interval;
extern int duckpipe_batch_size_per_table;
extern int duckpipe_batch_size_per_group;
extern bool duckpipe_enabled;

/* Sync state machine: PENDING → SNAPSHOT → CATCHUP → STREAMING */
typedef enum SyncState { SYNC_STATE_PENDING, SYNC_STATE_SNAPSHOT, SYNC_STATE_CATCHUP, SYNC_STATE_STREAMING } SyncState;

static inline const char *
sync_state_to_string(SyncState state) {
	switch (state) {
	case SYNC_STATE_PENDING:
		return "PENDING";
	case SYNC_STATE_SNAPSHOT:
		return "SNAPSHOT";
	case SYNC_STATE_CATCHUP:
		return "CATCHUP";
	case SYNC_STATE_STREAMING:
		return "STREAMING";
	}
	return "UNKNOWN";
}

static inline SyncState
sync_state_from_string(const char *str) {
	if (strcmp(str, "PENDING") == 0)
		return SYNC_STATE_PENDING;
	if (strcmp(str, "SNAPSHOT") == 0)
		return SYNC_STATE_SNAPSHOT;
	if (strcmp(str, "CATCHUP") == 0)
		return SYNC_STATE_CATCHUP;
	if (strcmp(str, "STREAMING") == 0)
		return SYNC_STATE_STREAMING;
	elog(ERROR, "unknown sync state: %s", str);
	return SYNC_STATE_PENDING; /* unreachable */
}

/* Data Structures */
typedef struct SyncGroup {
	int id;
	char *name;
	char *publication;
	char *slot_name;
	XLogRecPtr pending_lsn;
} SyncGroup;

typedef struct TableMapping {
	int id;
	int group_id;
	char *source_schema;
	char *source_table;
	char *target_schema;
	char *target_table;
	SyncState state;
	XLogRecPtr snapshot_lsn;
	bool enabled;
} TableMapping;

/* We need to cache Relation info from 'R' messages to know column names/types
 */
typedef struct RelationCacheEntry {
	LogicalRepRelId remote_relid; /* Key */
	LogicalRepRelation *rel;      /* Deep copied */
	TableMapping *mapping;        /* Cached mapping, avoids SPI per message */
} RelationCacheEntry;

typedef enum SyncChangeType { SYNC_CHANGE_INSERT, SYNC_CHANGE_UPDATE, SYNC_CHANGE_DELETE } SyncChangeType;

typedef struct SyncChange {
	SyncChangeType type;
	XLogRecPtr lsn;
	/* We store stringified values for simplicity in V1 (constructing SQL)
	   In a real production version we might store Datums, but that requires more
	   type info. For now, let's store the raw LogicalRepTupleData but we MUST
	   deep copy it because the buffer is transient. Actually, deep copying
	   LogicalRepTupleData is hard. Alternative: Convert to SQL strings
	   immediately? No, we want to batch. Let's store a custom structure: List of
	   strings (values).
	*/
	List *col_values; /* List of char* (values for INSERT/UPDATE) */
	List *key_values; /* List of char* (PK values for UPDATE/DELETE) */
} SyncChange;

typedef struct SyncBatch {
	char target_table[NAMEDATALEN * 2 + 2]; /* Key for hash table (schema.table.\0) */
	int mapping_id;                         /* table_mappings.id for metric updates */
	List *changes;                          /* List of SyncChange* */
	int count;
	XLogRecPtr last_lsn;
	List *attnames; /* Column names from LogicalRepRelation */
	int nkeyattrs;  /* Number of key attributes */
	int *keyattrs;  /* Array of key attribute indices (0-based) */
} SyncBatch;

/* Memory context for sync operations - survives SPI_commit */
extern MemoryContext SyncMemoryContext;

/* Pre-copied WAL message that survives SPI_commit */
typedef struct WalMessage {
	XLogRecPtr lsn;
	char *data;
	int len;
} WalMessage;

/* Worker functions */
extern PGDLLEXPORT void duckpipe_worker_main(Datum main_arg);
extern int process_sync_group(SyncGroup *group);

/* Decoder functions - returns true when COMMIT message processed (batches flushed) */
extern bool decode_message(StringInfo buf, XLogRecPtr lsn, SyncGroup *group, HTAB *batches, HTAB *rel_cache);

/* Batch functions */
extern void batch_add_change(HTAB *batches, TableMapping *mapping, SyncChange *change, LogicalRepRelation *rel);
extern void flush_all_batches(HTAB *batches);

/* Apply functions */
extern void apply_batch(SyncBatch *batch);

/* Helper functions */
extern List *get_enabled_sync_groups(void);
extern TableMapping *get_table_mapping(SyncGroup *group, char *schemaname, char *relname);

#endif /* PG_DUCKPIPE_H */

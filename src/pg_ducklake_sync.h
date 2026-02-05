#ifndef PG_DUCKLAKE_SYNC_H
#define PG_DUCKLAKE_SYNC_H

#include "postgres.h"

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
extern int ducklake_sync_poll_interval;
extern int ducklake_sync_batch_size_per_table;
extern int ducklake_sync_batch_size_per_group;
extern bool ducklake_sync_enabled;

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
	char *state; /* PENDING, SNAPSHOT, CATCHUP, STREAMING */
	XLogRecPtr snapshot_lsn;
	bool enabled;
} TableMapping;

/* We need to cache Relation info from 'R' messages to know column names/types
 */
typedef struct RelationCacheEntry {
	LogicalRepRelId remote_relid; /* Key */
	LogicalRepRelation *rel;      /* Deep copied */
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
	char target_table[NAMEDATALEN * 2]; /* Key for hash table (schema.table) */
	List *changes;                      /* List of SyncChange* */
	int count;
	XLogRecPtr last_lsn;
	List *attnames; /* Column names from LogicalRepRelation */
	int nkeyattrs;  /* Number of key attributes */
	int *keyattrs;  /* Array of key attribute indices (0-based) */
} SyncBatch;

/* Worker functions */
extern PGDLLEXPORT void ducklake_sync_worker_main(Datum main_arg);
extern int process_sync_group(SyncGroup *group);

/* Decoder functions */
extern void decode_message(StringInfo buf, XLogRecPtr lsn, SyncGroup *group, HTAB *batches, HTAB *rel_cache);

/* Batch functions */
extern void batch_add_change(HTAB *batches, TableMapping *mapping, SyncChange *change, LogicalRepRelation *rel);
extern void flush_all_batches(HTAB *batches);

/* Apply functions */
extern void apply_batch(SyncBatch *batch);

/* Helper functions */
extern List *get_enabled_sync_groups(void);
extern TableMapping *get_table_mapping(SyncGroup *group, char *schemaname, char *relname);

#endif /* PG_DUCKLAKE_SYNC_H */

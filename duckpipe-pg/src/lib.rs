mod api;
mod worker;

use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::lwlock::PgLwLock;
use pgrx::pg_shmem_init;
use pgrx::prelude::*;
use pgrx::shmem::*;

pg_module_magic!();
extension_sql_file!("./sql/bootstrap.sql", bootstrap);

// ---------------------------------------------------------------------------
// Shared memory metrics
// ---------------------------------------------------------------------------

const MAX_METRICS_TABLES: usize = 128;
const MAX_METRICS_GROUPS: usize = 8;

#[derive(Copy, Clone)]
#[repr(C)]
pub struct TableMetricsSlot {
    pub mapping_id: i32, // 0 = unused slot
    pub queued_changes: i64,
    pub duckdb_memory_bytes: i64,
    pub flush_count: i64,
    pub flush_duration_ms: i64,
}

#[derive(Copy, Clone)]
#[repr(C)]
pub struct GroupMetricsSlot {
    pub group_id: i32, // 0 = unused slot
    pub total_queued_changes: i64,
    pub is_backpressured: i32, // 0/1
}

#[derive(Copy, Clone)]
#[repr(C)]
pub struct SharedMetrics {
    pub tables: [TableMetricsSlot; MAX_METRICS_TABLES],
    pub groups: [GroupMetricsSlot; MAX_METRICS_GROUPS],
}

impl Default for SharedMetrics {
    fn default() -> Self {
        SharedMetrics {
            tables: [TableMetricsSlot {
                mapping_id: 0,
                queued_changes: 0,
                duckdb_memory_bytes: 0,
                flush_count: 0,
                flush_duration_ms: 0,
            }; MAX_METRICS_TABLES],
            groups: [GroupMetricsSlot {
                group_id: 0,
                total_queued_changes: 0,
                is_backpressured: 0,
            }; MAX_METRICS_GROUPS],
        }
    }
}

unsafe impl PGRXSharedMemory for SharedMetrics {}

// SAFETY: PgLwLock::new is const and only initialises the lock name; no runtime invariants.
pub static METRICS_SHM: PgLwLock<SharedMetrics> = unsafe { PgLwLock::new(c"duckpipe_metrics") };

/// Update per-table metrics in shared memory.
pub fn update_shmem_table_metrics(
    mapping_id: i32,
    queued_changes: i64,
    duckdb_memory_bytes: i64,
    flush_count: i64,
    flush_duration_ms: i64,
) {
    let mut shm = METRICS_SHM.exclusive();
    // Find existing slot or first unused slot
    let mut free_idx: Option<usize> = None;
    for (i, slot) in shm.tables.iter_mut().enumerate() {
        if slot.mapping_id == mapping_id {
            slot.queued_changes = queued_changes;
            slot.duckdb_memory_bytes = duckdb_memory_bytes;
            slot.flush_count = flush_count;
            slot.flush_duration_ms = flush_duration_ms;
            return;
        }
        if slot.mapping_id == 0 && free_idx.is_none() {
            free_idx = Some(i);
        }
    }
    // Insert into free slot
    if let Some(idx) = free_idx {
        shm.tables[idx] = TableMetricsSlot {
            mapping_id,
            queued_changes,
            duckdb_memory_bytes,
            flush_count,
            flush_duration_ms,
        };
    }
}

/// Update per-group metrics in shared memory.
pub fn update_shmem_group_metrics(
    group_id: i32,
    total_queued_changes: i64,
    is_backpressured: bool,
) {
    let mut shm = METRICS_SHM.exclusive();
    let bp = if is_backpressured { 1 } else { 0 };
    for slot in shm.groups.iter_mut() {
        if slot.group_id == group_id {
            slot.total_queued_changes = total_queued_changes;
            slot.is_backpressured = bp;
            return;
        }
        if slot.group_id == 0 {
            *slot = GroupMetricsSlot {
                group_id,
                total_queued_changes,
                is_backpressured: bp,
            };
            return;
        }
    }
}

/// Read all active table metrics slots from shared memory.
pub fn read_shmem_table_metrics() -> Vec<(i32, i64, i64, i64, i64)> {
    let shm = METRICS_SHM.share();
    shm.tables
        .iter()
        .filter(|s| s.mapping_id != 0)
        .map(|s| {
            (
                s.mapping_id,
                s.queued_changes,
                s.duckdb_memory_bytes,
                s.flush_count,
                s.flush_duration_ms,
            )
        })
        .collect()
}

/// Read all active group metrics slots from shared memory.
pub fn read_shmem_group_metrics() -> Vec<(i32, i64, bool)> {
    let shm = METRICS_SHM.share();
    shm.groups
        .iter()
        .filter(|s| s.group_id != 0)
        .map(|s| (s.group_id, s.total_queued_changes, s.is_backpressured != 0))
        .collect()
}

// ---------------------------------------------------------------------------
// GUC variables
// ---------------------------------------------------------------------------

pub(crate) static POLL_INTERVAL: GucSetting<i32> = GucSetting::<i32>::new(1000);
pub(crate) static BATCH_SIZE_PER_GROUP: GucSetting<i32> = GucSetting::<i32>::new(100000);
pub(crate) static ENABLED: GucSetting<bool> = GucSetting::<bool>::new(true);
pub(crate) static DEBUG_LOG: GucSetting<bool> = GucSetting::<bool>::new(false);
pub(crate) static DATA_INLINING_ROW_LIMIT: GucSetting<i32> = GucSetting::<i32>::new(0);
pub(crate) static FLUSH_INTERVAL: GucSetting<i32> = GucSetting::<i32>::new(1000);
pub(crate) static FLUSH_BATCH_THRESHOLD: GucSetting<i32> = GucSetting::<i32>::new(10000);
pub(crate) static MAX_QUEUED_CHANGES: GucSetting<i32> = GucSetting::<i32>::new(500000);

#[pg_guard]
extern "C-unwind" fn _PG_init() {
    pg_shmem_init!(METRICS_SHM);

    GucRegistry::define_int_guc(
        c"duckpipe.poll_interval",
        c"Interval in milliseconds between polls",
        c"Interval in milliseconds between polls",
        &POLL_INTERVAL,
        100,
        3600000,
        GucContext::Sighup,
        GucFlags::UNIT_MS,
    );

    GucRegistry::define_int_guc(
        c"duckpipe.batch_size_per_group",
        c"Maximum WAL messages per group per sync cycle",
        c"Maximum WAL messages per group per sync cycle",
        &BATCH_SIZE_PER_GROUP,
        100,
        10000000,
        GucContext::Sighup,
        GucFlags::empty(),
    );

    GucRegistry::define_bool_guc(
        c"duckpipe.enabled",
        c"Enable pg_duckpipe background worker",
        c"Enable pg_duckpipe background worker",
        &ENABLED,
        GucContext::Sighup,
        GucFlags::empty(),
    );

    GucRegistry::define_bool_guc(
        c"duckpipe.debug_log",
        c"Emit critical-path timing logs for pg_duckpipe",
        c"Emit critical-path timing logs for pg_duckpipe",
        &DEBUG_LOG,
        GucContext::Sighup,
        GucFlags::empty(),
    );

    GucRegistry::define_int_guc(
        c"duckpipe.data_inlining_row_limit",
        c"DuckLake data inlining row limit (0 = disabled)",
        c"DuckLake data inlining row limit (0 = disabled)",
        &DATA_INLINING_ROW_LIMIT,
        0,
        1000000,
        GucContext::Userset,
        GucFlags::empty(),
    );

    GucRegistry::define_int_guc(
        c"duckpipe.flush_interval",
        c"Flush interval in milliseconds for self-triggered flush",
        c"Flush interval in milliseconds for self-triggered flush",
        &FLUSH_INTERVAL,
        100,
        60000,
        GucContext::Sighup,
        GucFlags::UNIT_MS,
    );

    GucRegistry::define_int_guc(
        c"duckpipe.flush_batch_threshold",
        c"Number of queued changes that triggers an immediate flush",
        c"Number of queued changes that triggers an immediate flush",
        &FLUSH_BATCH_THRESHOLD,
        100,
        1000000,
        GucContext::Sighup,
        GucFlags::empty(),
    );

    GucRegistry::define_int_guc(
        c"duckpipe.max_queued_changes",
        c"Maximum total queued changes before backpressure pauses WAL consumption",
        c"Maximum total queued changes before backpressure pauses WAL consumption",
        &MAX_QUEUED_CHANGES,
        1000,
        10000000,
        GucContext::Sighup,
        GucFlags::empty(),
    );
}

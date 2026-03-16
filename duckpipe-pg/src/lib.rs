mod api;
mod worker;

use std::sync::atomic::{AtomicBool, Ordering};

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

const MAX_METRICS_TABLES: usize = 1024;
const MAX_METRICS_GROUPS: usize = 64;

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

/// Set to `true` during `_PG_init` only when loaded via `shared_preload_libraries`.
/// pg_shmem_init! registers the lock for initialization in the shmem_startup_hook,
/// which only fires during postmaster startup.  If the extension is loaded later
/// (e.g. via CREATE EXTENSION without being in shared_preload_libraries), the hook
/// has already run and the PgLwLock is never initialised — accessing it panics.
/// This flag lets read/write helpers gracefully degrade instead of crashing.
static SHM_AVAILABLE: AtomicBool = AtomicBool::new(false);

fn shmem_available() -> bool {
    SHM_AVAILABLE.load(Ordering::Relaxed)
}

/// Write all metrics to shared memory in a single lock acquisition.
///
/// `group`: `(group_id, total_queued_changes, is_backpressured)`
/// `tables`: `Vec<(mapping_id, queued_changes, duckdb_memory_bytes, flush_count, flush_duration_ms)>`
pub fn write_shmem_metrics(group: (i32, i64, bool), tables: &[(i32, i64, i64, i64, i64)]) {
    if !shmem_available() {
        return;
    }
    let mut shm = METRICS_SHM.exclusive();

    // Group metrics
    let (group_id, total_queued, backpressured) = group;
    let bp = if backpressured { 1 } else { 0 };
    let mut found_group = false;
    for slot in shm.groups.iter_mut() {
        if slot.group_id == group_id {
            slot.total_queued_changes = total_queued;
            slot.is_backpressured = bp;
            found_group = true;
            break;
        }
        if slot.group_id == 0 {
            *slot = GroupMetricsSlot {
                group_id,
                total_queued_changes: total_queued,
                is_backpressured: bp,
            };
            found_group = true;
            break;
        }
    }
    if !found_group {
        tracing::warn!("duckpipe: SHM group slots full ({MAX_METRICS_GROUPS}), metrics for group_id={group_id} dropped");
    }

    // Per-table metrics
    for &(mapping_id, queued_changes, duckdb_memory_bytes, flush_count, flush_duration_ms) in tables
    {
        let mut free_idx: Option<usize> = None;
        let mut found = false;
        for (i, slot) in shm.tables.iter_mut().enumerate() {
            if slot.mapping_id == mapping_id {
                slot.queued_changes = queued_changes;
                slot.duckdb_memory_bytes = duckdb_memory_bytes;
                slot.flush_count = flush_count;
                slot.flush_duration_ms = flush_duration_ms;
                found = true;
                break;
            }
            if slot.mapping_id == 0 && free_idx.is_none() {
                free_idx = Some(i);
            }
        }
        if !found {
            if let Some(idx) = free_idx {
                shm.tables[idx] = TableMetricsSlot {
                    mapping_id,
                    queued_changes,
                    duckdb_memory_bytes,
                    flush_count,
                    flush_duration_ms,
                };
            } else {
                tracing::warn!("duckpipe: SHM table slots full ({MAX_METRICS_TABLES}), metrics for mapping_id={mapping_id} dropped");
            }
        }
    }
}

/// Clear a table slot from shared memory (called on table removal).
pub fn clear_shmem_table_slot(mapping_id: i32) {
    if !shmem_available() {
        return;
    }
    let mut shm = METRICS_SHM.exclusive();
    for slot in shm.tables.iter_mut() {
        if slot.mapping_id == mapping_id {
            *slot = TableMetricsSlot {
                mapping_id: 0,
                queued_changes: 0,
                duckdb_memory_bytes: 0,
                flush_count: 0,
                flush_duration_ms: 0,
            };
            return;
        }
    }
}

/// Clear a group slot from shared memory (called on group drop).
pub fn clear_shmem_group_slot(group_id: i32) {
    if !shmem_available() {
        return;
    }
    let mut shm = METRICS_SHM.exclusive();
    for slot in shm.groups.iter_mut() {
        if slot.group_id == group_id {
            *slot = GroupMetricsSlot {
                group_id: 0,
                total_queued_changes: 0,
                is_backpressured: 0,
            };
            return;
        }
    }
}

/// Read all active table metrics slots from shared memory as a HashMap.
/// Returns an empty map when SHM is not available (extension not in shared_preload_libraries).
pub fn read_shmem_table_metrics() -> std::collections::HashMap<i32, (i64, i64, i64, i64)> {
    if !shmem_available() {
        pgrx::notice!(
            "pg_duckpipe: shared-memory metrics unavailable \
             (not in shared_preload_libraries); queued_changes will show 0"
        );
        return std::collections::HashMap::new();
    }
    let shm = METRICS_SHM.share();
    shm.tables
        .iter()
        .filter(|s| s.mapping_id != 0)
        .map(|s| {
            (
                s.mapping_id,
                (
                    s.queued_changes,
                    s.duckdb_memory_bytes,
                    s.flush_count,
                    s.flush_duration_ms,
                ),
            )
        })
        .collect()
}

/// Read all active group metrics slots from shared memory as a HashMap.
/// Returns an empty map when SHM is not available (extension not in shared_preload_libraries).
pub fn read_shmem_group_metrics() -> std::collections::HashMap<i32, (i64, bool)> {
    if !shmem_available() {
        pgrx::notice!(
            "pg_duckpipe: shared-memory metrics unavailable \
             (not in shared_preload_libraries); worker metrics will show defaults"
        );
        return std::collections::HashMap::new();
    }
    let shm = METRICS_SHM.share();
    shm.groups
        .iter()
        .filter(|s| s.group_id != 0)
        .map(|s| {
            (
                s.group_id,
                (s.total_queued_changes, s.is_backpressured != 0),
            )
        })
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
pub(crate) static FLUSH_INTERVAL: GucSetting<i32> = GucSetting::<i32>::new(5000);
pub(crate) static FLUSH_BATCH_THRESHOLD: GucSetting<i32> = GucSetting::<i32>::new(10000);
pub(crate) static MAX_QUEUED_CHANGES: GucSetting<i32> = GucSetting::<i32>::new(500000);
pub(crate) static DUCKDB_BUFFER_MEMORY_MB: GucSetting<i32> = GucSetting::<i32>::new(16);
pub(crate) static DUCKDB_FLUSH_MEMORY_MB: GucSetting<i32> = GucSetting::<i32>::new(512);
pub(crate) static MAX_CONCURRENT_FLUSHES: GucSetting<i32> = GucSetting::<i32>::new(4);

#[pg_guard]
extern "C-unwind" fn _PG_init() {
    // pg_shmem_init! registers the lock for the shmem_startup_hook which only
    // fires during postmaster startup — i.e. when loaded via shared_preload_libraries.
    pg_shmem_init!(METRICS_SHM);

    // SAFETY: reading a global bool set by PostgreSQL during shared_preload_libraries processing.
    let in_spl = unsafe { pg_sys::process_shared_preload_libraries_in_progress };
    if in_spl {
        SHM_AVAILABLE.store(true, Ordering::Relaxed);
    } else {
        pgrx::warning!(
            "pg_duckpipe is not in shared_preload_libraries — \
             shared-memory metrics are unavailable.  \
             Add pg_duckpipe to shared_preload_libraries and restart PostgreSQL."
        );
    }

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
        i32::MAX,
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

    GucRegistry::define_int_guc(
        c"duckpipe.duckdb_buffer_memory_mb",
        c"DuckDB memory limit in MB during buffer accumulation (low limit for spilling)",
        c"DuckDB memory limit in MB during buffer accumulation (low limit for spilling)",
        &DUCKDB_BUFFER_MEMORY_MB,
        1,
        4096,
        GucContext::Sighup,
        GucFlags::empty(),
    );

    GucRegistry::define_int_guc(
        c"duckpipe.duckdb_flush_memory_mb",
        c"DuckDB memory limit in MB during flush/compaction (high limit for performance)",
        c"DuckDB memory limit in MB during flush/compaction (high limit for performance)",
        &DUCKDB_FLUSH_MEMORY_MB,
        16,
        65536,
        GucContext::Sighup,
        GucFlags::empty(),
    );

    GucRegistry::define_int_guc(
        c"duckpipe.max_concurrent_flushes",
        c"Maximum concurrent flush operations per sync group",
        c"Maximum concurrent flush operations per sync group",
        &MAX_CONCURRENT_FLUSHES,
        1,
        1000,
        GucContext::Sighup,
        GucFlags::empty(),
    );
}

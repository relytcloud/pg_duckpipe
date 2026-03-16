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
    pub avg_row_bytes: i64,
}

#[derive(Copy, Clone)]
#[repr(C)]
pub struct GroupMetricsSlot {
    pub group_id: i32, // 0 = unused slot
    pub total_queued_changes: i64,
    pub is_backpressured: i32, // 0/1
    pub active_flushes: i32,
    pub gate_wait_avg_ms: i64,
    pub gate_timeouts: i64,
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
                avg_row_bytes: 0,
            }; MAX_METRICS_TABLES],
            groups: [GroupMetricsSlot {
                group_id: 0,
                total_queued_changes: 0,
                is_backpressured: 0,
                active_flushes: 0,
                gate_wait_avg_ms: 0,
                gate_timeouts: 0,
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

pub use duckpipe_core::flush_coordinator::{GroupMetrics, TableMetrics};

/// Write all metrics to shared memory in a single lock acquisition.
pub fn write_shmem_metrics(group: &GroupMetrics, tables: &[TableMetrics]) {
    if !shmem_available() {
        return;
    }
    let mut shm = METRICS_SHM.exclusive();

    // Group metrics
    let bp = if group.is_backpressured { 1 } else { 0 };
    let mut found_group = false;
    for slot in shm.groups.iter_mut() {
        if slot.group_id == group.group_id {
            slot.total_queued_changes = group.total_queued_changes;
            slot.is_backpressured = bp;
            slot.active_flushes = group.active_flushes;
            slot.gate_wait_avg_ms = group.gate_wait_avg_ms;
            slot.gate_timeouts = group.gate_timeouts;
            found_group = true;
            break;
        }
        if slot.group_id == 0 {
            *slot = GroupMetricsSlot {
                group_id: group.group_id,
                total_queued_changes: group.total_queued_changes,
                is_backpressured: bp,
                active_flushes: group.active_flushes,
                gate_wait_avg_ms: group.gate_wait_avg_ms,
                gate_timeouts: group.gate_timeouts,
            };
            found_group = true;
            break;
        }
    }
    if !found_group {
        tracing::warn!("duckpipe: SHM group slots full ({MAX_METRICS_GROUPS}), metrics for group_id={} dropped", group.group_id);
    }

    // Per-table metrics
    for t in tables {
        let mut free_idx: Option<usize> = None;
        let mut found = false;
        for (i, slot) in shm.tables.iter_mut().enumerate() {
            if slot.mapping_id == t.mapping_id {
                slot.queued_changes = t.queued_changes;
                slot.duckdb_memory_bytes = t.duckdb_memory_bytes;
                slot.flush_count = t.flush_count;
                slot.flush_duration_ms = t.flush_duration_ms;
                slot.avg_row_bytes = t.avg_row_bytes;
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
                    mapping_id: t.mapping_id,
                    queued_changes: t.queued_changes,
                    duckdb_memory_bytes: t.duckdb_memory_bytes,
                    flush_count: t.flush_count,
                    flush_duration_ms: t.flush_duration_ms,
                    avg_row_bytes: t.avg_row_bytes,
                };
            } else {
                tracing::warn!("duckpipe: SHM table slots full ({MAX_METRICS_TABLES}), metrics for mapping_id={} dropped", t.mapping_id);
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
                avg_row_bytes: 0,
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
                active_flushes: 0,
                gate_wait_avg_ms: 0,
                gate_timeouts: 0,
            };
            return;
        }
    }
}

/// Read all active table metrics slots from shared memory as a HashMap.
/// Returns an empty map when SHM is not available (extension not in shared_preload_libraries).
pub fn read_shmem_table_metrics() -> std::collections::HashMap<i32, TableMetrics> {
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
                TableMetrics {
                    mapping_id: s.mapping_id,
                    queued_changes: s.queued_changes,
                    duckdb_memory_bytes: s.duckdb_memory_bytes,
                    flush_count: s.flush_count,
                    flush_duration_ms: s.flush_duration_ms,
                    avg_row_bytes: s.avg_row_bytes,
                },
            )
        })
        .collect()
}

/// Read all active group metrics slots from shared memory as a HashMap keyed by group_id.
pub fn read_shmem_group_metrics() -> std::collections::HashMap<i32, GroupMetrics> {
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
                GroupMetrics {
                    group_id: s.group_id,
                    total_queued_changes: s.total_queued_changes,
                    is_backpressured: s.is_backpressured != 0,
                    active_flushes: s.active_flushes,
                    gate_wait_avg_ms: s.gate_wait_avg_ms,
                    gate_timeouts: s.gate_timeouts,
                },
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
}

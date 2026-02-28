mod api;
mod worker;

use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::prelude::*;

pg_module_magic!();
extension_sql_file!("./sql/bootstrap.sql", bootstrap);

// GUC variables
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

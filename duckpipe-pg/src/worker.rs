use pgrx::pg_sys::panic::CaughtError;
use pgrx::prelude::*;
use std::ffi::CString;
use std::sync::Arc;

use tokio::sync::Notify;

use duckpipe_core::flush_coordinator::FlushCoordinator;
use duckpipe_core::listen;
use duckpipe_core::service::{self, ServiceConfig, SlotConnectParams, SlotState};
use duckpipe_core::snapshot_manager::SnapshotManager;

use crate::{
    BATCH_SIZE_PER_GROUP, DEBUG_LOG, ENABLED, FLUSH_BATCH_THRESHOLD, FLUSH_INTERVAL,
    MAX_QUEUED_CHANGES, POLL_INTERVAL,
};

/// Check if shutdown has been requested.
fn should_shutdown() -> bool {
    unsafe { std::ptr::read_volatile(std::ptr::addr_of!(pg_sys::ShutdownRequestPending)) != 0 }
}

/// Read current GUC config.
fn read_config(connstr: &str, duckdb_pg_connstr: &str) -> ServiceConfig {
    ServiceConfig {
        poll_interval_ms: POLL_INTERVAL.get(),
        batch_size_per_group: BATCH_SIZE_PER_GROUP.get(),
        debug_log: DEBUG_LOG.get(),
        connstr: connstr.to_string(),
        duckdb_pg_connstr: duckdb_pg_connstr.to_string(),
        ducklake_schema: "ducklake".to_string(),
        flush_interval_ms: FLUSH_INTERVAL.get(),
        flush_batch_threshold: FLUSH_BATCH_THRESHOLD.get(),
        max_queued_changes: MAX_QUEUED_CHANGES.get(),
    }
}

/// Background worker entry point — one instance per sync group.
///
/// The group name is passed via `bgw_extra` (set by `launch_worker` in api.rs).
#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn duckpipe_worker_main(arg: pg_sys::Datum) {
    use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};

    let dboid = pg_sys::Oid::from(arg.value() as u32);

    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    unsafe {
        pg_sys::BackgroundWorkerInitializeConnectionByOid(dboid, pg_sys::InvalidOid, 0);
    }

    // Read group name from bgw_extra (NUL-terminated C string packed by launch_worker).
    let group_name: String = {
        let extra = pgrx::bgworkers::BackgroundWorker::get_extra();
        let extra_bytes = extra.as_bytes();
        // Find NUL or use full length
        let len = extra_bytes
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(extra_bytes.len());
        String::from_utf8_lossy(&extra_bytes[..len]).to_string()
    };

    // Set application_name so the worker is identifiable in pg_stat_activity.
    unsafe {
        let app_name = format!("pg_duckpipe:{}", group_name);
        let c_app = CString::new(app_name).unwrap();
        let c_key = CString::new("application_name").unwrap();
        pg_sys::SetConfigOption(
            c_key.as_ptr(),
            c_app.as_ptr(),
            pg_sys::GucContext::PGC_USERSET,
            pg_sys::GucSource::PGC_S_SESSION,
        );
    }

    // Get database name and connection info (need a transaction context for catalog access)
    let (db, port, socket_dir) = unsafe {
        pg_sys::StartTransactionCommand();
        let name = pg_sys::get_database_name(dboid);
        let dbname = if name.is_null() {
            pg_sys::CommitTransactionCommand();
            ereport!(
                FATAL,
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                format!("database with OID {} does not exist", dboid)
            );
        } else {
            std::ffi::CStr::from_ptr(name).to_string_lossy().to_string()
        };

        // Read port and socket dir while still in transaction
        let port_cstr = CString::new("port").unwrap();
        let port_ptr =
            pg_sys::GetConfigOptionByName(port_cstr.as_ptr(), std::ptr::null_mut(), false);
        let port = if port_ptr.is_null() {
            "5432".to_string()
        } else {
            std::ffi::CStr::from_ptr(port_ptr)
                .to_string_lossy()
                .to_string()
        };

        let sock_cstr = CString::new("unix_socket_directories").unwrap();
        let sock_ptr =
            pg_sys::GetConfigOptionByName(sock_cstr.as_ptr(), std::ptr::null_mut(), false);
        let socket_dir = if sock_ptr.is_null() {
            "/tmp".to_string()
        } else {
            let dirs = std::ffi::CStr::from_ptr(sock_ptr)
                .to_string_lossy()
                .to_string();
            dirs.split(',').next().unwrap_or("/tmp").trim().to_string()
        };

        pg_sys::CommitTransactionCommand();

        log!(
            "pg_duckpipe worker started for database '{}', group '{}'",
            dbname,
            group_name
        );

        (dbname, port, socket_dir)
    };

    // Initialize tracing — stderr output is captured by PostgreSQL's log collector.
    duckpipe_core::log::init_subscriber(DEBUG_LOG.get());

    // Build tokio runtime (single-threaded — bgworker safety)
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to create tokio runtime");

    // Connection string is constant (socket_dir, port, db don't change)
    let connstr = format!("host={} port={} dbname={}", socket_dir, port, db);

    // Precompute slot connection params (Unix socket for bgworker)
    let port_num: u16 = port.parse().unwrap_or(5432);
    let os_user = std::env::var("USER")
        .or_else(|_| std::env::var("PGUSER"))
        .unwrap_or_else(|_| "postgres".to_string());
    let slot_params = SlotConnectParams::Unix {
        socket_dir: socket_dir.clone(),
        port: port_num,
        user: os_user.clone(),
        dbname: db.clone(),
    };

    // DuckDB's postgres_scanner speaks TCP, not unix sockets.
    let duckdb_pg_connstr = format!(
        "host=127.0.0.1 port={} dbname={} user={}",
        port, db, os_user
    );

    // Create persistent flush coordinator (survives across cycles, cleared on panic)
    let config = read_config(&connstr, &duckdb_pg_connstr);
    let mut coordinator = FlushCoordinator::new(
        connstr.clone(),
        "ducklake".to_string(),
        config.flush_batch_threshold,
        config.flush_interval_ms,
        config.max_queued_changes,
    );
    let mut snapshot_manager = SnapshotManager::new();

    // Per-group LISTEN channel
    let listen_channel = listen::wakeup_channel(&group_name);

    // Main worker loop — wraps block_on in catch_unwind for panic recovery.
    loop {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let coord = &mut coordinator;
            let snap_mgr = &mut snapshot_manager;
            rt.block_on(async {
                let mut consumer: Option<SlotState> = None;

                // Shared wakeup signal — notified by the LISTEN task when
                // add_table / resync_table / enable_group fires NOTIFY.
                let wakeup = Arc::new(Notify::new());

                // Spawn the LISTEN helper on the per-group channel.
                let listen_connstr =
                    format!("{} user={} connect_timeout=5", connstr, os_user);
                let mut listen_handle: Option<tokio::task::JoinHandle<()>> =
                    match listen::spawn_listen_task(
                        &listen_connstr,
                        &listen_channel,
                        Arc::clone(&wakeup),
                    )
                    .await
                    {
                        Ok(h) => {
                            log!(
                                "pg_duckpipe LISTEN task started on channel '{}' ({})",
                                listen_channel,
                                listen_connstr
                            );
                            Some(h)
                        }
                        Err(e) => {
                            log!(
                                "pg_duckpipe failed to start LISTEN task: {} (connstr: {})",
                                e,
                                listen_connstr
                            );
                            None
                        }
                    };

                while !should_shutdown() {
                    if BackgroundWorker::sighup_received() {
                        unsafe {
                            pg_sys::ProcessConfigFile(pg_sys::GucContext::PGC_SIGHUP);
                        }
                    }

                    // Auto-respawn the LISTEN task if it was running but exited
                    if matches!(&listen_handle, Some(h) if h.is_finished()) {
                        match listen::spawn_listen_task(
                            &listen_connstr,
                            &listen_channel,
                            Arc::clone(&wakeup),
                        )
                        .await
                        {
                            Ok(h) => listen_handle = Some(h),
                            Err(e) => {
                                tracing::warn!("failed to respawn LISTEN task: {e}");
                                listen_handle = None;
                            }
                        }
                    }

                    if !ENABLED.get() {
                        tokio::time::sleep(std::time::Duration::from_millis(
                            POLL_INTERVAL.get() as u64,
                        ))
                        .await;
                        continue;
                    }

                    let config = read_config(&connstr, &duckdb_pg_connstr);

                    match service::run_group_sync_cycle(
                        &config,
                        &group_name,
                        coord,
                        &slot_params,
                        &mut consumer,
                        snap_mgr,
                    )
                    .await
                    {
                        Ok(any_work) => {
                            if should_shutdown() {
                                break;
                            }
                            if !any_work {
                                let poll_dur = std::time::Duration::from_millis(
                                    POLL_INTERVAL.get() as u64,
                                );
                                let reason = tokio::select! {
                                    _ = tokio::time::sleep(poll_dur) => "timeout",
                                    _ = snap_mgr.snapshot_notify().notified(), if snap_mgr.has_in_flight() => "snapshot",
                                    _ = wakeup.notified() => "notify",
                                };
                                log!(
                                    "pg_duckpipe [{}] woke: reason={}, poll_interval={}ms",
                                    group_name,
                                    reason,
                                    POLL_INTERVAL.get()
                                );
                            }
                        }
                        Err(msg) => {
                            tracing::error!(
                                "pg_duckpipe [{}] worker error: {}",
                                group_name,
                                msg
                            );
                            consumer = None;
                            if should_shutdown() {
                                break;
                            }
                            let poll_dur = std::time::Duration::from_millis(
                                POLL_INTERVAL.get() as u64,
                            );
                            tokio::select! {
                                _ = tokio::time::sleep(poll_dur) => {}
                                _ = wakeup.notified() => {}
                            }
                        }
                    }
                }

                // Shutdown: abort the LISTEN task.
                if let Some(h) = listen_handle {
                    h.abort();
                }
            })
        }));

        match result {
            Ok(()) => {
                // Normal shutdown (should_shutdown() was true)
                break;
            }
            Err(err) => {
                // Panic recovery: clear all persistent state
                coordinator.clear();
                snapshot_manager.clear();
                let msg = if let Some(caught) = err.downcast_ref::<CaughtError>() {
                    match caught {
                        CaughtError::PostgresError(e) => format!("PG error: {}", e.message()),
                        CaughtError::ErrorReport(e) => format!("Error report: {}", e.message()),
                        CaughtError::RustPanic { ereport, .. } => {
                            format!("Rust panic: {}", ereport.message())
                        }
                    }
                } else if let Some(s) = err.downcast_ref::<String>() {
                    format!("panic: {}", s)
                } else if let Some(s) = err.downcast_ref::<&str>() {
                    format!("panic: {}", s)
                } else {
                    "unknown error".to_string()
                };
                tracing::error!("pg_duckpipe [{}] worker caught error: {}", group_name, msg);
                if should_shutdown() {
                    break;
                }
                BackgroundWorker::wait_latch(Some(std::time::Duration::from_millis(
                    POLL_INTERVAL.get() as u64,
                )));
            }
        }
    }

    coordinator.shutdown();
    log!("pg_duckpipe [{}] worker shutting down", group_name);
}

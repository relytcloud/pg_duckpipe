//! Standalone DuckPipe daemon — CDC sync engine outside PostgreSQL.
//!
//! Shares the same `duckpipe-core` logic as the PG background worker,
//! but runs as an independent process connecting over TCP.
//!
//! Each daemon instance handles exactly one sync group.  The group can be
//! specified at startup via `--group` (pre-bound) or bound later via the
//! REST API (`POST /groups`).

mod api;

use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tokio::signal;
use tracing::{error, info};

use duckpipe_core::connstr::to_slot_connect_params;
use duckpipe_core::flush_coordinator::FlushCoordinator;
use duckpipe_core::service::{self, ServiceConfig, SlotState};
use duckpipe_core::snapshot_manager::SnapshotManager;

/// DuckPipe — standalone CDC sync daemon for PostgreSQL → DuckLake.
#[derive(Parser, Debug)]
#[command(name = "duckpipe", version, about)]
struct Args {
    /// PostgreSQL connection string (libpq key=value format).
    /// Example: "host=localhost port=5432 dbname=mydb user=replicator password=secret"
    #[arg(long)]
    connstr: String,

    /// Poll interval in milliseconds (min 100).
    #[arg(long, default_value_t = 1000, value_parser = clap::value_parser!(i32).range(100..))]
    poll_interval: i32,

    /// Maximum WAL messages per group per sync cycle (min 100).
    #[arg(long, default_value_t = 100000, value_parser = clap::value_parser!(i32).range(100..))]
    batch_size_per_group: i32,

    /// DuckLake metadata schema name.
    #[arg(long, default_value = "ducklake")]
    ducklake_schema: String,

    /// Enable debug timing logs.
    #[arg(long, default_value_t = false)]
    debug: bool,

    /// Flush interval in milliseconds (min 100, default 1000).
    #[arg(long, default_value_t = 1000, value_parser = clap::value_parser!(i32).range(100..=60000))]
    flush_interval: i32,

    /// Number of queued changes that triggers an immediate flush (min 100, default 10000).
    #[arg(long, default_value_t = 10000, value_parser = clap::value_parser!(i32).range(100..=1000000))]
    flush_batch_threshold: i32,

    /// Maximum total queued changes before backpressure pauses WAL consumption (min 1000, default 500000).
    #[arg(long, default_value_t = 500000, value_parser = clap::value_parser!(i32).range(1000..=10000000))]
    max_queued_changes: i32,

    /// Sync group to process. If omitted, daemon starts unbound and waits
    /// for a group to be created via POST /groups.
    #[arg(long)]
    group: Option<String>,

    /// HTTP API port (0 to disable).
    #[arg(long, default_value_t = 8080)]
    api_port: u16,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    duckpipe_core::log::init_subscriber(args.debug);

    let connstr = args.connstr.clone();
    let state = Arc::new(api::AppState::new(connstr.clone()));

    // Pre-bind to a group if --group is specified
    if let Some(ref group_name) = args.group {
        info!("Pre-binding to group '{}'", group_name);
        match pre_bind_group(&state, group_name).await {
            Ok(()) => info!("Bound to group '{}'", group_name),
            Err(e) => {
                error!("Failed to bind to group '{}': {}", group_name, e);
                std::process::exit(1);
            }
        }
    }

    let config = ServiceConfig {
        poll_interval_ms: args.poll_interval,
        batch_size_per_group: args.batch_size_per_group,
        debug_log: args.debug,
        connstr: connstr.clone(),
        duckdb_pg_connstr: connstr.clone(),
        ducklake_schema: args.ducklake_schema.clone(),
        flush_interval_ms: args.flush_interval,
        flush_batch_threshold: args.flush_batch_threshold,
        max_queued_changes: args.max_queued_changes,
    };

    let slot_params = to_slot_connect_params(&connstr).unwrap_or_else(|e| {
        eprintln!("Invalid connection string: {}", e);
        std::process::exit(1);
    });

    // Start HTTP API server
    if args.api_port > 0 {
        let api_state = state.clone();
        let api_port = args.api_port;
        tokio::spawn(async move {
            let router = api::router(api_state);
            let addr = format!("127.0.0.1:{}", api_port);
            let listener = match tokio::net::TcpListener::bind(&addr).await {
                Ok(l) => l,
                Err(e) => {
                    error!("Failed to bind API server to {}: {}", addr, e);
                    std::process::exit(1);
                }
            };
            info!("REST API listening on http://{}", addr);
            if let Err(e) = axum::serve(listener, router).await {
                error!("API server error: {}", e);
            }
        });
    }

    info!(
        "DuckPipe daemon starting (connstr={}, group={}, poll={}ms, api_port={})",
        duckpipe_core::connstr::redact_password(&connstr),
        args.group.as_deref().unwrap_or("<unbound>"),
        args.poll_interval,
        args.api_port,
    );

    let poll_interval = Duration::from_millis(args.poll_interval as u64);

    // Sync loop
    run_sync_loop(&state, &config, &slot_params, poll_interval).await;
}

/// Pre-bind the daemon to a group at startup.
/// Ensures the group exists (auto-creates with mode=daemon if not) and acquires the advisory lock.
async fn pre_bind_group(state: &Arc<api::AppState>, group_name: &str) -> Result<(), String> {
    // Acquire advisory lock first
    let lock_conn = api::groups::acquire_advisory_lock(&state.connstr, group_name)
        .await
        .map_err(|e| match e {
            api::error::ApiError::LockConflict(_) => {
                format!(
                    "another daemon is already running for group '{}'",
                    group_name
                )
            }
            _ => format!("failed to acquire lock: {:?}", e),
        })?;

    // Auto-create group if it doesn't exist
    let client = api::pg_connect(&state.connstr)
        .await
        .map_err(|_| "failed to connect to PG".to_string())?;

    let exists = client
        .query_opt(
            "SELECT 1 FROM duckpipe.sync_groups WHERE name = $1",
            &[&group_name],
        )
        .await
        .map_err(|e| e.to_string())?;

    if exists.is_none() {
        info!(
            "Group '{}' does not exist, creating with mode=daemon",
            group_name
        );
        client
            .execute(
                "SELECT duckpipe.create_group($1, NULL, NULL, NULL, 'daemon')",
                &[&group_name],
            )
            .await
            .map_err(|e| e.to_string())?;
    } else {
        // Ensure the existing group is in daemon mode
        client
            .execute(
                "UPDATE duckpipe.sync_groups SET mode = 'daemon' WHERE name = $1",
                &[&group_name],
            )
            .await
            .map_err(|e| e.to_string())?;
    }

    // Set binding
    {
        let mut group = state.group.write().await;
        *group = Some(group_name.to_string());
    }
    {
        let mut lock = state.lock_conn.lock().await;
        *lock = Some(lock_conn);
    }

    Ok(())
}

/// Main sync loop — waits for group binding if unbound, then runs the sync cycle.
/// Uses an outer loop (not recursion) so unbind/rebind cycles don't grow the stack.
async fn run_sync_loop(
    state: &Arc<api::AppState>,
    config: &ServiceConfig,
    slot_params: &duckpipe_core::service::SlotConnectParams,
    poll_interval: Duration,
) {
    loop {
        // Wait for group binding if not already bound.
        // Register the Notified future BEFORE dropping the read lock to avoid
        // a lost-wakeup race with notify_one() in create_group.
        {
            let group = state.group.read().await;
            if group.is_none() {
                let notified = state.sync_start.notified();
                drop(group);
                info!("Daemon unbound — waiting for POST /groups to bind");
                notified.await;
                info!("Group bound via API — starting sync loop");
            }
        }

        let group_name = state.group.read().await.clone().unwrap();

        let mut coordinator = FlushCoordinator::new(
            config.connstr.clone(),
            config.ducklake_schema.clone(),
            group_name.clone(),
            config.flush_batch_threshold,
            config.flush_interval_ms,
            config.max_queued_changes,
        );
        let mut snapshot_manager = SnapshotManager::new();
        let mut consumer: Option<SlotState> = None;

        info!("Sync loop starting for group '{}'", group_name);

        loop {
            // Check if group has been unbound (e.g., by DELETE /groups)
            {
                let group = state.group.read().await;
                if group.is_none() {
                    info!("Group unbound — stopping sync loop");
                    coordinator.shutdown();
                    snapshot_manager.clear();
                    drop(consumer);
                    break; // Back to outer loop to wait for re-binding
                }
            }

            tokio::select! {
                _ = signal::ctrl_c() => {
                    info!("DuckPipe daemon shutting down (Ctrl+C)");
                    return; // Exit entirely
                }
                _ = state.sync_stop.notified() => {
                    info!("Sync loop received stop signal");
                    coordinator.shutdown();
                    snapshot_manager.clear();
                    drop(consumer);
                    break; // Back to outer loop
                }
                result = service::run_group_sync_cycle(config, &group_name, &mut coordinator, slot_params, &mut consumer, &mut snapshot_manager) => {
                    match result {
                        Ok(any_work) => {
                            if !any_work {
                                snapshot_manager.sleep_unless_snapshot_ready(poll_interval).await;
                            }
                        }
                        Err(e) => {
                            error!("DuckPipe cycle error: {}", e);
                            coordinator.clear();
                            snapshot_manager.clear();
                            consumer = None;
                            tokio::time::sleep(poll_interval).await;
                        }
                    }
                }
            }
        }
    }
}

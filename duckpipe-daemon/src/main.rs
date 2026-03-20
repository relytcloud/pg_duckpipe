//! Standalone DuckPipe daemon — CDC sync engine outside PostgreSQL.
//!
//! Shares the same `duckpipe-core` logic as the PG background worker,
//! but runs as an independent process connecting over TCP.
//!
//! Each daemon instance handles exactly one sync group.  The group can be
//! specified at startup via `--group` (pre-bound) or bound later via the
//! REST API (`POST /groups`).

mod api;

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tokio::signal;
use tracing::{error, info};

use duckpipe_core::connstr::to_slot_connect_params;
use duckpipe_core::flush_coordinator::FlushCoordinator;
use duckpipe_core::service::{self, ServiceConfig, SlotState};
use duckpipe_core::snapshot_manager::SnapshotManager;
use duckpipe_core::types::{GroupConfig, ResolvedConfig};

/// DuckPipe — standalone CDC sync daemon for PostgreSQL → DuckLake.
#[derive(Parser, Debug)]
#[command(name = "duckpipe", version, about)]
struct Args {
    /// PostgreSQL connection string (libpq key=value format).
    /// Example: "host=localhost port=5432 dbname=mydb user=replicator password=secret"
    #[arg(long, env = "CONNSTR")]
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

    /// Sync group to process. If omitted, daemon starts unbound and waits
    /// for a group to be created via POST /groups.
    #[arg(long)]
    group: Option<String>,

    /// HTTP API port (0 to disable).
    #[arg(long, default_value_t = 8080)]
    api_port: u16,

    /// HTTP API bind address.
    #[arg(long, default_value_t = IpAddr::from([127, 0, 0, 1]))]
    api_bind: IpAddr,

    /// Directory containing libduckdb.so and ducklake.duckdb_extension.
    /// Defaults to DUCKDB_LIB_DIR env var, then "/usr/local/lib".
    #[arg(long, env = "DUCKDB_LIB_DIR", default_value = "/usr/local/lib")]
    duckdb_lib_dir: String,
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
    };

    let slot_params = to_slot_connect_params(&connstr).unwrap_or_else(|e| {
        eprintln!("Invalid connection string: {}", e);
        std::process::exit(1);
    });

    // Start HTTP API server
    if args.api_port > 0 {
        let api_state = state.clone();
        let api_port = args.api_port;
        let api_bind = args.api_bind;
        tokio::spawn(async move {
            let router = api::router(api_state);
            let addr = format!("{}:{}", api_bind, api_port);
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
    run_sync_loop(
        &state,
        &config,
        &slot_params,
        poll_interval,
        &args.duckdb_lib_dir,
    )
    .await;
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

/// Read global_config and per-group config from PG, resolve into ResolvedConfig.
async fn read_resolved_config(connstr: &str, group_name: &str) -> ResolvedConfig {
    let connect_result =
        duckpipe_core::connstr::pg_connect_with_app_name(connstr, "duckpipe-daemon-config").await;
    let (client, conn_handle) = match connect_result {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to read config from PG: {}", e);
            return ResolvedConfig::default();
        }
    };

    // Read global config
    let global = match client
        .query("SELECT key, value FROM duckpipe.global_config", &[])
        .await
    {
        Ok(rows) => {
            let kv: Vec<(String, String)> = rows
                .iter()
                .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
                .collect();
            GroupConfig::from_kv_rows(&kv)
        }
        Err(e) => {
            error!("Failed to read global_config: {}", e);
            GroupConfig::default()
        }
    };

    // Read per-group config
    let group_config = match client
        .query(
            "SELECT config::text FROM duckpipe.sync_groups WHERE name = $1",
            &[&group_name],
        )
        .await
    {
        Ok(rows) => {
            if let Some(row) = rows.first() {
                let config_str: Option<String> = row.get(0);
                config_str
                    .and_then(|s| GroupConfig::from_json_str(&s).ok())
                    .unwrap_or_default()
            } else {
                GroupConfig::default()
            }
        }
        Err(e) => {
            error!("Failed to read group config: {}", e);
            GroupConfig::default()
        }
    };

    drop(client);
    let _ = conn_handle.await;

    ResolvedConfig::resolve(&global, &group_config)
}

/// Main sync loop — waits for group binding if unbound, then runs the sync cycle.
/// Uses an outer loop (not recursion) so unbind/rebind cycles don't grow the stack.
async fn run_sync_loop(
    state: &Arc<api::AppState>,
    config: &ServiceConfig,
    slot_params: &duckpipe_core::service::SlotConnectParams,
    poll_interval: Duration,
    pkglibdir: &str,
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

        let resolved_config = read_resolved_config(&config.connstr, &group_name).await;
        let mut coordinator = FlushCoordinator::new(
            config.connstr.clone(),
            config.ducklake_schema.clone(),
            pkglibdir.to_string(),
            group_name.clone(),
            resolved_config,
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
                            // Update cached metrics for HTTP /metrics endpoint
                            {
                                let mut cache = state.metrics_cache.lock().await;
                                cache.tables = coordinator.table_combined_metrics();
                                let gs = coordinator.gate_stats();
                                cache.group = duckpipe_core::flush_coordinator::GroupMetrics {
                                    group_id: 0, // daemon doesn't use PG group_id
                                    total_queued_changes: coordinator.total_queued(),
                                    is_backpressured: coordinator.is_backpressured(),
                                    active_flushes: coordinator.active_flush_count() as i32,
                                    gate_wait_avg_ms: gs.avg_wait_ms,
                                    gate_timeouts: gs.timeout_count,
                                };
                            }

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

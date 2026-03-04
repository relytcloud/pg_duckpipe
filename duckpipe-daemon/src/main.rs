//! Standalone DuckPipe daemon — CDC sync engine outside PostgreSQL.
//!
//! Shares the same `duckpipe-core` logic as the PG background worker,
//! but runs as an independent process connecting over TCP.

use std::collections::HashMap;
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
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args = Args::parse();

    duckpipe_core::log::init_subscriber(args.debug);

    let config = ServiceConfig {
        poll_interval_ms: args.poll_interval,
        batch_size_per_group: args.batch_size_per_group,
        debug_log: args.debug,
        connstr: args.connstr.clone(),
        duckdb_pg_connstr: args.connstr.clone(),
        ducklake_schema: args.ducklake_schema.clone(),
        flush_interval_ms: args.flush_interval,
        flush_batch_threshold: args.flush_batch_threshold,
        max_queued_changes: args.max_queued_changes,
    };

    let slot_params = to_slot_connect_params(&args.connstr).unwrap_or_else(|e| {
        eprintln!("Invalid connection string: {}", e);
        std::process::exit(1);
    });

    let mut coordinator = FlushCoordinator::new(
        args.connstr.clone(),
        args.ducklake_schema,
        args.flush_batch_threshold,
        args.flush_interval,
        args.max_queued_changes,
    );
    let mut snapshot_manager = SnapshotManager::new();

    info!(
        "DuckPipe daemon starting (connstr={}, poll={}ms)",
        duckpipe_core::connstr::redact_password(&args.connstr),
        args.poll_interval
    );

    let poll_interval = Duration::from_millis(args.poll_interval as u64);

    // Persistent slot state (consumer + rel_cache) — reused across cycles, cleared on error
    let mut consumers: HashMap<String, SlotState> = HashMap::new();

    loop {
        tokio::select! {
            _ = signal::ctrl_c() => {
                info!("DuckPipe daemon shutting down (Ctrl+C)");
                break;
            }
            result = service::run_sync_cycle(&config, &mut coordinator, &slot_params, &mut consumers, &mut snapshot_manager) => {
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
                        consumers.clear();
                        tokio::time::sleep(poll_interval).await;
                    }
                }
            }
        }
    }
}

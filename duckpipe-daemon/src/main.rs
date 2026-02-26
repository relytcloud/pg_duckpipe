//! Standalone DuckPipe daemon — CDC sync engine outside PostgreSQL.
//!
//! Shares the same `duckpipe-core` logic as the PG background worker,
//! but runs as an independent process connecting over TCP.

use std::collections::HashMap;
use std::time::Duration;

use clap::Parser;
use tokio::signal;
use tracing::{error, info};

use duckpipe_core::flush_coordinator::FlushCoordinator;
use duckpipe_core::service::{self, ServiceConfig, SlotConnectParams, SlotState};

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

/// Parsed connection parameters for pgwire-replication (TCP).
struct ConnParams {
    host: String,
    port: u16,
    user: String,
    password: String,
    dbname: String,
}

/// Parse a libpq-style key=value connection string into individual fields.
fn parse_connstr(connstr: &str) -> ConnParams {
    let mut host = "localhost".to_string();
    let mut port: u16 = 5432;
    let mut user = std::env::var("USER")
        .or_else(|_| std::env::var("PGUSER"))
        .unwrap_or_else(|_| "postgres".to_string());
    let mut password = String::new();
    let mut dbname = "postgres".to_string();

    for part in connstr.split_whitespace() {
        if let Some((key, value)) = part.split_once('=') {
            match key {
                "host" => host = value.to_string(),
                "hostaddr" => host = value.to_string(),
                "port" => {
                    if let Ok(p) = value.parse() {
                        port = p;
                    }
                }
                "user" => user = value.to_string(),
                "password" => password = value.to_string(),
                "dbname" => dbname = value.to_string(),
                _ => {}
            }
        }
    }

    ConnParams {
        host,
        port,
        user,
        password,
        dbname,
    }
}

/// Build a tokio-postgres connection string from ConnParams.
/// For tokio-postgres, Unix sockets use host= with a path, TCP uses host= with hostname.
fn build_tokio_pg_connstr(params: &ConnParams) -> String {
    let mut parts = vec![
        format!("host={}", params.host),
        format!("port={}", params.port),
        format!("user={}", params.user),
        format!("dbname={}", params.dbname),
    ];
    if !params.password.is_empty() {
        parts.push(format!("password={}", params.password));
    }
    parts.join(" ")
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args = Args::parse();

    // Initialize tracing
    let filter = if args.debug {
        "duckpipe=debug,duckpipe_core=debug,info"
    } else {
        "duckpipe=info,duckpipe_core=info,warn"
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(filter)),
        )
        .init();

    let conn_params = parse_connstr(&args.connstr);
    let pg_connstr = build_tokio_pg_connstr(&conn_params);

    let config = ServiceConfig {
        poll_interval_ms: args.poll_interval,
        batch_size_per_group: args.batch_size_per_group,
        debug_log: args.debug,
        connstr: pg_connstr.clone(),
        duckdb_pg_connstr: pg_connstr.clone(),
        ducklake_schema: args.ducklake_schema.clone(),
        flush_interval_ms: args.flush_interval,
        flush_batch_threshold: args.flush_batch_threshold,
        max_queued_changes: args.max_queued_changes,
    };

    let slot_params = SlotConnectParams::Tcp {
        host: conn_params.host.clone(),
        port: conn_params.port,
        user: conn_params.user.clone(),
        password: conn_params.password.clone(),
        dbname: conn_params.dbname.clone(),
    };

    let mut coordinator = FlushCoordinator::new(
        pg_connstr,
        args.ducklake_schema,
        args.flush_batch_threshold,
        args.flush_interval,
        args.max_queued_changes,
    );

    info!(
        "DuckPipe daemon starting (host={}, port={}, dbname={}, poll={}ms)",
        conn_params.host, conn_params.port, conn_params.dbname, args.poll_interval
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
            result = service::run_sync_cycle(&config, &mut coordinator, &slot_params, &mut consumers) => {
                match result {
                    Ok(any_work) => {
                        if !any_work {
                            tokio::time::sleep(poll_interval).await;
                        }
                    }
                    Err(e) => {
                        error!("DuckPipe cycle error: {}", e);
                        coordinator.clear();
                        consumers.clear();
                        tokio::time::sleep(poll_interval).await;
                    }
                }
            }
        }
    }
}

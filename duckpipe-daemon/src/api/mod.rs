pub mod error;
pub mod groups;
pub mod health;
pub mod metrics;
pub mod status;
pub mod tables;

use std::sync::Arc;
use std::time::Instant;

use axum::Router;
use tokio::sync::{Mutex, Notify, RwLock};

use self::error::ApiError;

/// Serde default for `bool` fields that should default to `true`.
pub(crate) fn default_true() -> bool {
    true
}

/// Cached in-memory metrics from FlushCoordinator, updated after each sync cycle.
#[derive(Clone, Default)]
pub struct MetricsCache {
    /// Per-table: (mapping_id, queued_changes, memory_bytes, flush_count, flush_duration_ms)
    pub tables: Vec<(i32, i64, i64, i64, i64)>,
    /// Group: (total_queued_changes, is_backpressured, active_flushes)
    pub group: (i64, bool, usize),
}

/// Shared state for the HTTP API server.
pub struct AppState {
    /// PostgreSQL connection string (the local PG with duckpipe extension).
    pub connstr: String,
    /// When the daemon process started.
    pub started_at: Instant,
    /// Bound group name. `None` = unbound. Set once by `POST /groups` or `--group`.
    pub group: RwLock<Option<String>>,
    /// Notify the sync loop to start after group binding.
    pub sync_start: Notify,
    /// Notify the sync loop to stop (e.g., when group is dropped).
    pub sync_stop: Notify,
    /// Persistent PG connection holding the advisory lock (keeps lock alive).
    pub lock_conn: Mutex<Option<LockConn>>,
    /// Cached in-memory metrics from FlushCoordinator, updated each sync cycle.
    pub metrics_cache: Mutex<MetricsCache>,
}

/// A persistent PG connection that holds an advisory lock.
/// The `client` field is kept alive to maintain the advisory lock session.
pub struct LockConn {
    #[allow(dead_code)]
    pub client: tokio_postgres::Client,
    pub _handle: tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
}

impl AppState {
    pub fn new(connstr: String) -> Self {
        Self {
            connstr,
            started_at: Instant::now(),
            group: RwLock::new(None),
            sync_start: Notify::new(),
            sync_stop: Notify::new(),
            lock_conn: Mutex::new(None),
            metrics_cache: Mutex::new(MetricsCache::default()),
        }
    }

    /// Get the bound group name, or return `ApiError::NotBound`.
    pub async fn require_group(&self) -> Result<String, ApiError> {
        self.group.read().await.clone().ok_or(ApiError::NotBound)
    }
}

/// Create a short-lived PG connection for an API request.
/// The connection task (already spawned by pg_connect) runs until the client is dropped.
pub async fn pg_connect(connstr: &str) -> Result<tokio_postgres::Client, ApiError> {
    let (client, _handle) = duckpipe_core::connstr::pg_connect(connstr)
        .await
        .map_err(|e| ApiError::Internal(format!("pg connect: {}", e)))?;
    Ok(client)
}

/// Build the axum router with all API routes.
pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", axum::routing::get(health::get_health))
        .route("/status", axum::routing::get(status::get_status))
        .route("/metrics", axum::routing::get(metrics::get_metrics))
        .route("/groups", axum::routing::post(groups::create_group))
        .route("/groups", axum::routing::delete(groups::drop_group))
        .route("/groups/enable", axum::routing::post(groups::enable_group))
        .route(
            "/groups/disable",
            axum::routing::post(groups::disable_group),
        )
        .route("/tables", axum::routing::get(tables::list_tables))
        .route("/tables", axum::routing::post(tables::add_table))
        .route(
            "/tables/{source_table}/resync",
            axum::routing::post(tables::resync_table),
        )
        .route(
            "/tables/{source_table}",
            axum::routing::delete(tables::remove_table),
        )
        .with_state(state)
}

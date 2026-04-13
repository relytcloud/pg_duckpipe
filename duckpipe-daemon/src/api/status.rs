use std::sync::Arc;

use axum::extract::State;
use axum::response::IntoResponse;
use axum::Json;
use serde_json::{json, Value};

use super::error::ApiError;
use super::{pg_connect, AppState};

pub async fn get_status(State(state): State<Arc<AppState>>) -> Result<impl IntoResponse, ApiError> {
    let group_name = state.require_group().await?;
    let client = pg_connect(&state.connstr).await?;

    // Per-table status (cast pg_lsn and timestamptz to text for easy handling)
    let rows = client
        .query(
            "SELECT tm.source_schema || '.' || tm.source_table AS source_table,
                    tm.target_schema || '.' || tm.target_table AS target_table,
                    tm.state, tm.enabled, tm.rows_synced,
                    tm.last_sync_at::text AS last_sync_at,
                    tm.error_message, tm.consecutive_failures,
                    tm.retry_at::text AS retry_at,
                    tm.applied_lsn::text AS applied_lsn,
                    tm.snapshot_duration_ms, tm.snapshot_rows
             FROM duckpipe.table_mappings tm
             JOIN duckpipe.sync_groups sg ON sg.id = tm.group_id
             WHERE sg.name = $1
             ORDER BY tm.id",
            &[&group_name],
        )
        .await?;

    let tables: Vec<Value> = rows
        .iter()
        .map(|r| {
            json!({
                "source_table": r.get::<_, String>("source_table"),
                "target_table": r.get::<_, String>("target_table"),
                "state": r.get::<_, String>("state"),
                "enabled": r.get::<_, bool>("enabled"),
                "rows_synced": r.get::<_, i64>("rows_synced"),
                "last_sync_at": r.get::<_, Option<String>>("last_sync_at"),
                "error_message": r.get::<_, Option<String>>("error_message"),
                "consecutive_failures": r.get::<_, i32>("consecutive_failures"),
                "retry_at": r.get::<_, Option<String>>("retry_at"),
                "applied_lsn": r.get::<_, Option<String>>("applied_lsn"),
                "snapshot_duration_ms": r.get::<_, Option<i64>>("snapshot_duration_ms"),
                "snapshot_rows": r.get::<_, Option<i64>>("snapshot_rows"),
            })
        })
        .collect();

    // Worker state from in-memory metrics cache (same as /metrics endpoint)
    let worker = {
        let cache = state.metrics_cache.lock().await;
        let gm = &cache.group;
        json!({
            "total_queued_bytes": gm.total_queued_bytes,
            "is_backpressured": gm.is_backpressured,
        })
    };

    // Group info
    let group_row = client
        .query_opt(
            "SELECT enabled, confirmed_lsn::text AS confirmed_lsn
             FROM duckpipe.sync_groups WHERE name = $1",
            &[&group_name],
        )
        .await?;

    let group_info = group_row.map(|r| {
        json!({
            "enabled": r.get::<_, bool>("enabled"),
            "confirmed_lsn": r.get::<_, Option<String>>("confirmed_lsn"),
        })
    });

    Ok(Json(json!({
        "group": group_name,
        "group_info": group_info,
        "tables": tables,
        "worker": worker,
    })))
}

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
                    tm.state, tm.enabled, tm.rows_synced, tm.queued_changes,
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
                "queued_changes": r.get::<_, i64>("queued_changes"),
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

    // Worker state
    let worker_row = client
        .query_opt(
            "SELECT total_queued_changes, is_backpressured,
                    updated_at::text AS updated_at
             FROM duckpipe.worker_state ws
             JOIN duckpipe.sync_groups sg ON sg.id = ws.group_id
             WHERE sg.name = $1",
            &[&group_name],
        )
        .await?;

    let worker = worker_row.map(|r| {
        json!({
            "total_queued_changes": r.get::<_, i64>("total_queued_changes"),
            "is_backpressured": r.get::<_, bool>("is_backpressured"),
            "updated_at": r.get::<_, Option<String>>("updated_at"),
        })
    });

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

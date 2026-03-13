use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::State;
use axum::response::IntoResponse;
use axum::Json;
use serde_json::{json, Value};

use super::error::ApiError;
use super::{pg_connect, AppState};

/// GET /metrics — full metrics snapshot (in-memory + PG persisted).
///
/// Returns the same JSON shape as the PG `duckpipe.metrics()` function:
/// ```json
/// {
///   "tables": [{ "group", "source_table", "state", "rows_synced",
///                 "queued_changes", "duckdb_memory_bytes", "flush_count",
///                 "flush_duration_ms", "consecutive_failures",
///                 "snapshot_duration_ms", "snapshot_rows", "applied_lsn" }],
///   "groups": [{ "name", "total_queued_changes", "is_backpressured" }]
/// }
/// ```
pub async fn get_metrics(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, ApiError> {
    let group_name = state.require_group().await?;

    // Read cached in-memory metrics from the sync loop
    let cache = state.metrics_cache.lock().await.clone();
    let shm_table_map: HashMap<i32, (i64, i64, i64, i64)> = cache
        .tables
        .into_iter()
        .map(|(id, q, m, fc, fd)| (id, (q, m, fc, fd)))
        .collect();

    // Query PG for persisted metrics
    let client = pg_connect(&state.connstr).await?;

    let rows = client
        .query(
            "SELECT g.name, m.source_schema || '.' || m.source_table AS source_table, \
             m.state, m.rows_synced, m.consecutive_failures, \
             m.snapshot_duration_ms, m.snapshot_rows, m.applied_lsn::text, m.id \
             FROM duckpipe.table_mappings m \
             JOIN duckpipe.sync_groups g ON m.group_id = g.id \
             WHERE g.name = $1 \
             ORDER BY m.id",
            &[&group_name],
        )
        .await?;

    let tables: Vec<Value> = rows
        .iter()
        .map(|r| {
            let mapping_id: i32 = r.get("id");
            let (queued, mem, fc, fd) = shm_table_map
                .get(&mapping_id)
                .copied()
                .unwrap_or((0, 0, 0, 0));

            json!({
                "group": r.get::<_, String>("name"),
                "source_table": r.get::<_, String>("source_table"),
                "state": r.get::<_, String>("state"),
                "rows_synced": r.get::<_, i64>("rows_synced"),
                "queued_changes": queued,
                "duckdb_memory_bytes": mem,
                "consecutive_failures": r.get::<_, i32>("consecutive_failures"),
                "flush_count": fc,
                "flush_duration_ms": fd,
                "snapshot_duration_ms": r.get::<_, Option<i64>>("snapshot_duration_ms"),
                "snapshot_rows": r.get::<_, Option<i64>>("snapshot_rows"),
                "applied_lsn": r.get::<_, Option<String>>("applied_lsn"),
            })
        })
        .collect();

    let groups: Vec<Value> = vec![json!({
        "name": group_name,
        "total_queued_changes": cache.group.0,
        "is_backpressured": cache.group.1,
    })];

    Ok(Json(json!({
        "tables": tables,
        "groups": groups,
    })))
}

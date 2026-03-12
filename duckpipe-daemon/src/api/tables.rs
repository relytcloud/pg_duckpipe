use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use super::error::ApiError;
use super::{default_true, pg_connect, AppState};

#[derive(Deserialize)]
pub struct AddTableRequest {
    pub source_table: String,
    pub target_table: Option<String>,
    #[serde(default = "default_true")]
    pub copy_data: bool,
}

#[derive(Deserialize)]
pub struct RemoveTableQuery {
    #[serde(default)]
    pub drop_target: bool,
}

/// GET /tables — list table mappings for the bound group.
pub async fn list_tables(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, ApiError> {
    let group_name = state.require_group().await?;
    let client = pg_connect(&state.connstr).await?;

    let rows = client
        .query(
            "SELECT tm.source_schema || '.' || tm.source_table AS source_table,
                    tm.target_schema || '.' || tm.target_table AS target_table,
                    tm.state, tm.enabled, tm.rows_synced,
                    tm.last_sync_at::text AS last_sync_at
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
            })
        })
        .collect();

    Ok(Json(json!({
        "group": group_name,
        "tables": tables,
    })))
}

/// POST /tables — add a table to the bound group.
pub async fn add_table(
    State(state): State<Arc<AppState>>,
    Json(body): Json<AddTableRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let group_name = state.require_group().await?;
    let client = pg_connect(&state.connstr).await?;

    let target = body.target_table.as_deref();

    client
        .execute(
            "SELECT duckpipe.add_table($1, $2, $3, $4)",
            &[&body.source_table, &target, &group_name, &body.copy_data],
        )
        .await?;

    Ok(Json(json!({
        "source_table": body.source_table,
        "group": group_name,
        "status": "added",
    })))
}

/// DELETE /tables/{source_table} — remove a table from the bound group.
pub async fn remove_table(
    State(state): State<Arc<AppState>>,
    Path(source_table): Path<String>,
    query: Query<RemoveTableQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let _group_name = state.require_group().await?;
    let client = pg_connect(&state.connstr).await?;

    client
        .execute(
            "SELECT duckpipe.remove_table($1, $2)",
            &[&source_table, &query.drop_target],
        )
        .await?;

    Ok(Json(json!({
        "source_table": source_table,
        "status": "removed",
    })))
}

/// POST /tables/{source_table}/resync — resync a table.
pub async fn resync_table(
    State(state): State<Arc<AppState>>,
    Path(source_table): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let _group_name = state.require_group().await?;
    let client = pg_connect(&state.connstr).await?;

    client
        .execute("SELECT duckpipe.resync_table($1)", &[&source_table])
        .await?;

    Ok(Json(json!({
        "source_table": source_table,
        "status": "resync_initiated",
    })))
}

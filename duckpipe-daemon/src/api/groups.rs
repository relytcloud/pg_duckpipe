use std::sync::Arc;

use axum::extract::{Query, State};
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;
use serde_json::json;

use super::error::ApiError;
use super::{default_true, pg_connect, AppState, LockConn};

#[derive(Deserialize)]
pub struct CreateGroupRequest {
    pub name: String,
    pub conninfo: Option<String>,
}

#[derive(Deserialize)]
pub struct DropGroupQuery {
    #[serde(default = "default_true")]
    pub drop_slot: bool,
}

/// Acquire an advisory lock for the given group name.
/// Returns a persistent connection holding the lock, or an error if already locked.
pub async fn acquire_advisory_lock(connstr: &str, group_name: &str) -> Result<LockConn, ApiError> {
    let (client, handle) = duckpipe_core::connstr::pg_connect(connstr)
        .await
        .map_err(|e| ApiError::Internal(format!("pg connect for lock: {}", e)))?;

    let lock_key = format!("duckpipe_daemon:{}", group_name);
    let row = client
        .query_one("SELECT pg_try_advisory_lock(hashtext($1))", &[&lock_key])
        .await?;

    let acquired: bool = row.get(0);
    if !acquired {
        return Err(ApiError::LockConflict(group_name.to_string()));
    }

    Ok(LockConn {
        client,
        _handle: handle,
    })
}

/// POST /groups — create a sync group and bind the daemon to it.
pub async fn create_group(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CreateGroupRequest>,
) -> Result<impl IntoResponse, ApiError> {
    // Hold write lock for the entire bind operation to prevent TOCTOU races
    // (two concurrent POST /groups requests binding different groups).
    let mut group = state.group.write().await;
    if let Some(ref name) = *group {
        return Err(ApiError::AlreadyBound(name.clone()));
    }

    // Acquire advisory lock
    let lock_conn = acquire_advisory_lock(&state.connstr, &body.name).await?;

    // Create group via extension SQL function
    let client = pg_connect(&state.connstr).await?;

    let conninfo = body.conninfo.as_deref();
    client
        .execute(
            "SELECT duckpipe.create_group($1, NULL, NULL, $2, 'daemon')",
            &[&body.name, &conninfo],
        )
        .await?;

    // Bind the daemon (write lock still held)
    *group = Some(body.name.clone());
    drop(group);

    {
        let mut lock = state.lock_conn.lock().await;
        *lock = Some(lock_conn);
    }

    // Notify sync loop to start
    state.sync_start.notify_one();

    Ok(Json(json!({
        "group": body.name,
        "status": "bound",
    })))
}

/// DELETE /groups — drop the bound group.
///
/// Unbinds the daemon and stops the sync loop first so that the replication
/// slot is released before `duckpipe.drop_group()` tries to drop it.
pub async fn drop_group(
    State(state): State<Arc<AppState>>,
    query: Query<DropGroupQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let group_name = state.require_group().await?;

    // 1. Unbind the daemon so the sync loop will notice and stop.
    {
        let mut group = state.group.write().await;
        *group = None;
    }

    // 2. Signal the sync loop to stop immediately (cancels in-progress cycle
    //    via tokio::select!, which drops the replication connection).
    state.sync_stop.notify_one();

    // 3. Release the advisory lock connection.
    {
        let mut lock = state.lock_conn.lock().await;
        *lock = None;
    }

    // 4. Brief wait for the sync loop to actually drop its replication
    //    connection after the tokio::select! cancellation fires.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // 5. Remove all tables from the group first (FK constraint prevents
    //    deleting sync_groups while table_mappings reference it).
    let client = pg_connect(&state.connstr).await?;
    let tables = client
        .query(
            "SELECT tm.source_schema || '.' || tm.source_table AS source_table
             FROM duckpipe.table_mappings tm
             JOIN duckpipe.sync_groups sg ON sg.id = tm.group_id
             WHERE sg.name = $1",
            &[&group_name],
        )
        .await?;

    for row in &tables {
        let source_table: String = row.get("source_table");
        let drop_target = true;
        client
            .execute(
                "SELECT duckpipe.remove_table($1, $2)",
                &[&source_table, &drop_target],
            )
            .await?;
    }

    // 6. Now safe to drop the group (slot should be released).
    client
        .execute(
            "SELECT duckpipe.drop_group($1, $2)",
            &[&group_name, &query.drop_slot],
        )
        .await?;

    Ok(Json(json!({
        "status": "dropped",
    })))
}

/// POST /groups/enable — enable the bound group.
pub async fn enable_group(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, ApiError> {
    let group_name = state.require_group().await?;
    let client = pg_connect(&state.connstr).await?;

    client
        .execute("SELECT duckpipe.enable_group($1)", &[&group_name])
        .await?;

    Ok(Json(json!({
        "group": group_name,
        "enabled": true,
    })))
}

/// POST /groups/disable — disable the bound group.
pub async fn disable_group(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, ApiError> {
    let group_name = state.require_group().await?;
    let client = pg_connect(&state.connstr).await?;

    client
        .execute("SELECT duckpipe.disable_group($1)", &[&group_name])
        .await?;

    Ok(Json(json!({
        "group": group_name,
        "enabled": false,
    })))
}

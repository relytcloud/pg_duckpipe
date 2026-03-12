use std::sync::Arc;

use axum::extract::State;
use axum::Json;
use serde_json::{json, Value};

use super::AppState;

pub async fn get_health(State(state): State<Arc<AppState>>) -> Json<Value> {
    let uptime_secs = state.started_at.elapsed().as_secs();
    let group = state.group.read().await;
    let locked = state.lock_conn.lock().await.is_some();

    Json(json!({
        "status": "ok",
        "uptime_secs": uptime_secs,
        "group": *group,
        "locked": locked,
    }))
}

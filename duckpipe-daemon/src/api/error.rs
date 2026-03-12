use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

/// API error type with automatic HTTP status code mapping.
#[derive(Debug)]
#[allow(dead_code)]
pub enum ApiError {
    /// Daemon is not bound to any group yet (400).
    NotBound,
    /// Daemon is already bound to a group (409).
    AlreadyBound(String),
    /// Another daemon holds the advisory lock for this group (409).
    LockConflict(String),
    /// Resource not found (404).
    NotFound(String),
    /// PostgreSQL error (500, or 400 for user errors).
    Pg(String),
    /// Internal error (500).
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::NotBound => (
                StatusCode::BAD_REQUEST,
                "no group bound — POST /groups first".to_string(),
            ),
            ApiError::AlreadyBound(name) => (
                StatusCode::CONFLICT,
                format!("daemon already bound to group '{}'", name),
            ),
            ApiError::LockConflict(name) => (
                StatusCode::CONFLICT,
                format!("another daemon is already running for group '{}'", name),
            ),
            ApiError::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
            ApiError::Pg(msg) => {
                tracing::error!("PG error: {}", msg);
                let status = if is_user_error(&msg) {
                    StatusCode::BAD_REQUEST
                } else {
                    StatusCode::INTERNAL_SERVER_ERROR
                };
                (status, msg)
            }
            ApiError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
        };

        let body = json!({ "error": message });
        (status, axum::Json(body)).into_response()
    }
}

impl From<tokio_postgres::Error> for ApiError {
    fn from(e: tokio_postgres::Error) -> Self {
        ApiError::Pg(format!("{:?}", e))
    }
}

/// Classify PG errors that are likely user mistakes as 400 rather than 500.
fn is_user_error(msg: &str) -> bool {
    let lower = msg.to_lowercase();
    lower.contains("does not exist")
        || lower.contains("already exists")
        || lower.contains("not found")
        || lower.contains("must have a primary key")
        || lower.contains("violates")
        || lower.contains("permission denied")
}

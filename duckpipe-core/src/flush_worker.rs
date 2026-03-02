//! Flush worker utilities — metrics update via PG connection.

use crate::metadata::MetadataClient;

/// Update metrics and applied_lsn via a short-lived PG connection (used after DuckDB flush).
pub async fn update_metrics_via_pg(
    connstr: &str,
    mapping_id: i32,
    applied_count: i64,
    last_lsn: u64,
) -> Result<(), String> {
    let (client, conn_handle) = crate::connstr::pg_connect(connstr)
        .await
        .map_err(|e| format!("metrics connect: {}", e))?;

    let meta = MetadataClient::new(&client);
    meta.update_table_metrics(mapping_id, applied_count)
        .await
        .map_err(|e| format!("update_metrics: {}", e))?;
    if last_lsn > 0 {
        meta.update_applied_lsn(mapping_id, last_lsn)
            .await
            .map_err(|e| format!("update_applied_lsn: {}", e))?;
    }

    drop(client);
    let _ = conn_handle.await;

    Ok(())
}

/// Update error state in PG metadata after a flush failure.
/// Records error message, increments consecutive failures, and conditionally
/// transitions to ERRORED state with exponential backoff retry.
pub async fn update_error_state(
    connstr: &str,
    mapping_id: i32,
    error: &str,
    errored_threshold: i32,
) -> Result<(), String> {
    if mapping_id < 0 {
        return Ok(());
    }

    let (client, conn_handle) = crate::connstr::pg_connect(connstr)
        .await
        .map_err(|e| format!("error_state connect: {}", e))?;

    let meta = MetadataClient::new(&client);
    let _ = meta.record_error_message(mapping_id, error).await;

    if let Ok(count) = meta.increment_consecutive_failures(mapping_id).await {
        if count >= errored_threshold {
            // Exponential backoff: 30s * 2^(count - threshold)
            let backoff = 30i64 * (1i64 << (count - errored_threshold).min(6) as u32);
            let _ = meta
                .set_errored_with_retry(mapping_id, error, backoff)
                .await;
            tracing::warn!(
                "pg_duckpipe: mapping {} transitioned to ERRORED after {} failures, retry in {}s",
                mapping_id,
                count,
                backoff
            );
        }
    }

    drop(client);
    let _ = conn_handle.await;

    Ok(())
}

/// Clear error state on successful flush: reset consecutive_failures and error_message.
pub async fn clear_error_on_success(connstr: &str, mapping_id: i32) -> Result<(), String> {
    let (client, conn_handle) = crate::connstr::pg_connect(connstr)
        .await
        .map_err(|e| format!("clear_error connect: {}", e))?;

    let meta = MetadataClient::new(&client);
    let _ = meta.clear_consecutive_failures(mapping_id).await;
    let _ = meta.record_error_message(mapping_id, "").await;

    drop(client);
    let _ = conn_handle.await;

    Ok(())
}

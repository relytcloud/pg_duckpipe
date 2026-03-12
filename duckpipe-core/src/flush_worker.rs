//! Flush worker utilities — metrics update via PG connection.

use crate::metadata::MetadataClient;

/// Update metrics and applied_lsn via a short-lived PG connection (used after DuckDB flush).
pub async fn update_metrics_via_pg(
    connstr: &str,
    mapping_id: i32,
    applied_count: i64,
    last_lsn: u64,
    group_name: &str,
) -> Result<(), String> {
    let app_name = crate::connstr::app_name(group_name, "flush");
    let (client, conn_handle) = crate::connstr::pg_connect_with_app_name(connstr, &app_name)
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
    group_name: &str,
) -> Result<(), String> {
    if mapping_id < 0 {
        return Ok(());
    }

    let app_name = crate::connstr::app_name(group_name, "flush");
    let (client, conn_handle) = crate::connstr::pg_connect_with_app_name(connstr, &app_name)
        .await
        .map_err(|e| format!("error_state connect: {}", e))?;

    let meta = MetadataClient::new(&client);
    if let Ok(count) = meta
        .record_failure_with_backoff(mapping_id, error, errored_threshold)
        .await
    {
        if count >= errored_threshold {
            let backoff = crate::metadata::compute_backoff_secs(count, errored_threshold);
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
pub async fn clear_error_on_success(
    connstr: &str,
    mapping_id: i32,
    group_name: &str,
) -> Result<(), String> {
    let app_name = crate::connstr::app_name(group_name, "flush");
    let (client, conn_handle) = crate::connstr::pg_connect_with_app_name(connstr, &app_name)
        .await
        .map_err(|e| format!("clear_error connect: {}", e))?;

    let meta = MetadataClient::new(&client);
    let _ = meta.clear_consecutive_failures(mapping_id).await;
    let _ = meta.record_error_message(mapping_id, "").await;

    drop(client);
    let _ = conn_handle.await;

    Ok(())
}

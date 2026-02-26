//! Pure Rust snapshot logic using `tokio-postgres`.
//!
//! Uses SQL functions (`pg_create_logical_replication_slot` + `pg_export_snapshot`)
//! instead of the replication protocol, so no special `replication=database` connection
//! is needed. Works over both Unix sockets and TCP.
//!
//! Algorithm:
//! 1. Connection A: BEGIN REPEATABLE READ
//! 2. Connection A: pg_create_logical_replication_slot(temp) → consistent_point
//! 3. Connection A: pg_export_snapshot() → snapshot_name
//! 4. Connection B: SET TRANSACTION SNAPSHOT, DELETE target, INSERT INTO target SELECT * FROM source
//! 5. Connection A: COMMIT (temp slot auto-drops when connection closes)

use std::time::Instant;

use tokio_postgres::NoTls;

use crate::types::{format_lsn, parse_lsn};

/// Result of a snapshot task.
pub struct SnapshotResult {
    pub task_id: i32,
    pub source_schema: String,
    pub source_table: String,
    /// Ok((consistent_point_lsn, rows_copied)) on success, Err(message) on failure.
    pub result: Result<(u64, u64), String>,
}

/// Process snapshot for a single table with its own connections.
///
/// Creates a temporary logical replication slot via SQL to get a consistent_point,
/// exports the transaction snapshot, copies data on a second connection, then cleans up.
///
/// Returns `(consistent_point_lsn, rows_copied)` on success.
///
/// `task_id` is used to create a unique temporary slot name, allowing
/// concurrent snapshot tasks for different tables.
///
/// This function is self-contained and can be spawned as an independent tokio task.
pub async fn process_snapshot_task(
    source_schema: &str,
    source_table: &str,
    target_schema: &str,
    target_table: &str,
    connstr: &str,
    timing: bool,
    task_id: i32,
) -> Result<(u64, u64), String> {
    let table_start = if timing { Some(Instant::now()) } else { None };

    // Step 1: Open control connection — creates temp slot and exports snapshot.
    // The REPEATABLE READ snapshot is taken at the first query (slot creation),
    // ensuring the snapshot and consistent_point are at the same point in time.
    // The exported snapshot remains valid while this connection's transaction is open.
    let (ctrl_client, ctrl_connection) =
        tokio_postgres::connect(connstr, NoTls).await.map_err(|e| {
            format!(
                "snapshot control connect for {}.{}: {}",
                source_schema, source_table, e
            )
        })?;

    let ctrl_conn_handle = tokio::spawn(async move {
        if let Err(e) = ctrl_connection.await {
            tracing::warn!("DuckPipe: snapshot control connection error: {}", e);
        }
    });

    // Use task_id in the slot name to avoid collisions under concurrent snapshots.
    let slot_name = format!("duckpipe_snap_{}", task_id);

    // Begin REPEATABLE READ — snapshot is taken at the first query.
    ctrl_client
        .execute("BEGIN ISOLATION LEVEL REPEATABLE READ", &[])
        .await
        .map_err(|e| format!("snapshot BEGIN: {}", e))?;

    // Create temp logical slot — this is the first query, so the REPEATABLE READ
    // snapshot is established here. The slot's consistent_point is the WAL position
    // at which the slot becomes consistent, coordinated with this snapshot.
    let row = ctrl_client
        .query_one(
            "SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput', true)",
            &[&slot_name],
        )
        .await
        .map_err(|e| {
            format!(
                "pg_create_logical_replication_slot failed for {}.{}: {}",
                source_schema, source_table, e
            )
        })?;

    let consistent_point_str: String = row.get(0);
    let consistent_point = parse_lsn(&consistent_point_str);

    // Export the snapshot so a second connection can use it for the data copy.
    let row = ctrl_client
        .query_one("SELECT pg_export_snapshot()", &[])
        .await
        .map_err(|e| format!("pg_export_snapshot: {}", e))?;

    let snapshot_name: String = row.get(0);

    // Step 2: Open data connection and copy data using the exported snapshot.
    // Wrap in an inner async block so we always clean up the control connection.
    let copy_result = async {
        let (client, connection) =
            tokio_postgres::connect(connstr, NoTls).await.map_err(|e| {
                format!(
                    "snapshot data connect for {}.{}: {}",
                    source_schema, source_table, e
                )
            })?;

        let conn_handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::warn!("DuckPipe: snapshot data connection error: {}", e);
            }
        });

        let result = async {
            client
                .execute("BEGIN ISOLATION LEVEL REPEATABLE READ", &[])
                .await
                .map_err(|e| format!("data BEGIN: {}", e))?;

            // Import the snapshot from the control connection.
            // Single-quote escaping is sufficient (snapshot names are hex strings from PG).
            let snapshot_cmd = format!(
                "SET TRANSACTION SNAPSHOT '{}'",
                snapshot_name.replace('\'', "''")
            );
            client
                .execute(&snapshot_cmd, &[])
                .await
                .map_err(|e| format!("SET TRANSACTION SNAPSHOT: {}", e))?;

            // Clear target before snapshot copy
            let delete_sql = format!(
                "DELETE FROM \"{}\".\"{}\"",
                target_schema.replace('"', "\"\""),
                target_table.replace('"', "\"\"")
            );
            client
                .execute(&delete_sql, &[])
                .await
                .map_err(|e| format!("DELETE target: {}", e))?;

            // Copy data
            let copy_sql = format!(
                "INSERT INTO \"{}\".\"{}\" SELECT * FROM \"{}\".\"{}\"",
                target_schema.replace('"', "\"\""),
                target_table.replace('"', "\"\""),
                source_schema.replace('"', "\"\""),
                source_table.replace('"', "\"\"")
            );

            tracing::info!(
                "DuckPipe: Copying data for {}.{} (snapshot={} consistent_point={})",
                source_schema,
                source_table,
                snapshot_name,
                format_lsn(consistent_point)
            );

            let rows_copied = client
                .execute(&copy_sql, &[])
                .await
                .map_err(|e| format!("INSERT SELECT: {}", e))?;

            client
                .execute("COMMIT", &[])
                .await
                .map_err(|e| format!("COMMIT: {}", e))?;

            Ok::<u64, String>(rows_copied)
        }
        .await;

        // Clean up data connection
        drop(client);
        let _ = conn_handle.await;

        result
    }
    .await;

    // Step 3: Always clean up the control connection, regardless of copy outcome.
    // COMMIT ends the REPEATABLE READ transaction; dropping the client closes
    // the connection and auto-drops the temporary replication slot.
    let _ = ctrl_client.execute("COMMIT", &[]).await;
    drop(ctrl_client);
    let _ = ctrl_conn_handle.await;

    let rows_copied = copy_result?;

    tracing::info!(
        "DuckPipe: Snapshot complete for {}.{}: {} rows copied (consistent_point={})",
        source_schema,
        source_table,
        rows_copied,
        format_lsn(consistent_point)
    );

    if let Some(start) = table_start {
        tracing::info!(
            "DuckPipe timing: action=snapshot_table source={}.{} target={}.{} rows={} elapsed_ms={:.3}",
            source_schema,
            source_table,
            target_schema,
            target_table,
            rows_copied,
            start.elapsed().as_secs_f64() * 1000.0
        );
    }

    Ok((consistent_point, rows_copied))
}

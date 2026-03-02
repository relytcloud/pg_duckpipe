//! DuckDB-side chunked COPY snapshot pipeline.
//!
//! Uses a single PG connection (async) for slot creation + COPY TO STDOUT,
//! and a DuckDB session (blocking, spawn_blocking) for direct writes to DuckLake.
//!
//! Algorithm:
//! 1. PG: BEGIN REPEATABLE READ → pg_create_logical_replication_slot(temp) → consistent_point
//! 2. PG: COPY (SELECT * FROM source) TO STDOUT WITH (FORMAT csv, HEADER, NULL '__DUCKPIPE_NULL__')
//! 3. Producer: stream COPY bytes → chunk CSV files (~500MB each) with header, send paths via channel
//! 4. Consumer (spawn_blocking): DuckDB read_csv(chunk, header=true, nullstr='__DUCKPIPE_NULL__') → INSERT INTO lake.target
//! 5. PG: COMMIT (temp slot auto-drops)

use std::fs;
use std::io::Write;
use std::path::Path;
use std::pin::pin;
use std::time::Instant;

use duckdb::{Config, Connection};
use futures_util::StreamExt;

use crate::types::{format_lsn, parse_lsn};

/// Max chunk size in bytes before splitting to a new file (~500MB).
const CHUNK_SIZE_BYTES: u64 = 500 * 1024 * 1024;

/// Messages from the CSV producer to the DuckDB consumer.
enum ChunkMessage {
    /// Path to a completed chunk CSV file.
    Ready(String),
    /// All chunks have been written.
    Done,
}

/// Result of a snapshot task.
pub struct SnapshotResult {
    pub task_id: i32,
    pub source_schema: String,
    pub source_table: String,
    pub target_schema: String,
    pub target_table: String,
    /// Ok((consistent_point_lsn, rows_copied, duration_ms)) on success, Err(message) on failure.
    pub result: Result<(u64, u64, u64), String>,
}

/// Process snapshot for a single table using a chunked COPY pipeline.
///
/// Creates a temporary logical replication slot via SQL to get a consistent_point,
/// then streams COPY TO STDOUT through CSV chunk files into DuckDB via read_csv.
///
/// Returns `(consistent_point_lsn, rows_copied, duration_ms)` on success.
pub async fn process_snapshot_task(
    source_schema: &str,
    source_table: &str,
    target_schema: &str,
    target_table: &str,
    connstr: &str,
    duckdb_pg_connstr: &str,
    ducklake_schema: &str,
    timing: bool,
    task_id: i32,
) -> Result<(u64, u64, u64), String> {
    let table_start = Instant::now();

    // Step 1: Open control connection — creates temp slot and runs COPY.
    let (ctrl_client, ctrl_conn_handle) =
        crate::connstr::pg_connect(connstr).await.map_err(|e| {
            format!(
                "snapshot control connect for {}.{}: {}",
                source_schema, source_table, e
            )
        })?;

    // Use task_id in the slot name to avoid collisions under concurrent snapshots.
    let slot_name = format!("duckpipe_snap_{}", task_id);

    // Begin REPEATABLE READ — snapshot is taken at the first query.
    ctrl_client
        .execute("BEGIN ISOLATION LEVEL REPEATABLE READ", &[])
        .await
        .map_err(|e| format!("snapshot BEGIN: {}", e))?;

    // Create temp logical slot — this is the first query, so the REPEATABLE READ
    // snapshot is established here.
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

    tracing::info!(
        "DuckPipe: Starting chunked COPY snapshot for {}.{} (consistent_point={})",
        source_schema,
        source_table,
        format_lsn(consistent_point)
    );

    // Step 2: Run COPY TO STDOUT + DuckDB consumer pipeline.
    // Wrap in an inner block so we always clean up the control connection.
    // Use DuckDB's temp_directory for chunk files (defaults to system temp on empty).
    let tmp_base = duckdb_temp_directory();
    let tmp_dir = tmp_base.join(format!("duckpipe_snap_{}", task_id));
    let copy_result = async {
        // Create temp directory for CSV chunk files
        fs::create_dir_all(&tmp_dir)
            .map_err(|e| format!("create temp dir {:?}: {}", tmp_dir, e))?;

        // Start COPY TO STDOUT with header for DuckDB column name mapping.
        // Custom NULL marker distinguishes NULL from empty string in CSV:
        //   NULL → __DUCKPIPE_NULL__ (unquoted) → DuckDB nullstr → NULL
        //   ''   → (empty, unquoted) → DuckDB → empty string
        // PG auto-quotes data fields that happen to contain this marker.
        let copy_sql = format!(
            "COPY (SELECT * FROM \"{}\".\"{}\") TO STDOUT WITH (FORMAT csv, HEADER true, NULL '__DUCKPIPE_NULL__')",
            source_schema.replace('"', "\"\""),
            source_table.replace('"', "\"\"")
        );

        let copy_stream = ctrl_client
            .copy_out(copy_sql.as_str())
            .await
            .map_err(|e| format!("COPY TO STDOUT: {}", e))?;

        // Bounded channel for backpressure — max ~1.5GB on disk.
        // Uses tokio::sync::mpsc so the async producer can .await on send
        // instead of blocking the Tokio worker thread.
        let (chunk_tx, mut chunk_rx) = tokio::sync::mpsc::channel::<ChunkMessage>(2);

        // Spawn blocking DuckDB consumer
        let consumer_duckdb_connstr = duckdb_pg_connstr.to_string();
        let consumer_ducklake_schema = ducklake_schema.to_string();
        let consumer_target_schema = target_schema.to_string();
        let consumer_target_table = target_table.to_string();
        let consumer_timing = timing;

        let consumer_handle = tokio::task::spawn_blocking(move || {
            run_duckdb_consumer(
                &mut chunk_rx,
                &consumer_duckdb_connstr,
                &consumer_ducklake_schema,
                &consumer_target_schema,
                &consumer_target_table,
                consumer_timing,
            )
        });

        // Producer: stream COPY bytes into chunk files
        let producer_result = run_csv_producer(copy_stream, &chunk_tx, &tmp_dir).await;

        // Signal completion or propagate error
        match producer_result {
            Ok(()) => {
                let _ = chunk_tx.send(ChunkMessage::Done).await;
            }
            Err(e) => {
                // Drop sender to unblock consumer, then return error
                drop(chunk_tx);
                // Wait for consumer to finish (it will see channel closed)
                let _ = consumer_handle.await;
                return Err(e);
            }
        }

        // Drop sender so consumer sees hangup after Done
        drop(chunk_tx);

        // Wait for consumer result
        let total_rows = consumer_handle
            .await
            .map_err(|e| format!("consumer task join: {}", e))?
            .map_err(|e| format!("DuckDB consumer: {}", e))?;

        Ok::<u64, String>(total_rows)
    }
    .await;

    // Step 3: Always clean up the control connection.
    let _ = ctrl_client.execute("COMMIT", &[]).await;
    drop(ctrl_client);
    let _ = ctrl_conn_handle.await;

    // Clean up temp dir
    let _ = fs::remove_dir_all(&tmp_dir);

    let rows_copied = copy_result?;

    let elapsed_ms = table_start.elapsed().as_millis() as u64;

    tracing::info!(
        "DuckPipe: Snapshot complete for {}.{}: {} rows copied (consistent_point={})",
        source_schema,
        source_table,
        rows_copied,
        format_lsn(consistent_point)
    );

    if timing {
        tracing::info!(
            "DuckPipe timing: action=snapshot_table source={}.{} target={}.{} rows={} elapsed_ms={}",
            source_schema,
            source_table,
            target_schema,
            target_table,
            rows_copied,
            elapsed_ms
        );
    }

    Ok((consistent_point, rows_copied, elapsed_ms))
}

/// CSV producer: streams COPY TO STDOUT bytes into chunk files with headers.
///
/// Extracts the CSV header from the first line and prepends it to each chunk.
/// Splits at line boundaries when exceeding CHUNK_SIZE_BYTES.
/// Tracks CSV quoting state (RFC 4180) to ensure splits only occur at true
/// row boundaries — never inside quoted fields that contain embedded newlines.
async fn run_csv_producer(
    stream: tokio_postgres::CopyOutStream,
    chunk_tx: &tokio::sync::mpsc::Sender<ChunkMessage>,
    tmp_dir: &Path,
) -> Result<(), String> {
    let mut stream = pin!(stream);
    let mut chunk_idx: u32 = 0;
    let mut current_file: Option<fs::File> = None;
    let mut current_path: Option<String> = None;
    let mut current_size: u64 = 0;
    // CSV header line, extracted from the first bytes of the stream.
    let mut header: Vec<u8> = Vec::new();
    let mut header_extracted = false;
    let mut pending: Vec<u8> = Vec::new();
    // Track whether we are inside a quoted CSV field. PostgreSQL COPY CSV
    // uses RFC 4180: fields with newlines/commas/quotes are enclosed in
    // double-quotes; literal quotes are escaped as "". The toggle handles
    // "" correctly (two toggles = same state).
    let mut in_quotes = false;

    while let Some(result) = stream.next().await {
        let bytes = result.map_err(|e| format!("COPY stream: {}", e))?;

        // Extract the header line from the first bytes (header is never quoted)
        if !header_extracted {
            pending.extend_from_slice(&bytes);
            if let Some(nl_pos) = pending.iter().position(|&b| b == b'\n') {
                header = pending[..=nl_pos].to_vec();
                let remainder = pending[nl_pos + 1..].to_vec();
                header_extracted = true;
                pending = Vec::new();

                // Open first chunk file with header
                let path = tmp_dir
                    .join(format!("chunk_{}.csv", chunk_idx))
                    .to_string_lossy()
                    .to_string();
                let mut file =
                    fs::File::create(&path).map_err(|e| format!("create chunk file: {}", e))?;
                file.write_all(&header)
                    .map_err(|e| format!("write header: {}", e))?;
                current_size = header.len() as u64;

                if !remainder.is_empty() {
                    for &b in &remainder {
                        if b == b'"' {
                            in_quotes = !in_quotes;
                        }
                    }
                    file.write_all(&remainder)
                        .map_err(|e| format!("write remainder: {}", e))?;
                    current_size += remainder.len() as u64;
                }

                current_file = Some(file);
                current_path = Some(path);
            }
            continue;
        }

        // Open new chunk file if needed (after a split)
        if current_file.is_none() {
            let path = tmp_dir
                .join(format!("chunk_{}.csv", chunk_idx))
                .to_string_lossy()
                .to_string();
            let mut file =
                fs::File::create(&path).map_err(|e| format!("create chunk file: {}", e))?;
            file.write_all(&header)
                .map_err(|e| format!("write header: {}", e))?;
            current_size = header.len() as u64;
            current_file = Some(file);
            current_path = Some(path);
            in_quotes = false;
        }

        // Update quoting state for this chunk of bytes
        for &b in bytes.iter() {
            if b == b'"' {
                in_quotes = !in_quotes;
            }
        }

        let file = current_file.as_mut().unwrap();
        file.write_all(&bytes)
            .map_err(|e| format!("write chunk data: {}", e))?;
        current_size += bytes.len() as u64;

        // Split when: size exceeded AND last byte is newline AND we are
        // outside a quoted field (so the newline is a real row terminator).
        if current_size >= CHUNK_SIZE_BYTES && bytes.last() == Some(&b'\n') && !in_quotes {
            file.flush().map_err(|e| format!("flush chunk: {}", e))?;
            let path = current_path.take().unwrap();
            chunk_tx
                .send(ChunkMessage::Ready(path))
                .await
                .map_err(|_| "consumer disconnected".to_string())?;

            current_file = None;
            current_size = 0;
            chunk_idx += 1;
        }
    }

    // Send the last chunk if any data was written
    if let Some(ref mut file) = current_file {
        file.flush()
            .map_err(|e| format!("flush final chunk: {}", e))?;
        let path = current_path.take().unwrap();
        chunk_tx
            .send(ChunkMessage::Ready(path))
            .await
            .map_err(|_| "consumer disconnected".to_string())?;
    }

    Ok(())
}

/// DuckDB consumer: reads chunk CSV files and inserts into DuckLake.
///
/// Runs in a single DuckDB transaction: DELETE + all chunk INSERTs + COMMIT.
fn run_duckdb_consumer(
    chunk_rx: &mut tokio::sync::mpsc::Receiver<ChunkMessage>,
    duckdb_pg_connstr: &str,
    ducklake_schema: &str,
    target_schema: &str,
    target_table: &str,
    timing: bool,
) -> Result<u64, String> {
    let t_start = if timing { Some(Instant::now()) } else { None };

    // Setup DuckDB: open in-memory, INSTALL/LOAD ducklake, ATTACH
    let config = Config::default()
        .allow_unsigned_extensions()
        .map_err(|e| format!("duckdb config: {}", e))?;
    let db =
        Connection::open_in_memory_with_flags(config).map_err(|e| format!("duckdb open: {}", e))?;

    db.execute_batch("INSTALL ducklake; LOAD ducklake;")
        .map_err(|e| format!("duckdb install ducklake: {}", e))?;

    let attach_sql = format!(
        "ATTACH 'ducklake:postgres:{}' AS lake (METADATA_SCHEMA '{}')",
        duckdb_pg_connstr.replace('\'', "''"),
        ducklake_schema.replace('\'', "''")
    );
    db.execute_batch(&attach_sql)
        .map_err(|e| format!("duckdb attach: {}", e))?;

    db.execute_batch(
        "SET ducklake_retry_wait_ms = 100; \
         SET ducklake_retry_backoff = 2.0; \
         SET ducklake_max_retry_count = 10;",
    )
    .map_err(|e| format!("duckdb set retry: {}", e))?;

    // Discover lake schema + column names from information_schema
    let schema_sql = format!(
        "SELECT table_schema FROM information_schema.tables \
         WHERE table_catalog = 'lake' AND table_name = '{}'",
        target_table.replace('\'', "''")
    );
    let mut schema_stmt = db
        .prepare(&schema_sql)
        .map_err(|e| format!("discover schema prepare: {}", e))?;
    let schema_rows: Vec<String> = schema_stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|e| format!("discover schema query: {}", e))?
        .filter_map(|r| r.ok())
        .collect();

    if schema_rows.is_empty() {
        return Err(format!(
            "table '{}' not found in DuckLake catalog",
            target_table
        ));
    }

    let lake_schema = schema_rows
        .iter()
        .find(|s| s.as_str() == target_schema)
        .cloned()
        .ok_or_else(|| {
            format!(
                "schema '{}' not found for table '{}' in DuckLake catalog (found: {:?})",
                target_schema, target_table, schema_rows
            )
        })?;

    // Get column names in ordinal order
    let cols_sql = format!(
        "SELECT column_name FROM information_schema.columns \
         WHERE table_catalog = 'lake' AND table_schema = '{}' AND table_name = '{}' \
         ORDER BY ordinal_position",
        lake_schema.replace('\'', "''"),
        target_table.replace('\'', "''")
    );
    let mut cols_stmt = db
        .prepare(&cols_sql)
        .map_err(|e| format!("discover columns prepare: {}", e))?;
    let col_names: Vec<String> = cols_stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|e| format!("discover columns query: {}", e))?
        .filter_map(|r| r.ok())
        .collect();

    if col_names.is_empty() {
        return Err(format!(
            "no columns found for {}.{} in DuckLake catalog",
            lake_schema, target_table
        ));
    }

    let quoted_cols: Vec<String> = col_names
        .iter()
        .map(|n| format!("\"{}\"", n.replace('"', "\"\"")))
        .collect();
    let cols_list = quoted_cols.join(", ");

    let target_ref = format!(
        "lake.\"{}\".\"{}\"",
        lake_schema.replace('"', "\"\""),
        target_table.replace('"', "\"\"")
    );

    // Single transaction: DELETE + all chunk INSERTs + COMMIT
    db.execute_batch("BEGIN")
        .map_err(|e| format!("duckdb begin: {}", e))?;

    let delete_sql = format!("DELETE FROM {}", target_ref);
    db.execute_batch(&delete_sql).map_err(|e| {
        let _ = db.execute_batch("ROLLBACK");
        format!("duckdb delete: {}", e)
    })?;

    let mut total_rows: u64 = 0;
    let mut chunk_count: u32 = 0;

    loop {
        match chunk_rx.blocking_recv() {
            Some(ChunkMessage::Ready(path)) => {
                chunk_count += 1;
                let t_chunk = if timing { Some(Instant::now()) } else { None };

                let insert_sql = format!(
                    "INSERT INTO {} ({}) SELECT {} FROM read_csv('{}', header=true, nullstr='__DUCKPIPE_NULL__')",
                    target_ref,
                    cols_list,
                    cols_list,
                    path.replace('\'', "''")
                );

                let rows = db.execute(&insert_sql, []).map_err(|e| {
                    let _ = db.execute_batch("ROLLBACK");
                    // Clean up chunk file on error
                    let _ = fs::remove_file(&path);
                    format!("duckdb insert chunk {}: {}", chunk_count, e)
                })?;

                total_rows += rows as u64;

                // Remove chunk file after successful load
                let _ = fs::remove_file(&path);

                if let Some(t) = t_chunk {
                    tracing::debug!(
                        "DuckPipe: snapshot chunk {} loaded: {} rows, {:.1}ms",
                        chunk_count,
                        rows,
                        t.elapsed().as_secs_f64() * 1000.0
                    );
                }
            }
            Some(ChunkMessage::Done) => break,
            None => {
                // Channel closed unexpectedly — producer failed
                let _ = db.execute_batch("ROLLBACK");
                return Err("producer channel closed unexpectedly".to_string());
            }
        }
    }

    db.execute_batch("COMMIT").map_err(|e| {
        let _ = db.execute_batch("ROLLBACK");
        format!("duckdb commit: {}", e)
    })?;

    if let Some(t) = t_start {
        tracing::info!(
            "DuckPipe timing: action=snapshot_duckdb_consumer target={}.{} rows={} chunks={} elapsed_ms={:.1}",
            target_schema,
            target_table,
            total_rows,
            chunk_count,
            t.elapsed().as_secs_f64() * 1000.0
        );
    }

    Ok(total_rows)
}

/// Query DuckDB's configured temp_directory for placing chunk files.
/// Falls back to the system temp directory if DuckDB returns an empty string.
fn duckdb_temp_directory() -> std::path::PathBuf {
    if let Ok(db) = Connection::open_in_memory() {
        if let Ok(mut stmt) = db.prepare("SELECT current_setting('temp_directory')") {
            if let Ok(dir) = stmt.query_row([], |row| row.get::<_, String>(0)) {
                if !dir.is_empty() {
                    return std::path::PathBuf::from(dir);
                }
            }
        }
    }
    std::env::temp_dir()
}

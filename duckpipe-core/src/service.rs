//! DuckPipe service — async WAL processing with decoupled per-table flush threads.
//!
//! Architecture:
//! 1. Slot consumer: streaming replication via `START_REPLICATION` (near-zero latency)
//! 2. Flush coordinator: decoupled producer-consumer with self-triggered flush threads
//!    (no synchronous barrier between WAL processing and flush)
//! 3. Checkpoint: crash-safe StandbyStatusUpdate with min(applied_lsn) from PG metadata
//! 4. Backpressure: WAL consumption pauses when total queued changes exceed threshold

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tokio_postgres::Client;

use crate::decoder::{
    extract_key_values, parse_relation_message, parse_tuple_data, read_byte, read_i32, read_i64,
};
use crate::flush_coordinator::{FlushCoordinator, FlushThreadResult};
use crate::metadata::MetadataClient;
use crate::slot_consumer::SlotConsumer;
use crate::snapshot;
use crate::types::{Change, ChangeType, RelCacheEntry, SyncGroup, TableMapping};

/// Configuration for the DuckPipe service.
#[derive(Debug, Clone)]
pub struct ServiceConfig {
    pub poll_interval_ms: i32,
    pub batch_size_per_group: i32,
    pub debug_log: bool,
    /// Connection string for flush workers to create their own connections.
    pub connstr: String,
    /// DuckDB-style PostgreSQL connection string for DuckLake ATTACH
    pub duckdb_pg_connstr: String,
    /// DuckLake metadata schema name (e.g., "ducklake")
    pub ducklake_schema: String,
    /// Flush interval in ms for self-triggered flush (default 1000)
    pub flush_interval_ms: i32,
    /// Number of queued changes that triggers an immediate flush (default 10000)
    pub flush_batch_threshold: i32,
    /// Maximum total queued changes before backpressure pauses WAL consumption (default 500000)
    pub max_queued_changes: i32,
}

/// How often the per-slot relation cache refreshes `enabled` (and `state`) from PG.
///
/// `state` transitions initiated by the service itself are mirrored in-place
/// immediately, so this interval only needs to catch external changes — e.g. a
/// user toggling `enabled` via direct SQL.  30 s gives responsive behaviour
/// without adding per-round metadata queries.
const ENABLED_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

/// Result of processing one sync group round.
pub struct ProcessResult {
    pub total_processed: i32,
    pub any_work: bool,
    /// Crash-safe confirmed LSN: min(applied_lsn) across all active tables.
    /// Safe to report to the replication slot via StandbyStatusUpdate.
    pub confirmed_lsn: u64,
}

/// Relation cache entry with optional cached mapping.
struct RelCacheWithMapping {
    entry: RelCacheEntry,
    mapping: Option<TableMapping>,
}

/// Persistent per-slot state: consumer + relation cache.
///
/// The relation cache must persist across cycles because with a persistent
/// connection the server only sends Relation messages once per relation.
/// Subsequent Update/Delete/Insert messages reference already-sent relation IDs.
pub struct SlotState {
    pub consumer: SlotConsumer,
    rel_cache: HashMap<u32, RelCacheWithMapping>,
    /// When the cache last did a periodic refresh of `enabled`/`state` from PG.
    last_enabled_check: Instant,
}

/// Helper: ensure a coordinator queue exists for a target table, loading metadata if needed.
async fn ensure_coordinator_queue(
    coordinator: &mut FlushCoordinator,
    target_key: &str,
    mapping: &TableMapping,
    entry: &RelCacheEntry,
    meta: &MetadataClient<'_>,
) -> Result<(), String> {
    let (attnames, key_attrs, atttypes) = if !entry.attnames.is_empty() {
        (
            entry.attnames.clone(),
            entry.attkeys.clone(),
            entry.atttypes.clone(),
        )
    } else {
        let (names, keys) = meta
            .load_batch_metadata_from_source(&mapping.source_schema, &mapping.source_table)
            .await
            .map_err(|e| format!("load_batch_metadata: {}", e))?;
        let types = vec![0u32; names.len()]; // unknown type → Text fallback
        (names, keys, types)
    };
    coordinator.ensure_queue(target_key, mapping.id, mapping.applied_lsn, attnames, key_attrs, atttypes);
    Ok(())
}

/// Process one sync group using streaming replication for WAL consumption.
///
/// Reads WAL via `SlotConsumer` (START_REPLICATION protocol), decodes, dispatches,
/// flushes, and sends StandbyStatusUpdate. Near-zero latency.
async fn process_sync_group_streaming(
    client: &Client,
    meta: &MetadataClient<'_>,
    group: &mut SyncGroup,
    config: &ServiceConfig,
    consumer: &mut SlotConsumer,
    coordinator: &mut FlushCoordinator,
    rel_cache: &mut HashMap<u32, RelCacheWithMapping>,
) -> Result<ProcessResult, String> {
    let timing = config.debug_log;
    let group_start = if timing { Some(Instant::now()) } else { None };

    // Check backpressure — if flush threads are lagging behind, skip this poll round
    if coordinator.is_backpressured() {
        // Still compute confirmed_lsn and advance slot even when backpressured.
        // Prefer in-memory state (consistent with the main confirmed_lsn path);
        // fall back to PG query when coordinator has no tables yet.
        let min_lsn = {
            let in_mem = coordinator.get_min_applied_lsn_in_coordinator();
            if in_mem > 0 {
                in_mem
            } else {
                meta.get_min_applied_lsn(group.id).await.unwrap_or(0)
            }
        };
        if min_lsn > 0 {
            let _ = meta.update_confirmed_lsn(group.id, min_lsn).await;
            consumer.send_status_update(min_lsn);
        }
        return Ok(ProcessResult {
            total_processed: 0,
            any_work: false,
            confirmed_lsn: min_lsn,
        });
    }

    // Poll for WAL messages from the streaming consumer.
    // Wait up to poll_interval for the first message, then drain quickly.
    let timeout_ms = (config.poll_interval_ms / 2).max(100) as u64;
    let wal_messages = consumer
        .poll_messages(config.batch_size_per_group, timeout_ms)
        .await?;

    if wal_messages.is_empty() {
        return Ok(ProcessResult {
            total_processed: 0,
            any_work: false,
            confirmed_lsn: 0,
        });
    }

    let num_messages = wal_messages.len();

    // Process the WAL messages (shared logic)
    let result =
        process_wal_messages(client, meta, group, &wal_messages, coordinator, rel_cache).await?;

    // Send StandbyStatusUpdate to advance the replication slot.
    // With streaming, we explicitly control slot advancement (unlike SQL polling
    // which advances automatically). Use the crash-safe confirmed_lsn
    // (min of applied_lsn across all active tables) rather than pending_lsn,
    // so the slot is only advanced to the point all tables have durably flushed.
    if result.confirmed_lsn != 0 {
        consumer.send_status_update(result.confirmed_lsn);
    }

    if let Some(start) = group_start {
        eprintln!(
            "DuckPipe timing: action=process_sync_group_streaming group={} slot={} processed_changes={} fetched_messages={} elapsed_ms={:.3}",
            group.name,
            group.slot_name,
            result.total_processed,
            num_messages,
            start.elapsed().as_secs_f64() * 1000.0
        );
    }

    Ok(result)
}

/// Core WAL message processing: decode pgoutput binary messages, dispatch to
/// per-table queues via FlushCoordinator, drain+flush at cycle end,
/// transition CATCHUP → STREAMING, update confirmed_lsn.
async fn process_wal_messages(
    client: &Client,
    meta: &MetadataClient<'_>,
    group: &mut SyncGroup,
    wal_messages: &[(u64, Vec<u8>)],
    coordinator: &mut FlushCoordinator,
    rel_cache: &mut HashMap<u32, RelCacheWithMapping>,
) -> Result<ProcessResult, String> {
    let mut total_processed: i32 = 0;

    for (lsn, data) in wal_messages.iter() {
        if data.is_empty() {
            continue;
        }

        let mut cursor: usize = 0;
        let msgtype = read_byte(data, &mut cursor) as char;

        match msgtype {
            'R' => {
                let (rel_id, entry) = parse_relation_message(data, &mut cursor);
                rel_cache.insert(
                    rel_id,
                    RelCacheWithMapping {
                        entry,
                        mapping: None,
                    },
                );
            }
            'I' => {
                let rel_id = read_i32(data, &mut cursor) as u32;
                let _new_marker = read_byte(data, &mut cursor);
                let atttypes = rel_cache
                    .get(&rel_id)
                    .map(|c| c.entry.atttypes.as_slice())
                    .unwrap_or(&[]);
                let (values, _unchanged) = parse_tuple_data(data, &mut cursor, atttypes);

                if let Some(cached) = rel_cache.get_mut(&rel_id) {
                    if cached.mapping.is_none() {
                        cached.mapping =
                            resolve_mapping(meta, group.id, rel_id, &cached.entry).await?;
                    }
                    if let Some(ref mapping) = cached.mapping {
                        if !mapping.enabled || mapping.state == "ERRORED" {
                            total_processed += 1;
                            continue;
                        }
                        if mapping.state == "CATCHUP"
                            && mapping.snapshot_lsn != 0
                            && *lsn <= mapping.snapshot_lsn
                        {
                            total_processed += 1;
                            continue;
                        }

                        let target_key =
                            format!("{}.{}", mapping.target_schema, mapping.target_table);
                        ensure_coordinator_queue(
                            coordinator,
                            &target_key,
                            mapping,
                            &cached.entry,
                            meta,
                        )
                        .await?;

                        coordinator.push_change(
                            &target_key,
                            Change {
                                change_type: ChangeType::Insert,
                                lsn: *lsn,
                                col_values: values,
                                key_values: Vec::new(),
                                col_unchanged: Vec::new(),
                            },
                        );
                    }
                }
            }
            'U' => {
                let rel_id = read_i32(data, &mut cursor) as u32;
                let atttypes = rel_cache
                    .get(&rel_id)
                    .map(|c| c.entry.atttypes.as_slice())
                    .unwrap_or(&[]);
                let marker = read_byte(data, &mut cursor) as char;
                let (old_values, has_old) = if marker == 'K' || marker == 'O' {
                    let (vals, _) = parse_tuple_data(data, &mut cursor, atttypes);
                    let _new_marker = read_byte(data, &mut cursor);
                    (vals, true)
                } else {
                    (Vec::new(), false)
                };

                let (new_values, new_unchanged) = parse_tuple_data(data, &mut cursor, atttypes);
                let has_unchanged = new_unchanged.iter().any(|&u| u);

                if let Some(cached) = rel_cache.get_mut(&rel_id) {
                    if cached.mapping.is_none() {
                        cached.mapping =
                            resolve_mapping(meta, group.id, rel_id, &cached.entry).await?;
                    }
                    if let Some(ref mapping) = cached.mapping {
                        if !mapping.enabled || mapping.state == "ERRORED" {
                            total_processed += 1;
                            continue;
                        }
                        if mapping.state == "CATCHUP"
                            && mapping.snapshot_lsn != 0
                            && *lsn <= mapping.snapshot_lsn
                        {
                            total_processed += 1;
                            continue;
                        }

                        let target_key =
                            format!("{}.{}", mapping.target_schema, mapping.target_table);
                        ensure_coordinator_queue(
                            coordinator,
                            &target_key,
                            mapping,
                            &cached.entry,
                            meta,
                        )
                        .await?;

                        let key_values = if has_old {
                            old_values
                        } else {
                            extract_key_values(&new_values, &cached.entry.attkeys)
                        };

                        if has_unchanged {
                            coordinator.push_change(
                                &target_key,
                                Change {
                                    change_type: ChangeType::Update,
                                    lsn: *lsn,
                                    col_values: new_values,
                                    key_values,
                                    col_unchanged: new_unchanged,
                                },
                            );
                        } else {
                            coordinator.push_change(
                                &target_key,
                                Change {
                                    change_type: ChangeType::Delete,
                                    lsn: *lsn,
                                    col_values: Vec::new(),
                                    key_values: key_values.clone(),
                                    col_unchanged: Vec::new(),
                                },
                            );
                            coordinator.push_change(
                                &target_key,
                                Change {
                                    change_type: ChangeType::Insert,
                                    lsn: *lsn,
                                    col_values: new_values,
                                    key_values: Vec::new(),
                                    col_unchanged: Vec::new(),
                                },
                            );
                        }
                    }
                }
            }
            'D' => {
                let rel_id = read_i32(data, &mut cursor) as u32;
                let atttypes = rel_cache
                    .get(&rel_id)
                    .map(|c| c.entry.atttypes.as_slice())
                    .unwrap_or(&[]);
                let _marker = read_byte(data, &mut cursor);
                let (old_values, _) = parse_tuple_data(data, &mut cursor, atttypes);

                if let Some(cached) = rel_cache.get_mut(&rel_id) {
                    if cached.mapping.is_none() {
                        cached.mapping =
                            resolve_mapping(meta, group.id, rel_id, &cached.entry).await?;
                    }
                    if let Some(ref mapping) = cached.mapping {
                        if !mapping.enabled || mapping.state == "ERRORED" {
                            total_processed += 1;
                            continue;
                        }
                        if mapping.state == "CATCHUP"
                            && mapping.snapshot_lsn != 0
                            && *lsn <= mapping.snapshot_lsn
                        {
                            total_processed += 1;
                            continue;
                        }

                        let target_key =
                            format!("{}.{}", mapping.target_schema, mapping.target_table);
                        ensure_coordinator_queue(
                            coordinator,
                            &target_key,
                            mapping,
                            &cached.entry,
                            meta,
                        )
                        .await?;

                        coordinator.push_change(
                            &target_key,
                            Change {
                                change_type: ChangeType::Delete,
                                lsn: *lsn,
                                col_values: Vec::new(),
                                key_values: old_values,
                                col_unchanged: Vec::new(),
                            },
                        );
                    }
                }
            }
            'B' => {
                let _final_lsn = read_i64(data, &mut cursor) as u64;
                let _commit_time = read_i64(data, &mut cursor);
                let _xid = read_i32(data, &mut cursor);
            }
            'C' => {
                let _flags = read_byte(data, &mut cursor);
                let _commit_lsn = read_i64(data, &mut cursor) as u64;
                let end_lsn = read_i64(data, &mut cursor) as u64;
                let _commit_time = read_i64(data, &mut cursor);
                group.pending_lsn = end_lsn;
            }
            'T' => {
                // TRUNCATE — flush per-table pending queues, then DELETE all from targets.
                // Uses DELETE FROM instead of TRUNCATE because DuckLake tables
                // (via pg_duckdb) silently ignore TRUNCATE — it succeeds but
                // doesn't actually remove rows.
                let nrels = read_i32(data, &mut cursor);
                let _options = read_byte(data, &mut cursor);

                for _ in 0..nrels {
                    let rel_id = read_i32(data, &mut cursor) as u32;
                    if let Some(cached) = rel_cache.get(&rel_id) {
                        let mapping =
                            resolve_mapping(meta, group.id, rel_id, &cached.entry).await?;
                        if let Some(mapping) = mapping {
                            if mapping.enabled && mapping.state != "ERRORED" {
                                let target_key =
                                    format!("{}.{}", mapping.target_schema, mapping.target_table);
                                // Drain this table's queue before DELETE
                                coordinator.drain_and_wait_table(&target_key);

                                let delete_sql = format!(
                                    "DELETE FROM \"{}\".\"{}\"",
                                    mapping.target_schema.replace('"', "\"\""),
                                    mapping.target_table.replace('"', "\"\"")
                                );
                                if let Err(e) = client.execute(&delete_sql, &[]).await {
                                    eprintln!(
                                        "Failed to clear target table {}.{}: {}",
                                        mapping.target_schema, mapping.target_table, e
                                    );
                                }
                            }
                        }
                    }
                }
            }
            _ => {}
        }

        total_processed += 1;
    }

    // Drain flush results and update in-memory per-table LSN state.
    // Must happen before confirmed_lsn computation — the ordering guarantee:
    // flush threads commit applied_lsn to PG *before* sending the mpsc result,
    // so any Success result here means PG is already up-to-date for that table.
    for r in &coordinator.collect_results() {
        if let FlushThreadResult::Error { target_key, error, mapping_id, .. } = r {
            eprintln!("pg_duckpipe: flush error for {}: {}", target_key, error);
            // Invalidate the cached mapping so the next WAL event for this table
            // re-fetches state from PG — it may have transitioned to ERRORED.
            for cached in rel_cache.values_mut() {
                if cached.mapping.as_ref().map_or(false, |m| &m.id == mapping_id) {
                    cached.mapping = None;
                    break;
                }
            }
        }
    }

    // Auto-retry ERRORED tables whose retry_at has passed
    if let Ok(retryable) = meta.get_retryable_errored_tables(group.id).await {
        for table in &retryable {
            if let Err(e) = meta.retry_errored_table(table.id).await {
                eprintln!(
                    "pg_duckpipe: failed to retry errored table {}.{}: {}",
                    table.source_schema, table.source_table, e
                );
            } else {
                eprintln!(
                    "pg_duckpipe: auto-retrying errored table {}.{}",
                    table.source_schema, table.source_table
                );
                // Mirror ERRORED → STREAMING in the rel_cache so routing resumes
                // immediately without waiting for the next periodic refresh.
                for cached in rel_cache.values_mut() {
                    if let Some(ref mut mapping) = cached.mapping {
                        if mapping.id == table.id {
                            mapping.state = "STREAMING".to_string();
                            break;
                        }
                    }
                }
            }
        }
    }

    // Transition CATCHUP → STREAMING
    if group.pending_lsn != 0 {
        meta.transition_catchup_to_streaming(group.id, group.pending_lsn)
            .await
            .map_err(|e| format!("transition_catchup: {}", e))?;

        // Mirror the transition in the rel_cache so routing reflects the new state
        // immediately without waiting for the next periodic refresh.
        for cached in rel_cache.values_mut() {
            if let Some(ref mut mapping) = cached.mapping {
                if mapping.state == "CATCHUP"
                    && mapping.snapshot_lsn != 0
                    && mapping.snapshot_lsn <= group.pending_lsn
                {
                    mapping.state = "STREAMING".to_string();
                }
            }
        }
    }

    // Update confirmed_lsn using the in-memory per-table LSN state for crash-safe slot
    // advancement.  All active STREAMING/CATCHUP tables are seeded into per_table_lsn
    // at cycle start (via seed_table_lsns), so the min covers all tables regardless of
    // whether they received changes this cycle.
    //
    // Fall back to the PG query when the coordinator has no tables yet (e.g., first
    // cycle after startup before any changes arrive, or all tables just errored out).
    let mut safe_confirmed_lsn: u64 = 0;
    if total_processed > 0 && group.pending_lsn != 0 {
        let min_lsn = {
            let in_mem = coordinator.get_min_applied_lsn_in_coordinator();
            if in_mem > 0 {
                in_mem
            } else {
                // No in-memory state yet — read from PG (startup / post-clear path).
                meta.get_min_applied_lsn(group.id)
                    .await
                    .map_err(|e| format!("get_min_applied_lsn: {}", e))?
            }
        };
        if min_lsn > 0 {
            meta.update_confirmed_lsn(group.id, min_lsn)
                .await
                .map_err(|e| format!("update_confirmed_lsn: {}", e))?;
            safe_confirmed_lsn = min_lsn;
        }
    }

    Ok(ProcessResult {
        total_processed,
        any_work: total_processed > 0,
        confirmed_lsn: safe_confirmed_lsn,
    })
}

/// Helper: resolve table mapping for a relation, with OID fallback for rename safety.
/// Tries name-based lookup first. If not found, falls back to OID-based lookup.
async fn resolve_mapping(
    meta: &MetadataClient<'_>,
    group_id: i32,
    rel_id: u32,
    entry: &RelCacheEntry,
) -> Result<Option<TableMapping>, String> {
    // Try name-based lookup first
    let mapping = meta
        .get_table_mapping(group_id, &entry.nspname, &entry.relname)
        .await
        .map_err(|e| format!("get_table_mapping: {}", e))?;

    if mapping.is_some() {
        return Ok(mapping);
    }

    // Fallback: OID-based lookup (handles table renames)
    let mapping = meta
        .get_table_mapping_by_oid(group_id, rel_id as i64)
        .await
        .map_err(|e| format!("get_table_mapping_by_oid: {}", e))?;

    if let Some(ref m) = mapping {
        // Update source name in metadata to reflect the rename
        let _ = meta
            .update_source_name(m.id, &entry.nspname, &entry.relname)
            .await;
    }

    Ok(mapping)
}

// ---------------------------------------------------------------------------
// Shared sync cycle — used by both PG background worker and standalone daemon
// ---------------------------------------------------------------------------

/// Parameters for connecting the streaming replication consumer.
pub enum SlotConnectParams {
    /// Unix socket connection (used by PG background worker).
    Unix {
        socket_dir: String,
        port: u16,
        user: String,
        dbname: String,
    },
    /// TCP connection (used by standalone daemon).
    Tcp {
        host: String,
        port: u16,
        user: String,
        password: String,
        dbname: String,
    },
}

/// Connect a SlotConsumer using the appropriate transport.
async fn connect_slot_consumer(
    params: &SlotConnectParams,
    slot_name: &str,
    publication: &str,
    confirmed_lsn: u64,
) -> Result<SlotConsumer, String> {
    match params {
        SlotConnectParams::Unix {
            socket_dir,
            port,
            user,
            dbname,
        } => {
            SlotConsumer::connect(
                socket_dir,
                *port,
                user,
                dbname,
                slot_name,
                publication,
                confirmed_lsn,
            )
            .await
        }
        SlotConnectParams::Tcp {
            host,
            port,
            user,
            password,
            dbname,
        } => {
            SlotConsumer::connect_tcp(
                host,
                *port,
                user,
                password,
                dbname,
                slot_name,
                publication,
                confirmed_lsn,
            )
            .await
        }
    }
}

/// Process snapshot tasks for a sync group.
///
/// Spawns all snapshot tasks concurrently, each with its own connection.
/// Results are recorded in metadata (set_catchup_state on success,
/// record_error_message on failure).
async fn process_snapshots(
    meta: &MetadataClient<'_>,
    group: &SyncGroup,
    connstr: &str,
    timing: bool,
) -> Result<(), String> {
    let snapshot_tasks = meta
        .get_snapshot_tasks(group.id)
        .await
        .map_err(|e| format!("get_snapshot_tasks: {}", e))?;

    if snapshot_tasks.is_empty() {
        return Ok(());
    }

    let mut handles = Vec::new();
    for task in &snapshot_tasks {
        let task_id = task.id;
        let src_schema = task.source_schema.clone();
        let src_table = task.source_table.clone();
        let tgt_schema = task.target_schema.clone();
        let tgt_table = task.target_table.clone();
        let cs = connstr.to_string();

        let handle = tokio::spawn(async move {
            let result = snapshot::process_snapshot_task(
                &src_schema, &src_table, &tgt_schema, &tgt_table, &cs, timing, task_id,
            )
            .await;
            snapshot::SnapshotResult {
                task_id,
                source_schema: src_schema,
                source_table: src_table,
                result,
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        match handle.await {
            Ok(snap_result) => match snap_result.result {
                Ok(snapshot_lsn) => {
                    meta.set_catchup_state(snap_result.task_id, snapshot_lsn)
                        .await
                        .map_err(|e| format!("set_catchup_state: {}", e))?;
                }
                Err(e) => {
                    let _ = meta
                        .record_error_message(snap_result.task_id, &e)
                        .await;
                    tracing::warn!(
                        "DuckPipe: snapshot failed for {}.{}: {}",
                        snap_result.source_schema,
                        snap_result.source_table,
                        e
                    );
                }
            },
            Err(e) => {
                tracing::warn!("DuckPipe: snapshot task panicked: {}", e);
            }
        }
    }

    Ok(())
}

/// Run one complete sync cycle with persistent slot connections.
///
/// Shared by both the PG background worker and the standalone daemon.
/// `consumers` holds long-lived `SlotState` (consumer + relation cache) keyed by slot name.
/// On error, the failed consumer is removed (drop = close); the next cycle
/// reconnects from `confirmed_lsn` (the crash-safe restart point from PG).
/// Returns whether any work was done (callers use this to decide whether to sleep).
pub async fn run_sync_cycle(
    config: &ServiceConfig,
    coordinator: &mut FlushCoordinator,
    slot_params: &SlotConnectParams,
    consumers: &mut HashMap<String, SlotState>,
) -> Result<bool, String> {
    // Establish metadata connection (short-lived per cycle)
    // TODO tls mode
    let (client, connection) = tokio_postgres::connect(&config.connstr, tokio_postgres::NoTls)
        .await
        .map_err(|e| format!("connect: {}", e))?;

    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!("DuckPipe: metadata connection error: {}", e);
        }
    });

    let meta = MetadataClient::new(&client);

    // Get all enabled sync groups
    let mut groups = meta
        .get_enabled_sync_groups()
        .await
        .map_err(|e| format!("get_sync_groups: {}", e))?;

    let mut any_work = false;

    for group in groups.iter_mut() {
        // Process snapshots — spawn all as parallel tasks
        process_snapshots(&meta, group, &config.connstr, config.debug_log).await?;

        // Seed per_table_lsn with all active (STREAMING/CATCHUP) tables from PG.
        // This ensures CATCHUP tables that received no WAL changes this session are
        // still visible to get_min_applied_lsn_in_coordinator(), preventing unsafe
        // confirmed_lsn advancement past their needs.
        if let Ok(active_lsns) = meta.get_active_table_lsns(group.id).await {
            coordinator.seed_table_lsns(&active_lsns);
        }

        // Drain flush results from the previous cycle.  This updates per_table_lsn
        // (flushed state) and logs errors.  Done before both the confirmed_lsn
        // persistence step and the slot connection.  Results are saved so we can
        // later do targeted rel_cache invalidation for errored mappings.
        let prev_results = coordinator.collect_results();
        for r in &prev_results {
            if let FlushThreadResult::Error { target_key, error, .. } = r {
                eprintln!("pg_duckpipe: flush error for {}: {}", target_key, error);
            }
        }

        // Persist the latest flushed position to PG (for crash recovery and WAL
        // retention via StandbyStatusUpdate).  This is purely a bookkeeping update —
        // it does NOT affect where the slot connects this cycle.
        let fresh_flushed = coordinator.get_min_applied_lsn_in_coordinator();
        if fresh_flushed > group.confirmed_lsn {
            group.confirmed_lsn = fresh_flushed;
            if let Err(e) = meta.update_confirmed_lsn(group.id, fresh_flushed).await {
                eprintln!("pg_duckpipe: failed to persist confirmed_lsn for group {}: {}", group.id, e);
            }
        }

        // Reuse persistent consumer or create a new one.
        // Always start from confirmed_lsn — the crash-safe restart point from PG.
        // Re-reading a small WAL window is cheap and avoids needing in-memory consumed_lsn.
        let slot_start_lsn = group.confirmed_lsn;

        let needs_connect = match consumers.get(&group.slot_name) {
            Some(s) => !s.consumer.is_connected(),
            None => true,
        };
        if needs_connect {
            // Remove dead state if present (drop = close connection, clear rel_cache)
            consumers.remove(&group.slot_name);
            let consumer = connect_slot_consumer(
                slot_params,
                &group.slot_name,
                &group.publication,
                slot_start_lsn,
            )
            .await
            .map_err(|e| format!("streaming connect failed for {}: {}", group.slot_name, e))?;
            consumers.insert(group.slot_name.clone(), SlotState {
                consumer,
                rel_cache: HashMap::new(),
                // Fresh connection — cache is empty, so no immediate refresh needed.
                last_enabled_check: Instant::now(),
            });
        }

        let slot_state = consumers.get_mut(&group.slot_name).unwrap();

        // Targeted invalidation: mappings that had flush errors may have transitioned
        // to ERRORED in PG.  Clearing only those entries forces a re-fetch before the
        // next WAL event for those tables, so routing picks up the new state promptly.
        for r in &prev_results {
            if let FlushThreadResult::Error { mapping_id, .. } = r {
                for cached in slot_state.rel_cache.values_mut() {
                    if cached.mapping.as_ref().map_or(false, |m| &m.id == mapping_id) {
                        cached.mapping = None;
                        break;
                    }
                }
            }
        }

        // Periodic refresh of `enabled` + `state` for all cached mappings.
        // State transitions owned by the service (CATCHUP→STREAMING, retries, flush
        // errors) are mirrored in-place as they happen, so this only needs to catch
        // external changes such as a user toggling `enabled` via direct SQL.
        if slot_state.last_enabled_check.elapsed() >= ENABLED_REFRESH_INTERVAL {
            let ids: Vec<i32> = slot_state.rel_cache.values()
                .filter_map(|c| c.mapping.as_ref().map(|m| m.id))
                .collect();
            if !ids.is_empty() {
                match meta.get_mapping_enabled_states(&ids).await {
                    Ok(meta_map) => {
                        for cached in slot_state.rel_cache.values_mut() {
                            if let Some(ref mut mapping) = cached.mapping {
                                if let Some((enabled, state_str)) = meta_map.get(&mapping.id) {
                                    mapping.enabled = *enabled;
                                    mapping.state = state_str.clone();
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!(
                            "pg_duckpipe: periodic enabled/state refresh failed for slot {}: {}",
                            group.slot_name, e
                        );
                    }
                }
            }
            slot_state.last_enabled_check = Instant::now();
        }

        let result = match process_sync_group_streaming(
            &client, &meta, group, config, &mut slot_state.consumer, coordinator,
            &mut slot_state.rel_cache,
        )
        .await
        {
            Ok(r) => r,
            Err(e) => {
                // Remove failed state — next cycle reconnects from confirmed_lsn
                consumers.remove(&group.slot_name);
                return Err(format!("process_sync_group_streaming: {}", e));
            }
        };

        if result.any_work {
            any_work = true;
        }
    }

    // Update worker runtime state for observability
    let _ = meta
        .update_worker_state(coordinator.total_queued(), coordinator.is_backpressured())
        .await;

    // Clean up metadata connection
    drop(client);
    let _ = conn_handle.await;

    Ok(any_work)
}

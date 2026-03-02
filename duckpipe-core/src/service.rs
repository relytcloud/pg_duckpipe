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
use crate::snapshot_manager::SnapshotManager;
use crate::types::{Change, ChangeType, RelCacheEntry, SyncGroup, TableMapping, Value};

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

/// Heartbeat triggers: bookkeeping (flush results, auto-retry, CATCHUP→STREAMING,
/// confirmed_lsn update) runs every N commits OR every T ms, whichever comes first.
const HEARTBEAT_COMMITS: u32 = 10_000;
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(500);

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
    /// Real PK column indices from `pg_index` (loaded once, cached).
    /// With REPLICA IDENTITY FULL, `entry.attkeys` includes ALL columns,
    /// but this field contains only the actual PRIMARY KEY columns.
    pk_key_attrs: Option<Vec<usize>>,
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
///
/// Also populates `pk_key_attrs_cache` (real PK columns from `pg_index`) on first
/// call for this relation.  The cache avoids a catalog query on every WAL message.
async fn ensure_coordinator_queue(
    coordinator: &mut FlushCoordinator,
    target_key: &str,
    mapping: &TableMapping,
    entry: &RelCacheEntry,
    pk_key_attrs_cache: &mut Option<Vec<usize>>,
    meta: &MetadataClient<'_>,
) -> Result<(), String> {
    // Load the *real* PK columns from pg_index on the first call, then cache.
    // With REPLICA IDENTITY FULL, pgoutput marks ALL columns as key (flags & 1),
    // so `entry.attkeys` would be [0, 1, ..., ncols-1].  Using that as
    // `key_attrs` causes the flush DELETE WHERE clause to match on ALL columns,
    // which is both wasteful and broken for types whose SQL literal format
    // doesn't round-trip (e.g., DECIMAL, TIMESTAMP).
    //
    // Use the RELATION message's name (`entry.nspname`/`entry.relname`) for the
    // pg_index lookup, not `mapping.source_table`.  After ALTER TABLE RENAME the
    // in-memory mapping may still carry the old name (update_source_name writes
    // to the DB but doesn't mutate the cached mapping struct), so a lookup by
    // `mapping.source_table` would fail to find the renamed table in pg_class.
    let key_attrs = if let Some(ref cached) = pk_key_attrs_cache {
        cached.clone()
    } else {
        let (_, attrs) = meta
            .load_batch_metadata_from_source(&entry.nspname, &entry.relname)
            .await
            .map_err(|e| format!("load_batch_metadata (pk): {}", e))?;
        *pk_key_attrs_cache = Some(attrs.clone());
        attrs
    };

    let (attnames, atttypes) = if !entry.attnames.is_empty() {
        (entry.attnames.clone(), entry.atttypes.clone())
    } else {
        let (names, _keys) = meta
            .load_batch_metadata_from_source(&entry.nspname, &entry.relname)
            .await
            .map_err(|e| format!("load_batch_metadata: {}", e))?;
        let types = vec![0u32; names.len()]; // unknown type → Text fallback
        (names, types)
    };

    let paused = mapping.state == "SNAPSHOT";
    coordinator.ensure_queue(
        target_key,
        mapping.id,
        mapping.applied_lsn,
        attnames,
        key_attrs,
        atttypes,
        paused,
    );
    Ok(())
}

/// Decode and dispatch a single pgoutput WAL message to the flush coordinator.
///
/// Returns `true` if the message was a COMMIT (used by the caller to trigger
/// periodic heartbeats). All other message types return `false`.
async fn process_one_wal_message(
    client: &Client,
    meta: &MetadataClient<'_>,
    group: &mut SyncGroup,
    lsn: u64,
    data: &[u8],
    coordinator: &mut FlushCoordinator,
    rel_cache: &mut HashMap<u32, RelCacheWithMapping>,
) -> Result<bool, String> {
    if data.is_empty() {
        return Ok(false);
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
                    pk_key_attrs: None,
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
                    cached.mapping = resolve_mapping(meta, group.id, rel_id, &cached.entry).await?;
                }
                if let Some(ref mapping) = cached.mapping {
                    if !mapping.enabled || mapping.state == "ERRORED" {
                        return Ok(false);
                    }
                    if mapping.state == "CATCHUP"
                        && mapping.snapshot_lsn != 0
                        && lsn <= mapping.snapshot_lsn
                    {
                        return Ok(false);
                    }
                    let target_key = format!("{}.{}", mapping.target_schema, mapping.target_table);
                    ensure_coordinator_queue(
                        coordinator,
                        &target_key,
                        mapping,
                        &cached.entry,
                        &mut cached.pk_key_attrs,
                        meta,
                    )
                    .await?;
                    coordinator.push_change(
                        &target_key,
                        Change {
                            change_type: ChangeType::Insert,
                            lsn,
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
            // 'O' = old full row (REPLICA IDENTITY FULL)
            // 'K' = old key columns only (default replica identity)
            // 'N' = no old tuple (marker is already the new-tuple marker)
            let (old_values, old_marker) = if marker == 'K' || marker == 'O' {
                let (vals, _) = parse_tuple_data(data, &mut cursor, atttypes);
                let _new_marker = read_byte(data, &mut cursor);
                (vals, marker)
            } else {
                (Vec::new(), 'N')
            };
            let (new_values, new_unchanged) = parse_tuple_data(data, &mut cursor, atttypes);
            let has_unchanged = new_unchanged.iter().any(|&u| u);

            if let Some(cached) = rel_cache.get_mut(&rel_id) {
                if cached.mapping.is_none() {
                    cached.mapping = resolve_mapping(meta, group.id, rel_id, &cached.entry).await?;
                }
                if let Some(ref mapping) = cached.mapping {
                    if !mapping.enabled || mapping.state == "ERRORED" {
                        return Ok(false);
                    }
                    if mapping.state == "CATCHUP"
                        && mapping.snapshot_lsn != 0
                        && lsn <= mapping.snapshot_lsn
                    {
                        return Ok(false);
                    }
                    let target_key = format!("{}.{}", mapping.target_schema, mapping.target_table);
                    ensure_coordinator_queue(
                        coordinator,
                        &target_key,
                        mapping,
                        &cached.entry,
                        &mut cached.pk_key_attrs,
                        meta,
                    )
                    .await?;

                    // key_values for the DELETE half: extract only the real PK column
                    // values (from pg_index), not all columns from RELATION attkeys.
                    let pk_attrs = cached.pk_key_attrs.as_ref().unwrap();
                    let key_values = if !old_values.is_empty() {
                        extract_key_values(&old_values, pk_attrs)
                    } else {
                        extract_key_values(&new_values, pk_attrs)
                    };

                    if has_unchanged && old_marker == 'O' {
                        // REPLICA IDENTITY FULL: old tuple carries full row values.
                        // pgoutput still uses 'u' for unchanged TOAST columns in the NEW
                        // tuple, but we can fill them from the old tuple right here —
                        // no flush-time resolution query needed.
                        let filled_values: Vec<Value> = (0..new_values.len())
                            .map(|i| {
                                if new_unchanged.get(i).copied().unwrap_or(false) {
                                    old_values.get(i).cloned().unwrap_or(Value::Null)
                                } else {
                                    new_values[i].clone()
                                }
                            })
                            .collect();
                        coordinator.push_change(
                            &target_key,
                            Change {
                                change_type: ChangeType::Delete,
                                lsn,
                                col_values: Vec::new(),
                                key_values: key_values.clone(),
                                col_unchanged: Vec::new(),
                            },
                        );
                        coordinator.push_change(
                            &target_key,
                            Change {
                                change_type: ChangeType::Insert,
                                lsn,
                                col_values: filled_values,
                                key_values: Vec::new(),
                                col_unchanged: Vec::new(),
                            },
                        );
                    } else if has_unchanged {
                        // No full old tuple (not REPLICA IDENTITY FULL).
                        // Push Update with col_unchanged; the flush path will surface an
                        // error telling the operator to restore REPLICA IDENTITY FULL.
                        coordinator.push_change(
                            &target_key,
                            Change {
                                change_type: ChangeType::Update,
                                lsn,
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
                                lsn,
                                col_values: Vec::new(),
                                key_values: key_values.clone(),
                                col_unchanged: Vec::new(),
                            },
                        );
                        coordinator.push_change(
                            &target_key,
                            Change {
                                change_type: ChangeType::Insert,
                                lsn,
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
                    cached.mapping = resolve_mapping(meta, group.id, rel_id, &cached.entry).await?;
                }
                if let Some(ref mapping) = cached.mapping {
                    if !mapping.enabled || mapping.state == "ERRORED" {
                        return Ok(false);
                    }
                    if mapping.state == "CATCHUP"
                        && mapping.snapshot_lsn != 0
                        && lsn <= mapping.snapshot_lsn
                    {
                        return Ok(false);
                    }
                    let target_key = format!("{}.{}", mapping.target_schema, mapping.target_table);
                    ensure_coordinator_queue(
                        coordinator,
                        &target_key,
                        mapping,
                        &cached.entry,
                        &mut cached.pk_key_attrs,
                        meta,
                    )
                    .await?;
                    // Extract only PK values — with REPLICA IDENTITY FULL,
                    // old_values contains ALL columns, not just keys.
                    let pk_attrs = cached.pk_key_attrs.as_ref().unwrap();
                    let key_values = extract_key_values(&old_values, pk_attrs);
                    coordinator.push_change(
                        &target_key,
                        Change {
                            change_type: ChangeType::Delete,
                            lsn,
                            col_values: Vec::new(),
                            key_values,
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
            return Ok(true);
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
                    let mapping = resolve_mapping(meta, group.id, rel_id, &cached.entry).await?;
                    if let Some(mapping) = mapping {
                        if mapping.enabled && mapping.state != "ERRORED" {
                            let target_key =
                                format!("{}.{}", mapping.target_schema, mapping.target_table);
                            coordinator.drain_and_wait_table(&target_key);
                            let delete_sql = format!(
                                "DELETE FROM \"{}\".\"{}\"",
                                mapping.target_schema.replace('"', "\"\""),
                                mapping.target_table.replace('"', "\"\"")
                            );
                            if let Err(e) = client.execute(&delete_sql, &[]).await {
                                tracing::error!(
                                    "pg_duckpipe: failed to clear target table {}.{}: {}",
                                    mapping.target_schema,
                                    mapping.target_table,
                                    e
                                );
                            }
                        }
                    }
                }
            }
        }
        _ => {}
    }

    Ok(false)
}

/// Run post-batch bookkeeping: collect flush results, auto-retry errored tables,
/// transition CATCHUP → STREAMING, and persist confirmed_lsn + StandbyStatusUpdate.
///
/// Called periodically from the inline streaming loop (every `HEARTBEAT_COMMITS`
/// commits or `HEARTBEAT_INTERVAL` ms) and once after the loop ends.
/// Returns the current confirmed_lsn (0 if not yet established).
async fn run_heartbeat(
    meta: &MetadataClient<'_>,
    group: &mut SyncGroup,
    coordinator: &mut FlushCoordinator,
    consumer: &mut SlotConsumer,
    rel_cache: &mut HashMap<u32, RelCacheWithMapping>,
) -> Result<u64, String> {
    // 1. Drain flush results; invalidate rel_cache entries for errored tables so
    //    the next WAL event re-fetches state (table may have transitioned to ERRORED).
    for r in &coordinator.collect_results() {
        if let FlushThreadResult::Error {
            target_key,
            error,
            mapping_id,
            ..
        } = r
        {
            tracing::error!("pg_duckpipe: flush error for {}: {}", target_key, error);
            for cached in rel_cache.values_mut() {
                if cached
                    .mapping
                    .as_ref()
                    .map_or(false, |m| &m.id == mapping_id)
                {
                    cached.mapping = None;
                    break;
                }
            }
        }
    }

    // 2. Auto-retry ERRORED tables whose retry_at has passed.
    if let Ok(retryable) = meta.get_retryable_errored_tables(group.id).await {
        for table in &retryable {
            if let Err(e) = meta.retry_errored_table(table.id).await {
                tracing::error!(
                    "pg_duckpipe: failed to retry errored table {}.{}: {}",
                    table.source_schema,
                    table.source_table,
                    e
                );
            } else {
                tracing::info!(
                    "pg_duckpipe: auto-retrying errored table {}.{}",
                    table.source_schema,
                    table.source_table
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

    // 3. Transition CATCHUP → STREAMING for tables that have caught up.
    if group.pending_lsn != 0 {
        meta.transition_catchup_to_streaming(group.id, group.pending_lsn)
            .await
            .map_err(|e| format!("transition_catchup: {}", e))?;
        // Mirror the transition immediately in rel_cache.
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

    // 4. Compute confirmed_lsn from in-memory per-table state and persist + report it.
    //    No PG fallback needed here — run_sync_cycle seeds table lsns at cycle start.
    let min_lsn = coordinator.get_min_applied_lsn_in_coordinator();
    if min_lsn > 0 {
        meta.update_confirmed_lsn(group.id, min_lsn)
            .await
            .map_err(|e| format!("update_confirmed_lsn: {}", e))?;
        consumer.send_status_update(min_lsn);
    }

    Ok(min_lsn)
}

/// Process one sync group using streaming replication for WAL consumption.
///
/// Inline hot path: `recv → decode → push_change` with no intermediate buffer.
/// Bookkeeping (flush results, auto-retry, CATCHUP→STREAMING, confirmed_lsn)
/// runs via `run_heartbeat` every `HEARTBEAT_COMMITS` commits or
/// `HEARTBEAT_INTERVAL` ms, whichever comes first.
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

    // Check backpressure — if flush threads are lagging behind, skip this poll round.
    // Still compute confirmed_lsn and advance slot.
    if coordinator.is_backpressured() {
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

    // first_msg_timeout: how long to wait for the very first WAL message this cycle.
    // loop_deadline: absolute deadline for the entire batch (set once, checked each iteration).
    // recv_fast: greedily drain subsequent messages with a minimal per-call timeout.
    let first_msg_timeout = Duration::from_millis((config.poll_interval_ms / 2).max(100) as u64);
    let loop_deadline = Instant::now() + first_msg_timeout;
    let recv_fast = Duration::from_millis(1);

    let mut have_first = false;
    let mut total_processed: i32 = 0;
    let mut commits_since_heartbeat: u32 = 0;
    let mut last_heartbeat = Instant::now();

    loop {
        // Hard cap: stop accumulating once we reach batch_size_per_group.
        if total_processed >= config.batch_size_per_group {
            break;
        }
        // After receiving the first message, honour the overall batch deadline.
        if have_first && Instant::now() >= loop_deadline {
            break;
        }
        // Yield to flush threads when queues are saturated.
        if coordinator.is_backpressured() {
            break;
        }

        let timeout = if !have_first {
            first_msg_timeout
        } else {
            recv_fast
        };
        match consumer.recv_one(timeout).await? {
            None => break,
            Some((lsn, data)) => {
                have_first = true;
                let is_commit = process_one_wal_message(
                    client,
                    meta,
                    group,
                    lsn,
                    &data,
                    coordinator,
                    rel_cache,
                )
                .await?;
                total_processed += 1;
                if is_commit {
                    commits_since_heartbeat += 1;
                }
                // Periodic heartbeat: bookkeeping every N commits or T ms.
                if commits_since_heartbeat >= HEARTBEAT_COMMITS
                    || last_heartbeat.elapsed() >= HEARTBEAT_INTERVAL
                {
                    run_heartbeat(meta, group, coordinator, consumer, rel_cache).await?;
                    commits_since_heartbeat = 0;
                    last_heartbeat = Instant::now();
                }
            }
        }
    }

    // Final heartbeat: always runs regardless of message count.
    // Handles the no-WAL path and ensures confirmed_lsn is always up-to-date.
    let confirmed_lsn = run_heartbeat(meta, group, coordinator, consumer, rel_cache).await?;

    // If no messages were processed, send StandbyStatusUpdate so the slot's
    // confirmed_flush_lsn reflects any flush progress since the last cycle
    // (e.g. flush threads completing catch-up while WAL is quiet).
    if total_processed == 0 {
        let min = coordinator.get_min_applied_lsn_in_coordinator();
        if min > 0 {
            consumer.send_status_update(min);
        }
    }

    if let Some(start) = group_start {
        tracing::debug!(
            "DuckPipe timing: action=process_sync_group_streaming group={} slot={} processed_changes={} elapsed_ms={:.3}",
            group.name,
            group.slot_name,
            total_processed,
            start.elapsed().as_secs_f64() * 1000.0
        );
    }

    Ok(ProcessResult {
        total_processed,
        any_work: total_processed > 0,
        confirmed_lsn,
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
    snapshot_manager: &mut SnapshotManager,
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

    // Collect completed snapshot results once (globally, before the group loop).
    // This ensures results from any group are processed and rel_cache invalidation
    // is applied across ALL consumers, not just the current group's slot.
    let snap_results = snapshot_manager.collect_results();
    for snap in &snap_results {
        match &snap.result {
            Ok((snapshot_lsn, rows_copied)) => {
                meta.set_catchup_state(snap.task_id, *snapshot_lsn)
                    .await
                    .map_err(|e| format!("set_catchup_state: {}", e))?;
                if *rows_copied > 0 {
                    let _ = meta
                        .update_table_metrics(snap.task_id, *rows_copied as i64)
                        .await;
                }
                // Unpause flush thread — buffered WAL changes can now be flushed
                let target_key = format!("{}.{}", snap.target_schema, snap.target_table);
                coordinator.unpause_table(&target_key);
            }
            Err(e) => {
                let _ = meta.record_error_message(snap.task_id, e).await;
                tracing::warn!(
                    "DuckPipe: snapshot failed for {}.{}: {}",
                    snap.source_schema,
                    snap.source_table,
                    e
                );
            }
        }
    }

    // Invalidate cached mappings for tables that just transitioned SNAPSHOT → CATCHUP
    // so the WAL handler re-resolves them with the new state (CATCHUP + snapshot_lsn).
    // Scanned across ALL consumers (all groups) to avoid stale rel_cache entries.
    for snap in &snap_results {
        if snap.result.is_ok() {
            for slot_state in consumers.values_mut() {
                for cached in slot_state.rel_cache.values_mut() {
                    if cached
                        .mapping
                        .as_ref()
                        .map_or(false, |m| m.id == snap.task_id)
                    {
                        cached.mapping = None;
                        break;
                    }
                }
            }
        }
    }

    for group in groups.iter_mut() {
        // Kick new snapshots (fire-and-forget, never blocks)
        let snapshot_tasks = meta
            .get_snapshot_tasks(group.id)
            .await
            .map_err(|e| format!("get_snapshot_tasks: {}", e))?;
        if !snapshot_tasks.is_empty() {
            snapshot_manager.kick_snapshots(
                snapshot_tasks,
                &config.connstr,
                coordinator.pg_connstr(),
                coordinator.ducklake_schema(),
                config.debug_log,
            );
        }

        // Continue to streaming immediately

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
            if let FlushThreadResult::Error {
                target_key, error, ..
            } = r
            {
                tracing::error!("pg_duckpipe: flush error for {}: {}", target_key, error);
            }
        }

        // Persist the latest flushed position to PG (for crash recovery and WAL
        // retention via StandbyStatusUpdate).  This is purely a bookkeeping update —
        // it does NOT affect where the slot connects this cycle.
        let fresh_flushed = coordinator.get_min_applied_lsn_in_coordinator();
        if fresh_flushed > group.confirmed_lsn {
            group.confirmed_lsn = fresh_flushed;
            if let Err(e) = meta.update_confirmed_lsn(group.id, fresh_flushed).await {
                tracing::error!(
                    "pg_duckpipe: failed to persist confirmed_lsn for group {}: {}",
                    group.id,
                    e
                );
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
            consumers.insert(
                group.slot_name.clone(),
                SlotState {
                    consumer,
                    rel_cache: HashMap::new(),
                    // Fresh connection — cache is empty, so no immediate refresh needed.
                    last_enabled_check: Instant::now(),
                },
            );
        }

        let slot_state = consumers.get_mut(&group.slot_name).unwrap();

        // Targeted invalidation: mappings that had flush errors may have transitioned
        // to ERRORED in PG.  Clearing only those entries forces a re-fetch before the
        // next WAL event for those tables, so routing picks up the new state promptly.
        for r in &prev_results {
            if let FlushThreadResult::Error { mapping_id, .. } = r {
                for cached in slot_state.rel_cache.values_mut() {
                    if cached
                        .mapping
                        .as_ref()
                        .map_or(false, |m| &m.id == mapping_id)
                    {
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
            let ids: Vec<i32> = slot_state
                .rel_cache
                .values()
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
                        tracing::warn!(
                            "pg_duckpipe: periodic enabled/state refresh failed for slot {}: {}",
                            group.slot_name,
                            e
                        );
                    }
                }
            }
            slot_state.last_enabled_check = Instant::now();
        }

        let result = match process_sync_group_streaming(
            &client,
            &meta,
            group,
            config,
            &mut slot_state.consumer,
            coordinator,
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

    // Update per-table queued_changes for per-table accumulator visibility
    let pending_counts = coordinator.table_pending_counts();
    if !pending_counts.is_empty() {
        let _ = meta.update_table_queued_changes(&pending_counts).await;
    }

    // Clean up metadata connection
    drop(client);
    let _ = conn_handle.await;

    Ok(any_work)
}

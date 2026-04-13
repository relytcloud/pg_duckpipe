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
use crate::flush_coordinator::{
    DdlCommand, FlushCoordinator, FlushThreadResult, PendingDdl, QueueMeta,
};
use crate::metadata::{compute_backoff_secs, MetadataClient, ERRORED_THRESHOLD};
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
    /// DuckLake metadata schema name (e.g., "ducklake")
    pub ducklake_schema: String,
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
///
/// `source_client`: optional separate PG connection for catalog queries (remote groups).
/// Falls back to `meta.client()` when `None` (local groups).
async fn ensure_coordinator_queue(
    coordinator: &mut FlushCoordinator,
    target_key: &str,
    mapping: &TableMapping,
    entry: &RelCacheEntry,
    pk_key_attrs_cache: &mut Option<Vec<usize>>,
    meta: &MetadataClient<'_>,
    source_client: Option<&Client>,
) -> Result<(), String> {
    let catalog_client = source_client.unwrap_or(meta.client());

    // The pgoutput RELATION message always populates attnames (one per column).
    // Use them directly — no catalog fallback needed.
    if entry.attnames.is_empty() {
        return Err(format!(
            "RELATION message for {}.{} has no columns",
            entry.nspname, entry.relname
        ));
    }
    let attnames = &entry.attnames;
    let atttypes = &entry.atttypes;

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
        let (_, attrs) =
            crate::metadata::load_pk_metadata(catalog_client, &entry.nspname, &entry.relname)
                .await
                .map_err(|e| format!("load_batch_metadata (pk): {}", e))?;
        // No PK → fall back to all columns as key. This ensures DELETE rows in
        // append-mode changelog carry full old-row values (via REPLICA IDENTITY
        // FULL) instead of all NULLs. Upsert mode rejects no-PK tables at
        // add_table() time, so this path is only hit for append mode.
        let final_attrs = if attrs.is_empty() {
            (0..attnames.len()).collect()
        } else {
            attrs
        };
        *pk_key_attrs_cache = Some(final_attrs.clone());
        final_attrs
    };

    let paused = mapping.state == "SNAPSHOT";
    coordinator.ensure_queue(
        target_key,
        mapping.id,
        mapping.applied_lsn,
        attnames.clone(),
        key_attrs,
        atttypes.clone(),
        paused,
        mapping.source_label.clone(),
        mapping.sync_mode.clone(),
        &mapping.config,
    );
    Ok(())
}

/// Returns true if this WAL change should be skipped: disabled, errored,
/// not yet caught up (CATCHUP with lsn <= snapshot_lsn), or already applied
/// in append mode (lsn <= applied_lsn).
fn should_skip_change(mapping: &TableMapping, lsn: u64) -> bool {
    if !mapping.enabled || mapping.state == "ERRORED" {
        return true;
    }
    if mapping.state == "CATCHUP" && mapping.snapshot_lsn != 0 && lsn <= mapping.snapshot_lsn {
        return true;
    }
    // Append-mode exactly-once: skip already-applied WAL changes
    if mapping.sync_mode == "append"
        && mapping.state == "STREAMING"
        && mapping.applied_lsn != 0
        && lsn <= mapping.applied_lsn
    {
        return true;
    }
    false
}

/// Fill unchanged (TOAST) columns in `new_values` from `old_values`.
fn fill_toast_columns(
    new_values: &[Value],
    old_values: &[Value],
    unchanged: &[bool],
) -> Vec<Value> {
    (0..new_values.len())
        .map(|i| {
            if unchanged.get(i).copied().unwrap_or(false) {
                old_values.get(i).cloned().unwrap_or(Value::Null)
            } else {
                new_values[i].clone()
            }
        })
        .collect()
}

/// Detect column-level schema changes between old and new RELATION entries.
///
/// Algorithm:
/// 1. Compute sets of column names in old and new entries
/// 2. Pure ADD: names in new but not in old → query catalog for type
/// 3. Pure DROP: names in old but not in new
/// 4. Mixed (possible RENAME): match by position (same index, same type OID)
async fn detect_schema_changes(
    old: &RelCacheEntry,
    new: &RelCacheEntry,
    catalog_client: &Client,
) -> Result<Vec<DdlCommand>, String> {
    use std::collections::HashSet;

    let old_names: HashSet<&str> = old.attnames.iter().map(|s| s.as_str()).collect();
    let new_names: HashSet<&str> = new.attnames.iter().map(|s| s.as_str()).collect();

    let removed: HashSet<&str> = old_names.difference(&new_names).copied().collect();
    let added: HashSet<&str> = new_names.difference(&old_names).copied().collect();

    let mut changes = Vec::new();

    // Detect ALTER COLUMN TYPE: same name, different type OID.
    for (i, new_name) in new.attnames.iter().enumerate() {
        if let Some(old_pos) = old.attnames.iter().position(|n| n == new_name) {
            if old_pos < old.atttypes.len()
                && i < new.atttypes.len()
                && old.atttypes[old_pos] != new.atttypes[i]
            {
                let old_type = crate::types::pg_oid_to_type_name(old.atttypes[old_pos]);
                let new_type = crate::types::pg_oid_to_type_name(new.atttypes[i]);
                changes.push(DdlCommand::UnsupportedAlterColumnType {
                    col_name: new_name.clone(),
                    old_type,
                    new_type,
                });
            }
        }
    }
    if changes
        .iter()
        .any(|c| matches!(c, DdlCommand::UnsupportedAlterColumnType { .. }))
    {
        return Ok(changes); // Skip ADD/DROP/RENAME — table will error
    }

    if removed.is_empty() && added.is_empty() {
        return Ok(Vec::new());
    }

    if !removed.is_empty() && !added.is_empty() {
        // Mixed: try to detect renames by position matching.
        // A rename keeps the same position and same type OID but changes the name.
        let mut matched_old: HashSet<&str> = HashSet::new();
        let mut matched_new: HashSet<&str> = HashSet::new();

        let max_pos = old.attnames.len().min(new.attnames.len());
        for i in 0..max_pos {
            let old_name = &old.attnames[i];
            let new_name = &new.attnames[i];
            if removed.contains(old_name.as_str())
                && added.contains(new_name.as_str())
                && i < old.atttypes.len()
                && i < new.atttypes.len()
                && old.atttypes[i] == new.atttypes[i]
            {
                changes.push(DdlCommand::RenameColumn {
                    old_name: old_name.clone(),
                    new_name: new_name.clone(),
                });
                matched_old.insert(old_name.as_str());
                matched_new.insert(new_name.as_str());
            }
        }

        // Remaining removed → DROP
        for name in &removed {
            if !matched_old.contains(name) {
                changes.push(DdlCommand::DropColumn {
                    col_name: name.to_string(),
                });
            }
        }

        // Remaining added → ADD
        for name in &added {
            if !matched_new.contains(name) {
                let col_type = crate::metadata::get_column_type(
                    catalog_client,
                    &new.nspname,
                    &new.relname,
                    name,
                )
                .await
                .map_err(|e| format!("get_column_type: {}", e))?;
                let mapped_type = crate::types::map_pg_type_for_duckdb(&col_type);
                changes.push(DdlCommand::AddColumn {
                    col_name: name.to_string(),
                    col_type: mapped_type,
                });
            }
        }
    } else if !added.is_empty() {
        // Pure ADD
        for name in &added {
            let col_type =
                crate::metadata::get_column_type(catalog_client, &new.nspname, &new.relname, name)
                    .await
                    .map_err(|e| format!("get_column_type: {}", e))?;
            let mapped_type = crate::types::map_pg_type_for_duckdb(&col_type);
            changes.push(DdlCommand::AddColumn {
                col_name: name.to_string(),
                col_type: mapped_type,
            });
        }
    } else {
        // Pure DROP
        for name in &removed {
            changes.push(DdlCommand::DropColumn {
                col_name: name.to_string(),
            });
        }
    }

    Ok(changes)
}

/// Decode and dispatch a single pgoutput WAL message to the flush coordinator.
///
/// Returns `true` if the message was a COMMIT (used by the caller to trigger
/// periodic heartbeats). All other message types return `false`.
///
/// `source_client`: optional separate PG connection for catalog queries (remote groups).
async fn process_one_wal_message(
    client: &Client,
    meta: &MetadataClient<'_>,
    group: &mut SyncGroup,
    lsn: u64,
    data: &[u8],
    coordinator: &mut FlushCoordinator,
    rel_cache: &mut HashMap<u32, RelCacheWithMapping>,
    source_client: Option<&Client>,
) -> Result<bool, String> {
    if data.is_empty() {
        return Ok(false);
    }

    let mut cursor: usize = 0;
    let msgtype = read_byte(data, &mut cursor) as char;

    match msgtype {
        'R' => {
            let (rel_id, new_entry) = parse_relation_message(data, &mut cursor);

            // Detect schema changes by comparing with the cached RELATION entry.
            if let Some(old_cached) = rel_cache.get(&rel_id) {
                if let Some(ref mapping) = old_cached.mapping {
                    if mapping.enabled && mapping.state != "ERRORED" {
                        let catalog_client = source_client.unwrap_or(meta.client());

                        // Detect column-level schema changes
                        let changes =
                            detect_schema_changes(&old_cached.entry, &new_entry, catalog_client)
                                .await?;

                        // Detect table rename (same rel_id, different relname)
                        let table_renamed = old_cached.entry.relname != new_entry.relname;

                        if !changes.is_empty() {
                            let target_key =
                                format!("{}.{}", mapping.target_schema, mapping.target_table);

                            // Load PK attrs from catalog for the new schema
                            let (_, new_key_attrs) = crate::metadata::load_pk_metadata(
                                catalog_client,
                                &new_entry.nspname,
                                &new_entry.relname,
                            )
                            .await
                            .map_err(|e| format!("load_pk_metadata for DDL: {}", e))?;

                            let new_meta = QueueMeta {
                                target_key: target_key.clone(),
                                mapping_id: mapping.id,
                                attnames: new_entry.attnames.clone(),
                                key_attrs: new_key_attrs,
                                atttypes: new_entry.atttypes.clone(),
                                source_label: mapping.source_label.clone(),
                                sync_mode: mapping.sync_mode.clone(),
                            };

                            coordinator.set_pending_ddl(
                                mapping.id,
                                PendingDdl {
                                    commands: changes,
                                    new_meta,
                                    target_oid: mapping.target_oid,
                                    post_changes: Vec::new(),
                                },
                            );

                            tracing::info!(
                                "pg_duckpipe: DDL change detected for {}.{}, \
                                 setting barrier on {}",
                                new_entry.nspname,
                                new_entry.relname,
                                target_key,
                            );
                        }

                        // Update source name metadata on table rename
                        if table_renamed {
                            let _ = meta
                                .update_source_name(
                                    mapping.id,
                                    &new_entry.nspname,
                                    &new_entry.relname,
                                )
                                .await;
                        }

                        // Preserve mapping for the new entry (carry forward)
                        let mut carried_mapping = mapping.clone();
                        if table_renamed {
                            carried_mapping.source_schema = new_entry.nspname.clone();
                            carried_mapping.source_table = new_entry.relname.clone();
                        }
                        rel_cache.insert(
                            rel_id,
                            RelCacheWithMapping {
                                entry: new_entry,
                                mapping: Some(carried_mapping),
                                // Reset pk_key_attrs — will be re-resolved on next DML
                                pk_key_attrs: None,
                            },
                        );
                        return Ok(false);
                    }
                }
            }

            // Default: cache the new entry (first RELATION for this rel_id, or
            // table is disabled/ERRORED — no DDL propagation needed).
            rel_cache.insert(
                rel_id,
                RelCacheWithMapping {
                    entry: new_entry,
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
                    if should_skip_change(mapping, lsn) {
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
                        source_client,
                    )
                    .await?;
                    coordinator.push_change(
                        mapping.id,
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
                    if should_skip_change(mapping, lsn) {
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
                        source_client,
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

                    if mapping.sync_mode == "append" {
                        // Append mode: push a single Update change (preserves raw op type)
                        let final_values = if has_unchanged && old_marker == 'O' {
                            fill_toast_columns(&new_values, &old_values, &new_unchanged)
                        } else {
                            new_values
                        };
                        coordinator.push_change(
                            mapping.id,
                            Change {
                                change_type: ChangeType::Update,
                                lsn,
                                col_values: final_values,
                                key_values,
                                col_unchanged: if has_unchanged && old_marker != 'O' {
                                    new_unchanged
                                } else {
                                    Vec::new()
                                },
                            },
                        );
                    } else if has_unchanged && old_marker == 'O' {
                        // REPLICA IDENTITY FULL: old tuple carries full row values.
                        // pgoutput still uses 'u' for unchanged TOAST columns in the NEW
                        // tuple, but we can fill them from the old tuple right here —
                        // no flush-time resolution query needed.
                        let filled_values =
                            fill_toast_columns(&new_values, &old_values, &new_unchanged);
                        coordinator.push_change(
                            mapping.id,
                            Change {
                                change_type: ChangeType::Delete,
                                lsn,
                                col_values: Vec::new(),
                                key_values: key_values.clone(),
                                col_unchanged: Vec::new(),
                            },
                        );
                        coordinator.push_change(
                            mapping.id,
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
                            mapping.id,
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
                            mapping.id,
                            Change {
                                change_type: ChangeType::Delete,
                                lsn,
                                col_values: Vec::new(),
                                key_values: key_values.clone(),
                                col_unchanged: Vec::new(),
                            },
                        );
                        coordinator.push_change(
                            mapping.id,
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
                    if should_skip_change(mapping, lsn) {
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
                        source_client,
                    )
                    .await?;
                    // Extract only PK values — with REPLICA IDENTITY FULL,
                    // old_values contains ALL columns, not just keys.
                    let pk_attrs = cached.pk_key_attrs.as_ref().unwrap();
                    let key_values = extract_key_values(&old_values, pk_attrs);
                    coordinator.push_change(
                        mapping.id,
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
            // TRUNCATE — enqueue a barrier so the flush thread drains pending
            // changes, then executes DELETE FROM on the target (non-blocking).
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
                            if mapping.sync_mode == "append" {
                                // Append mode: skip DELETE, log that TRUNCATE was received.
                                // The changelog is immutable — TRUNCATE is not propagated.
                                tracing::info!(
                                    "pg_duckpipe: TRUNCATE on append-mode table {}.{} — skipping (changelog is immutable)",
                                    mapping.source_schema,
                                    mapping.source_table
                                );
                            } else if let Some(meta) = coordinator.get_meta(mapping.id) {
                                coordinator.set_pending_ddl(
                                    mapping.id,
                                    PendingDdl {
                                        commands: vec![DdlCommand::Truncate {
                                            source_label: mapping.source_label.clone(),
                                        }],
                                        new_meta: meta,
                                        target_oid: mapping.target_oid,
                                        post_changes: Vec::new(),
                                    },
                                );
                            } else {
                                tracing::warn!(
                                    "pg_duckpipe: TRUNCATE on {}.{} — no flush queue yet, skipping",
                                    mapping.source_schema,
                                    mapping.source_table
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
            match meta.retry_errored_table(table.id).await {
                Err(e) => {
                    tracing::error!(
                        "pg_duckpipe: failed to retry errored table {}.{}: {}",
                        table.source_schema,
                        table.source_table,
                        e
                    );
                }
                Ok(None) => {
                    // Row no longer in ERRORED state (race or manually resolved)
                    tracing::debug!(
                        "pg_duckpipe: skipping retry for {}.{} (no longer ERRORED)",
                        table.source_schema,
                        table.source_table
                    );
                }
                Ok(Some(new_state)) => {
                    tracing::info!(
                        "pg_duckpipe: auto-retrying errored table {}.{} → {}",
                        table.source_schema,
                        table.source_table,
                        new_state
                    );
                    if new_state == "STREAMING" {
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
                    // SNAPSHOT retries need no rel_cache update — snapshot_manager picks them up
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
///
/// `source_client`: optional separate PG connection for catalog queries (remote groups).
async fn process_sync_group_streaming(
    client: &Client,
    meta: &MetadataClient<'_>,
    group: &mut SyncGroup,
    config: &ServiceConfig,
    consumer: &mut SlotConsumer,
    coordinator: &mut FlushCoordinator,
    rel_cache: &mut HashMap<u32, RelCacheWithMapping>,
    source_client: Option<&Client>,
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
    let first_msg_timeout =
        Duration::from_millis((config.poll_interval_ms / 2).clamp(100, 2000) as u64);
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
                    source_client,
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
    /// TCP connection (used by standalone daemon and remote groups).
    Tcp {
        host: String,
        port: u16,
        user: String,
        password: String,
        dbname: String,
        sslmode: tokio_postgres::config::SslMode,
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
            sslmode,
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
                *sslmode,
            )
            .await
        }
    }
}

/// Run one complete sync cycle for a single named group.
///
/// Used by both the PG background worker (one worker per group) and
/// the standalone daemon (`--group` flag).
///
/// `consumer` is `Option<SlotState>` (at most one slot per group).
/// Returns (any_work, pending_lsn) — whether any work was done and the latest
/// LSN received from the WAL stream (used for replication lag tracking).
pub async fn run_group_sync_cycle(
    config: &ServiceConfig,
    group_name: &str,
    coordinator: &mut FlushCoordinator,
    slot_params: &SlotConnectParams,
    consumer: &mut Option<SlotState>,
    snapshot_manager: &mut SnapshotManager,
) -> Result<(bool, u64), String> {
    // Establish metadata connection (short-lived per cycle)
    let meta_app_name = crate::connstr::app_name(group_name, "meta");
    let (client, conn_handle) =
        crate::connstr::pg_connect_with_app_name(&config.connstr, &meta_app_name)
            .await
            .map_err(|e| format!("connect: {}", e))?;

    let meta = MetadataClient::new(&client);

    // Get this specific group (returns None if not found or disabled)
    let mut group = match meta
        .get_sync_group_by_name(group_name)
        .await
        .map_err(|e| format!("get_sync_group_by_name: {}", e))?
    {
        Some(g) => g,
        None => {
            drop(client);
            let _ = conn_handle.await;
            return Ok((false, 0));
        }
    };

    let mut any_work = false;

    // Collect completed snapshot results.
    let snap_results = snapshot_manager.collect_results();
    for snap in &snap_results {
        match &snap.result {
            Ok((snapshot_lsn, rows_copied, duration_ms)) => {
                meta.set_catchup_state(snap.task_id, *snapshot_lsn, *duration_ms, *rows_copied)
                    .await
                    .map_err(|e| format!("set_catchup_state: {}", e))?;
                if *rows_copied > 0 {
                    let _ = meta
                        .update_table_metrics(snap.task_id, *rows_copied as i64)
                        .await;
                }
                coordinator.unpause_table(snap.task_id);
            }
            Err(e) => {
                match meta
                    .record_failure_with_backoff(snap.task_id, e, ERRORED_THRESHOLD)
                    .await
                {
                    Ok(count) if count >= ERRORED_THRESHOLD => {
                        let backoff = compute_backoff_secs(count, ERRORED_THRESHOLD);
                        tracing::warn!(
                            "DuckPipe: snapshot for {}.{} → ERRORED after {} failures, retry in {}s",
                            snap.source_schema, snap.source_table, count, backoff
                        );
                    }
                    Ok(count) => {
                        tracing::warn!(
                            "DuckPipe: snapshot failed for {}.{} ({}/{}): {}",
                            snap.source_schema,
                            snap.source_table,
                            count,
                            ERRORED_THRESHOLD,
                            e
                        );
                    }
                    Err(db_err) => {
                        tracing::warn!(
                            "DuckPipe: snapshot failed for {}.{}: {} (failure tracking error: {})",
                            snap.source_schema,
                            snap.source_table,
                            e,
                            db_err
                        );
                    }
                }
            }
        }
    }

    // Invalidate cached mappings for tables that just transitioned SNAPSHOT → CATCHUP
    for snap in &snap_results {
        if snap.result.is_ok() {
            if let Some(ref mut slot_state) = consumer {
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

    // --- Remote source connection ---
    // For remote groups (conninfo IS NOT NULL), open a separate connection to
    // the remote PG for catalog queries (PK lookup) and snapshot COPY.
    let remote_source: Option<(
        tokio_postgres::Client,
        tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
    )> = if let Some(ref conninfo) = group.conninfo {
        let remote_app_name = crate::connstr::app_name(group_name, "remote");
        let (rc, rh) = crate::connstr::pg_connect_with_app_name(conninfo, &remote_app_name)
            .await
            .map_err(|e| format!("remote source connect for group {}: {}", group.name, e))?;
        Some((rc, rh))
    } else {
        None
    };

    let source_client_ref = remote_source.as_ref().map(|(c, _)| c);

    // Slot params: use remote PG for WAL replication if conninfo set.
    let remote_slot_params: Option<SlotConnectParams> = match group.conninfo.as_ref() {
        Some(ci) => Some(
            crate::connstr::to_slot_connect_params(ci)
                .map_err(|e| format!("parse conninfo for group {}: {}", group.name, e))?,
        ),
        None => None,
    };
    let effective_slot_params = remote_slot_params.as_ref().unwrap_or(slot_params);

    // Snapshot connstr: for remote groups, use the remote PG connstr directly.
    let snapshot_connstr = if let Some(ref conninfo) = group.conninfo {
        conninfo.clone()
    } else {
        config.connstr.clone()
    };

    // Kick new snapshots (fire-and-forget, never blocks)
    let snapshot_tasks = meta
        .get_snapshot_tasks(group.id)
        .await
        .map_err(|e| format!("get_snapshot_tasks: {}", e))?;
    if !snapshot_tasks.is_empty() {
        snapshot_manager.kick_snapshots(
            snapshot_tasks,
            &snapshot_connstr,
            coordinator.ducklake_pg_connstr(),
            coordinator.ducklake_schema(),
            config.debug_log,
            group_name,
        );
    }

    // Seed per_table_lsn with all active tables from PG.
    if let Ok(active_lsns) = meta.get_active_table_lsns(group.id).await {
        coordinator.seed_table_lsns(&active_lsns);
    }

    // Drain flush results from the previous cycle.  Must happen BEFORE prune
    // so that late results from about-to-be-pruned threads are collected first
    // and their stale per_table_lsn entries are removed in the same pass.
    let prev_results = coordinator.collect_results();
    for r in &prev_results {
        if let FlushThreadResult::Error {
            target_key, error, ..
        } = r
        {
            tracing::error!("pg_duckpipe: flush error for {}: {}", target_key, error);
        }
    }

    // Prune flush threads and per_table_lsn entries for tables removed via
    // remove_table(). Without this, stale entries hold back confirmed_lsn,
    // preventing slot advancement and causing unbounded lag growth.
    if let Ok(all_ids) = meta.get_all_mapping_ids(group.id).await {
        coordinator.prune_removed_tables(&all_ids);
    }

    // Persist the latest flushed position.
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
    let slot_start_lsn = group.confirmed_lsn;

    let needs_connect = match consumer {
        Some(s) => !s.consumer.is_connected(),
        None => true,
    };
    if needs_connect {
        *consumer = None;
        let new_consumer = connect_slot_consumer(
            effective_slot_params,
            &group.slot_name,
            &group.publication,
            slot_start_lsn,
        )
        .await
        .map_err(|e| format!("streaming connect failed for {}: {}", group.slot_name, e))?;
        *consumer = Some(SlotState {
            consumer: new_consumer,
            rel_cache: HashMap::new(),
            last_enabled_check: Instant::now(),
        });
    }

    let slot_state = consumer.as_mut().unwrap();

    // Targeted invalidation for errored mappings.
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

    // Periodic refresh of `enabled` + `state`.
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
        &mut group,
        config,
        &mut slot_state.consumer,
        coordinator,
        &mut slot_state.rel_cache,
        source_client_ref,
    )
    .await
    {
        Ok(r) => r,
        Err(e) => {
            // Clean up remote connection before returning error
            if let Some((rc, rh)) = remote_source {
                drop(rc);
                let _ = rh.await;
            }
            *consumer = None;
            return Err(format!("process_sync_group_streaming: {}", e));
        }
    };

    // Clean up remote source connection
    if let Some((rc, rh)) = remote_source {
        drop(rc);
        let _ = rh.await;
    }

    if result.any_work {
        any_work = true;
    }

    // Clean up metadata connection
    drop(client);
    let _ = conn_handle.await;

    Ok((any_work, group.pending_lsn))
}

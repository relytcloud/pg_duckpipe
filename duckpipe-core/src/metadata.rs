//! Async metadata client for accessing duckpipe schema tables via tokio-postgres.

use std::collections::HashMap;

use tokio_postgres::{Client, Row};

use crate::state::SyncState;
use crate::types::{
    format_lsn, parse_lsn, GroupConfig, GroupMode, SyncGroup, SyncMode, TableConfig, TableMapping,
};

/// Parse a `tokio_postgres::Row` from the standard 14-column TableMapping SELECT into a `TableMapping`.
/// Column order: id, source_schema, source_table, target_schema, target_table,
///               state, snapshot_lsn::text, enabled, source_oid, error_message,
///               applied_lsn::text, source_label, sync_mode, config::text
fn table_mapping_from_row(row: &Row) -> TableMapping {
    let state_str: String = row.get(5);
    let sync_mode_str: Option<String> = row.get(13);
    TableMapping {
        id: row.get(0),
        source_schema: row.get(1),
        source_table: row.get(2),
        target_schema: row.get(3),
        target_table: row.get(4),
        state: SyncState::from_str(&state_str).unwrap_or(SyncState::Pending),
        snapshot_lsn: row
            .get::<_, Option<String>>(6)
            .map(|s| parse_lsn(&s))
            .unwrap_or(0),
        enabled: row.get(7),
        source_oid: row.get(8),
        error_message: row.get(9),
        applied_lsn: row
            .get::<_, Option<String>>(10)
            .map(|s| parse_lsn(&s))
            .unwrap_or(0),
        target_oid: row.get::<_, Option<i64>>(11).unwrap_or(0),
        source_label: row.get::<_, Option<String>>(12).unwrap_or_default(),
        sync_mode: sync_mode_str
            .and_then(|s| s.parse().ok())
            .unwrap_or(SyncMode::Upsert),
        config: row
            .get::<_, Option<String>>(14)
            .and_then(|s| TableConfig::from_json_str(&s).ok())
            .unwrap_or_default(),
    }
}

/// Maximum consecutive failures before transitioning to ERRORED with backoff.
pub const ERRORED_THRESHOLD: i32 = 3;

/// Compute exponential backoff: 30s * 2^(failures - threshold), capped at ~32 min.
pub fn compute_backoff_secs(failure_count: i32, threshold: i32) -> i64 {
    30i64 * (1i64 << (failure_count - threshold).min(6) as u32)
}

/// Async metadata client wrapping a tokio-postgres connection.
pub struct MetadataClient<'a> {
    client: &'a Client,
}

impl<'a> MetadataClient<'a> {
    pub fn new(client: &'a Client) -> Self {
        Self { client }
    }

    /// Access the underlying tokio-postgres client.
    pub fn client(&self) -> &Client {
        self.client
    }

    /// Get a single sync group by name (regardless of enabled state).
    pub async fn get_sync_group_by_name(
        &self,
        name: &str,
    ) -> Result<Option<SyncGroup>, tokio_postgres::Error> {
        let rows = self
            .client
            .query(
                "SELECT id, name, publication, slot_name, confirmed_lsn::text, enabled, conninfo, mode, config::text \
                 FROM duckpipe.sync_groups WHERE name = $1",
                &[&name],
            )
            .await?;

        if rows.is_empty() {
            return Ok(None);
        }

        let row = &rows[0];
        let id: i32 = row.get(0);
        let name: String = row.get(1);
        let publication: String = row.get(2);
        let slot_name: String = row.get(3);
        let confirmed_lsn_str: Option<String> = row.get(4);
        let confirmed_lsn = confirmed_lsn_str.map(|s| parse_lsn(&s)).unwrap_or(0);
        let enabled: bool = row.get(5);
        let conninfo: Option<String> = row.get(6);
        let mode_str: String = row.get(7);
        let mode: GroupMode = mode_str.parse().unwrap_or(GroupMode::BgWorker);
        let config_str: Option<String> = row.get(8);
        let config = config_str
            .and_then(|s| GroupConfig::from_json_str(&s).ok())
            .unwrap_or_default();

        if !enabled {
            return Ok(None);
        }

        Ok(Some(SyncGroup {
            id,
            name,
            publication,
            slot_name,
            pending_lsn: 0,
            confirmed_lsn,
            conninfo,
            mode,
            config,
        }))
    }

    /// Get all enabled sync groups.
    pub async fn get_enabled_sync_groups(&self) -> Result<Vec<SyncGroup>, tokio_postgres::Error> {
        let rows = self
            .client
            .query(
                "SELECT id, name, publication, slot_name, confirmed_lsn::text, conninfo, mode, config::text \
                 FROM duckpipe.sync_groups WHERE enabled = true",
                &[],
            )
            .await?;

        Ok(rows
            .iter()
            .map(|row| {
                let id: i32 = row.get(0);
                let name: String = row.get(1);
                let publication: String = row.get(2);
                let slot_name: String = row.get(3);
                let confirmed_lsn_str: Option<String> = row.get(4);
                let confirmed_lsn = confirmed_lsn_str.map(|s| parse_lsn(&s)).unwrap_or(0);
                let conninfo: Option<String> = row.get(5);
                let mode_str: String = row.get(6);
                let mode: GroupMode = mode_str.parse().unwrap_or(GroupMode::BgWorker);
                let config_str: Option<String> = row.get(7);
                let config = config_str
                    .and_then(|s| GroupConfig::from_json_str(&s).ok())
                    .unwrap_or_default();
                SyncGroup {
                    id,
                    name,
                    publication,
                    slot_name,
                    pending_lsn: 0,
                    confirmed_lsn,
                    conninfo,
                    mode,
                    config,
                }
            })
            .collect())
    }

    /// Get table mapping for a source table within a group.
    pub async fn get_table_mapping(
        &self,
        group_id: i32,
        schema: &str,
        table: &str,
    ) -> Result<Option<TableMapping>, tokio_postgres::Error> {
        let rows = self
            .client
            .query(
                "SELECT id, source_schema, source_table, target_schema, target_table, \
                 state, snapshot_lsn::text, enabled, source_oid, error_message, \
                 applied_lsn::text, target_oid, source_label, sync_mode, config::text \
                 FROM duckpipe.table_mappings \
                 WHERE group_id = $1 AND source_schema = $2 AND source_table = $3",
                &[&group_id, &schema, &table],
            )
            .await?;

        if rows.is_empty() {
            return Ok(None);
        }

        Ok(Some(table_mapping_from_row(&rows[0])))
    }

    /// Transition a table to ERRORED state with an error message.
    pub async fn set_errored_state(
        &self,
        mapping_id: i32,
        error_message: &str,
    ) -> Result<(), tokio_postgres::Error> {
        self.client
            .execute(
                "UPDATE duckpipe.table_mappings SET state = 'ERRORED', \
                 error_message = $1 WHERE id = $2",
                &[&error_message, &mapping_id],
            )
            .await?;
        Ok(())
    }

    /// Record an error message without changing state (for transient errors that will auto-retry).
    pub async fn record_error_message(
        &self,
        mapping_id: i32,
        error_message: &str,
    ) -> Result<(), tokio_postgres::Error> {
        self.client
            .execute(
                "UPDATE duckpipe.table_mappings SET error_message = $1 WHERE id = $2",
                &[&error_message, &mapping_id],
            )
            .await?;
        Ok(())
    }

    /// Clear error state and transition back to a given state (for auto-retry).
    pub async fn clear_error_state(
        &self,
        mapping_id: i32,
        new_state: &str,
    ) -> Result<(), tokio_postgres::Error> {
        self.client
            .execute(
                "UPDATE duckpipe.table_mappings SET state = $1, \
                 error_message = NULL WHERE id = $2",
                &[&new_state, &mapping_id],
            )
            .await?;
        Ok(())
    }

    /// Increment consecutive_failures and return the new count.
    pub async fn increment_consecutive_failures(
        &self,
        mapping_id: i32,
    ) -> Result<i32, tokio_postgres::Error> {
        let rows = self
            .client
            .query(
                "UPDATE duckpipe.table_mappings SET consecutive_failures = consecutive_failures + 1 \
                 WHERE id = $1 RETURNING consecutive_failures",
                &[&mapping_id],
            )
            .await?;
        Ok(rows.first().map(|r| r.get::<_, i32>(0)).unwrap_or(0))
    }

    /// Reset consecutive_failures to 0.
    pub async fn clear_consecutive_failures(
        &self,
        mapping_id: i32,
    ) -> Result<(), tokio_postgres::Error> {
        self.client
            .execute(
                "UPDATE duckpipe.table_mappings SET consecutive_failures = 0 WHERE id = $1",
                &[&mapping_id],
            )
            .await?;
        Ok(())
    }

    /// Transition to ERRORED with retry_at for auto-recovery.
    pub async fn set_errored_with_retry(
        &self,
        mapping_id: i32,
        error_message: &str,
        backoff_secs: i64,
    ) -> Result<(), tokio_postgres::Error> {
        let backoff_f64 = backoff_secs as f64;
        self.client
            .execute(
                "UPDATE duckpipe.table_mappings SET state = 'ERRORED', \
                 error_message = $1, \
                 retry_at = now() + ($3 * interval '1 second') \
                 WHERE id = $2",
                &[&error_message, &mapping_id, &backoff_f64],
            )
            .await?;
        Ok(())
    }

    /// Record error, increment consecutive failures, and conditionally transition to
    /// ERRORED with exponential backoff. Returns the new failure count.
    pub async fn record_failure_with_backoff(
        &self,
        mapping_id: i32,
        error_message: &str,
        errored_threshold: i32,
    ) -> Result<i32, tokio_postgres::Error> {
        let _ = self.record_error_message(mapping_id, error_message).await;
        let count = self.increment_consecutive_failures(mapping_id).await?;
        if count >= errored_threshold {
            let backoff = compute_backoff_secs(count, errored_threshold);
            let _ = self
                .set_errored_with_retry(mapping_id, error_message, backoff)
                .await;
        }
        Ok(count)
    }

    /// Get tables in ERRORED state whose retry_at has passed.
    pub async fn get_retryable_errored_tables(
        &self,
        group_id: i32,
    ) -> Result<Vec<TableMapping>, tokio_postgres::Error> {
        let rows = self
            .client
            .query(
                "SELECT id, source_schema, source_table, target_schema, target_table, \
                 state, snapshot_lsn::text, enabled, source_oid, error_message, \
                 applied_lsn::text, target_oid, source_label, sync_mode, config::text \
                 FROM duckpipe.table_mappings \
                 WHERE group_id = $1 AND state = 'ERRORED' AND enabled = true \
                 AND retry_at IS NOT NULL AND retry_at <= now()",
                &[&group_id],
            )
            .await?;

        Ok(rows.iter().map(table_mapping_from_row).collect())
    }

    /// Auto-retry: transition ERRORED table back to SNAPSHOT (if never snapshotted) or
    /// STREAMING, clear error and failures. Returns the new state, or `None` if the
    /// row was not in ERRORED state (race / already resolved).
    pub async fn retry_errored_table(
        &self,
        mapping_id: i32,
    ) -> Result<Option<SyncState>, tokio_postgres::Error> {
        let rows = self
            .client
            .query(
                "UPDATE duckpipe.table_mappings SET \
                 state = CASE WHEN snapshot_lsn IS NULL THEN 'SNAPSHOT' ELSE 'STREAMING' END, \
                 error_message = NULL, consecutive_failures = 0, retry_at = NULL \
                 WHERE id = $1 AND state = 'ERRORED' \
                 RETURNING state",
                &[&mapping_id],
            )
            .await?;
        Ok(rows.first().map(|r| {
            let s: String = r.get(0);
            SyncState::from_str(&s).unwrap_or(SyncState::Pending)
        }))
    }

    /// Update source schema and table name (e.g., after a table rename detected via OID match).
    pub async fn update_source_name(
        &self,
        mapping_id: i32,
        new_schema: &str,
        new_table: &str,
    ) -> Result<(), tokio_postgres::Error> {
        self.client
            .execute(
                "UPDATE duckpipe.table_mappings SET source_schema = $1, source_table = $2 \
                 WHERE id = $3",
                &[&new_schema, &new_table, &mapping_id],
            )
            .await?;
        Ok(())
    }

    /// Update source_oid for a table mapping.
    pub async fn update_source_oid(
        &self,
        mapping_id: i32,
        source_oid: i64,
    ) -> Result<(), tokio_postgres::Error> {
        self.client
            .execute(
                "UPDATE duckpipe.table_mappings SET source_oid = $1 WHERE id = $2",
                &[&source_oid, &mapping_id],
            )
            .await?;
        Ok(())
    }

    /// Get table mapping by source OID within a group.
    pub async fn get_table_mapping_by_oid(
        &self,
        group_id: i32,
        source_oid: i64,
    ) -> Result<Option<TableMapping>, tokio_postgres::Error> {
        let rows = self
            .client
            .query(
                "SELECT id, source_schema, source_table, target_schema, target_table, \
                 state, snapshot_lsn::text, enabled, source_oid, error_message, \
                 applied_lsn::text, target_oid, source_label, sync_mode, config::text \
                 FROM duckpipe.table_mappings \
                 WHERE group_id = $1 AND source_oid = $2",
                &[&group_id, &source_oid],
            )
            .await?;

        if rows.is_empty() {
            return Ok(None);
        }

        Ok(Some(table_mapping_from_row(&rows[0])))
    }

    /// Update per-table applied_lsn after a successful flush.
    pub async fn update_applied_lsn(
        &self,
        mapping_id: i32,
        lsn: u64,
    ) -> Result<(), tokio_postgres::Error> {
        let lsn_str = format_lsn(lsn);
        self.client
            .execute(
                "UPDATE duckpipe.table_mappings SET applied_lsn = $1::text::pg_lsn WHERE id = $2",
                &[&lsn_str, &mapping_id],
            )
            .await?;
        Ok(())
    }

    /// Update per-table sync metrics (rows_synced, last_sync_at).
    pub async fn update_table_metrics(
        &self,
        mapping_id: i32,
        delta_rows: i64,
    ) -> Result<(), tokio_postgres::Error> {
        if delta_rows <= 0 {
            return Ok(());
        }
        self.client
            .execute(
                "UPDATE duckpipe.table_mappings SET rows_synced = rows_synced + $1, \
                 last_sync_at = now() WHERE id = $2",
                &[&delta_rows, &mapping_id],
            )
            .await?;
        Ok(())
    }

    /// Get the minimum applied_lsn across all active (STREAMING/CATCHUP) tables in a group.
    /// Returns 0 if any active table has NULL applied_lsn (not yet flushed), or if
    /// there are no active tables.
    pub async fn get_min_applied_lsn(&self, group_id: i32) -> Result<u64, tokio_postgres::Error> {
        // For CATCHUP tables with NULL applied_lsn, use snapshot_lsn as the
        // effective floor: everything up to snapshot_lsn is either in the
        // snapshot copy or will be skipped by CATCHUP skip logic, so the slot
        // does not need to replay WAL before snapshot_lsn for these tables.
        // STREAMING tables with NULL applied_lsn (shouldn't normally occur)
        // still block advancement — treat as not-yet-safe.
        let rows = self
            .client
            .query(
                "SELECT \
                     count(*) FILTER ( \
                         WHERE COALESCE(applied_lsn, \
                             CASE WHEN state = 'CATCHUP' THEN snapshot_lsn END \
                         ) IS NULL \
                     ) AS null_count, \
                     min(COALESCE(applied_lsn, \
                         CASE WHEN state = 'CATCHUP' THEN snapshot_lsn END \
                     ))::text AS min_lsn \
                 FROM duckpipe.table_mappings \
                 WHERE group_id = $1 AND enabled = true \
                 AND state IN ('STREAMING', 'CATCHUP')",
                &[&group_id],
            )
            .await?;

        if rows.is_empty() {
            return Ok(0);
        }

        let null_count: i64 = rows[0].get(0);
        if null_count > 0 {
            // Some active tables have no safe LSN floor yet — don't advance slot
            return Ok(0);
        }

        let min_lsn_str: Option<String> = rows[0].get(1);
        Ok(min_lsn_str.map(|s| parse_lsn(&s)).unwrap_or(0))
    }

    /// Get (mapping_id, effective_lsn) for all enabled STREAMING/CATCHUP tables in a group.
    ///
    /// Used to seed the coordinator's `per_table_lsn` at the start of each cycle so that
    /// CATCHUP tables with no WAL changes are still visible for confirmed_lsn computation.
    ///
    /// For CATCHUP tables with NULL applied_lsn, returns snapshot_lsn as the effective
    /// floor — everything before snapshot_lsn is either already in the snapshot copy or
    /// skipped by CATCHUP logic, so the slot need not replay that WAL window.
    /// Returns 0 for any table where no safe floor is available.
    pub async fn get_active_table_lsns(
        &self,
        group_id: i32,
    ) -> Result<Vec<(i32, u64)>, tokio_postgres::Error> {
        let rows = self
            .client
            .query(
                "SELECT id, \
                     COALESCE(applied_lsn, \
                         CASE WHEN state = 'CATCHUP' THEN snapshot_lsn END \
                     )::text \
                 FROM duckpipe.table_mappings \
                 WHERE group_id = $1 AND enabled = true \
                 AND state IN ('STREAMING', 'CATCHUP')",
                &[&group_id],
            )
            .await?;

        Ok(rows
            .iter()
            .map(|row| {
                let id: i32 = row.get(0);
                let applied_lsn: u64 = row
                    .get::<_, Option<String>>(1)
                    .map(|s| parse_lsn(&s))
                    .unwrap_or(0);
                (id, applied_lsn)
            })
            .collect())
    }

    /// Return all mapping IDs for a group (any state, any enabled flag).
    /// Used by the coordinator to detect and prune stale flush threads for
    /// tables that have been removed via `remove_table()`.
    pub async fn get_all_mapping_ids(
        &self,
        group_id: i32,
    ) -> Result<std::collections::HashSet<i32>, tokio_postgres::Error> {
        let rows = self
            .client
            .query(
                "SELECT id FROM duckpipe.table_mappings WHERE group_id = $1",
                &[&group_id],
            )
            .await?;
        Ok(rows.iter().map(|r| r.get::<_, i32>(0)).collect())
    }

    /// Update confirmed_lsn and last_sync_at for a sync group.
    pub async fn update_confirmed_lsn(
        &self,
        group_id: i32,
        lsn: u64,
    ) -> Result<(), tokio_postgres::Error> {
        let lsn_str = format_lsn(lsn);
        self.client
            .execute(
                "UPDATE duckpipe.sync_groups SET confirmed_lsn = $1::text::pg_lsn, \
                 last_sync_at = now() WHERE id = $2",
                &[&lsn_str, &group_id],
            )
            .await?;
        Ok(())
    }

    /// Transition CATCHUP tables to STREAMING when WAL has advanced past their snapshot_lsn.
    pub async fn transition_catchup_to_streaming(
        &self,
        group_id: i32,
        pending_lsn: u64,
    ) -> Result<u64, tokio_postgres::Error> {
        let lsn_str = format_lsn(pending_lsn);
        let result = self
            .client
            .execute(
                "UPDATE duckpipe.table_mappings SET state = 'STREAMING' \
                 WHERE group_id = $1 AND state = 'CATCHUP' \
                 AND snapshot_lsn <= $2::text::pg_lsn",
                &[&group_id, &lsn_str],
            )
            .await?;
        Ok(result)
    }

    /// Load column names and primary key attribute indices from source table catalogs.
    pub async fn load_batch_metadata_from_source(
        &self,
        source_schema: &str,
        source_table: &str,
    ) -> Result<(Vec<String>, Vec<usize>), tokio_postgres::Error> {
        load_pk_metadata(self.client, source_schema, source_table).await
    }

    /// Get snapshot tasks (tables in SNAPSHOT state).
    pub async fn get_snapshot_tasks(
        &self,
        group_id: i32,
    ) -> Result<Vec<SnapshotTask>, tokio_postgres::Error> {
        let rows = self
            .client
            .query(
                "SELECT id, source_schema, source_table, target_schema, target_table, source_label, sync_mode \
                 FROM duckpipe.table_mappings \
                 WHERE group_id = $1 AND state = 'SNAPSHOT' AND enabled = true",
                &[&group_id],
            )
            .await?;

        Ok(rows
            .iter()
            .map(|row| SnapshotTask {
                id: row.get(0),
                source_schema: row.get(1),
                source_table: row.get(2),
                target_schema: row.get(3),
                target_table: row.get(4),
                source_label: row.get::<_, Option<String>>(5).unwrap_or_default(),
                sync_mode: row
                    .get::<_, Option<String>>(6)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(SyncMode::Upsert),
            })
            .collect())
    }

    /// Update table state to CATCHUP with snapshot_lsn, duration, and row count.
    pub async fn set_catchup_state(
        &self,
        mapping_id: i32,
        snapshot_lsn: u64,
        duration_ms: u64,
        snapshot_rows: u64,
    ) -> Result<(), tokio_postgres::Error> {
        let lsn_str = format_lsn(snapshot_lsn);
        let duration_ms_i64 = duration_ms as i64;
        let snapshot_rows_i64 = snapshot_rows as i64;
        self.client
            .execute(
                "UPDATE duckpipe.table_mappings SET state = 'CATCHUP', \
                 snapshot_lsn = $1::text::pg_lsn, \
                 snapshot_duration_ms = $2, snapshot_rows = $3, \
                 consecutive_failures = 0, error_message = NULL \
                 WHERE id = $4",
                &[&lsn_str, &duration_ms_i64, &snapshot_rows_i64, &mapping_id],
            )
            .await?;
        Ok(())
    }

    /// Batch-fetch `(enabled, state)` for a set of mapping IDs.
    ///
    /// Used for the periodic refresh of externally-mutable fields without
    /// invalidating the entire rel_cache each sync round.  Only `enabled` and
    /// `state` are fetched — other fields (column info, snapshot_lsn, etc.) are
    /// not externally mutable and are left unchanged in the cache.
    pub async fn get_mapping_enabled_states(
        &self,
        ids: &[i32],
    ) -> Result<HashMap<i32, (bool, SyncState)>, tokio_postgres::Error> {
        let rows = self
            .client
            .query(
                "SELECT id, enabled, state FROM duckpipe.table_mappings \
                 WHERE id = ANY($1)",
                &[&ids],
            )
            .await?;

        Ok(rows
            .iter()
            .map(|row| {
                let id: i32 = row.get(0);
                let enabled: bool = row.get(1);
                let state_str: String = row.get(2);
                let state = SyncState::from_str(&state_str).unwrap_or(SyncState::Pending);
                (id, (enabled, state))
            })
            .collect())
    }

    /// Read all rows from duckpipe.global_config and return as GroupConfig.
    pub async fn get_global_config(&self) -> Result<GroupConfig, tokio_postgres::Error> {
        let rows = self
            .client
            .query("SELECT key, value FROM duckpipe.global_config", &[])
            .await?;

        let kv: Vec<(String, String)> = rows
            .iter()
            .map(|row| {
                let key: String = row.get(0);
                let value: String = row.get(1);
                (key, value)
            })
            .collect();

        Ok(GroupConfig::from_kv_rows(&kv))
    }

    /// Check if a replication slot exists.
    pub async fn slot_exists(&self, slot_name: &str) -> Result<bool, tokio_postgres::Error> {
        let rows = self
            .client
            .query(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot_name],
            )
            .await?;
        Ok(!rows.is_empty())
    }
}

/// Snapshot task metadata.
#[derive(Debug, Clone)]
pub struct SnapshotTask {
    pub id: i32,
    pub source_schema: String,
    pub source_table: String,
    pub target_schema: String,
    pub target_table: String,
    pub source_label: String,
    pub sync_mode: SyncMode,
}

/// Load column names and primary key attribute indices from any PG connection.
///
/// Standalone function that can operate on the local metadata client or a
/// separate remote PG connection (for remote sync groups).
pub async fn load_pk_metadata(
    client: &Client,
    source_schema: &str,
    source_table: &str,
) -> Result<(Vec<String>, Vec<usize>), tokio_postgres::Error> {
    let rows = client
        .query(
            "SELECT a.attname FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             JOIN pg_attribute a ON a.attrelid = c.oid \
             WHERE n.nspname = $1 AND c.relname = $2 \
             AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
            &[&source_schema, &source_table],
        )
        .await?;
    let attnames: Vec<String> = rows.iter().map(|row| row.get(0)).collect();

    let rows = client
        .query(
            "SELECT k.attnum::int2 FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             JOIN pg_index i ON i.indrelid = c.oid AND i.indisprimary \
             JOIN unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true \
             WHERE n.nspname = $1 AND c.relname = $2 \
             ORDER BY k.ord",
            &[&source_schema, &source_table],
        )
        .await?;
    let key_attrs: Vec<usize> = rows
        .iter()
        .map(|row| row.get::<_, i16>(0))
        .filter(|&a| a > 0)
        .map(|a| (a - 1) as usize)
        .collect();

    Ok((attnames, key_attrs))
}

/// Look up the SQL type name for a column via `format_type()`.
///
/// Standalone function that can operate on either the local or remote PG connection.
pub async fn get_column_type(
    client: &Client,
    schema: &str,
    table: &str,
    col_name: &str,
) -> Result<String, tokio_postgres::Error> {
    let rows = client
        .query(
            "SELECT pg_catalog.format_type(a.atttypid, a.atttypmod) \
             FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             JOIN pg_attribute a ON a.attrelid = c.oid \
             WHERE n.nspname = $1 AND c.relname = $2 AND a.attname = $3 \
             AND a.attnum > 0 AND NOT a.attisdropped",
            &[&schema, &table, &col_name],
        )
        .await?;

    if let Some(row) = rows.first() {
        Ok(row.get(0))
    } else {
        Ok("text".to_string()) // fallback
    }
}

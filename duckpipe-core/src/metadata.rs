//! Async metadata client for accessing duckpipe schema tables via tokio-postgres.

use std::collections::HashMap;

use tokio_postgres::Client;

use crate::types::{format_lsn, parse_lsn, SyncGroup, TableMapping};

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

    /// Get all enabled sync groups.
    pub async fn get_enabled_sync_groups(&self) -> Result<Vec<SyncGroup>, tokio_postgres::Error> {
        let rows = self
            .client
            .query(
                "SELECT id, name, publication, slot_name, confirmed_lsn::text, conninfo \
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
                SyncGroup {
                    id,
                    name,
                    publication,
                    slot_name,
                    pending_lsn: 0,
                    confirmed_lsn,
                    conninfo,
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
                 applied_lsn::text \
                 FROM duckpipe.table_mappings \
                 WHERE group_id = $1 AND source_schema = $2 AND source_table = $3",
                &[&group_id, &schema, &table],
            )
            .await?;

        if rows.is_empty() {
            return Ok(None);
        }

        let row = &rows[0];
        let id: i32 = row.get(0);
        let source_schema: String = row.get(1);
        let source_table: String = row.get(2);
        let target_schema: String = row.get(3);
        let target_table: String = row.get(4);
        let state: String = row.get(5);
        let snapshot_lsn_str: Option<String> = row.get(6);
        let snapshot_lsn = snapshot_lsn_str.map(|s| parse_lsn(&s)).unwrap_or(0);
        let enabled: bool = row.get(7);
        let source_oid: Option<i64> = row.get(8);
        let error_message: Option<String> = row.get(9);
        let applied_lsn_str: Option<String> = row.get(10);
        let applied_lsn = applied_lsn_str.map(|s| parse_lsn(&s)).unwrap_or(0);

        Ok(Some(TableMapping {
            id,
            source_schema,
            source_table,
            target_schema,
            target_table,
            state,
            snapshot_lsn,
            applied_lsn,
            enabled,
            source_oid,
            error_message,
        }))
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
        let interval = format!("{} seconds", backoff_secs);
        self.client
            .execute(
                "UPDATE duckpipe.table_mappings SET state = 'ERRORED', \
                 error_message = $1, \
                 retry_at = now() + $3::interval \
                 WHERE id = $2",
                &[&error_message, &mapping_id, &interval],
            )
            .await?;
        Ok(())
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
                 applied_lsn::text \
                 FROM duckpipe.table_mappings \
                 WHERE group_id = $1 AND state = 'ERRORED' AND enabled = true \
                 AND retry_at IS NOT NULL AND retry_at <= now()",
                &[&group_id],
            )
            .await?;

        Ok(rows
            .iter()
            .map(|row| TableMapping {
                id: row.get(0),
                source_schema: row.get(1),
                source_table: row.get(2),
                target_schema: row.get(3),
                target_table: row.get(4),
                state: row.get(5),
                snapshot_lsn: row
                    .get::<_, Option<String>>(6)
                    .map(|s| parse_lsn(&s))
                    .unwrap_or(0),
                applied_lsn: row
                    .get::<_, Option<String>>(10)
                    .map(|s| parse_lsn(&s))
                    .unwrap_or(0),
                enabled: row.get(7),
                source_oid: row.get(8),
                error_message: row.get(9),
            })
            .collect())
    }

    /// Auto-retry: transition ERRORED table back to STREAMING, clear error and failures.
    pub async fn retry_errored_table(&self, mapping_id: i32) -> Result<(), tokio_postgres::Error> {
        self.client
            .execute(
                "UPDATE duckpipe.table_mappings SET state = 'STREAMING', \
                 error_message = NULL, consecutive_failures = 0, retry_at = NULL \
                 WHERE id = $1",
                &[&mapping_id],
            )
            .await?;
        Ok(())
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
                 applied_lsn::text \
                 FROM duckpipe.table_mappings \
                 WHERE group_id = $1 AND source_oid = $2",
                &[&group_id, &source_oid],
            )
            .await?;

        if rows.is_empty() {
            return Ok(None);
        }

        let row = &rows[0];
        Ok(Some(TableMapping {
            id: row.get(0),
            source_schema: row.get(1),
            source_table: row.get(2),
            target_schema: row.get(3),
            target_table: row.get(4),
            state: row.get(5),
            snapshot_lsn: row
                .get::<_, Option<String>>(6)
                .map(|s| parse_lsn(&s))
                .unwrap_or(0),
            applied_lsn: row
                .get::<_, Option<String>>(10)
                .map(|s| parse_lsn(&s))
                .unwrap_or(0),
            enabled: row.get(7),
            source_oid: row.get(8),
            error_message: row.get(9),
        }))
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
                "SELECT id, source_schema, source_table, target_schema, target_table \
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
                 snapshot_duration_ms = $2, snapshot_rows = $3 \
                 WHERE id = $4",
                &[&lsn_str, &duration_ms_i64, &snapshot_rows_i64, &mapping_id],
            )
            .await?;
        Ok(())
    }

    /// Update per-table queued_changes (called once per sync cycle for observability).
    ///
    /// Sets `queued_changes` on each row to the current in-flight change count
    /// (shared queue + local accumulator).  Best-effort — errors are logged but
    /// not propagated, as this is purely diagnostic.
    pub async fn update_table_queued_changes(
        &self,
        counts: &[(i32, i64)],
    ) -> Result<(), tokio_postgres::Error> {
        for &(mapping_id, count) in counts {
            self.client
                .execute(
                    "UPDATE duckpipe.table_mappings SET queued_changes = $1 WHERE id = $2",
                    &[&count, &mapping_id],
                )
                .await?;
        }
        Ok(())
    }

    /// Update worker runtime state (called once per sync cycle for observability).
    pub async fn update_worker_state(
        &self,
        total_queued_changes: i64,
        is_backpressured: bool,
    ) -> Result<(), tokio_postgres::Error> {
        self.client
            .execute(
                "UPDATE duckpipe.worker_state SET total_queued_changes = $1, \
                 is_backpressured = $2, updated_at = now() WHERE id = 1",
                &[&total_queued_changes, &is_backpressured],
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
    ) -> Result<HashMap<i32, (bool, String)>, tokio_postgres::Error> {
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
                let state: String = row.get(2);
                (id, (enabled, state))
            })
            .collect())
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
    let mut attnames = Vec::new();

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

    for row in &rows {
        let name: String = row.get(0);
        attnames.push(name);
    }

    let mut key_attrs = Vec::new();
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

    for row in &rows {
        let attnum: i16 = row.get(0);
        if attnum > 0 {
            key_attrs.push((attnum - 1) as usize);
        }
    }

    Ok((attnames, key_attrs))
}

//! Non-blocking snapshot manager — fire-and-forget spawning with async result collection.
//!
//! Snapshots are independent background tasks that run concurrently with WAL streaming.
//! The sync cycle kicks new snapshots and collects completed results each iteration,
//! never blocking on snapshot completion.
//!
//! Completion notification: spawned tasks signal a `tokio::sync::Notify` when they finish,
//! so callers can wake from their poll sleep immediately instead of waiting the full
//! poll interval.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Notify;

use crate::metadata::SnapshotTask;
use crate::snapshot;

/// Non-blocking snapshot manager.
///
/// Spawns snapshot tasks as independent tokio tasks and collects results via mpsc.
/// The sync cycle calls `kick_snapshots()` to start new ones and `collect_results()`
/// to harvest completed ones — neither call blocks.
pub struct SnapshotManager {
    /// In-flight snapshot tasks: task_id → spawn time.
    in_flight: HashMap<i32, Instant>,
    result_tx: std::sync::mpsc::Sender<snapshot::SnapshotResult>,
    result_rx: std::sync::mpsc::Receiver<snapshot::SnapshotResult>,
    /// Notified by spawned snapshot tasks on completion so the poll loop can
    /// wake early instead of sleeping the full poll interval.
    notify: Arc<Notify>,
}

impl SnapshotManager {
    /// Create a new SnapshotManager with an internal mpsc channel.
    pub fn new() -> Self {
        let (tx, rx) = std::sync::mpsc::channel();
        SnapshotManager {
            in_flight: HashMap::new(),
            result_tx: tx,
            result_rx: rx,
            notify: Arc::new(Notify::new()),
        }
    }

    /// Kick snapshot tasks for tables not already in flight.
    ///
    /// Each task is spawned as an independent tokio task with its own connections.
    /// Already-running snapshots (by task_id) are skipped.
    ///
    /// Note: no stale eviction — panicked tasks that never send a result stay in
    /// `in_flight` permanently (preventing respawn). This is intentional: the
    /// snapshot slot name is deterministic (`duckpipe_snap_{task_id}`), so
    /// respawning would cause slot-creation failures and churn. Panicked entries
    /// are cleaned up on `clear()` (panic recovery) or process restart.
    pub fn kick_snapshots(
        &mut self,
        tasks: Vec<SnapshotTask>,
        connstr: &str,
        duckdb_pg_connstr: &str,
        ducklake_schema: &str,
        timing: bool,
    ) {
        for task in tasks {
            if self.in_flight.contains_key(&task.id) {
                continue; // Already in flight
            }

            self.in_flight.insert(task.id, Instant::now());

            let task_id = task.id;
            let src_schema = task.source_schema.clone();
            let src_table = task.source_table.clone();
            let tgt_schema = task.target_schema.clone();
            let tgt_table = task.target_table.clone();
            let cs = connstr.to_string();
            let ddb_cs = duckdb_pg_connstr.to_string();
            let dl_schema = ducklake_schema.to_string();
            let tx = self.result_tx.clone();
            let notify = Arc::clone(&self.notify);

            tokio::spawn(async move {
                let result = snapshot::process_snapshot_task(
                    &src_schema,
                    &src_table,
                    &tgt_schema,
                    &tgt_table,
                    &cs,
                    &ddb_cs,
                    &dl_schema,
                    timing,
                    task_id,
                )
                .await;
                let _ = tx.send(snapshot::SnapshotResult {
                    task_id,
                    source_schema: src_schema,
                    source_table: src_table,
                    target_schema: tgt_schema,
                    target_table: tgt_table,
                    result,
                });
                // Wake the poll loop so it processes the result immediately
                // instead of sleeping for the full poll interval.
                notify.notify_one();
            });
        }
    }

    /// Non-blocking drain of completed snapshot results.
    ///
    /// Removes completed task_ids from `in_flight`.
    pub fn collect_results(&mut self) -> Vec<snapshot::SnapshotResult> {
        let mut results = Vec::new();
        while let Ok(r) = self.result_rx.try_recv() {
            self.in_flight.remove(&r.task_id);
            results.push(r);
        }
        results
    }

    /// Returns true if there are any in-flight snapshot tasks.
    pub fn has_in_flight(&self) -> bool {
        !self.in_flight.is_empty()
    }

    /// Sleep for `duration`, but wake early if a snapshot task completes.
    ///
    /// Callers should use this instead of `tokio::time::sleep` when snapshots
    /// may be in flight, so that snapshot completion is processed promptly.
    pub async fn sleep_unless_snapshot_ready(&self, duration: Duration) {
        if self.in_flight.is_empty() {
            // No snapshots in flight — plain sleep.
            tokio::time::sleep(duration).await;
        } else {
            tokio::select! {
                _ = self.notify.notified() => {}
                _ = tokio::time::sleep(duration) => {}
            }
        }
    }

    /// Clear all state and recreate the mpsc channel (for panic recovery).
    pub fn clear(&mut self) {
        self.in_flight.clear();
        let (tx, rx) = std::sync::mpsc::channel();
        self.result_tx = tx;
        self.result_rx = rx;
        self.notify = Arc::new(Notify::new());
    }
}

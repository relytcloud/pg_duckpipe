//! Producer-consumer flush coordinator with persistent per-table flush threads.
//!
//! Flush threads are fully decoupled from WAL consumption:
//! - Self-trigger based on queue size (batch_threshold) or time (flush_interval)
//! - Handle PG metadata updates (applied_lsn, metrics, error state) independently
//! - Backpressure via AtomicI64 total_queued prevents unbounded memory growth
//!
//! `drain_and_wait_all()` is retained for shutdown. `drain_and_wait_table()` is
//! used for TRUNCATE (must flush before DELETE).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::duckdb_flush::FlushWorker;
use crate::flush_worker;
use crate::metadata::ERRORED_THRESHOLD;
use crate::queue::TableQueue;
use crate::types::Change;

/// Cloneable table schema info for constructing TableQueue inside the flush thread.
#[derive(Clone, Debug)]
struct QueueMeta {
    target_key: String,
    mapping_id: i32,
    attnames: Vec<String>,
    key_attrs: Vec<usize>,
    atttypes: Vec<u32>,
}

/// Shared queue data protected by Mutex.
struct SharedTableQueue {
    meta: QueueMeta,
    changes: Vec<Change>,
    last_lsn: u64,
}

/// Arc-shared handle between producer (main thread) and consumer (flush thread).
struct TableQueueHandle {
    inner: Mutex<SharedTableQueue>,
    condvar: Condvar,
}

/// Lock-free thread control signals.
struct ThreadControl {
    shutdown: AtomicBool,
    drain_requested: AtomicBool,
    /// When true, the flush thread accumulates changes but does not flush.
    /// Used during SNAPSHOT: WAL changes are buffered so they can be applied
    /// after the snapshot completes (CATCHUP phase).
    paused: AtomicBool,
}

/// Per-table coordinator entry tracking queue, control, and thread handle.
struct FlushThreadEntry {
    queue_handle: Arc<TableQueueHandle>,
    control: Arc<ThreadControl>,
    join_handle: Option<JoinHandle<()>>,
    drain_complete: Arc<(Mutex<bool>, Condvar)>,
    /// Count of changes currently held in the flush thread's local accumulator.
    /// Updated atomically by the flush thread after each drain / flush / clear.
    /// Relaxed ordering is sufficient — the coordinator reads this only for diagnostics.
    pending_local: Arc<AtomicI64>,
    /// Mapping ID, cached here so table_pending_counts() doesn't need to lock the queue.
    mapping_id: i32,
}

/// Result sent from flush thread to main thread via mpsc channel.
#[derive(Debug)]
pub enum FlushThreadResult {
    Success {
        target_key: String,
        mapping_id: i32,
        applied_count: i64,
        last_lsn: u64,
        memory_bytes: i64,
    },
    Error {
        target_key: String,
        mapping_id: i32,
        error: String,
    },
}

/// Shared backpressure state between producer (WAL consumer) and flush threads.
struct BackpressureState {
    /// Total number of in-flight changes (shared queues + local accumulators).
    /// Incremented by producer on push_change(), decremented by flush threads
    /// after flush completes or accumulated data is cleared. Using Relaxed ordering
    /// is acceptable since backpressure is best-effort (off-by-a-few is fine).
    total_queued: AtomicI64,
    /// Subset of total_queued for paused (SNAPSHOT) tables.
    /// Excluded from backpressure so buffered snapshot changes don't block
    /// WAL streaming for other tables.
    snapshot_queued: AtomicI64,
    /// Maximum allowed queued changes before backpressure kicks in.
    max_queued: i64,
}

/// Producer-consumer flush coordinator with persistent per-table flush threads.
pub struct FlushCoordinator {
    pg_connstr: String,
    ducklake_schema: String,
    group_name: String,
    threads: HashMap<String, FlushThreadEntry>,
    result_tx: mpsc::Sender<FlushThreadResult>,
    result_rx: mpsc::Receiver<FlushThreadResult>,
    backpressure: Arc<BackpressureState>,
    flush_batch_threshold: usize,
    flush_interval_ms: u64,
    /// In-memory per-table applied (flushed) LSN, updated from flush thread results via mpsc.
    ///
    /// Used to compute `flushed_lsn` for `StandbyStatusUpdate` and PG `confirmed_lsn`
    /// persistence without the PG race (flush threads commit to PG *before* sending
    /// the mpsc result, so values here are always >= the corresponding PG applied_lsn).
    per_table_lsn: HashMap<i32, u64>, // mapping_id -> latest applied LSN

    /// In-memory per-table DuckDB memory usage, updated from flush thread results.
    per_table_memory: HashMap<i32, i64>, // mapping_id -> memory_bytes

    /// Cache: target_key → mapping_id.  Populated in `ensure_queue`, used in
    /// `get_min_applied_lsn_in_coordinator` to avoid O(n) mutex locks per call.
    target_to_mapping: HashMap<String, i32>,
}

impl FlushCoordinator {
    /// Create a new coordinator. No flush threads are spawned yet.
    pub fn new(
        pg_connstr: String,
        ducklake_schema: String,
        group_name: String,
        flush_batch_threshold: i32,
        flush_interval_ms: i32,
        max_queued_changes: i32,
    ) -> Self {
        let (tx, rx) = mpsc::channel();
        FlushCoordinator {
            pg_connstr,
            ducklake_schema,
            group_name,
            threads: HashMap::new(),
            result_tx: tx,
            result_rx: rx,
            backpressure: Arc::new(BackpressureState {
                total_queued: AtomicI64::new(0),
                snapshot_queued: AtomicI64::new(0),
                max_queued: max_queued_changes as i64,
            }),
            flush_batch_threshold: flush_batch_threshold as usize,
            flush_interval_ms: flush_interval_ms as u64,
            per_table_lsn: HashMap::new(),
            per_table_memory: HashMap::new(),
            target_to_mapping: HashMap::new(),
        }
    }

    /// Ensure a queue + flush thread exists for the given target table.
    /// Creates and spawns if new. No-op if the entry already exists and the thread is alive.
    /// If the thread has died (join_handle finished), respawns it.
    ///
    /// `initial_applied_lsn` seeds the in-memory LSN map if no entry exists yet.
    /// Callers should pass the PG `applied_lsn` from the table mapping so that the
    /// in-memory state is bootstrapped correctly from persistent metadata on first use.
    ///
    /// `paused`: when true, the flush thread accumulates changes but does not flush.
    /// Used for SNAPSHOT tables — WAL changes are buffered until the snapshot completes.
    pub fn ensure_queue(
        &mut self,
        target_key: &str,
        mapping_id: i32,
        initial_applied_lsn: u64,
        attnames: Vec<String>,
        key_attrs: Vec<usize>,
        atttypes: Vec<u32>,
        paused: bool,
    ) {
        // Seed in-memory LSN from the persistent PG value if we haven't seen this table yet.
        // `or_insert` preserves any higher value already tracked from a completed flush.
        self.per_table_lsn
            .entry(mapping_id)
            .or_insert(initial_applied_lsn);
        // Cache target_key → mapping_id for lock-free lookup in get_min_applied_lsn_in_coordinator.
        self.target_to_mapping
            .insert(target_key.to_string(), mapping_id);
        // Check if entry exists and thread is alive
        let needs_spawn = match self.threads.get(target_key) {
            None => true,
            Some(entry) => match &entry.join_handle {
                Some(h) => h.is_finished(),
                None => true,
            },
        };

        if !needs_spawn {
            return;
        }

        // If the old thread is finished, join it and remove entry
        if let Some(mut old) = self.threads.remove(target_key) {
            old.control.shutdown.store(true, Ordering::Release);
            old.queue_handle.condvar.notify_one();
            if let Some(h) = old.join_handle.take() {
                let _ = h.join();
            }
        }

        let meta = QueueMeta {
            target_key: target_key.to_string(),
            mapping_id,
            attnames,
            key_attrs,
            atttypes,
        };

        let queue_handle = Arc::new(TableQueueHandle {
            inner: Mutex::new(SharedTableQueue {
                meta: meta.clone(),
                changes: Vec::new(),
                last_lsn: 0,
            }),
            condvar: Condvar::new(),
        });

        let control = Arc::new(ThreadControl {
            shutdown: AtomicBool::new(false),
            drain_requested: AtomicBool::new(false),
            paused: AtomicBool::new(paused),
        });

        let drain_complete = Arc::new((Mutex::new(false), Condvar::new()));
        let pending_local = Arc::new(AtomicI64::new(0));

        // Clone Arcs for the thread
        let qh = Arc::clone(&queue_handle);
        let ctrl = Arc::clone(&control);
        let dc = Arc::clone(&drain_complete);
        let pl = Arc::clone(&pending_local);
        let tx = self.result_tx.clone();
        let pg_connstr = self.pg_connstr.clone();
        let ducklake_schema = self.ducklake_schema.clone();
        let group_name = self.group_name.clone();
        let bp = Arc::clone(&self.backpressure);
        let batch_threshold = self.flush_batch_threshold;
        let interval_ms = self.flush_interval_ms;

        let join_handle = std::thread::Builder::new()
            .name(format!("duckpipe-flush-{}", target_key))
            .spawn(move || {
                flush_thread_main(
                    qh,
                    ctrl,
                    dc,
                    pl,
                    tx,
                    &pg_connstr,
                    &ducklake_schema,
                    &group_name,
                    bp,
                    batch_threshold,
                    interval_ms,
                );
            })
            .expect("failed to spawn flush thread");

        self.threads.insert(
            target_key.to_string(),
            FlushThreadEntry {
                queue_handle,
                control,
                join_handle: Some(join_handle),
                drain_complete,
                pending_local,
                mapping_id,
            },
        );
    }

    /// Push a change into the shared queue for the given target table.
    /// The target queue must have been created via `ensure_queue()`.
    pub fn push_change(&self, target_key: &str, change: Change) {
        if let Some(entry) = self.threads.get(target_key) {
            let mut guard = entry.queue_handle.inner.lock().unwrap();
            if change.lsn > guard.last_lsn {
                guard.last_lsn = change.lsn;
            }
            guard.changes.push(change);
            self.backpressure
                .total_queued
                .fetch_add(1, Ordering::Relaxed);
            if entry.control.paused.load(Ordering::Relaxed) {
                self.backpressure
                    .snapshot_queued
                    .fetch_add(1, Ordering::Relaxed);
            }
            entry.queue_handle.condvar.notify_one();
        }
    }

    /// Check if backpressure should pause WAL consumption.
    /// Excludes changes queued for paused (SNAPSHOT) tables so that buffered
    /// snapshot WAL changes don't block streaming for other tables.
    pub fn is_backpressured(&self) -> bool {
        let total = self.backpressure.total_queued.load(Ordering::Relaxed);
        let snapshot = self.backpressure.snapshot_queued.load(Ordering::Relaxed);
        (total - snapshot) >= self.backpressure.max_queued
    }

    /// Get the current total queued changes count (for observability).
    pub fn total_queued(&self) -> i64 {
        self.backpressure.total_queued.load(Ordering::Relaxed)
    }

    /// Return per-table pending change counts as `(mapping_id, queued_changes)`.
    ///
    /// `queued_changes` = changes in the shared queue (not yet drained by the flush thread)
    /// + changes in the flush thread's local accumulator (drained but not yet flushed).
    /// Both components are best-effort (Relaxed ordering); suitable for diagnostics only.
    pub fn table_pending_counts(&self) -> Vec<(i32, i64)> {
        self.threads
            .values()
            .map(|entry| {
                let shared = entry.queue_handle.inner.lock().unwrap().changes.len() as i64;
                let local = entry.pending_local.load(Ordering::Relaxed);
                (entry.mapping_id, shared + local)
            })
            .collect()
    }

    /// Return per-table DuckDB memory usage as `(mapping_id, memory_bytes)`.
    pub fn table_memory_bytes(&self) -> Vec<(i32, i64)> {
        self.per_table_memory
            .iter()
            .map(|(&id, &bytes)| (id, bytes))
            .collect()
    }

    /// Compute the minimum applied LSN across all tables tracked in `per_table_lsn`.
    ///
    /// Returns 0 if:
    /// - `per_table_lsn` is empty (no tables seeded or received changes yet).
    /// - Any tracked table has applied_lsn == 0 (never completed a flush).
    ///
    /// The map is populated from two sources:
    /// 1. `seed_table_lsns()` — called each cycle with PG metadata for all active
    ///    STREAMING/CATCHUP tables (ensures CATCHUP tables with no WAL changes are visible).
    /// 2. `ensure_queue()` / `collect_results()` — tables that received WAL changes or
    ///    completed flushes during this session.
    ///
    /// Because flush threads commit to PG *before* sending the mpsc result, values here
    /// are always >= the corresponding PG applied_lsn, eliminating the stale-read race.
    pub fn get_min_applied_lsn_in_coordinator(&self) -> u64 {
        if self.per_table_lsn.is_empty() {
            return 0;
        }

        let mut min_lsn: Option<u64> = None;

        for &lsn in self.per_table_lsn.values() {
            if lsn == 0 {
                // A table has never completed a flush: don't advance.
                return 0;
            }
            min_lsn = Some(min_lsn.map_or(lsn, |m: u64| m.min(lsn)));
        }

        min_lsn.unwrap_or(0)
    }

    /// Seed the in-memory per-table LSN map from a list of (mapping_id, lsn) pairs.
    ///
    /// Takes `max(current, seed)` so:
    /// - Entries already set by flush-thread results (which may be ahead of PG due to
    ///   in-flight flushes) are not regressed.
    /// - Entries at 0 (never seeded, or seeded before snapshot_lsn was available) are
    ///   updated to the snapshot_lsn floor provided by `get_active_table_lsns()`.
    ///
    /// Called at the start of each cycle so CATCHUP tables with no WAL changes are
    /// visible to `get_min_applied_lsn_in_coordinator()`.
    pub fn seed_table_lsns(&mut self, table_lsns: &[(i32, u64)]) {
        for &(mapping_id, lsn) in table_lsns {
            let entry = self.per_table_lsn.entry(mapping_id).or_insert(lsn);
            if lsn > *entry {
                *entry = lsn;
            }
        }
    }

    /// Remove flush threads, per_table_lsn entries, and target_to_mapping entries
    /// for tables whose mapping_id is no longer present in PG metadata.
    ///
    /// Called each cycle with the set of all mapping IDs still in `table_mappings`
    /// (any state). Tables removed via `remove_table()` will not be in this set,
    /// so their stale flush threads are shut down and their frozen per_table_lsn
    /// entries are removed — preventing confirmed_lsn from being held back.
    pub fn prune_removed_tables(&mut self, active_mapping_ids: &HashSet<i32>) {
        // Find target_keys whose mapping_id is no longer in metadata.
        let stale_keys: Vec<String> = self
            .target_to_mapping
            .iter()
            .filter(|(_, &id)| !active_mapping_ids.contains(&id))
            .map(|(key, _)| key.clone())
            .collect();

        for key in &stale_keys {
            // Shut down the flush thread and reclaim leaked backpressure counters.
            if let Some(mut entry) = self.threads.remove(key) {
                // Drain backpressure for any changes still in the shared queue or
                // local accumulator — push_change() incremented total_queued (and
                // snapshot_queued if paused) but the thread will never flush them.
                let shared = {
                    let guard = entry.queue_handle.inner.lock().unwrap();
                    guard.changes.len() as i64
                };
                let local = entry.pending_local.load(Ordering::Relaxed);
                let abandoned = shared + local;
                if abandoned > 0 {
                    self.backpressure
                        .total_queued
                        .fetch_sub(abandoned, Ordering::Relaxed);
                    if entry.control.paused.load(Ordering::Relaxed) {
                        self.backpressure
                            .snapshot_queued
                            .fetch_sub(abandoned, Ordering::Relaxed);
                    }
                }

                entry.control.shutdown.store(true, Ordering::Release);
                entry.queue_handle.condvar.notify_one();
                if let Some(h) = entry.join_handle.take() {
                    let _ = h.join();
                }
                tracing::info!(
                    "pg_duckpipe: pruned stale flush thread for {} (mapping removed)",
                    key
                );
            }

            // Remove target_to_mapping entry.
            self.target_to_mapping.remove(key);
        }

        // Final sweep: prune per_table_lsn and per_table_memory directly by active
        // IDs.  This catches stale entries injected by collect_results() from late
        // flush-thread results that arrived after the thread was pruned above.
        self.per_table_lsn
            .retain(|id, _| active_mapping_ids.contains(id));
        self.per_table_memory
            .retain(|id, _| active_mapping_ids.contains(id));
    }

    /// Per-table synchronous drain: signal one flush thread to drain, wait for completion.
    /// Used for TRUNCATE — must flush pending changes before DELETE.
    pub fn drain_and_wait_table(&mut self, target_key: &str) {
        if let Some(entry) = self.threads.get(target_key) {
            // Reset drain_complete flag
            {
                let mut done = entry.drain_complete.0.lock().unwrap();
                *done = false;
            }
            entry.control.drain_requested.store(true, Ordering::Release);
            entry.queue_handle.condvar.notify_one();

            // Wait for completion
            let (lock, cvar) = &*entry.drain_complete;
            let guard = lock.lock().unwrap();
            let _guard = cvar
                .wait_timeout_while(guard, Duration::from_secs(30), |done| !*done)
                .unwrap()
                .0;
        }
    }

    /// Synchronous barrier: signal all flush threads to drain their queues,
    /// wait for all to complete, then collect results.
    /// Retained for shutdown.
    pub fn drain_and_wait_all(&mut self) -> Vec<FlushThreadResult> {
        // Signal all threads to drain
        let active_keys: Vec<String> = self.threads.keys().cloned().collect();

        for key in &active_keys {
            if let Some(entry) = self.threads.get(key) {
                // Reset drain_complete flag
                {
                    let mut done = entry.drain_complete.0.lock().unwrap();
                    *done = false;
                }
                entry.control.drain_requested.store(true, Ordering::Release);
                entry.queue_handle.condvar.notify_one();
            }
        }

        // Wait for each thread to signal drain_complete
        for key in &active_keys {
            if let Some(entry) = self.threads.get(key) {
                let (lock, cvar) = &*entry.drain_complete;
                let guard = lock.lock().unwrap();
                // Wait with timeout to avoid deadlock if thread died
                let _guard = cvar
                    .wait_timeout_while(guard, Duration::from_secs(30), |done| !*done)
                    .unwrap()
                    .0;
            }
        }

        // Collect all pending results from the channel
        self.collect_results()
    }

    /// Non-blocking drain of the mpsc result channel.
    ///
    /// Also updates the in-memory per-table LSN map from Success results.
    /// The ordering guarantee holds here: flush threads commit applied_lsn to PG
    /// *before* sending the mpsc result, so entries added to `per_table_lsn` by
    /// this method imply PG is already up-to-date for those mapping_ids.
    pub fn collect_results(&mut self) -> Vec<FlushThreadResult> {
        let mut results = Vec::new();
        while let Ok(r) = self.result_rx.try_recv() {
            if let FlushThreadResult::Success {
                mapping_id,
                last_lsn,
                memory_bytes,
                ..
            } = &r
            {
                let entry = self.per_table_lsn.entry(*mapping_id).or_insert(0);
                if *last_lsn > *entry {
                    *entry = *last_lsn;
                }
                if *memory_bytes > 0 {
                    self.per_table_memory.insert(*mapping_id, *memory_bytes);
                }
            }
            results.push(r);
        }
        results
    }

    /// Signal all flush threads to stop and join them.
    pub fn shutdown(&mut self) {
        for (_, entry) in self.threads.iter() {
            entry.control.shutdown.store(true, Ordering::Release);
            entry.queue_handle.condvar.notify_one();
        }
        for (_, entry) in self.threads.iter_mut() {
            if let Some(h) = entry.join_handle.take() {
                let _ = h.join();
            }
        }
        self.threads.clear();
    }

    /// Shutdown all threads + recreate mpsc channel (used for panic recovery).
    /// Clears all in-memory LSN state — per_table_lsn re-seeds from PG on next
    /// ensure_queue. Callers must also clear the consumers map so the next cycle
    /// reconnects from `confirmed_lsn` (the crash-safe restart point from PG).
    pub fn clear(&mut self) {
        self.shutdown();
        let (tx, rx) = mpsc::channel();
        self.result_tx = tx;
        self.result_rx = rx;
        self.per_table_lsn.clear();
        self.per_table_memory.clear();
        self.target_to_mapping.clear();
    }

    /// Unpause a table's flush thread after snapshot completion.
    ///
    /// The entire adjustment (snapshot_queued subtraction + paused=false) is done
    /// while holding the queue lock. Since `push_change` also holds this lock when
    /// checking `paused` and incrementing `snapshot_queued`, there is no race window
    /// where a concurrent push sees stale `paused=true` after the subtraction.
    pub fn unpause_table(&self, target_key: &str) {
        if let Some(entry) = self.threads.get(target_key) {
            // Hold queue lock for the entire operation to synchronize with push_change.
            let guard = entry.queue_handle.inner.lock().unwrap();
            let shared = guard.changes.len() as i64;
            let local = entry.pending_local.load(Ordering::Relaxed);
            self.backpressure
                .snapshot_queued
                .fetch_sub(shared + local, Ordering::Relaxed);
            // Clear paused while still holding the lock ��� push_change cannot
            // observe paused=true after this point.
            entry.control.paused.store(false, Ordering::Release);
            drop(guard);
            // Wake the flush thread so it starts flushing accumulated data.
            entry.queue_handle.condvar.notify_one();
        }
    }

    /// Get the PG connection string.
    pub fn pg_connstr(&self) -> &str {
        &self.pg_connstr
    }

    /// Get the DuckLake metadata schema name.
    pub fn ducklake_schema(&self) -> &str {
        &self.ducklake_schema
    }

    /// Get the sync group name.
    pub fn group_name(&self) -> &str {
        &self.group_name
    }
}

impl Drop for FlushCoordinator {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Main loop for a persistent flush thread (self-triggered).
///
/// Each thread owns its own `FlushWorker` (with a DuckDB connection).
/// On flush error, the worker is dropped and lazily recreated on the next iteration.
///
/// Self-trigger logic: flush when accumulated changes >= batch_threshold OR
/// time since last flush >= flush_interval. Flush threads also handle PG metadata
/// updates (applied_lsn, metrics, error state) via their own tokio runtime.
fn flush_thread_main(
    queue_handle: Arc<TableQueueHandle>,
    control: Arc<ThreadControl>,
    drain_complete: Arc<(Mutex<bool>, Condvar)>,
    pending_local: Arc<AtomicI64>,
    result_tx: mpsc::Sender<FlushThreadResult>,
    pg_connstr: &str,
    ducklake_schema: &str,
    group_name: &str,
    backpressure: Arc<BackpressureState>,
    batch_threshold: usize,
    flush_interval_ms: u64,
) {
    // Each flush thread owns its own tokio runtime for async PG metadata updates.
    // This avoids deadlock with the main thread's current_thread runtime, since
    // tokio_postgres::connect spawns a connection driver task that must be polled.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to create flush thread runtime");

    let mut worker: Option<FlushWorker> = None;
    let mut accumulated: Vec<Change> = Vec::new();
    let mut accumulated_count: i64 = 0; // tracks changes in local accumulator for backpressure
    let mut accumulated_lsn: u64 = 0;
    let mut accumulated_meta: Option<QueueMeta> = None;
    let mut last_flush = Instant::now();
    let flush_interval = Duration::from_millis(flush_interval_ms);

    loop {
        // Calculate remaining time until next time-based flush
        let elapsed = last_flush.elapsed();
        let wait_timeout = if accumulated.is_empty() {
            // No accumulated changes — wait for full interval
            flush_interval
        } else if elapsed >= flush_interval {
            // Already past interval — don't wait
            Duration::ZERO
        } else {
            flush_interval - elapsed
        };

        // Lock shared queue, drain new changes and check signals
        let drain_requested;
        {
            let mut guard = queue_handle.inner.lock().unwrap();

            // Wait with timeout for new changes, drain request, or shutdown
            if guard.changes.is_empty()
                && !control.shutdown.load(Ordering::Acquire)
                && !control.drain_requested.load(Ordering::Acquire)
                && wait_timeout > Duration::ZERO
            {
                let (new_guard, _timeout_result) = queue_handle
                    .condvar
                    .wait_timeout(guard, wait_timeout)
                    .unwrap();
                guard = new_guard;
            }

            // Check shutdown
            if control.shutdown.load(Ordering::Acquire) {
                // Drop in-memory changes — confirmed_lsn was never advanced past them,
                // so PostgreSQL will re-deliver them on the next startup.
                backpressure
                    .total_queued
                    .fetch_sub(accumulated_count, Ordering::Relaxed);
                pending_local.store(0, Ordering::Relaxed);
                if control.drain_requested.load(Ordering::Acquire) {
                    signal_drain_complete(&drain_complete, &control);
                }
                return;
            }

            // Drain up to `batch_threshold` changes from the shared queue into
            // the local accumulator.
            //
            // Capping the drain is critical for progress visibility: when the
            // WAL consumer has queued a large backlog (e.g. 200k catch-up
            // changes), draining all at once produces one monolithic DuckDB
            // transaction that takes 25+ seconds with no intermediate commits.
            // The benchmark (and any monitoring query) sees 0 rows/s until the
            // transaction commits, then a sudden jump.  With a per-drain cap of
            // `batch_threshold`, large queues are processed in chunks: each
            // chunk flushes independently, commits, and becomes immediately
            // visible in DuckLake.
            //
            // LSN safety: `accumulated_lsn` is derived from the drained
            // changes only (max of their per-change `.lsn`).  We must NOT use
            // `guard.last_lsn` here because that reflects the last change in
            // the *entire* queue — setting applied_lsn past unflushed changes
            // would be crash-unsafe (slot could advance past un-flushed WAL).
            if !guard.changes.is_empty() {
                let drain_n = guard.changes.len().min(batch_threshold);
                let changes: Vec<Change> = guard.changes.drain(..drain_n).collect();
                let count = changes.len() as i64;
                // Max LSN across drained changes (WAL is ordered, so last = max).
                let drained_lsn = changes.last().map(|c| c.lsn).unwrap_or(0);
                if drained_lsn > accumulated_lsn {
                    accumulated_lsn = drained_lsn;
                }
                if accumulated_meta.is_none() {
                    accumulated_meta = Some(guard.meta.clone());
                }
                drop(guard); // Release lock before extending accumulator

                accumulated.extend(changes);
                accumulated_count += count;
                // Update local-accumulator counter so the coordinator can read it
                // without locking.  Relaxed ordering is fine for diagnostics.
                pending_local.store(accumulated_count, Ordering::Relaxed);
                // Note: backpressure counter is NOT decremented here.
                // It is decremented after flush/clear to accurately reflect
                // all in-flight data (shared queues + local accumulators).
            } else {
                drop(guard);
            }

            drain_requested = control.drain_requested.load(Ordering::Acquire);
        }

        // Determine whether to flush
        let should_flush = if control.paused.load(Ordering::Acquire) {
            // Paused (SNAPSHOT in progress): accumulate only, don't flush.
            // WAL changes are buffered until snapshot completes and unpause_table() is called.
            false
        } else if drain_requested {
            // Explicit drain request (TRUNCATE or shutdown) — always flush
            true
        } else if accumulated.is_empty() {
            false
        } else if accumulated.len() >= batch_threshold {
            // Batch threshold reached
            true
        } else {
            // Time threshold reached
            last_flush.elapsed() >= flush_interval
        };

        if should_flush && !accumulated.is_empty() {
            if let Some(meta) = accumulated_meta.as_ref() {
                do_flush(
                    &mut worker,
                    &mut accumulated,
                    accumulated_lsn,
                    meta,
                    pg_connstr,
                    ducklake_schema,
                    group_name,
                    &result_tx,
                    &rt,
                );
            }
            accumulated.clear();
            backpressure
                .total_queued
                .fetch_sub(accumulated_count, Ordering::Relaxed);
            accumulated_count = 0;
            pending_local.store(0, Ordering::Relaxed);
            accumulated_lsn = 0;
            // Reset from AFTER flush completion, not before. This gives a
            // cooldown of flush_interval between flushes. Under a slow flush,
            // the next trigger comes from batch_threshold, not the timer.
            last_flush = Instant::now();
        }

        // Signal drain_complete if a drain was requested
        if drain_requested {
            signal_drain_complete(&drain_complete, &control);
        }
    }
}

/// Execute a flush: build TableQueue, run FlushWorker, update PG metadata.
fn do_flush(
    worker: &mut Option<FlushWorker>,
    accumulated: &mut Vec<Change>,
    last_lsn: u64,
    meta: &QueueMeta,
    pg_connstr: &str,
    ducklake_schema: &str,
    group_name: &str,
    result_tx: &mpsc::Sender<FlushThreadResult>,
    rt: &tokio::runtime::Runtime,
) {
    // Build a TableQueue from accumulated changes
    let mut table_queue = TableQueue::new(
        meta.target_key.clone(),
        meta.mapping_id,
        meta.attnames.clone(),
        meta.key_attrs.clone(),
        meta.atttypes.clone(),
    );
    for change in accumulated.drain(..) {
        table_queue.push(change);
    }

    // Ensure we have a FlushWorker
    if worker.is_none() {
        match FlushWorker::new(pg_connstr, ducklake_schema) {
            Ok(w) => *worker = Some(w),
            Err(e) => {
                let error_msg = format!("failed to create FlushWorker: {}", e);
                let _ = result_tx.send(FlushThreadResult::Error {
                    target_key: meta.target_key.clone(),
                    mapping_id: meta.mapping_id,
                    error: error_msg.clone(),
                });
                // Update error state in PG
                let _ = rt.block_on(flush_worker::update_error_state(
                    pg_connstr,
                    meta.mapping_id,
                    &error_msg,
                    ERRORED_THRESHOLD,
                    group_name,
                ));
                return;
            }
        }
    }

    // Flush
    match worker.as_mut().unwrap().flush(table_queue) {
        Ok(result) => {
            // Update PG metadata: metrics + applied_lsn
            if let Err(e) = rt.block_on(flush_worker::update_metrics_via_pg(
                pg_connstr,
                result.mapping_id,
                result.applied_count,
                last_lsn,
                group_name,
            )) {
                tracing::error!(
                    "pg_duckpipe: metrics update failed for {}: {}",
                    result.target_key,
                    e
                );
            }
            // Clear error state on success
            let _ = rt.block_on(flush_worker::clear_error_on_success(
                pg_connstr,
                result.mapping_id,
                group_name,
            ));

            let _ = result_tx.send(FlushThreadResult::Success {
                target_key: result.target_key,
                mapping_id: result.mapping_id,
                applied_count: result.applied_count,
                last_lsn,
                memory_bytes: result.memory_bytes,
            });
        }
        Err(e) => {
            // Drop worker on error — will be recreated on next iteration
            *worker = None;
            let _ = result_tx.send(FlushThreadResult::Error {
                target_key: meta.target_key.clone(),
                mapping_id: meta.mapping_id,
                error: e.clone(),
            });
            // Update error state in PG
            let _ = rt.block_on(flush_worker::update_error_state(
                pg_connstr,
                meta.mapping_id,
                &e,
                ERRORED_THRESHOLD,
                group_name,
            ));
        }
    }
}

/// Helper: signal the drain_complete condvar and clear the drain_requested flag.
fn signal_drain_complete(
    drain_complete: &Arc<(Mutex<bool>, Condvar)>,
    control: &Arc<ThreadControl>,
) {
    let (lock, cvar) = &**drain_complete;
    let mut done = lock.lock().unwrap();
    *done = true;
    control.drain_requested.store(false, Ordering::Release);
    cvar.notify_one();
}

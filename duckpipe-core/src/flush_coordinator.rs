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

/// In-memory group-level metrics from FlushCoordinator.
#[derive(Clone, Copy, Default)]
pub struct GroupMetrics {
    pub group_id: i32,
    pub total_queued_changes: i64,
    pub is_backpressured: bool,
    pub active_flushes: i32,
    pub gate_wait_avg_ms: i64,
    pub gate_timeouts: i64,
}

/// In-memory per-table metrics from FlushCoordinator.
#[derive(Clone, Copy, Default)]
pub struct TableMetrics {
    pub mapping_id: i32,
    pub queued_changes: i64,
    pub duckdb_memory_bytes: i64,
    pub flush_count: i64,
    pub flush_duration_ms: i64,
}
use crate::types::Change;

/// Cloneable table schema info used by the flush thread for DuckDB buffer operations.
#[derive(Clone, Debug)]
struct QueueMeta {
    target_key: String,
    mapping_id: i32,
    attnames: Vec<String>,
    key_attrs: Vec<usize>,
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
        flush_duration_ms: i64,
    },
    Error {
        target_key: String,
        mapping_id: i32,
        error: String,
    },
}

/// Ticket-based FIFO semaphore limiting concurrent flush operations.
///
/// With 100+ tables syncing, all flush threads can execute `flush_buffer()` concurrently,
/// causing peak memory spikes (each gets `flush_memory_mb`) and DuckLake commit lock
/// contention. FlushGate ensures only N threads flush at once while the rest continue
/// buffering in low-memory mode.
///
/// Fairness: threads acquire tickets in arrival order. Only the thread whose ticket
/// equals `now_serving` can acquire a slot — strict FIFO, no starvation possible.
/// On timeout, the thread forfeits its ticket (advances `now_serving`) so the queue
/// doesn't block.
struct FlushGate {
    inner: Mutex<FlushGateInner>,
    turn: Condvar,
    /// Fallback timeout used until enough flush durations are recorded.
    default_timeout: Duration,
}

/// Number of recent flush durations to keep for median calculation.
const FLUSH_DURATION_RING_SIZE: usize = 64;

/// Minimum acquire timeout to avoid churn when flushes are very fast.
const MIN_GATE_TIMEOUT: Duration = Duration::from_secs(1);

struct FlushGateInner {
    active: usize,
    max_concurrent: usize,
    next_ticket: u64,
    now_serving: u64,
    /// Ring buffer of recent flush durations (milliseconds) for adaptive timeout.
    duration_ring: [u64; FLUSH_DURATION_RING_SIZE],
    duration_count: usize, // total recorded (may exceed ring size)
    duration_write: usize, // next write index (wraps around)
    /// Cumulative wait time in milliseconds across all acquire() calls.
    total_wait_ms: u64,
    /// Number of successful acquire() calls.
    acquire_count: u64,
    /// Number of timed-out (failed) acquire() calls.
    timeout_count: u64,
}

impl FlushGate {
    fn new(max_concurrent: usize, default_timeout: Duration) -> Self {
        FlushGate {
            inner: Mutex::new(FlushGateInner {
                active: 0,
                max_concurrent,
                next_ticket: 0,
                now_serving: 0,
                duration_ring: [0; FLUSH_DURATION_RING_SIZE],
                duration_count: 0,
                duration_write: 0,
                total_wait_ms: 0,
                acquire_count: 0,
                timeout_count: 0,
            }),
            turn: Condvar::new(),
            default_timeout,
        }
    }

    /// Take a ticket and wait until it's our turn AND a slot is available.
    /// Timeout is adaptive: median of recent flush durations, or the default
    /// timeout until enough data is collected.
    /// Returns true if a slot was acquired, false on timeout.
    fn acquire(&self) -> bool {
        let start = Instant::now();
        let mut inner = self.inner.lock().unwrap();
        let timeout = Self::median_duration(&inner).unwrap_or(self.default_timeout);
        let my_ticket = inner.next_ticket;
        inner.next_ticket += 1;

        let deadline = start + timeout;
        loop {
            if my_ticket == inner.now_serving && inner.active < inner.max_concurrent {
                inner.active += 1;
                inner.now_serving += 1;
                inner.total_wait_ms += start.elapsed().as_millis() as u64;
                inner.acquire_count += 1;
                return true;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                // Timed out: if it's our turn, skip our ticket so we don't
                // block everyone behind us.
                inner.total_wait_ms += start.elapsed().as_millis() as u64;
                inner.timeout_count += 1;
                if my_ticket == inner.now_serving {
                    inner.now_serving += 1;
                    drop(inner);
                    self.turn.notify_all();
                }
                return false;
            }
            let (guard, _) = self.turn.wait_timeout(inner, remaining).unwrap();
            inner = guard;
        }
    }

    /// Release a flush slot and record the flush duration for adaptive timeout.
    /// Wakes all waiters so the next-in-line thread can check its turn.
    fn release(&self, flush_duration: Duration) {
        {
            let mut inner = self.inner.lock().unwrap();
            debug_assert!(
                inner.active > 0,
                "FlushGate::release without matching acquire"
            );
            inner.active -= 1;

            // Record flush duration into ring buffer
            let idx = inner.duration_write;
            inner.duration_ring[idx] = flush_duration.as_millis() as u64;
            inner.duration_write = (idx + 1) % FLUSH_DURATION_RING_SIZE;
            inner.duration_count += 1;
        }
        self.turn.notify_all();
    }

    /// Dynamically update the max concurrent flushes (for runtime GUC changes).
    /// Wakes waiters in case more slots are now available.
    fn set_max(&self, n: usize) {
        {
            let mut inner = self.inner.lock().unwrap();
            inner.max_concurrent = n;
        }
        self.turn.notify_all();
    }

    /// Compute the median flush duration from the ring buffer (called with lock held).
    fn median_duration(inner: &FlushGateInner) -> Option<Duration> {
        let n = inner.duration_count.min(FLUSH_DURATION_RING_SIZE);
        if n == 0 {
            return None;
        }
        let mut sorted = [0u64; FLUSH_DURATION_RING_SIZE];
        sorted[..n].copy_from_slice(&inner.duration_ring[..n]);
        sorted[..n].sort_unstable();
        let median_ms = sorted[n / 2];
        Some(Duration::from_millis(median_ms).max(MIN_GATE_TIMEOUT))
    }

    /// Current number of active flushes.
    fn active_count(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.active
    }

    /// Gate statistics for monitoring.
    fn stats(&self) -> GateStats {
        let inner = self.inner.lock().unwrap();
        let total_calls = inner.acquire_count + inner.timeout_count;
        let avg_wait_ms = if total_calls > 0 {
            (inner.total_wait_ms / total_calls) as i64
        } else {
            0
        };
        GateStats {
            avg_wait_ms,
            timeout_count: inner.timeout_count as i64,
        }
    }
}

/// Gate-level metrics returned by [`FlushGate::stats`].
#[derive(Clone, Copy, Default)]
pub struct GateStats {
    /// Mean wait time in ms across all acquire() calls (success + timeout).
    pub avg_wait_ms: i64,
    /// Number of timed-out acquire() calls.
    pub timeout_count: i64,
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

    /// In-memory per-table cumulative flush count, updated from flush thread results.
    per_table_flush_count: HashMap<i32, i64>, // mapping_id -> flush_count

    /// In-memory per-table last flush duration in ms, updated from flush thread results.
    per_table_flush_duration: HashMap<i32, i64>, // mapping_id -> flush_duration_ms

    /// Cache: target_key → mapping_id.  Populated in `ensure_queue`, used in
    /// `get_min_applied_lsn_in_coordinator` to avoid O(n) mutex locks per call.
    target_to_mapping: HashMap<String, i32>,
    /// DuckDB memory limit in MB during buffer accumulation.
    buffer_memory_mb: i32,
    /// DuckDB memory limit in MB during flush/compaction.
    flush_memory_mb: i32,
    /// Global-per-group semaphore limiting concurrent flush operations.
    flush_gate: Arc<FlushGate>,
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
        buffer_memory_mb: i32,
        flush_memory_mb: i32,
        max_concurrent_flushes: i32,
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
            per_table_flush_count: HashMap::new(),
            per_table_flush_duration: HashMap::new(),
            target_to_mapping: HashMap::new(),
            buffer_memory_mb,
            flush_memory_mb,
            flush_gate: Arc::new(FlushGate::new(
                max_concurrent_flushes as usize,
                Duration::from_millis(flush_interval_ms as u64),
            )),
        }
    }

    /// Update the maximum concurrent flushes (for runtime GUC changes).
    pub fn set_max_concurrent_flushes(&self, n: i32) {
        self.flush_gate.set_max(n as usize);
    }

    /// Current number of active flush operations.
    pub fn active_flush_count(&self) -> usize {
        self.flush_gate.active_count()
    }

    /// Gate statistics for monitoring.
    pub fn gate_stats(&self) -> GateStats {
        self.flush_gate.stats()
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
        _atttypes: Vec<u32>,
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
        let fg = Arc::clone(&self.flush_gate);
        let batch_threshold = self.flush_batch_threshold;
        let interval_ms = self.flush_interval_ms;
        let buffer_memory_mb = self.buffer_memory_mb;
        let flush_memory_mb = self.flush_memory_mb;

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
                    fg,
                    batch_threshold,
                    interval_ms,
                    buffer_memory_mb,
                    flush_memory_mb,
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

    /// Return per-table flush metrics as `(mapping_id, flush_count, flush_duration_ms)`.
    pub fn table_flush_metrics(&self) -> Vec<(i32, i64, i64)> {
        let mut result = Vec::new();
        for (&id, &count) in &self.per_table_flush_count {
            let dur = self.per_table_flush_duration.get(&id).copied().unwrap_or(0);
            result.push((id, count, dur));
        }
        result
    }

    /// Return all per-table metrics combined in a single pass.
    pub fn table_combined_metrics(&self) -> Vec<TableMetrics> {
        self.threads
            .values()
            .map(|entry| {
                let shared = entry.queue_handle.inner.lock().unwrap().changes.len() as i64;
                let local = entry.pending_local.load(Ordering::Relaxed);
                let mid = entry.mapping_id;
                TableMetrics {
                    mapping_id: mid,
                    queued_changes: shared + local,
                    duckdb_memory_bytes: self.per_table_memory.get(&mid).copied().unwrap_or(0),
                    flush_count: self.per_table_flush_count.get(&mid).copied().unwrap_or(0),
                    flush_duration_ms: self
                        .per_table_flush_duration
                        .get(&mid)
                        .copied()
                        .unwrap_or(0),
                }
            })
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

        // Final sweep: prune per_table_lsn, per_table_memory, and flush tracking
        // maps directly by active IDs.  This catches stale entries injected by
        // collect_results() from late flush-thread results that arrived after the
        // thread was pruned above.
        self.per_table_lsn
            .retain(|id, _| active_mapping_ids.contains(id));
        self.per_table_memory
            .retain(|id, _| active_mapping_ids.contains(id));
        self.per_table_flush_count
            .retain(|id, _| active_mapping_ids.contains(id));
        self.per_table_flush_duration
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
                flush_duration_ms,
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
                *self.per_table_flush_count.entry(*mapping_id).or_insert(0) += 1;
                self.per_table_flush_duration
                    .insert(*mapping_id, *flush_duration_ms);
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
        self.per_table_flush_count.clear();
        self.per_table_flush_duration.clear();
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
/// Changes are drained from the shared queue into a DuckDB buffer table (spillable
/// to disk), decoupling queue consumption from flushing and enabling larger batch
/// windows without unbounded Rust memory growth.
///
/// Self-trigger logic: flush when buffered changes >= batch_threshold OR
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
    flush_gate: Arc<FlushGate>,
    batch_threshold: usize,
    flush_interval_ms: u64,
    buffer_memory_mb: i32,
    flush_memory_mb: i32,
) {
    // Each flush thread owns its own tokio runtime for async PG metadata updates.
    // This avoids deadlock with the main thread's current_thread runtime, since
    // tokio_postgres::connect spawns a connection driver task that must be polled.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to create flush thread runtime");

    let mut worker: Option<FlushWorker> = None;
    let mut buffered_count: i64 = 0; // tracks changes in DuckDB buffer for backpressure
    let mut buffered_lsn: u64 = 0;
    let mut buffered_meta: Option<QueueMeta> = None;
    let mut next_seq: i32 = 0; // monotonic counter for _seq across appends
    let mut last_flush = Instant::now();
    let flush_interval = Duration::from_millis(flush_interval_ms);

    loop {
        // Calculate remaining time until next time-based flush
        let elapsed = last_flush.elapsed();
        let wait_timeout = if buffered_count == 0 {
            // No buffered changes — wait for full interval
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
                // Clear DuckDB buffer — confirmed_lsn was never advanced past
                // buffered data, so PostgreSQL will re-deliver on next startup.
                if let Some(w) = worker.as_mut() {
                    w.clear_buffer();
                }
                backpressure
                    .total_queued
                    .fetch_sub(buffered_count, Ordering::Relaxed);
                pending_local.store(0, Ordering::Relaxed);
                if control.drain_requested.load(Ordering::Acquire) {
                    signal_drain_complete(&drain_complete, &control);
                }
                return;
            }

            // Drain up to `batch_threshold` changes from the shared queue,
            // append them to the DuckDB buffer table, then drop the local Vec.
            //
            // Capping the drain is critical for progress visibility: when the
            // WAL consumer has queued a large backlog (e.g. 200k catch-up
            // changes), draining all at once produces one monolithic DuckDB
            // transaction that takes 25+ seconds with no intermediate commits.
            // With a per-drain cap of `batch_threshold`, large queues are
            // processed in chunks: each chunk flushes independently, commits,
            // and becomes immediately visible in DuckLake.
            //
            // LSN safety: `buffered_lsn` is derived from the drained changes
            // only (max of their per-change `.lsn`).  We must NOT use
            // `guard.last_lsn` here because that reflects the last change in
            // the *entire* queue — setting applied_lsn past unflushed changes
            // would be crash-unsafe (slot could advance past un-flushed WAL).
            if !guard.changes.is_empty() {
                let drain_n = guard.changes.len().min(batch_threshold);
                let changes: Vec<Change> = guard.changes.drain(..drain_n).collect();
                let count = changes.len() as i64;
                // Max LSN across drained changes (WAL is ordered, so last = max).
                let drained_lsn = changes.last().map(|c| c.lsn).unwrap_or(0);
                if drained_lsn > buffered_lsn {
                    buffered_lsn = drained_lsn;
                }
                if buffered_meta.is_none() {
                    buffered_meta = Some(guard.meta.clone());
                }
                drop(guard); // Release lock before DuckDB work

                // buffered_meta is a thread-local Option, safe to borrow after drop(guard)
                let meta = buffered_meta.as_ref().unwrap();

                // Ensure FlushWorker exists before appending
                if worker.is_none() {
                    match FlushWorker::new(
                        pg_connstr,
                        ducklake_schema,
                        buffer_memory_mb,
                        flush_memory_mb,
                    ) {
                        Ok(w) => worker = Some(w),
                        Err(e) => {
                            let error_msg = format!("failed to create FlushWorker: {}", e);
                            backpressure
                                .total_queued
                                .fetch_sub(buffered_count + count, Ordering::Relaxed);
                            buffered_count = 0;
                            next_seq = 0;
                            pending_local.store(0, Ordering::Relaxed);
                            buffered_lsn = 0;
                            report_flush_error(
                                &result_tx,
                                &rt,
                                pg_connstr,
                                group_name,
                                &meta.target_key,
                                meta.mapping_id,
                                &error_msg,
                            );
                            drain_requested = control.drain_requested.load(Ordering::Acquire);
                            if drain_requested {
                                signal_drain_complete(&drain_complete, &control);
                            }
                            continue;
                        }
                    }
                }

                // Append to DuckDB buffer
                match worker.as_mut().unwrap().append_to_buffer(
                    &changes,
                    &meta.target_key,
                    &meta.attnames,
                    &meta.key_attrs,
                    next_seq,
                ) {
                    Ok(new_seq) => {
                        next_seq = new_seq;
                        buffered_count += count;
                        pending_local.store(buffered_count, Ordering::Relaxed);
                    }
                    Err(e) => {
                        // Append failed — drop worker (loses DuckDB buffer),
                        // decrement backpressure for all lost data, send error.
                        worker = None;
                        backpressure
                            .total_queued
                            .fetch_sub(buffered_count + count, Ordering::Relaxed);
                        buffered_count = 0;
                        next_seq = 0;
                        pending_local.store(0, Ordering::Relaxed);
                        buffered_lsn = 0;
                        report_flush_error(
                            &result_tx,
                            &rt,
                            pg_connstr,
                            group_name,
                            &meta.target_key,
                            meta.mapping_id,
                            &e,
                        );
                        drain_requested = control.drain_requested.load(Ordering::Acquire);
                        if drain_requested {
                            signal_drain_complete(&drain_complete, &control);
                        }
                        continue;
                    }
                }
                // Local Vec<Change> is dropped here — Rust memory freed.
            } else {
                drop(guard);
            }

            drain_requested = control.drain_requested.load(Ordering::Acquire);
        }

        // Determine whether to flush
        let should_flush = if control.paused.load(Ordering::Acquire) {
            // Paused (SNAPSHOT in progress): accumulate in DuckDB buffer only, don't flush.
            // WAL changes are buffered (spillable to disk) until snapshot completes
            // and unpause_table() is called.
            false
        } else if drain_requested {
            // Explicit drain request (TRUNCATE or shutdown) — always flush
            true
        } else if buffered_count == 0 {
            false
        } else if buffered_count as usize >= batch_threshold {
            // Batch threshold reached
            true
        } else {
            // Time threshold reached
            last_flush.elapsed() >= flush_interval
        };

        if should_flush && buffered_count > 0 {
            // Gate: ticket-based FIFO to guarantee fairness.
            //   - drain_requested: bypass gate entirely (TRUNCATE must flush now)
            //   - otherwise: acquire() blocks until our ticket is served or timeout
            let gate_acquired = if drain_requested {
                false // drain bypasses gate; don't acquire a slot
            } else if flush_gate.acquire() {
                true
            } else {
                // Timed out — continue to next iteration (will re-check queue & retry).
                continue;
            };

            let flush_start = Instant::now();

            if let Some(meta) = buffered_meta.as_ref() {
                do_flush_buffer(
                    &mut worker,
                    buffered_count,
                    buffered_lsn,
                    meta,
                    pg_connstr,
                    group_name,
                    &result_tx,
                    &rt,
                );
            }

            if gate_acquired {
                flush_gate.release(flush_start.elapsed());
            }

            backpressure
                .total_queued
                .fetch_sub(buffered_count, Ordering::Relaxed);
            buffered_count = 0;
            next_seq = 0;
            pending_local.store(0, Ordering::Relaxed);
            buffered_lsn = 0;
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

/// Execute a flush from the DuckDB buffer: compact, apply to DuckLake, update PG metadata.
fn do_flush_buffer(
    worker: &mut Option<FlushWorker>,
    buffered_count: i64,
    last_lsn: u64,
    meta: &QueueMeta,
    pg_connstr: &str,
    group_name: &str,
    result_tx: &mpsc::Sender<FlushThreadResult>,
    rt: &tokio::runtime::Runtime,
) {
    let w = match worker.as_mut() {
        Some(w) => w,
        None => {
            // Worker should exist if buffered_count > 0, but handle gracefully
            report_flush_error(
                result_tx,
                rt,
                pg_connstr,
                group_name,
                &meta.target_key,
                meta.mapping_id,
                "FlushWorker missing at flush time",
            );
            return;
        }
    };

    match w.flush_buffer(&meta.target_key, meta.mapping_id, buffered_count) {
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
                flush_duration_ms: result.flush_duration_ms,
            });

            // Drop worker after successful flush — recreated lazily on next cycle.
            // This releases the DuckDB connection and its allocated memory back to
            // the OS, preventing RSS from staying at the flush-phase high-water mark.
            *worker = None;
        }
        Err(e) => {
            // Drop worker on error — will be recreated on next iteration
            *worker = None;
            report_flush_error(
                result_tx,
                rt,
                pg_connstr,
                group_name,
                &meta.target_key,
                meta.mapping_id,
                &e,
            );
        }
    }
}

/// Send error result via mpsc and update PG error state.
fn report_flush_error(
    result_tx: &mpsc::Sender<FlushThreadResult>,
    rt: &tokio::runtime::Runtime,
    pg_connstr: &str,
    group_name: &str,
    target_key: &str,
    mapping_id: i32,
    error: &str,
) {
    let _ = result_tx.send(FlushThreadResult::Error {
        target_key: target_key.to_string(),
        mapping_id,
        error: error.to_string(),
    });
    let _ = rt.block_on(flush_worker::update_error_state(
        pg_connstr,
        mapping_id,
        error,
        ERRORED_THRESHOLD,
        group_name,
    ));
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

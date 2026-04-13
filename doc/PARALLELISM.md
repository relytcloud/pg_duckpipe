# pg_duckpipe Parallelism Model

How pg_duckpipe organizes threads, async tasks, and inter-thread communication to achieve sub-second CDC latency with bounded memory.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Thread Architecture](#2-thread-architecture)
3. [Parallel Table Processing](#3-parallel-table-processing)
4. [Inter-Thread Communication](#4-inter-thread-communication)
5. [Backpressure](#5-backpressure)
6. [Crash Safety & LSN Management](#6-crash-safety--lsn-management)

---

## 1. Overview

pg_duckpipe implements a **decoupled producer-consumer architecture**. A single WAL consumer thread reads the replication stream, decodes changes, and dispatches them into per-table queues. Independent OS flush threads drain those queues and write to DuckDB/DuckLake. Initial table snapshots run as parallel async tasks alongside WAL streaming (see [known limitation](#32-parallel-snapshots) on snapshot I/O).

![Overview](img/1-overview.png)

**Key design properties:**

| Property | How |
|----------|-----|
| Non-blocking WAL consumption | Flush threads run independently; WAL consumer just pushes to queues |
| Bounded memory | Backpressure pauses WAL consumer when queues exceed threshold |
| Controlled flush parallelism | `FlushGate` limits concurrent flushes to `max_concurrent_flushes` (default 4); ticket-based FIFO ensures fairness |
| Sub-second latency | Flush threads self-trigger on time (interval) or size (batch threshold) |
| Crash safety | Slot never advances past durably flushed data (`confirmed_lsn = min(applied_lsn)`) |
| Parallel snapshots | Multiple tables snapshot concurrently via `tokio::spawn`; DuckDB loads run on separate threads (see [known limitation](#32-parallel-snapshots)) |

---

## 2. Thread Architecture

### 2.1 Thread Map

```
PostgreSQL postmaster
  │
  └── pg_duckpipe bgworker         ← OS process, one per sync group
        │
        ├── [main] tokio runtime    ← single-threaded async executor
        │     ├── WAL consumer      ← streaming replication loop
        │     ├── LISTEN/NOTIFY     ← wakeup task (persistent)
        │     ├── Snapshot task #1  ← tokio::spawn (per-table, temporary)
        │     ├── Snapshot task #2  ← runs in parallel with #1
        │     └── ...
        │
        ├── [thread] flush-orders           ← OS thread, persistent
        │     └── tokio runtime (own)       ← for async PG metadata calls
        │
        ├── [thread] flush-users            ← OS thread, persistent
        │     └── tokio runtime (own)
        │
        ├── [thread] flush-items            ← OS thread, persistent
        │     └── tokio runtime (own)
        │
        └── ...
```

### 2.2 Thread Responsibilities

| Thread | Type | Lifetime | Owns |
|--------|------|----------|------|
| **bgworker main** | OS process (PG) | Entire group lifetime | tokio runtime, WAL connection, FlushCoordinator |
| **Flush thread** | `std::thread` | Persistent per table | DuckDB connection, tokio runtime, local change accumulator |
| **Snapshot task** | `tokio::spawn` | One-shot per table | PG COPY connection, temp replication slot |
| **LISTEN task** | `tokio::spawn` | Persistent, auto-respawned | PG async connection |

### 2.3 OS Threads vs Async Tasks

DuckDB flush operations are **CPU-bound and blocking** — running them as async tasks would stall the single-threaded tokio runtime and block WAL consumption. Each flush thread is a real OS thread so it can do heavy DuckDB work (buffer, compact, merge) while the WAL consumer continues decoding and dispatching without interruption.

Snapshot tasks use async I/O (PG `COPY TO STDOUT`) with `spawn_blocking` for the DuckDB load step, keeping the async runtime responsive.

---

## 3. Parallel Table Processing

### 3.1 Per-Table Flush Parallelism

Each table has its own independent flush thread. The WAL consumer demultiplexes the interleaved WAL stream into per-table queues, and each flush thread processes its queue independently:

```
WAL stream (interleaved):
  [orders:I] [users:U] [orders:I] [items:D] [orders:I] [users:I] ...
       │          │          │          │          │          │
       ▼          ▼          ▼          ▼          ▼          ▼
  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐
  │ orders  │ │ users   │ │ orders  │ │ items   │ │ orders  │ │ users   │
  │ queue   │ │ queue   │ │ queue   │ │ queue   │ │ queue   │ │ queue   │
  └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘
       │           │           │           │           │           │
       ▼           ▼                       ▼                      ▼
  ┌─────────┐ ┌─────────┐            ┌─────────┐            ┌─────────┐
  │ flush   │ │ flush   │            │ flush   │            │ flush   │
  │ orders  │ │ users   │            │ items   │            │ users   │
  │ (batch) │ │ (batch) │            │ (batch) │            │ (batch) │
  └─────────┘ └─────────┘            └─────────┘            └─────────┘
```

A slow table doesn't block other flush threads directly — each drains its own queue and flushes to DuckDB on its own schedule. However, if total queued bytes across all streaming tables exceed `max_queued_bytes`, global [backpressure](#5-backpressure) pauses WAL consumption for all tables. Flush triggers independently when either:

- **Batch threshold**: accumulated changes reach `flush_batch_threshold`
- **Time interval**: `flush_interval_ms` has elapsed since last flush

#### FlushGate: Controlling Concurrent Flushes

While each table has its own flush thread, allowing all of them to `flush_buffer()` simultaneously would cause memory spikes (each gets `duckdb_flush_memory_mb` during flush) and DuckLake commit lock contention. The `FlushGate` is a **ticket-based FIFO semaphore** that limits concurrent flush operations to `max_concurrent_flushes` (default 4):

```
100 flush threads, max_concurrent_flushes = 4:

  Thread A  ──[buffering]──▶ takes ticket #0 ──▶ [FLUSHING] ──▶ release ──▶ [buffering]──▶
  Thread B  ──[buffering]──▶ takes ticket #1 ──▶ [FLUSHING] ──▶ release ──▶ [buffering]──▶
  Thread C  ──[buffering]──▶ takes ticket #2 ──▶ [FLUSHING] ──▶ release ──▶
  Thread D  ──[buffering]──▶ takes ticket #3 ──▶ [FLUSHING] ──▶
  Thread E  ──[buffering]──▶ takes ticket #4 ──▶ [waiting...] ──▶ (served when A releases)
  Thread F  ──[buffering]──▶ takes ticket #5 ──▶ [waiting...] ──▶ (served after E)
  ...
```

Key properties:
- **FIFO fairness**: threads acquire tickets in arrival order; `now_serving` counter ensures strict ordering — no starvation
- **Adaptive timeout**: the gate timeout is the median of the last 64 flush durations (floor 1s); falls back to `flush_interval` until enough data is collected — threads wait proportionally to how long flushes actually take
- **Timeout forfeit**: if a thread waits past the timeout for a slot, it forfeits its ticket (advances `now_serving`) so the queue doesn't block, and retries next iteration
- **Drain bypass**: TRUNCATE and shutdown bypass the gate entirely — correctness takes priority
- **Low-memory buffering**: threads waiting for a slot continue accumulating changes in their DuckDB buffer at `duckdb_buffer_memory_mb` (default 16 MB), which can spill to disk
- **Runtime adjustable**: `duckpipe.max_concurrent_flushes` is a SIGHUP GUC; the worker calls `set_max_concurrent_flushes()` each cycle

### 3.2 Parallel Snapshots

When multiple tables are added simultaneously, their initial snapshots (`COPY TO STDOUT` from PG → load into DuckDB) run as independent `tokio::spawn` tasks, executing concurrently:

```
Time ──────────────────────────────────────────────────────────▶

WAL Consumer:  ───[streaming all tables continuously]──────────▶

Table "orders":
  Snapshot:    ╠══ COPY orders ══╣
  Flush:       [paused: buffering WAL]  → unpause → [flushing]

Table "users":
  Snapshot:    ╠═══ COPY users ═══╣          (parallel with orders)
  Flush:       [paused: buffering WAL]    → unpause → [flushing]

Table "items":
  State:       ── STREAMING (already synced) ──────────────────▶
  Flush:       [flushing normally, unaffected by other snapshots]
```

During a snapshot, the table's flush thread is **paused** — it accumulates WAL changes but doesn't flush them. Once the snapshot completes, the flush thread unpauses and applies the buffered changes, filtering out any with `lsn ≤ snapshot_lsn` (already captured by the COPY).

> **Known limitation:** Each snapshot has an async CSV producer and a `spawn_blocking` DuckDB consumer. The DuckDB consumer runs on separate OS threads and parallelizes properly, but the CSV producer shares the single-threaded tokio runtime with the WAL consumer. Its synchronous file I/O (`fs::File::write_all`) and byte-by-byte quote tracking briefly block WAL consumption for all tables between `.await` points. This should be fixed by moving producers to `spawn_blocking` or dedicated threads so the data plane (snapshots) never interferes with the control plane (WAL streaming).

---

## 4. Inter-Thread Communication

![Communication Map](img/2-communication.png)

The WAL consumer pushes decoded changes into per-table queues (`Mutex<Vec<Change>>` + `Condvar`) with a brief lock; each flush thread blocks on its condvar until notified, then drains up to `batch_threshold` changes and releases the lock. Before flushing, threads acquire a `FlushGate` slot (`Mutex` + `Condvar`, ticket-based FIFO) that limits concurrent flushes. Flush threads report results (applied LSN or error) back to the main loop via `std::sync::mpsc` channels polled with non-blocking `try_recv`. Snapshot completion follows the same mpsc pattern. A `tokio::sync::Notify` lets the LISTEN/NOTIFY task instantly wake the main loop when `add_table`, `resync`, or `enable` fires.

---

## 5. Backpressure

Backpressure is applied **globally** — when triggered, the WAL consumer pauses entirely, affecting all tables. This is because WAL is a single ordered stream that cannot be selectively consumed per table.

### 5.1 Mechanism

![Backpressure](img/3-backpressure.png)

### 5.2 Snapshot Isolation from Backpressure

Without special handling, a long-running snapshot would inflate the total queued bytes and trigger backpressure, stalling WAL streaming for *all* tables — even healthy ones. The backpressure check simply skips paused (snapshot) queues when summing:

```rust
// flush_coordinator.rs
pub fn is_backpressured(&self) -> bool {
    self.total_queued_bytes_active() >= self.max_queued_bytes
}

// Only sums queued_bytes from non-paused queues
fn total_queued_bytes_active(&self) -> i64 {
    self.threads.values()
        .filter(|e| !e.control.paused.load(Ordering::Relaxed))
        .map(|e| e.queue_handle.inner.lock().unwrap().queued_bytes)
        .sum()
}
```

### 5.3 Impact on Parallelism

- **When not backpressured**: WAL consumer runs at full speed, all flush threads process independently — maximum parallelism (up to `max_concurrent_flushes` concurrent flushes).
- **When backpressured**: WAL consumer skips its polling round (all tables stall together), but flush threads continue draining their queues. Once enough changes are flushed, backpressure clears and WAL consumption resumes.
- **Snapshot tables are immune**: their buffered changes don't count toward the backpressure threshold, so adding a new table via `add_table()` won't degrade throughput for existing tables.
- **FlushGate complements backpressure**: backpressure limits the *producer* (WAL consumer) based on total queued bytes; FlushGate limits the *consumers* (flush threads) based on concurrent flush operations. Together they bound both memory from queued changes and memory from active DuckDB flush sessions.

---

## 6. Crash Safety & LSN Management

### 6.1 LSN Flow

Each flush thread independently tracks the LSN of its last successful flush. The main thread computes `confirmed_lsn` as the minimum across all tables — the safe restart point.

![LSN Flow](img/4-lsn-flow.png)

### 6.2 Safety Invariant

`confirmed_lsn = min(applied_lsn across all active tables)`

- If **any** table has never flushed (`applied_lsn = 0`), confirmed_lsn stays at 0 — the slot retains all WAL. Tables in CATCHUP use `snapshot_lsn` as a floor (via `COALESCE(applied_lsn, snapshot_lsn)`).
- On crash, the replication slot replays from `confirmed_lsn`. Changes already flushed to DuckDB are idempotent (DELETE + INSERT by PK).
- Flush threads commit to DuckDB **before** sending the mpsc result, so the in-memory `per_table_lsn` is always >= the persisted PG value.
- The slowest table determines how far the slot can advance. This is the cost of per-table parallelism — a stalled table holds back WAL retention for the entire group.

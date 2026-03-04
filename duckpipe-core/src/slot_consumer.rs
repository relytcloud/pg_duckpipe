//! Streaming replication slot consumer using `pgwire-replication`.
//!
//! Implements `START_REPLICATION` streaming via the PostgreSQL wire protocol
//! for near-zero latency WAL consumption.
//!
//! Design: persistent connection. The consumer is kept alive across sync
//! cycles, avoiding per-cycle reconnect overhead. On error, the consumer is
//! dropped and the next cycle reconnects from `confirmed_lsn` (the crash-safe
//! restart point from PG metadata). `is_connected()` is checked each cycle to
//! detect dead connections.

use std::time::Duration;

use pgwire_replication::{Lsn, ReplicationClient, ReplicationConfig, ReplicationEvent};

/// A streaming replication consumer for a single replication slot.
pub struct SlotConsumer {
    client: ReplicationClient,
}

impl SlotConsumer {
    /// Connect to a replication slot via Unix socket.
    ///
    /// `start_lsn` is typically `confirmed_lsn` from the sync group metadata.
    /// If 0, PostgreSQL starts from the slot's `restart_lsn`.
    pub async fn connect(
        socket_dir: &str,
        port: u16,
        user: &str,
        database: &str,
        slot: &str,
        publication: &str,
        start_lsn: u64,
    ) -> Result<Self, String> {
        let config =
            ReplicationConfig::unix(socket_dir, port, user, "", database, slot, publication)
                .with_start_lsn(Lsn(start_lsn))
                .with_status_interval(Duration::from_millis(500))
                .with_wakeup_interval(Duration::from_millis(100));

        let client = ReplicationClient::connect(config)
            .await
            .map_err(|e| format!("streaming connect: {}", e))?;

        Ok(Self { client })
    }

    /// Connect to a replication slot via TCP, with optional TLS.
    ///
    /// Used by the standalone daemon and remote sync groups which connect
    /// over the network instead of Unix domain sockets.
    pub async fn connect_tcp(
        host: &str,
        port: u16,
        user: &str,
        password: &str,
        database: &str,
        slot: &str,
        publication: &str,
        start_lsn: u64,
        sslmode: tokio_postgres::config::SslMode,
    ) -> Result<Self, String> {
        let tls_config = crate::connstr::make_pgwire_tls_config(sslmode);
        let config = ReplicationConfig::new(host, user, password, database, slot, publication)
            .with_port(port)
            .with_tls(tls_config)
            .with_start_lsn(Lsn(start_lsn))
            .with_status_interval(Duration::from_millis(500))
            .with_wakeup_interval(Duration::from_millis(100));

        let client = ReplicationClient::connect(config)
            .await
            .map_err(|e| format!("streaming tcp connect: {}", e))?;

        Ok(Self { client })
    }

    /// Check if the replication connection is still alive.
    pub fn is_connected(&self) -> bool {
        self.client.is_running()
    }

    /// Convert one replication event to a `(lsn, pgoutput_bytes)` tuple.
    ///
    /// Returns `None` for KeepAlive, Message, and StoppedAt events which carry
    /// no pgoutput payload. The crate pre-parses BEGIN/COMMIT into separate event
    /// variants, so we synthesize the corresponding pgoutput binary messages to
    /// keep the decoder feeding on identical data regardless of which receive
    /// path is used.
    fn event_to_message(event: ReplicationEvent) -> Option<(u64, Vec<u8>)> {
        match event {
            ReplicationEvent::XLogData {
                wal_start, data, ..
            } => Some((wal_start.0, data.to_vec())),
            ReplicationEvent::Begin {
                final_lsn,
                xid,
                commit_time_micros,
            } => {
                // Synthesize pgoutput 'B' (Begin) binary message:
                //   [type='B':u8] [final_lsn:i64] [commit_time:i64] [xid:i32]
                let mut buf = Vec::with_capacity(21);
                buf.push(b'B');
                buf.extend_from_slice(&(final_lsn.0 as i64).to_be_bytes());
                buf.extend_from_slice(&commit_time_micros.to_be_bytes());
                buf.extend_from_slice(&(xid as i32).to_be_bytes());
                Some((final_lsn.0, buf))
            }
            ReplicationEvent::Commit {
                lsn,
                end_lsn,
                commit_time_micros,
            } => {
                // Synthesize pgoutput 'C' (Commit) binary message:
                //   [type='C':u8] [flags=0:u8] [commit_lsn:i64] [end_lsn:i64] [commit_time:i64]
                let mut buf = Vec::with_capacity(26);
                buf.push(b'C');
                buf.push(0u8); // flags
                buf.extend_from_slice(&(lsn.0 as i64).to_be_bytes());
                buf.extend_from_slice(&(end_lsn.0 as i64).to_be_bytes());
                buf.extend_from_slice(&commit_time_micros.to_be_bytes());
                Some((end_lsn.0, buf))
            }
            // KeepAlive: handled internally by the crate (standby status updates)
            // Message: logical decoding messages — skip
            // StoppedAt: clean stream end
            _ => None,
        }
    }

    /// Receive exactly one WAL message within `timeout`.
    ///
    /// Loops internally, skipping KeepAlive and Message events, until either a
    /// data-bearing event (XLogData/Begin/Commit) arrives or the deadline elapses.
    /// Returns `Ok(None)` on timeout, StoppedAt, or clean stream end.
    /// Returns `Err` on a connection error.
    pub async fn recv_one(&mut self, timeout: Duration) -> Result<Option<(u64, Vec<u8>)>, String> {
        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            match tokio::time::timeout_at(deadline, self.client.recv()).await {
                Err(_elapsed) => return Ok(None),
                Ok(Err(e)) => return Err(format!("streaming recv: {}", e)),
                Ok(Ok(None)) => return Ok(None),
                Ok(Ok(Some(event))) => match event {
                    ReplicationEvent::StoppedAt { .. } => return Ok(None),
                    ReplicationEvent::KeepAlive { .. } | ReplicationEvent::Message { .. } => {
                        // Skip; loop to try again within the remaining timeout window
                    }
                    other => {
                        return Ok(Self::event_to_message(other));
                    }
                },
            }
        }
    }

    /// Poll for WAL messages up to `max_count` or until `timeout_ms` elapses.
    ///
    /// Delegates to `recv_one` for each message. Returns a vec of
    /// `(lsn, pgoutput_binary_data)` tuples.
    pub async fn poll_messages(
        &mut self,
        max_count: i32,
        timeout_ms: u64,
    ) -> Result<Vec<(u64, Vec<u8>)>, String> {
        let mut messages = Vec::new();
        let max = max_count as usize;
        let overall_deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);

        loop {
            if messages.len() >= max {
                break;
            }
            let now = tokio::time::Instant::now();
            if now >= overall_deadline {
                break;
            }
            let remaining = overall_deadline - now;

            match self.recv_one(remaining).await {
                Err(e) => {
                    // Connection error — return what we have if non-empty, else propagate
                    if messages.is_empty() {
                        return Err(e);
                    }
                    break;
                }
                Ok(None) => break,
                Ok(Some(msg)) => messages.push(msg),
            }
        }

        Ok(messages)
    }

    /// Send StandbyStatusUpdate to advance the replication slot.
    ///
    /// Reports that all WAL up to `lsn` has been durably applied.
    /// The crate's background task sends the actual protocol message
    /// on the next status interval.
    pub fn send_status_update(&self, lsn: u64) {
        self.client.update_applied_lsn(Lsn(lsn));
    }

    /// Gracefully shut down the streaming connection.
    ///
    /// Sends CopyDone, drains remaining events, and waits for the
    /// background task to finish (which sends a final StandbyStatusUpdate).
    pub async fn close(mut self) -> Result<(), String> {
        self.client
            .shutdown()
            .await
            .map_err(|e| format!("streaming shutdown: {}", e))
    }
}

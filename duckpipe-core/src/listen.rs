//! LISTEN/NOTIFY wakeup helper.
//!
//! Establishes a persistent `tokio-postgres` connection, executes
//! `LISTEN duckpipe_wakeup`, and bridges PG notifications to a
//! `tokio::sync::Notify` so the worker loop can wake immediately
//! when `add_table()`, `resync_table()`, or `enable_group()` fires
//! `NOTIFY duckpipe_wakeup`.

use std::sync::Arc;

use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio_postgres::AsyncMessage;

pub const WAKEUP_CHANNEL: &str = "duckpipe_wakeup";

/// Spawn a background task that LISTENs on `duckpipe_wakeup` and
/// notifies `wakeup` whenever a notification arrives.
///
/// The returned `JoinHandle` can be checked with `is_finished()` to
/// detect connection loss; the caller should respawn when needed.
pub async fn spawn_listen_task(
    connstr: &str,
    wakeup: Arc<Notify>,
) -> Result<JoinHandle<()>, String> {
    let (client, mut connection) = tokio_postgres::connect(connstr, tokio_postgres::NoTls)
        .await
        .map_err(|e| format!("listen connect: {e}"))?;

    // Spawn a single task that:
    //   1. Drives the connection (required for Client to work)
    //   2. Issues LISTEN via the client
    //   3. Watches for notification messages
    let listen_sql = format!("LISTEN {WAKEUP_CHANNEL}");
    let handle = tokio::spawn(async move {
        // Issue LISTEN.  We must drive the connection concurrently so
        // that the execute() response can be received.
        let listen_result = tokio::select! {
            // Drive connection — this arm never "completes" unless the
            // connection drops, in which case we treat it as an error.
            msg = futures_util::future::poll_fn(|cx| connection.poll_message(cx)) => {
                match msg {
                    Some(Err(e)) => Err(format!("connection error during LISTEN: {e}")),
                    None => Err("connection closed during LISTEN".to_string()),
                    Some(Ok(_)) => {
                        // Unexpected message before LISTEN completes — should not happen,
                        // but continue and let execute() resolve.
                        Ok(())
                    }
                }
            }
            // Issue LISTEN concurrently.
            res = client.execute(&listen_sql as &str, &[]) => {
                res.map(|_| ()).map_err(|e| format!("LISTEN: {e}"))
            }
        };

        if let Err(e) = listen_result {
            tracing::warn!("LISTEN task failed: {e}");
            return;
        }

        tracing::debug!("LISTEN command succeeded, watching for notifications");

        // LISTEN succeeded — now watch for notifications.
        // Keep `client` alive so the connection stays open.
        let _client = client;
        loop {
            match futures_util::future::poll_fn(|cx| connection.poll_message(cx)).await {
                Some(Ok(AsyncMessage::Notification(n))) => {
                    tracing::debug!("received notification on channel '{}'", n.channel());
                    wakeup.notify_one();
                }
                Some(Ok(_)) => {
                    // Notice or other async message — ignore.
                }
                Some(Err(e)) => {
                    tracing::warn!("listen connection error: {e}");
                    break;
                }
                None => {
                    // Connection closed gracefully.
                    break;
                }
            }
        }
    });

    Ok(handle)
}

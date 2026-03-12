//! LISTEN/NOTIFY wakeup helper.
//!
//! Establishes a persistent `tokio-postgres` connection, executes
//! `LISTEN <channel>`, and bridges PG notifications to a
//! `tokio::sync::Notify` so the worker loop can wake immediately
//! when `add_table()`, `resync_table()`, or `enable_group()` fires
//! `NOTIFY <channel>`.

use std::sync::Arc;

use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio_postgres::config::Config;
use tokio_postgres::AsyncMessage;

/// Return the per-group NOTIFY channel name: `duckpipe_wakeup_{group_name}`.
pub fn wakeup_channel(group_name: &str) -> String {
    format!("duckpipe_wakeup_{}", group_name)
}

/// Spawn a background task that LISTENs on `channel` and
/// notifies `wakeup` whenever a notification arrives.
///
/// `app_name` is set as `application_name` on the connection so it
/// appears in `pg_stat_activity`.
///
/// The returned `JoinHandle` can be checked with `is_finished()` to
/// detect connection loss; the caller should respawn when needed.
pub async fn spawn_listen_task(
    connstr: &str,
    channel: &str,
    wakeup: Arc<Notify>,
    app_name: &str,
) -> Result<JoinHandle<()>, String> {
    // Parse config manually rather than using pg_connect_with_app_name() because
    // we need the raw Connection object for poll_message() in the notification loop.
    // NoTls is acceptable: LISTEN is only used on local unix-socket connections
    // (the bgworker builds connstr from socket_dir, never remote conninfo).
    let mut config: Config = connstr
        .parse()
        .map_err(|e| format!("listen parse connstr: {e}"))?;
    config.application_name(app_name);
    let (client, mut connection) = config
        .connect(tokio_postgres::NoTls)
        .await
        .map_err(|e| format!("listen connect: {e}"))?;

    // Spawn a single task that:
    //   1. Drives the connection (required for Client to work)
    //   2. Issues LISTEN via the client
    //   3. Watches for notification messages
    let listen_sql = format!("LISTEN {}", channel);
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

//! Connection string utilities — thin wrappers around `tokio_postgres::Config`.
//!
//! Shared by both the PG extension (for remote group DDL) and the standalone daemon.
//! Supports both libpq key=value format and PostgreSQL URI format
//! (`postgresql://user:pass@host:port/dbname?params`) via `tokio_postgres::Config`.

use tokio_postgres::config::{Config, Host, SslMode};

use crate::service::SlotConnectParams;

/// Extract the first host from a `Config` as a string, defaulting to `"localhost"`.
fn host_string(config: &Config) -> String {
    match config.get_hosts().first() {
        Some(Host::Tcp(h)) => h.clone(),
        #[cfg(unix)]
        Some(Host::Unix(p)) => p.to_string_lossy().into_owned(),
        _ => "localhost".to_string(),
    }
}

/// Convert `SslMode` to the string form used by `pgwire-replication`.
fn sslmode_to_string(mode: SslMode) -> Option<String> {
    match mode {
        SslMode::Disable => None,
        SslMode::Prefer => Some("prefer".to_string()),
        SslMode::Require => Some("require".to_string()),
        _ => None,
    }
}

/// Convert a connection string to `SlotConnectParams::Tcp` for replication.
///
/// Parses the connection string (key=value or URI) via `tokio_postgres::Config`
/// and extracts the individual fields needed by `pgwire-replication`.
///
/// When `sslmode` is not explicitly set in the connstr, defaults to `Disable`
/// (rather than tokio_postgres's default of `Prefer`) for compatibility with
/// local PG instances that don't have TLS configured.
pub fn to_slot_connect_params(connstr: &str) -> Result<SlotConnectParams, String> {
    let config: Config = connstr
        .parse()
        .map_err(|e| format!("parse connstr: {}", e))?;

    let host = host_string(&config);
    let port = config.get_ports().first().copied().unwrap_or(5432);
    let user = config.get_user().unwrap_or("postgres").to_string();
    let password = config
        .get_password()
        .map(|p| String::from_utf8_lossy(p).into_owned())
        .unwrap_or_default();
    let dbname = config.get_dbname().unwrap_or("postgres").to_string();
    // Only use the parsed sslmode when explicitly specified; otherwise default to Disable
    let sslmode = if has_explicit_sslmode(connstr) {
        config.get_ssl_mode()
    } else {
        SslMode::Disable
    };

    Ok(SlotConnectParams::Tcp {
        host,
        port,
        user,
        password,
        dbname,
        sslmode,
    })
}

/// Whether the given `SslMode` requires TLS.
///
/// Note: `SslMode::Prefer` is treated as requiring TLS (no plaintext fallback).
/// This is stricter than libpq's behavior but avoids accidental plaintext connections.
pub fn sslmode_needs_tls(mode: SslMode) -> bool {
    matches!(mode, SslMode::Require | SslMode::Prefer)
}

/// Create a rustls `ClientConfig` with Mozilla root certificates for TLS connections.
pub fn make_rustls_config() -> rustls::ClientConfig {
    // Explicitly install the ring crypto provider — required when both ring and
    // aws-lc-rs features are pulled in by transitive dependencies.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let mut root_store = rustls::RootCertStore::empty();
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth()
}

/// Check whether the connection string explicitly sets `sslmode`.
///
/// `tokio_postgres::Config` defaults to `SslMode::Prefer`, but
/// `tokio-postgres-rustls` doesn't support the Prefer fallback
/// (`can_connect_without_tls` returns false). We need to distinguish
/// "user set sslmode=prefer" from "default Prefer" so we can use NoTls
/// for connections that never specified sslmode.
fn has_explicit_sslmode(connstr: &str) -> bool {
    // Works for both key=value (`sslmode=...`) and URI (`?sslmode=...`) formats
    connstr.to_ascii_lowercase().contains("sslmode")
}

/// Connect to PostgreSQL with automatic TLS negotiation.
///
/// Detects `sslmode` in the connection string — if it requires TLS, uses
/// `tokio-postgres-rustls` with Mozilla root certificates. Otherwise uses NoTls.
///
/// Note: `sslmode=prefer` is treated as `require` (TLS only, no plaintext
/// fallback) because `tokio-postgres-rustls` doesn't support fallback.
/// When `sslmode` is not specified, plaintext is used.
///
/// Accepts both libpq key=value and PostgreSQL URI formats.
///
/// Returns `(Client, JoinHandle)` — the join handle drives the connection
/// background task and should be kept alive as long as the client is in use.
pub async fn pg_connect(
    connstr: &str,
) -> Result<
    (
        tokio_postgres::Client,
        tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
    ),
    String,
> {
    let config: Config = connstr
        .parse()
        .map_err(|e| format!("parse connstr: {}", e))?;
    let use_tls = has_explicit_sslmode(connstr) && sslmode_needs_tls(config.get_ssl_mode());
    if use_tls {
        let tls = tokio_postgres_rustls::MakeRustlsConnect::new(make_rustls_config());
        let (client, connection) = config
            .connect(tls)
            .await
            .map_err(|e| format!("pg connect (tls): {}", e))?;
        let handle = tokio::spawn(connection);
        Ok((client, handle))
    } else {
        let (client, connection) = config
            .connect(tokio_postgres::NoTls)
            .await
            .map_err(|e| format!("pg connect: {}", e))?;
        let handle = tokio::spawn(connection);
        Ok((client, handle))
    }
}

/// Build a `pgwire_replication::TlsConfig` from an `SslMode`.
pub fn make_pgwire_tls_config(mode: SslMode) -> pgwire_replication::TlsConfig {
    match mode {
        SslMode::Require => pgwire_replication::TlsConfig::require(),
        SslMode::Prefer => pgwire_replication::TlsConfig::require(),
        _ => pgwire_replication::TlsConfig::disabled(),
    }
}

/// Parse a connection string and rebuild it with the password masked.
///
/// Used for logging and SRF output. Returns a key=value format string
/// with `password=********` replacing the actual password.
pub fn redact_password(connstr: &str) -> String {
    let config: Config = match connstr.parse() {
        Ok(c) => c,
        Err(_) => return connstr.to_string(),
    };

    let host = host_string(&config);
    let port = config.get_ports().first().copied().unwrap_or(5432);
    let user = config.get_user().unwrap_or("postgres");
    let dbname = config.get_dbname().unwrap_or("postgres");

    let mut parts = vec![
        format!("host={}", host),
        format!("port={}", port),
        format!("user={}", user),
        format!("dbname={}", dbname),
    ];
    if config.get_password().is_some() {
        parts.push("password=********".to_string());
    }
    if let Some(s) = sslmode_to_string(config.get_ssl_mode()) {
        parts.push(format!("sslmode={}", s));
    }
    parts.join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- to_slot_connect_params ---

    #[test]
    fn slot_params_from_keyvalue() {
        let params = to_slot_connect_params(
            "host=db.example.com port=5433 user=alice password=secret dbname=mydb",
        )
        .unwrap();
        match params {
            SlotConnectParams::Tcp {
                host,
                port,
                user,
                password,
                dbname,
                sslmode,
            } => {
                assert_eq!(host, "db.example.com");
                assert_eq!(port, 5433);
                assert_eq!(user, "alice");
                assert_eq!(password, "secret");
                assert_eq!(dbname, "mydb");
                // No explicit sslmode → defaults to Disable (not tokio_postgres's Prefer)
                assert_eq!(sslmode, SslMode::Disable);
            }
            _ => panic!("expected Tcp"),
        }
    }

    #[test]
    fn slot_params_from_uri() {
        let params =
            to_slot_connect_params("postgresql://alice:secret@db.example.com:5433/mydb").unwrap();
        match params {
            SlotConnectParams::Tcp {
                host,
                port,
                user,
                password,
                dbname,
                ..
            } => {
                assert_eq!(host, "db.example.com");
                assert_eq!(port, 5433);
                assert_eq!(user, "alice");
                assert_eq!(password, "secret");
                assert_eq!(dbname, "mydb");
            }
            _ => panic!("expected Tcp"),
        }
    }

    #[test]
    fn slot_params_with_sslmode() {
        let params =
            to_slot_connect_params("host=db sslmode=require user=bob dbname=prod").unwrap();
        match params {
            SlotConnectParams::Tcp { sslmode, .. } => {
                assert_eq!(sslmode, SslMode::Require);
            }
            _ => panic!("expected Tcp"),
        }
    }

    // --- sslmode_needs_tls ---

    #[test]
    fn sslmode_require_needs_tls() {
        assert!(sslmode_needs_tls(SslMode::Require));
    }

    #[test]
    fn sslmode_prefer_needs_tls() {
        assert!(sslmode_needs_tls(SslMode::Prefer));
    }

    #[test]
    fn sslmode_disable_no_tls() {
        assert!(!sslmode_needs_tls(SslMode::Disable));
    }

    // --- redact_password ---

    #[test]
    fn redact_with_password() {
        let result = redact_password("host=db port=5432 user=alice password=secret dbname=mydb");
        assert!(result.contains("password=********"));
        assert!(!result.contains("secret"));
        assert!(result.contains("host=db"));
        assert!(result.contains("user=alice"));
    }

    #[test]
    fn redact_without_password() {
        let result = redact_password("host=db user=alice dbname=mydb");
        assert!(!result.contains("password"));
        assert!(result.contains("host=db"));
    }

    #[test]
    fn redact_uri_format() {
        let result = redact_password("postgresql://alice:secret@db.example.com:5433/mydb");
        assert!(result.contains("password=********"));
        assert!(!result.contains("secret"));
    }

    #[test]
    fn redact_with_sslmode() {
        let result = redact_password("host=db user=alice dbname=mydb password=x sslmode=require");
        assert!(result.contains("sslmode=require"));
        assert!(result.contains("password=********"));
    }
}

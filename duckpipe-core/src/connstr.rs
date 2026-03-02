//! Connection string parsing and conversion utilities.
//!
//! Shared by both the PG extension (for remote group DDL) and the standalone daemon.
//! Supports both libpq key=value format and PostgreSQL URI format
//! (`postgresql://user:pass@host:port/dbname?params`).

use crate::service::SlotConnectParams;

/// Parsed connection parameters for pgwire / tokio-postgres TCP connections.
pub struct ConnParams {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub dbname: String,
    pub sslmode: Option<String>,
}

/// Parse a connection string (libpq key=value or PostgreSQL URI) into individual fields.
pub fn parse_connstr(connstr: &str) -> ConnParams {
    let trimmed = connstr.trim();
    if trimmed.starts_with("postgresql://") || trimmed.starts_with("postgres://") {
        parse_uri(trimmed)
    } else {
        parse_keyvalue(trimmed)
    }
}

/// Parse a libpq-style `key=value` connection string.
fn parse_keyvalue(connstr: &str) -> ConnParams {
    let mut host = "localhost".to_string();
    let mut port: u16 = 5432;
    let mut user = default_user();
    let mut password = String::new();
    let mut dbname = "postgres".to_string();
    let mut sslmode = None;

    for part in connstr.split_whitespace() {
        if let Some((key, value)) = part.split_once('=') {
            match key {
                "host" | "hostaddr" => host = value.to_string(),
                "port" => {
                    if let Ok(p) = value.parse() {
                        port = p;
                    }
                }
                "user" => user = value.to_string(),
                "password" => password = value.to_string(),
                "dbname" => dbname = value.to_string(),
                "sslmode" => sslmode = Some(value.to_string()),
                _ => {}
            }
        }
    }

    ConnParams {
        host,
        port,
        user,
        password,
        dbname,
        sslmode,
    }
}

/// Parse a PostgreSQL URI: `postgresql://user:password@host:port/dbname?params`
fn parse_uri(uri: &str) -> ConnParams {
    let mut host = "localhost".to_string();
    let mut port: u16 = 5432;
    let mut user = default_user();
    let mut password = String::new();
    let mut dbname = "postgres".to_string();
    let mut sslmode = None;

    // Strip scheme
    let rest = uri
        .strip_prefix("postgresql://")
        .or_else(|| uri.strip_prefix("postgres://"))
        .unwrap_or(uri);

    // Split query params
    let (main, query) = rest.split_once('?').unwrap_or((rest, ""));

    // Parse query params
    for param in query.split('&') {
        if param.is_empty() {
            continue;
        }
        if let Some((k, v)) = param.split_once('=') {
            match k {
                "sslmode" => sslmode = Some(url_decode(v)),
                "user" => user = url_decode(v),
                "password" => password = url_decode(v),
                "dbname" => dbname = url_decode(v),
                "port" => {
                    if let Ok(p) = v.parse() {
                        port = p;
                    }
                }
                "host" => host = url_decode(v),
                _ => {} // skip channel_binding, application_name, etc.
            }
        }
    }

    // Split userinfo@hostspec/dbname
    let (userinfo, hostpath) = if let Some(at_pos) = main.rfind('@') {
        (&main[..at_pos], &main[at_pos + 1..])
    } else {
        ("", main)
    };

    // Parse userinfo (user:password)
    if !userinfo.is_empty() {
        if let Some((u, p)) = userinfo.split_once(':') {
            user = url_decode(u);
            password = url_decode(p);
        } else {
            user = url_decode(userinfo);
        }
    }

    // Parse host:port/dbname
    let (hostport, db) = if let Some(slash_pos) = hostpath.find('/') {
        (&hostpath[..slash_pos], &hostpath[slash_pos + 1..])
    } else {
        (hostpath, "")
    };

    if !db.is_empty() {
        dbname = url_decode(db);
    }

    if !hostport.is_empty() {
        if let Some((h, p)) = hostport.rsplit_once(':') {
            if let Ok(parsed_port) = p.parse::<u16>() {
                host = url_decode(h);
                port = parsed_port;
            } else {
                host = url_decode(hostport);
            }
        } else {
            host = url_decode(hostport);
        }
    }

    ConnParams {
        host,
        port,
        user,
        password,
        dbname,
        sslmode,
    }
}

/// Minimal percent-decoding for connection string values.
fn url_decode(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '%' {
            let hex: String = chars.by_ref().take(2).collect();
            if let Ok(byte) = u8::from_str_radix(&hex, 16) {
                result.push(byte as char);
            } else {
                result.push('%');
                result.push_str(&hex);
            }
        } else {
            result.push(c);
        }
    }
    result
}

fn default_user() -> String {
    std::env::var("USER")
        .or_else(|_| std::env::var("PGUSER"))
        .unwrap_or_else(|_| "postgres".to_string())
}

/// Build a tokio-postgres connection string from ConnParams.
pub fn build_tokio_pg_connstr(params: &ConnParams) -> String {
    let mut parts = vec![
        format!("host={}", params.host),
        format!("port={}", params.port),
        format!("user={}", params.user),
        format!("dbname={}", params.dbname),
    ];
    if !params.password.is_empty() {
        parts.push(format!("password={}", params.password));
    }
    if let Some(ref mode) = params.sslmode {
        parts.push(format!("sslmode={}", mode));
    }
    parts.join(" ")
}

/// Convert ConnParams to SlotConnectParams::Tcp for replication connections.
pub fn to_slot_connect_params(params: &ConnParams) -> SlotConnectParams {
    SlotConnectParams::Tcp {
        host: params.host.clone(),
        port: params.port,
        user: params.user.clone(),
        password: params.password.clone(),
        dbname: params.dbname.clone(),
        sslmode: params.sslmode.clone(),
    }
}

/// Whether the given sslmode string requires TLS.
pub fn sslmode_needs_tls(sslmode: &Option<String>) -> bool {
    match sslmode.as_deref() {
        Some("require") | Some("verify-ca") | Some("verify-full") | Some("prefer") => true,
        _ => false,
    }
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

/// Connect to PostgreSQL with automatic TLS negotiation.
///
/// Detects `sslmode` in the connection string — if it requires TLS, uses
/// `tokio-postgres-rustls` with Mozilla root certificates. Otherwise uses NoTls.
///
/// Accepts both libpq key=value and PostgreSQL URI formats (tokio-postgres
/// handles both natively).
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
    let params = parse_connstr(connstr);
    if sslmode_needs_tls(&params.sslmode) {
        let tls = tokio_postgres_rustls::MakeRustlsConnect::new(make_rustls_config());
        let (client, connection) = tokio_postgres::connect(connstr, tls)
            .await
            .map_err(|e| format!("pg connect (tls): {}", e))?;
        let handle = tokio::spawn(connection);
        Ok((client, handle))
    } else {
        let (client, connection) = tokio_postgres::connect(connstr, tokio_postgres::NoTls)
            .await
            .map_err(|e| format!("pg connect: {}", e))?;
        let handle = tokio::spawn(connection);
        Ok((client, handle))
    }
}

/// Build a `pgwire_replication::TlsConfig` from an sslmode string.
pub fn make_pgwire_tls_config(sslmode: &Option<String>) -> pgwire_replication::TlsConfig {
    match sslmode.as_deref() {
        Some("require") => pgwire_replication::TlsConfig::require(),
        Some("verify-ca") => pgwire_replication::TlsConfig::verify_ca(None),
        Some("verify-full") => pgwire_replication::TlsConfig::verify_full(None),
        Some("prefer") => pgwire_replication::TlsConfig::require(),
        _ => pgwire_replication::TlsConfig::disabled(),
    }
}

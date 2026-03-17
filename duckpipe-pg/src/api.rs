use pgrx::datum::{DatumWithOid, TimestampWithTimeZone};
use pgrx::prelude::*;

use duckpipe_core::listen;

use crate::DATA_INLINING_ROW_LIMIT;

/// Run an async block on a short-lived, single-threaded tokio runtime.
/// Used for tokio-postgres calls from synchronous PG extension functions.
fn block_on<F: std::future::Future>(f: F) -> F::Output {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to create tokio runtime");
    rt.block_on(f)
}

/// Connect to a remote PG via tokio-postgres with automatic TLS negotiation.
/// Returns `(Client, JoinHandle)` — the handle drives the connection and must be
/// kept alive as long as the client is in use.
async fn remote_connect(
    conninfo: &str,
    app_name: &str,
) -> Result<
    (
        tokio_postgres::Client,
        tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
    ),
    String,
> {
    duckpipe_core::connstr::pg_connect_with_app_name(conninfo, app_name)
        .await
        .map_err(|e| format!("remote connect: {}", e))
}

/// Execute a SQL statement via raw SPI without marking the transaction as mutable.
/// This is needed for pg_create_logical_replication_slot() which cannot run in a
/// transaction that already has a TransactionId assigned.
unsafe fn spi_exec_raw(sql: &str) -> i32 {
    let c_sql = std::ffi::CString::new(sql).unwrap();
    pg_sys::SPI_execute(c_sql.as_ptr(), false, 0)
}

/// Redact the password from a conninfo string for display purposes.
/// Replaces `password=...` with `password=********`.
fn redact_conninfo_password(conninfo: &str) -> String {
    duckpipe_core::connstr::redact_password(conninfo)
}

/// Parse a source_table argument into (schema, table) parts
fn parse_source_table(source_table: &str) -> (String, String) {
    if let Some(dot) = source_table.find('.') {
        (
            source_table[..dot].to_string(),
            source_table[dot + 1..].to_string(),
        )
    } else {
        ("public".to_string(), source_table.to_string())
    }
}

/// Quote an identifier for SQL
fn quote_ident(name: &str) -> String {
    unsafe {
        let c_name = std::ffi::CString::new(name).unwrap();
        let quoted = pg_sys::quote_identifier(c_name.as_ptr());
        let result = std::ffi::CStr::from_ptr(quoted)
            .to_string_lossy()
            .to_string();
        result
    }
}

/// Map PG type names to DuckDB-compatible equivalents.
fn map_pg_type_for_duckdb(pg_type: &str) -> String {
    match pg_type {
        "jsonb" | "json" => "JSON".to_string(),
        other => other.to_string(),
    }
}

/// Quote a literal for SQL.
fn quote_literal(val: &str) -> String {
    unsafe {
        let c_val = std::ffi::CString::new(val).unwrap();
        let quoted = pg_sys::quote_literal_cstr(c_val.as_ptr());
        let result = std::ffi::CStr::from_ptr(quoted)
            .to_string_lossy()
            .to_string();
        if quoted != c_val.as_ptr() as *mut _ {
            pg_sys::pfree(quoted as *mut std::ffi::c_void);
        }
        result
    }
}

/// A connection to PostgreSQL — either the local instance (via SPI)
/// or a remote instance (via tokio-postgres).
enum PgConn {
    Local,
    Remote {
        rt: tokio::runtime::Runtime,
        client: tokio_postgres::Client,
        _conn: tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
    },
}

impl PgConn {
    fn local() -> Self {
        Self::Local
    }

    fn remote(conninfo: &str, app_name: &str) -> Result<Self, String> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("tokio runtime: {}", e))?;
        let (client, conn) = rt.block_on(remote_connect(conninfo, app_name))?;
        Ok(Self::Remote {
            rt,
            client,
            _conn: conn,
        })
    }

    fn is_remote(&self) -> bool {
        matches!(self, Self::Remote { .. })
    }

    /// Execute a DDL/DML statement (pre-formatted SQL, no parameters).
    fn execute(&self, sql: &str) -> Result<(), String> {
        match self {
            Self::Local => Spi::connect_mut(|client| {
                client
                    .update(sql, None, &[])
                    .map_err(|e| format!("SPI execute: {}", e))?;
                Ok(())
            }),
            Self::Remote { rt, client, .. } => {
                rt.block_on(async { client.execute(sql, &[]).await })
                    .map_err(|e| format!("remote execute: {}", e))?;
                Ok(())
            }
        }
    }

    /// Check whether a query returns any rows.
    fn exists(&self, sql: &str) -> Result<bool, String> {
        match self {
            Self::Local => Spi::connect(|client| {
                let result = client
                    .select(sql, Some(1), &[])
                    .map_err(|e| format!("SPI exists: {}", e))?;
                Ok(result.len() > 0)
            }),
            Self::Remote { rt, client, .. } => {
                let rows = rt
                    .block_on(async { client.query(sql, &[]).await })
                    .map_err(|e| format!("remote exists: {}", e))?;
                Ok(!rows.is_empty())
            }
        }
    }

    /// Query a single i64 value (e.g. OID).
    fn query_i64(&self, sql: &str) -> Result<i64, String> {
        match self {
            Self::Local => Spi::connect(|client| {
                let result = client
                    .select(sql, Some(1), &[])
                    .map_err(|e| format!("SPI query_i64: {}", e))?;
                for r in result {
                    if let Some(v) = r.get::<i64>(1).unwrap() {
                        return Ok(v);
                    }
                }
                Err("query_i64: no rows returned".to_string())
            }),
            Self::Remote { rt, client, .. } => {
                let row = rt
                    .block_on(async { client.query_one(sql, &[]).await })
                    .map_err(|e| format!("remote query_i64: {}", e))?;
                Ok(row.get(0))
            }
        }
    }

    /// Query rows of (String, String) pairs (e.g. column name + type).
    fn query_string_pairs(&self, sql: &str) -> Result<Vec<(String, String)>, String> {
        match self {
            Self::Local => Spi::connect(|client| {
                let result = client
                    .select(sql, None, &[])
                    .map_err(|e| format!("SPI query_string_pairs: {}", e))?;
                let mut pairs = Vec::new();
                for r in result {
                    let a: String = r.get::<String>(1).unwrap().unwrap_or_default();
                    let b: String = r.get::<String>(2).unwrap().unwrap_or_default();
                    pairs.push((a, b));
                }
                Ok(pairs)
            }),
            Self::Remote { rt, client, .. } => {
                let rows = rt
                    .block_on(async { client.query(sql, &[]).await })
                    .map_err(|e| format!("remote query_string_pairs: {}", e))?;
                Ok(rows.iter().map(|r| (r.get(0), r.get(1))).collect())
            }
        }
    }
}

/// Check if a per-group pg_duckpipe worker is running.
fn is_group_worker_running(group_name: &str) -> bool {
    let backend_type = format!("pg_duckpipe:{}", group_name);
    let args = unsafe {
        [DatumWithOid::new(
            backend_type.as_str(),
            PgBuiltInOids::TEXTOID.value(),
        )]
    };
    let result = Spi::connect(|client| {
        let r = client.select(
            "SELECT 1 FROM pg_stat_activity WHERE backend_type = $1",
            Some(1),
            &args,
        );
        matches!(r, Ok(t) if t.len() > 0)
    });
    result
}

/// Check if any pg_duckpipe worker is running.
fn is_any_worker_running() -> bool {
    let result = Spi::get_one::<i64>(
        "SELECT 1 FROM pg_stat_activity WHERE backend_type LIKE 'pg_duckpipe:%'",
    );
    matches!(result, Ok(Some(_)))
}

/// Register and start a dynamic background worker for a specific group.
fn launch_worker(group_name: &str) -> bool {
    unsafe {
        let dbname = pg_sys::get_database_name(pg_sys::MyDatabaseId);
        let dbname_str = if dbname.is_null() {
            "unknown".to_string()
        } else {
            std::ffi::CStr::from_ptr(dbname)
                .to_string_lossy()
                .to_string()
        };

        let mut worker: pg_sys::BackgroundWorker = std::mem::zeroed();
        worker.bgw_flags =
            (pg_sys::BGWORKER_SHMEM_ACCESS | pg_sys::BGWORKER_BACKEND_DATABASE_CONNECTION) as i32;
        worker.bgw_start_time = pg_sys::BgWorkerStartTime::BgWorkerStart_RecoveryFinished;
        worker.bgw_restart_time = -1; // BGW_NEVER_RESTART — re-launch on demand via add_table/start_worker

        // Set library name
        let lib_name = std::ffi::CString::new("pg_duckpipe").unwrap();
        let lib_bytes = lib_name.as_bytes_with_nul();
        let copy_len = lib_bytes.len().min(worker.bgw_library_name.len());
        for (i, &b) in lib_bytes[..copy_len].iter().enumerate() {
            worker.bgw_library_name[i] = b as std::ffi::c_char;
        }

        // Set function name
        let func_name = std::ffi::CString::new("duckpipe_worker_main").unwrap();
        let func_bytes = func_name.as_bytes_with_nul();
        let copy_len = func_bytes.len().min(worker.bgw_function_name.len());
        for (i, &b) in func_bytes[..copy_len].iter().enumerate() {
            worker.bgw_function_name[i] = b as std::ffi::c_char;
        }

        // Set worker name (visible in ps output)
        let name = format!("pg_duckpipe [{}:{}]", dbname_str, group_name);
        let name_c = std::ffi::CString::new(name.as_str()).unwrap();
        let name_bytes = name_c.as_bytes_with_nul();
        let copy_len = name_bytes.len().min(worker.bgw_name.len());
        for (i, &b) in name_bytes[..copy_len].iter().enumerate() {
            worker.bgw_name[i] = b as std::ffi::c_char;
        }

        // Set worker type (determines backend_type in pg_stat_activity)
        let type_str = format!("pg_duckpipe:{}", group_name);
        let type_name = std::ffi::CString::new(type_str.as_str()).unwrap();
        let type_bytes = type_name.as_bytes_with_nul();
        let copy_len = type_bytes.len().min(worker.bgw_type.len());
        for (i, &b) in type_bytes[..copy_len].iter().enumerate() {
            worker.bgw_type[i] = b as std::ffi::c_char;
        }

        // Pack group_name into bgw_extra (read by duckpipe_worker_main)
        let extra_c = std::ffi::CString::new(group_name).unwrap();
        let extra_bytes = extra_c.as_bytes_with_nul();
        let copy_len = extra_bytes.len().min(worker.bgw_extra.len());
        for (i, &b) in extra_bytes[..copy_len].iter().enumerate() {
            worker.bgw_extra[i] = b as std::ffi::c_char;
        }

        worker.bgw_main_arg = pg_sys::ObjectIdGetDatum(pg_sys::MyDatabaseId) as pg_sys::Datum;
        worker.bgw_notify_pid = pg_sys::MyProcPid;

        pg_sys::RegisterDynamicBackgroundWorker(&mut worker, std::ptr::null_mut())
    }
}

// --- SQL Functions ---

#[pg_extern(sql = "
CREATE FUNCTION duckpipe.create_group(
    name TEXT,
    publication TEXT DEFAULT NULL,
    slot_name TEXT DEFAULT NULL,
    conninfo TEXT DEFAULT NULL,
    mode TEXT DEFAULT NULL
) RETURNS TEXT
AS 'MODULE_PATHNAME', '@FUNCTION_NAME@'
LANGUAGE C;
REVOKE ALL ON FUNCTION duckpipe.create_group(TEXT, TEXT, TEXT, TEXT, TEXT) FROM PUBLIC;
")]
fn create_group(
    name: &str,
    publication: Option<&str>,
    slot_name: Option<&str>,
    conninfo: Option<&str>,
    mode: Option<&str>,
) -> String {
    // Validate and default mode
    let mode_val = mode.unwrap_or("bgworker");
    if mode_val.parse::<duckpipe_core::types::GroupMode>().is_err() {
        ereport!(
            ERROR,
            PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
            format!(
                "invalid mode '{}': must be 'bgworker' or 'daemon'",
                mode_val
            )
        );
    }

    let pub_name = publication
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("duckpipe_pub_{}", name));
    let slot = slot_name
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("duckpipe_slot_{}", name));

    if let Some(ci) = conninfo {
        // --- Remote group: create slot + publication on the remote PG ---
        let ci_owned = ci.to_string();
        let slot_owned = slot.clone();
        let pub_owned = pub_name.clone();
        let result: Result<(), String> = block_on(async {
            let ddl_app = duckpipe_core::connstr::app_name(name, "ddl");
            let (client, _conn) = remote_connect(&ci_owned, &ddl_app).await?;

            // Create replication slot on remote
            client
                .query(
                    "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
                    &[&slot_owned],
                )
                .await
                .map_err(|e| format!("remote create slot: {}", e))?;

            // Create empty publication on remote
            let sql = format!("CREATE PUBLICATION {}", quote_ident(&pub_owned));
            client
                .execute(&sql, &[])
                .await
                .map_err(|e| format!("remote create publication: {}", e))?;

            Ok(())
        });

        if let Err(e) = result {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                format!("Failed to set up remote group: {}", e)
            );
        }
    } else {
        // --- Local group: create slot + publication locally via SPI ---
        Spi::connect_mut(|client| {
            // Create replication slot (MUST be first — before any writes assign a txid)
            let sql = format!(
                "SELECT pg_create_logical_replication_slot({}, 'pgoutput')",
                quote_literal(&slot)
            );
            let ret = unsafe { spi_exec_raw(&sql) };
            if ret < 0 {
                ereport!(
                    ERROR,
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    format!("Failed to create slot {}", slot)
                );
            }

            // Create publication
            let sql = format!("CREATE PUBLICATION {}", quote_ident(&pub_name));
            client.update(&sql, None, &[]).unwrap_or_else(|e| {
                ereport!(
                    ERROR,
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    format!("Failed to create publication {}: {}", pub_name, e)
                );
            });
        });
    }

    // Insert into sync_groups (always local metadata).
    // If this fails for a remote group, clean up the remote slot+publication.
    let insert_result: Result<(), String> = Spi::connect_mut(|client| {
        if conninfo.is_some() {
            let args = unsafe {
                [
                    DatumWithOid::new(name, PgBuiltInOids::TEXTOID.value()),
                    DatumWithOid::new(pub_name.clone(), PgBuiltInOids::TEXTOID.value()),
                    DatumWithOid::new(slot.as_str(), PgBuiltInOids::TEXTOID.value()),
                    DatumWithOid::new(conninfo.unwrap(), PgBuiltInOids::TEXTOID.value()),
                    DatumWithOid::new(mode_val, PgBuiltInOids::TEXTOID.value()),
                ]
            };
            client
                .update(
                    "INSERT INTO duckpipe.sync_groups (name, publication, slot_name, conninfo, mode) \
                     VALUES ($1, $2, $3, $4, $5)",
                    None,
                    &args,
                )
                .map_err(|e| format!("Failed to insert into sync_groups: {}", e))?;
        } else {
            let args = unsafe {
                [
                    DatumWithOid::new(name, PgBuiltInOids::TEXTOID.value()),
                    DatumWithOid::new(pub_name.clone(), PgBuiltInOids::TEXTOID.value()),
                    DatumWithOid::new(slot.as_str(), PgBuiltInOids::TEXTOID.value()),
                    DatumWithOid::new(mode_val, PgBuiltInOids::TEXTOID.value()),
                ]
            };
            client
                .update(
                    "INSERT INTO duckpipe.sync_groups (name, publication, slot_name, mode) \
                     VALUES ($1, $2, $3, $4)",
                    None,
                    &args,
                )
                .map_err(|e| format!("Failed to insert into sync_groups: {}", e))?;
        }
        Ok(())
    });

    if let Err(e) = insert_result {
        // Compensating cleanup: drop remote objects if they were created
        if let Some(ci) = conninfo {
            let ci = ci.to_string();
            let slot_c = slot.clone();
            let pub_c = pub_name.clone();
            let _ = block_on(async {
                let cleanup_app = duckpipe_core::connstr::app_name(name, "ddl");
                if let Ok((client, _conn)) = remote_connect(&ci, &cleanup_app).await {
                    let _ = client
                        .execute("SELECT pg_drop_replication_slot($1)", &[&slot_c])
                        .await;
                    let sql = format!("DROP PUBLICATION IF EXISTS {}", quote_ident(&pub_c));
                    let _ = client.execute(&sql, &[]).await;
                }
            });
        }
        ereport!(ERROR, PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, e);
    }

    name.to_string()
}

#[pg_extern(sql = "
CREATE FUNCTION duckpipe.drop_group(
    name TEXT
) RETURNS void
AS 'MODULE_PATHNAME', '@FUNCTION_NAME@'
LANGUAGE C STRICT;
REVOKE ALL ON FUNCTION duckpipe.drop_group(TEXT) FROM PUBLIC;
")]
fn drop_group(name: &str) {
    // Terminate the group's worker before cleanup
    terminate_group_worker(name);

    let mut pub_name = String::new();
    let mut slot = String::new();
    let mut group_conninfo: Option<String> = None;
    let mut group_id: Option<i32> = None;

    Spi::connect_mut(|client| {
        // Get publication, slot_name, conninfo, id
        let args = unsafe { [DatumWithOid::new(name, PgBuiltInOids::TEXTOID.value())] };
        let row = client
            .select(
                "SELECT publication, slot_name, conninfo, id FROM duckpipe.sync_groups WHERE name = $1",
                None,
                &args,
            )
            .unwrap_or_else(|e| {
                ereport!(
                    ERROR,
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    format!("Failed to query sync_groups: {}", e)
                );
            });

        let mut found = false;

        for r in row {
            pub_name = r.get::<String>(1).unwrap().unwrap();
            slot = r.get::<String>(2).unwrap().unwrap();
            group_conninfo = r.get::<String>(3).unwrap();
            group_id = r.get::<i32>(4).unwrap();
            found = true;
            break;
        }

        if !found {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT,
                format!("Sync group not found: {}", name)
            );
        }

        if group_conninfo.is_some() {
            // Remote group: slot + publication cleanup via tokio-postgres (done below)
        } else {
            // Local group: drop slot + publication via SPI
            let sql = format!("SELECT pg_drop_replication_slot({})", quote_literal(&slot));
            let _ = client.update(&sql, None, &[]);

            let sql = format!("DROP PUBLICATION IF EXISTS {}", quote_ident(&pub_name));
            let _ = client.update(&sql, None, &[]);
        }

        // Delete from sync_groups (always local)
        let args = unsafe { [DatumWithOid::new(name, PgBuiltInOids::TEXTOID.value())] };
        client
            .update(
                "DELETE FROM duckpipe.sync_groups WHERE name = $1",
                None,
                &args,
            )
            .unwrap();
    });

    // Remote cleanup (outside SPI context to avoid nested SPI issues)
    if let Some(ref ci) = group_conninfo {
        let ci = ci.clone();
        let slot = slot.clone();
        let pub_name = pub_name.clone();
        let ddl_app = duckpipe_core::connstr::app_name(name, "ddl");
        let result: Result<(), String> = block_on(async {
            let (client, _conn) = remote_connect(&ci, &ddl_app).await?;
            let _ = client
                .execute("SELECT pg_drop_replication_slot($1)", &[&slot])
                .await;
            let sql = format!("DROP PUBLICATION IF EXISTS {}", quote_ident(&pub_name));
            let _ = client.execute(&sql, &[]).await;
            Ok(())
        });
        if let Err(e) = result {
            // Best-effort cleanup — warn but don't fail
            warning!("Failed to clean up remote objects: {}", e);
        }
    }

    // Clear SHM slot for dropped group
    if let Some(gid) = group_id {
        crate::clear_shmem_group_slot(gid);
    }
}

#[pg_extern(sql = "
CREATE FUNCTION duckpipe.enable_group(name TEXT) RETURNS void
AS 'MODULE_PATHNAME', '@FUNCTION_NAME@'
LANGUAGE C STRICT;
REVOKE ALL ON FUNCTION duckpipe.enable_group(TEXT) FROM PUBLIC;
")]
fn enable_group(name: &str) {
    Spi::connect_mut(|client| {
        let args = unsafe { [DatumWithOid::new(name, PgBuiltInOids::TEXTOID.value())] };
        let result = client.update(
            "UPDATE duckpipe.sync_groups SET enabled = true WHERE name = $1",
            None,
            &args,
        );
        match result {
            Ok(status) if status.len() > 0 => {
                // Wake the group's worker so it picks up the enabled group immediately.
                let channel = listen::wakeup_channel(name);
                let notify_sql = format!("NOTIFY {}", channel);
                let _ = client.update(&notify_sql, None, &[]);
            }
            _ => {
                ereport!(
                    ERROR,
                    PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT,
                    format!("Sync group '{}' not found", name)
                );
            }
        }
    });
    notice!("Sync group '{}' enabled", name);
}

#[pg_extern(sql = "
CREATE FUNCTION duckpipe.disable_group(name TEXT) RETURNS void
AS 'MODULE_PATHNAME', '@FUNCTION_NAME@'
LANGUAGE C STRICT;
REVOKE ALL ON FUNCTION duckpipe.disable_group(TEXT) FROM PUBLIC;
")]
fn disable_group(name: &str) {
    Spi::connect_mut(|client| {
        let args = unsafe { [DatumWithOid::new(name, PgBuiltInOids::TEXTOID.value())] };
        let result = client.update(
            "UPDATE duckpipe.sync_groups SET enabled = false WHERE name = $1",
            None,
            &args,
        );
        match result {
            Ok(status) if status.len() > 0 => {}
            _ => {
                ereport!(
                    ERROR,
                    PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT,
                    format!("Sync group '{}' not found", name)
                );
            }
        }
    });
    notice!("Sync group '{}' disabled", name);
}

#[pg_extern(sql = "
CREATE FUNCTION duckpipe.add_table(
    source_table TEXT,
    target_table TEXT DEFAULT NULL,
    sync_group TEXT DEFAULT 'default',
    copy_data BOOLEAN DEFAULT true
) RETURNS void
AS 'MODULE_PATHNAME', '@FUNCTION_NAME@'
LANGUAGE C;
REVOKE ALL ON FUNCTION duckpipe.add_table(TEXT, TEXT, TEXT, BOOLEAN) FROM PUBLIC;
")]
fn add_table(
    source_table: &str,
    target_table: Option<&str>,
    sync_group: Option<&str>,
    copy_data: Option<bool>,
) {
    let (schema, table) = parse_source_table(source_table);
    let group = sync_group.unwrap_or("default");
    let copy_data = copy_data.unwrap_or(true);

    let (t_schema, t_table) = if let Some(target) = target_table {
        parse_source_table(target)
    } else {
        (schema.clone(), format!("{}_ducklake", table))
    };

    // 1. Look up conninfo + publication + slot + mode from sync_groups (always local SPI).
    let (group_conninfo, publication, slot_name_val, group_mode): (
        Option<String>,
        String,
        String,
        duckpipe_core::types::GroupMode,
    ) = Spi::connect(|client| {
        let args = unsafe { [DatumWithOid::new(group, PgBuiltInOids::TEXTOID.value())] };
        let result = client
            .select(
                "SELECT conninfo, publication, slot_name, mode FROM duckpipe.sync_groups WHERE name = $1",
                Some(1),
                &args,
            )
            .unwrap();
        for r in result {
            let mode_str: String = r.get::<String>(4).unwrap().unwrap();
            return (
                r.get::<String>(1).unwrap(),
                r.get::<String>(2).unwrap().unwrap(),
                r.get::<String>(3).unwrap().unwrap(),
                mode_str
                    .parse()
                    .unwrap_or(duckpipe_core::types::GroupMode::BgWorker),
            );
        }
        ereport!(
            ERROR,
            PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT,
            format!("Sync group '{}' not found", group)
        );
    });

    // 2. Build PgConn: Local or Remote
    let ddl_app = duckpipe_core::connstr::app_name(group, "ddl");
    let source = match group_conninfo {
        Some(ref ci) => match PgConn::remote(ci, &ddl_app) {
            Ok(c) => c,
            Err(e) => {
                ereport!(
                    ERROR,
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    format!("Failed to connect to remote PG: {}", e)
                );
            }
        },
        None => PgConn::local(),
    };

    // 3. Publication setup
    if !source.is_remote() {
        // Local-only: check if publication exists; if not, create slot + publication
        let pub_exists_sql = format!(
            "SELECT 1 FROM pg_publication WHERE pubname = {}",
            quote_literal(&publication)
        );
        let pub_exists = source.exists(&pub_exists_sql).unwrap_or(false);

        if !pub_exists {
            // Slot + publication creation requires an active SPI context for spi_exec_raw.
            // Must happen before any client.update() to avoid assigning a TransactionId
            // (pg_create_logical_replication_slot requires a clean transaction).
            Spi::connect_mut(|client| {
                let sql = format!(
                    "SELECT pg_create_logical_replication_slot({}, 'pgoutput')",
                    quote_literal(&slot_name_val)
                );
                let ret = unsafe { spi_exec_raw(&sql) };
                if ret < 0 {
                    ereport!(
                        ERROR,
                        PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                        format!("Failed to create replication slot {}", slot_name_val)
                    );
                }

                let sql = format!(
                    "CREATE PUBLICATION {} FOR TABLE {}.{}",
                    quote_ident(&publication),
                    quote_ident(&schema),
                    quote_ident(&table)
                );
                client.update(&sql, None, &[]).unwrap();
            });
        } else {
            let sql = format!(
                "ALTER PUBLICATION {} ADD TABLE {}.{}",
                quote_ident(&publication),
                quote_ident(&schema),
                quote_ident(&table)
            );
            if let Err(e) = source.execute(&sql) {
                ereport!(
                    ERROR,
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    format!("Failed to alter publication: {}", e)
                );
            }
        }
    } else {
        // Remote: publication + slot already exist; just ADD TABLE
        let sql = format!(
            "ALTER PUBLICATION {} ADD TABLE {}.{}",
            quote_ident(&publication),
            quote_ident(&schema),
            quote_ident(&table)
        );
        if let Err(e) = source.execute(&sql) {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                format!("Failed to add table to remote publication: {}", e)
            );
        }
    }

    // 4. REPLICA IDENTITY FULL on source
    let sql = format!(
        "ALTER TABLE {}.{} REPLICA IDENTITY FULL",
        quote_ident(&schema),
        quote_ident(&table)
    );
    if let Err(e) = source.execute(&sql) {
        ereport!(
            ERROR,
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            format!("Failed to set REPLICA IDENTITY FULL: {}", e)
        );
    }

    // 5. Get source OID
    let source_oid_sql = format!(
        "SELECT c.oid::bigint FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = {} AND c.relname = {}",
        quote_literal(&schema),
        quote_literal(&table)
    );
    let source_oid = match source.query_i64(&source_oid_sql) {
        Ok(v) => v,
        Err(e) => {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                format!("Failed to get source OID: {}", e)
            );
        }
    };

    // 6. Column definitions (introspect for both local and remote — needed for PG→DuckDB type mapping)
    let col_defs: Vec<(String, String)> = {
        let col_sql = format!(
            "SELECT a.attname::text, pg_catalog.format_type(a.atttypid, a.atttypmod) \
             FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             JOIN pg_attribute a ON a.attrelid = c.oid \
             WHERE n.nspname = {} AND c.relname = {} \
             AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
            quote_literal(&schema),
            quote_literal(&table)
        );
        let cols = match source.query_string_pairs(&col_sql) {
            Ok(c) => c,
            Err(e) => {
                ereport!(
                    ERROR,
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    format!("Failed to introspect columns: {}", e)
                );
            }
        };
        if cols.is_empty() {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                format!("no columns found for {}.{}", schema, table)
            );
        }
        cols
    };

    // 7. Verify PK exists (remote only — local CREATE PUBLICATION would fail without one)
    if source.is_remote() {
        let pk_sql = format!(
            "SELECT 1 FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             JOIN pg_index i ON i.indrelid = c.oid AND i.indisprimary \
             WHERE n.nspname = {} AND c.relname = {}",
            quote_literal(&schema),
            quote_literal(&table)
        );
        match source.exists(&pk_sql) {
            Ok(true) => {}
            Ok(false) => {
                ereport!(
                    ERROR,
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    format!("table {}.{} on remote PG has no primary key", schema, table)
                );
            }
            Err(e) => {
                ereport!(
                    ERROR,
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    format!("Failed to check primary key: {}", e)
                );
            }
        }
    }

    // 8. Target creation + mapping INSERT (always local SPI)
    let local_result: Result<(), String> = Spi::connect_mut(|client| {
        // Auto-create target schema if needed
        {
            let args = unsafe {
                [DatumWithOid::new(
                    t_schema.as_str(),
                    PgBuiltInOids::TEXTOID.value(),
                )]
            };
            let result = client.select(
                "SELECT 1 FROM pg_namespace WHERE nspname = $1",
                Some(1),
                &args,
            );
            if matches!(result, Ok(t) if t.len() == 0) {
                let sql = format!("CREATE SCHEMA {}", quote_ident(&t_schema));
                let _ = client.update(&sql, None, &[]);
            }
        }

        // Create target table — always use explicit column definitions with PG→DuckDB type mapping
        {
            // Sanitize type_str: for remote PG the format_type() output is untrusted;
            // for local PG it's trusted but we check defensively anyway.
            for (name, type_str) in &col_defs {
                if type_str.contains(';') || type_str.contains("--") || type_str.contains("/*") {
                    return Err(format!(
                        "suspicious type '{}' for column '{}' — refusing to interpolate",
                        type_str, name
                    ));
                }
            }
        }
        let col_clauses: Vec<String> = col_defs
            .iter()
            .map(|(name, type_str)| {
                format!("{} {}", quote_ident(name), map_pg_type_for_duckdb(type_str))
            })
            .collect();
        let create_sql = format!(
            "CREATE TABLE IF NOT EXISTS {}.{} ({}) USING ducklake",
            quote_ident(&t_schema),
            quote_ident(&t_table),
            col_clauses.join(", "),
        );
        client
            .update(&create_sql, None, &[])
            .map_err(|e| format!("CREATE TABLE: {}", e))?;

        // Set data inlining if configured
        let inlining_limit = DATA_INLINING_ROW_LIMIT.get();
        if inlining_limit > 0 {
            let sql = format!(
                "CALL ducklake.set_option('data_inlining_row_limit', {})",
                inlining_limit
            );
            let _ = client.update(&sql, None, &[]);
        }

        // Insert table mapping with source OID
        let initial_state = if copy_data { "SNAPSHOT" } else { "STREAMING" };
        let args = unsafe {
            [
                DatumWithOid::new(schema.as_str(), PgBuiltInOids::TEXTOID.value()),
                DatumWithOid::new(table.as_str(), PgBuiltInOids::TEXTOID.value()),
                DatumWithOid::new(t_schema.as_str(), PgBuiltInOids::TEXTOID.value()),
                DatumWithOid::new(t_table.as_str(), PgBuiltInOids::TEXTOID.value()),
                DatumWithOid::new(initial_state, PgBuiltInOids::TEXTOID.value()),
                DatumWithOid::new(group, PgBuiltInOids::TEXTOID.value()),
                DatumWithOid::new(source_oid, PgBuiltInOids::INT8OID.value()),
            ]
        };
        client
            .update(
                "INSERT INTO duckpipe.table_mappings (group_id, source_schema, source_table, \
                 target_schema, target_table, state, source_oid) \
                 SELECT sg.id, $1, $2, $3, $4, $5, $7 \
                 FROM duckpipe.sync_groups sg WHERE sg.name = $6",
                None,
                &args,
            )
            .map_err(|e| format!("INSERT mapping: {}", e))?;
        Ok(())
    });

    if let Err(e) = local_result {
        // Compensating cleanup: remove table from source publication
        let _ = source.execute(&format!(
            "ALTER PUBLICATION {} DROP TABLE {}.{}",
            quote_ident(&publication),
            quote_ident(&schema),
            quote_ident(&table)
        ));
        ereport!(
            ERROR,
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            format!("Failed to set up local target: {}", e)
        );
    }

    // Auto-start background worker for this group or wake existing one
    // (only for bgworker-mode groups — daemon groups are managed externally)
    if group_mode == duckpipe_core::types::GroupMode::BgWorker {
        if !is_group_worker_running(group) {
            launch_worker(group);
        } else {
            // Worker is already running — send per-group NOTIFY so it picks up
            // the new table immediately instead of waiting for poll_interval.
            let channel = listen::wakeup_channel(group);
            let notify_sql = format!("NOTIFY {}", channel);
            Spi::connect_mut(|client| {
                let _ = client.update(&notify_sql, None, &[]);
            });
        }
    }
}

#[pg_extern(sql = "
CREATE FUNCTION duckpipe.remove_table(
    source_table TEXT,
    drop_target BOOLEAN DEFAULT false
) RETURNS void
AS 'MODULE_PATHNAME', '@FUNCTION_NAME@'
LANGUAGE C;
REVOKE ALL ON FUNCTION duckpipe.remove_table(TEXT, BOOLEAN) FROM PUBLIC;
")]
fn remove_table(source_table: &str, drop_target: Option<bool>) {
    let (schema, table) = parse_source_table(source_table);
    let drop_target = drop_target.unwrap_or(false);

    let mut publication = None;
    let mut t_schema = None;
    let mut t_table = None;
    let mut group_conninfo: Option<String> = None;
    let mut group_name_for_app = String::new();
    let mut mapping_id: Option<i32> = None;
    let mut group_id: Option<i32> = None;

    Spi::connect_mut(|client| {
        // Get publication, target info, conninfo, group name, group id, and mapping id
        let args = unsafe {
            [
                DatumWithOid::new(schema.as_str(), PgBuiltInOids::TEXTOID.value()),
                DatumWithOid::new(table.as_str(), PgBuiltInOids::TEXTOID.value()),
            ]
        };
        let result = client
            .select(
                "SELECT g.publication, m.target_schema, m.target_table, g.conninfo, g.name, m.id, g.id \
                 FROM duckpipe.table_mappings m \
                 JOIN duckpipe.sync_groups g ON m.group_id = g.id \
                 WHERE m.source_schema = $1 AND m.source_table = $2",
                Some(1),
                &args,
            )
            .unwrap();

        for r in result {
            publication = Some(r.get::<String>(1).unwrap().unwrap());
            t_schema = Some(r.get::<String>(2).unwrap().unwrap());
            t_table = Some(r.get::<String>(3).unwrap().unwrap());
            group_conninfo = r.get::<String>(4).unwrap();
            group_name_for_app = r.get::<String>(5).unwrap().unwrap_or_default();
            mapping_id = r.get::<i32>(6).unwrap();
            group_id = r.get::<i32>(7).unwrap();
            break;
        }

        // Drop target if requested
        if drop_target {
            if let (Some(ts), Some(tt)) = (&t_schema, &t_table) {
                let sql = format!(
                    "DROP TABLE IF EXISTS {}.{}",
                    quote_ident(ts),
                    quote_ident(tt)
                );
                let _ = client.update(&sql, None, &[]);
            }
        }

        // Delete mapping
        let args = unsafe {
            [
                DatumWithOid::new(schema.as_str(), PgBuiltInOids::TEXTOID.value()),
                DatumWithOid::new(table.as_str(), PgBuiltInOids::TEXTOID.value()),
            ]
        };
        client
            .update(
                "DELETE FROM duckpipe.table_mappings WHERE source_schema = $1 AND source_table = $2",
                None,
                &args,
            )
            .unwrap();
    });

    // Drop from publication (unified local/remote via PgConn)
    if let Some(ref pub_name) = publication {
        let rm_ddl_app = duckpipe_core::connstr::app_name(&group_name_for_app, "ddl");
        let source = match &group_conninfo {
            Some(ci) => match PgConn::remote(ci, &rm_ddl_app) {
                Ok(c) => c,
                Err(e) => {
                    warning!(
                        "Failed to connect to remote PG for publication cleanup: {}",
                        e
                    );
                    return;
                }
            },
            None => PgConn::local(),
        };
        let _ = source.execute(&format!(
            "ALTER PUBLICATION {} DROP TABLE {}.{}",
            quote_ident(pub_name),
            quote_ident(&schema),
            quote_ident(&table)
        ));
    }

    // Clear SHM slot for removed table
    if let Some(mid) = mapping_id {
        crate::clear_shmem_table_slot(mid);
    }

    // Warn if the group has no remaining tables (slot still holds WAL)
    if let Some(gid) = group_id {
        let remaining: i64 = Spi::connect(|client| {
            let args = unsafe { [DatumWithOid::new(gid, PgBuiltInOids::INT4OID.value())] };
            let result = client
                .select(
                    "SELECT count(*)::bigint FROM duckpipe.table_mappings WHERE group_id = $1",
                    None,
                    &args,
                )
                .unwrap();
            for r in result {
                return r.get::<i64>(1).unwrap().unwrap_or(0);
            }
            0
        });
        if remaining == 0 {
            warning!(
                "Group '{}' has no remaining tables — its replication slot is still holding WAL. \
                 Run SELECT duckpipe.drop_group('{}') to release it.",
                group_name_for_app,
                group_name_for_app
            );
        }
    }
}

#[pg_extern(sql = "
CREATE FUNCTION duckpipe.move_table(
    source_table TEXT,
    new_group TEXT
) RETURNS void
AS 'MODULE_PATHNAME', '@FUNCTION_NAME@'
LANGUAGE C STRICT;
REVOKE ALL ON FUNCTION duckpipe.move_table(TEXT, TEXT) FROM PUBLIC;
")]
fn move_table(source_table: &str, new_group: &str) {
    let (schema, table) = parse_source_table(source_table);

    // Fetch old and new group publication names, conninfo, and old group name.
    let (old_pub, old_ci, new_pub, new_ci, old_group_name): (
        String,
        Option<String>,
        String,
        Option<String>,
        String,
    ) = Spi::connect(|client| {
        let args = unsafe {
            [
                DatumWithOid::new(schema.as_str(), PgBuiltInOids::TEXTOID.value()),
                DatumWithOid::new(table.as_str(), PgBuiltInOids::TEXTOID.value()),
                DatumWithOid::new(new_group, PgBuiltInOids::TEXTOID.value()),
            ]
        };
        let result = client
            .select(
                "SELECT og.publication, og.conninfo, ng.publication, ng.conninfo, og.name \
                 FROM duckpipe.table_mappings m \
                 JOIN duckpipe.sync_groups og ON m.group_id = og.id \
                 CROSS JOIN duckpipe.sync_groups ng \
                 WHERE m.source_schema = $1 AND m.source_table = $2 AND ng.name = $3",
                Some(1),
                &args,
            )
            .unwrap();
        for r in result {
            return (
                r.get::<String>(1).unwrap().unwrap(),
                r.get::<String>(2).unwrap(),
                r.get::<String>(3).unwrap().unwrap(),
                r.get::<String>(4).unwrap(),
                r.get::<String>(5).unwrap().unwrap(),
            );
        }
        ereport!(
            ERROR,
            PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT,
            format!("Table mapping or target group '{}' not found", new_group)
        );
    });

    // Drop from old group's publication (unified via PgConn)
    {
        let old_ddl_app = duckpipe_core::connstr::app_name(&old_group_name, "ddl");
        let old_source = match &old_ci {
            Some(ci) => PgConn::remote(ci, &old_ddl_app).ok(),
            None => Some(PgConn::local()),
        };
        if let Some(src) = old_source {
            let _ = src.execute(&format!(
                "ALTER PUBLICATION {} DROP TABLE {}.{}",
                quote_ident(&old_pub),
                quote_ident(&schema),
                quote_ident(&table)
            ));
        }
    }

    // Add to new group's publication (unified via PgConn)
    {
        let ddl_app = duckpipe_core::connstr::app_name(new_group, "ddl");
        let new_source = match &new_ci {
            Some(ci) => PgConn::remote(ci, &ddl_app).ok(),
            None => Some(PgConn::local()),
        };
        if let Some(src) = new_source {
            let _ = src.execute(&format!(
                "ALTER PUBLICATION {} ADD TABLE {}.{}",
                quote_ident(&new_pub),
                quote_ident(&schema),
                quote_ident(&table)
            ));
        }
    }

    // Update the group_id in the mapping
    Spi::connect_mut(|client| {
        let args = unsafe {
            [
                DatumWithOid::new(new_group, PgBuiltInOids::TEXTOID.value()),
                DatumWithOid::new(schema.as_str(), PgBuiltInOids::TEXTOID.value()),
                DatumWithOid::new(table.as_str(), PgBuiltInOids::TEXTOID.value()),
            ]
        };
        client
            .update(
                "UPDATE duckpipe.table_mappings SET group_id = \
                 (SELECT id FROM duckpipe.sync_groups WHERE name = $1) \
                 WHERE source_schema = $2 AND source_table = $3",
                None,
                &args,
            )
            .unwrap();
    });
}

#[pg_extern(sql = "
CREATE FUNCTION duckpipe.resync_table(source_table TEXT) RETURNS void
AS 'MODULE_PATHNAME', '@FUNCTION_NAME@'
LANGUAGE C STRICT;
REVOKE ALL ON FUNCTION duckpipe.resync_table(TEXT) FROM PUBLIC;
")]
fn resync_table(source_table: &str) {
    let (schema, table) = parse_source_table(source_table);

    Spi::connect_mut(|client| {
        // Get target table info and group name
        let args = unsafe {
            [
                DatumWithOid::new(schema.as_str(), PgBuiltInOids::TEXTOID.value()),
                DatumWithOid::new(table.as_str(), PgBuiltInOids::TEXTOID.value()),
            ]
        };
        let result = client
            .select(
                "SELECT m.target_schema, m.target_table, g.name \
                 FROM duckpipe.table_mappings m \
                 JOIN duckpipe.sync_groups g ON m.group_id = g.id \
                 WHERE m.source_schema = $1 AND m.source_table = $2",
                Some(1),
                &args,
            )
            .unwrap();

        let mut t_schema = String::new();
        let mut t_table = String::new();
        let mut group_name = String::new();
        let mut found = false;

        for r in result {
            t_schema = r.get::<String>(1).unwrap().unwrap();
            t_table = r.get::<String>(2).unwrap().unwrap();
            group_name = r.get::<String>(3).unwrap().unwrap();
            found = true;
            break;
        }

        if !found {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT,
                format!("Table mapping for '{}.{}' not found", schema, table)
            );
        }

        // Clear target table
        let sql = format!(
            "TRUNCATE TABLE {}.{}",
            quote_ident(&t_schema),
            quote_ident(&t_table)
        );
        let _ = client.update(&sql, None, &[]);

        // Reset state to SNAPSHOT and clear error info
        let args = unsafe {
            [
                DatumWithOid::new(schema.as_str(), PgBuiltInOids::TEXTOID.value()),
                DatumWithOid::new(table.as_str(), PgBuiltInOids::TEXTOID.value()),
            ]
        };
        client
            .update(
                "UPDATE duckpipe.table_mappings SET state = 'SNAPSHOT', \
                 rows_synced = 0, last_sync_at = NULL, \
                 error_message = NULL, applied_lsn = NULL, snapshot_lsn = NULL, \
                 consecutive_failures = 0, retry_at = NULL, \
                 snapshot_duration_ms = NULL, snapshot_rows = NULL \
                 WHERE source_schema = $1 AND source_table = $2",
                None,
                &args,
            )
            .unwrap();

        // Wake the group's worker so it picks up the resync immediately.
        let channel = listen::wakeup_channel(&group_name);
        let notify_sql = format!("NOTIFY {}", channel);
        let _ = client.update(&notify_sql, None, &[]);
    });

    let (s, t) = parse_source_table(source_table);
    notice!("Table '{}.{}' marked for resync", s, t);
}

// --- Monitoring SRFs ---

#[pg_extern(sql = "
CREATE FUNCTION duckpipe.groups() RETURNS TABLE(
    name TEXT,
    publication TEXT,
    slot_name TEXT,
    enabled BOOLEAN,
    mode TEXT,
    table_count INTEGER,
    last_sync TIMESTAMPTZ,
    conninfo TEXT
)
AS 'MODULE_PATHNAME', '@FUNCTION_NAME@'
LANGUAGE C STRICT;
")]
fn groups() -> TableIterator<
    'static,
    (
        name!(name, String),
        name!(publication, String),
        name!(slot_name, String),
        name!(enabled, bool),
        name!(mode, String),
        name!(table_count, i32),
        name!(last_sync, Option<TimestampWithTimeZone>),
        name!(conninfo, Option<String>),
    ),
> {
    let mut rows = Vec::new();

    Spi::connect(|client| {
        let result = client.select(
            "SELECT g.name, g.publication, g.slot_name, g.enabled, g.mode, \
             (SELECT count(*) FROM duckpipe.table_mappings m WHERE m.group_id = g.id)::int4 as table_count, \
             g.last_sync_at, g.conninfo \
             FROM duckpipe.sync_groups g ORDER BY g.name",
            None,
            &[],
        );

        if let Ok(tuptable) = result {
            for row in tuptable {
                let name: String = row.get(1).unwrap().unwrap();
                let publication: String = row.get(2).unwrap().unwrap();
                let slot_name: String = row.get(3).unwrap().unwrap();
                let enabled: bool = row.get(4).unwrap().unwrap();
                let mode: String = row.get(5).unwrap().unwrap();
                let table_count: i32 = row.get(6).unwrap().unwrap();
                let last_sync: Option<TimestampWithTimeZone> = row.get(7).unwrap();
                let conninfo: Option<String> = row
                    .get::<String>(8)
                    .unwrap()
                    .map(|ci| redact_conninfo_password(&ci));

                rows.push((
                    name,
                    publication,
                    slot_name,
                    enabled,
                    mode,
                    table_count,
                    last_sync,
                    conninfo,
                ));
            }
        }
    });

    TableIterator::new(rows)
}

#[pg_extern(sql = "
CREATE FUNCTION duckpipe.tables() RETURNS TABLE(
    source_table TEXT,
    target_table TEXT,
    sync_group TEXT,
    enabled BOOLEAN,
    rows_synced BIGINT,
    last_sync TIMESTAMPTZ
)
AS 'MODULE_PATHNAME', '@FUNCTION_NAME@'
LANGUAGE C STRICT;
")]
fn tables() -> TableIterator<
    'static,
    (
        name!(source_table, String),
        name!(target_table, String),
        name!(sync_group, String),
        name!(enabled, bool),
        name!(rows_synced, i64),
        name!(last_sync, Option<TimestampWithTimeZone>),
    ),
> {
    let mut rows = Vec::new();

    Spi::connect(|client| {
        let result = client.select(
            "SELECT m.source_schema || '.' || m.source_table as source_table, \
             m.target_schema || '.' || m.target_table as target_table, \
             g.name as sync_group, m.enabled, m.rows_synced, m.last_sync_at \
             FROM duckpipe.table_mappings m \
             JOIN duckpipe.sync_groups g ON m.group_id = g.id \
             ORDER BY g.name, m.source_schema, m.source_table",
            None,
            &[],
        );

        if let Ok(tuptable) = result {
            for row in tuptable {
                let source_table: String = row.get(1).unwrap().unwrap();
                let target_table: String = row.get(2).unwrap().unwrap();
                let sync_group: String = row.get(3).unwrap().unwrap();
                let enabled: bool = row.get(4).unwrap().unwrap();
                let rows_synced: i64 = row.get(5).unwrap().unwrap();
                let last_sync: Option<TimestampWithTimeZone> = row.get(6).unwrap();

                rows.push((
                    source_table,
                    target_table,
                    sync_group,
                    enabled,
                    rows_synced,
                    last_sync,
                ));
            }
        }
    });

    TableIterator::new(rows)
}

#[pg_extern(sql = "
CREATE FUNCTION duckpipe.status() RETURNS TABLE(
    sync_group TEXT,
    source_table TEXT,
    target_table TEXT,
    state TEXT,
    enabled BOOLEAN,
    rows_synced BIGINT,
    queued_changes BIGINT,
    last_sync TIMESTAMPTZ,
    error_message TEXT,
    consecutive_failures INTEGER,
    retry_at TIMESTAMPTZ,
    applied_lsn TEXT,
    snapshot_duration_ms BIGINT,
    snapshot_rows BIGINT
)
AS 'MODULE_PATHNAME', '@FUNCTION_NAME@'
LANGUAGE C STRICT;
")]
fn status() -> TableIterator<
    'static,
    (
        name!(sync_group, String),
        name!(source_table, String),
        name!(target_table, String),
        name!(state, String),
        name!(enabled, bool),
        name!(rows_synced, i64),
        name!(queued_changes, i64),
        name!(last_sync, Option<TimestampWithTimeZone>),
        name!(error_message, Option<String>),
        name!(consecutive_failures, i32),
        name!(retry_at, Option<TimestampWithTimeZone>),
        name!(applied_lsn, Option<String>),
        name!(snapshot_duration_ms, Option<i64>),
        name!(snapshot_rows, Option<i64>),
    ),
> {
    // Read SHM metrics keyed by mapping_id (for queued_changes)
    let shm_map = crate::read_shmem_table_metrics();

    let mut rows = Vec::new();

    Spi::connect(|client| {
        let result = client.select(
            "SELECT g.name as sync_group, \
             m.source_schema || '.' || m.source_table as source_table, \
             m.target_schema || '.' || m.target_table as target_table, \
             m.state, m.enabled, m.rows_synced, m.last_sync_at, \
             m.error_message, m.consecutive_failures, m.retry_at, m.applied_lsn::text, \
             m.snapshot_duration_ms, m.snapshot_rows, m.id \
             FROM duckpipe.table_mappings m \
             JOIN duckpipe.sync_groups g ON m.group_id = g.id \
             ORDER BY g.name, m.source_schema, m.source_table",
            None,
            &[],
        );

        if let Ok(tuptable) = result {
            for row in tuptable {
                let sync_group: String = row.get(1).unwrap().unwrap();
                let source_table: String = row.get(2).unwrap().unwrap();
                let target_table: String = row.get(3).unwrap().unwrap();
                let state: String = row.get(4).unwrap().unwrap();
                let enabled: bool = row.get(5).unwrap().unwrap();
                let rows_synced: i64 = row.get(6).unwrap().unwrap();
                let last_sync: Option<TimestampWithTimeZone> = row.get(7).unwrap();
                let error_message: Option<String> = row.get(8).unwrap();
                let consecutive_failures: i32 = row.get::<i32>(9).unwrap().unwrap_or(0);
                let retry_at: Option<TimestampWithTimeZone> = row.get(10).unwrap();
                let applied_lsn: Option<String> = row.get(11).unwrap();
                let snapshot_duration_ms: Option<i64> = row.get(12).unwrap();
                let snapshot_rows: Option<i64> = row.get(13).unwrap();
                let mapping_id: i32 = row.get::<i32>(14).unwrap().unwrap_or(0);

                // Read queued_changes from SHM
                let queued_changes = shm_map
                    .get(&mapping_id)
                    .map(|m| m.queued_changes)
                    .unwrap_or(0);

                rows.push((
                    sync_group,
                    source_table,
                    target_table,
                    state,
                    enabled,
                    rows_synced,
                    queued_changes,
                    last_sync,
                    error_message,
                    consecutive_failures,
                    retry_at,
                    applied_lsn,
                    snapshot_duration_ms,
                    snapshot_rows,
                ));
            }
        }
    });

    TableIterator::new(rows)
}

#[pg_extern(sql = "
CREATE FUNCTION duckpipe.worker_status() RETURNS TABLE(
    sync_group TEXT,
    total_queued_changes BIGINT,
    is_backpressured BOOLEAN,
    active_flushes INT,
    gate_wait_avg_ms BIGINT,
    gate_timeouts BIGINT
)
AS 'MODULE_PATHNAME', '@FUNCTION_NAME@'
LANGUAGE C STRICT;
")]
fn worker_status() -> TableIterator<
    'static,
    (
        name!(sync_group, String),
        name!(total_queued_changes, i64),
        name!(is_backpressured, bool),
        name!(active_flushes, i32),
        name!(gate_wait_avg_ms, i64),
        name!(gate_timeouts, i64),
    ),
> {
    // Read group metrics from SHM keyed by group_id
    let shm_map = crate::read_shmem_group_metrics();

    let mut rows = Vec::new();

    Spi::connect(|client| {
        let result = client.select(
            "SELECT g.id, g.name FROM duckpipe.sync_groups g ORDER BY g.name",
            None,
            &[],
        );

        if let Ok(tuptable) = result {
            for row in tuptable {
                let group_id: i32 = row.get::<i32>(1).unwrap().unwrap_or(0);
                let sync_group: String = row.get::<String>(2).unwrap().unwrap_or_default();

                let gm = shm_map.get(&group_id).copied().unwrap_or_default();

                rows.push((
                    sync_group,
                    gm.total_queued_changes,
                    gm.is_backpressured,
                    gm.active_flushes,
                    gm.gate_wait_avg_ms,
                    gm.gate_timeouts,
                ));
            }
        }
    });

    TableIterator::new(rows)
}

#[pg_extern(sql = "
CREATE FUNCTION duckpipe.metrics() RETURNS TEXT
AS 'MODULE_PATHNAME', '@FUNCTION_NAME@'
LANGUAGE C STRICT;
")]
fn metrics() -> String {
    // Read in-memory metrics from SHM (already keyed by id)
    let shm_table_map = crate::read_shmem_table_metrics();
    let shm_group_map = crate::read_shmem_group_metrics();

    // Query persisted metrics from PG
    let mut table_entries = Vec::new();
    let mut group_entries = Vec::new();

    Spi::connect(|client| {
        // Tables
        let result = client.select(
            "SELECT g.name, m.source_schema || '.' || m.source_table, \
             m.state, m.rows_synced, m.consecutive_failures, \
             m.snapshot_duration_ms, m.snapshot_rows, m.applied_lsn::text, m.id \
             FROM duckpipe.table_mappings m \
             JOIN duckpipe.sync_groups g ON m.group_id = g.id \
             ORDER BY g.name, m.source_schema, m.source_table",
            None,
            &[],
        );
        if let Ok(tuptable) = result {
            for row in tuptable {
                let group_name: String = row.get(1).unwrap().unwrap();
                let source_table: String = row.get(2).unwrap().unwrap();
                let state: String = row.get(3).unwrap().unwrap();
                let rows_synced: i64 = row.get(4).unwrap().unwrap();
                let consecutive_failures: i32 = row.get::<i32>(5).unwrap().unwrap_or(0);
                let snapshot_duration_ms: Option<i64> = row.get(6).unwrap();
                let snapshot_rows: Option<i64> = row.get(7).unwrap();
                let applied_lsn: Option<String> = row.get(8).unwrap();
                let mapping_id: i32 = row.get::<i32>(9).unwrap().unwrap_or(0);

                let tm = shm_table_map.get(&mapping_id).copied().unwrap_or_default();

                table_entries.push(format!(
                    "{{\"group\":{},\"source_table\":{},\"state\":{},\"rows_synced\":{},\
                     \"queued_changes\":{},\"duckdb_memory_bytes\":{},\
                     \"consecutive_failures\":{},\"flush_count\":{},\"flush_duration_ms\":{},\
                     \"avg_row_bytes\":{},\
                     \"snapshot_duration_ms\":{},\"snapshot_rows\":{},\"applied_lsn\":{}}}",
                    json_str(&group_name),
                    json_str(&source_table),
                    json_str(&state),
                    rows_synced,
                    tm.queued_changes,
                    tm.duckdb_memory_bytes,
                    consecutive_failures,
                    tm.flush_count,
                    tm.flush_duration_ms,
                    tm.avg_row_bytes,
                    json_opt_i64(snapshot_duration_ms),
                    json_opt_i64(snapshot_rows),
                    json_opt_str(applied_lsn.as_deref()),
                ));
            }
        }

        // Groups
        let result = client.select(
            "SELECT g.id, g.name FROM duckpipe.sync_groups g ORDER BY g.name",
            None,
            &[],
        );
        if let Ok(tuptable) = result {
            for row in tuptable {
                let group_id: i32 = row.get::<i32>(1).unwrap().unwrap_or(0);
                let name: String = row.get::<String>(2).unwrap().unwrap_or_default();

                let gm = shm_group_map.get(&group_id).copied().unwrap_or_default();

                group_entries.push(format!(
                    "{{\"name\":{},\"total_queued_changes\":{},\"is_backpressured\":{},\"active_flushes\":{},\
                     \"gate_wait_avg_ms\":{},\"gate_timeouts\":{}}}",
                    json_str(&name),
                    gm.total_queued_changes,
                    gm.is_backpressured,
                    gm.active_flushes,
                    gm.gate_wait_avg_ms,
                    gm.gate_timeouts,
                ));
            }
        }
    });

    format!(
        "{{\"tables\":[{}],\"groups\":[{}]}}",
        table_entries.join(","),
        group_entries.join(","),
    )
}

/// Escape a string for JSON output.
fn json_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn json_opt_str(s: Option<&str>) -> String {
    match s {
        Some(v) => json_str(v),
        None => "null".to_string(),
    }
}

fn json_opt_i64(v: Option<i64>) -> String {
    match v {
        Some(n) => n.to_string(),
        None => "null".to_string(),
    }
}

#[pg_extern(sql = "
CREATE FUNCTION duckpipe.start_worker(group_name TEXT DEFAULT NULL) RETURNS void
AS 'MODULE_PATHNAME', '@FUNCTION_NAME@'
LANGUAGE C;
REVOKE ALL ON FUNCTION duckpipe.start_worker(TEXT) FROM PUBLIC;
")]
fn start_worker(group_name: Option<&str>) {
    match group_name {
        Some(name) => {
            // Check group mode — daemon groups should not use bgworker
            let group_mode: Option<duckpipe_core::types::GroupMode> = Spi::connect(|client| {
                let args = unsafe { [DatumWithOid::new(name, PgBuiltInOids::TEXTOID.value())] };
                let result = client
                    .select(
                        "SELECT mode FROM duckpipe.sync_groups WHERE name = $1",
                        Some(1),
                        &args,
                    )
                    .unwrap();
                for r in result {
                    let mode_str: String = r.get::<String>(1).unwrap()?;
                    return Some(
                        mode_str
                            .parse()
                            .unwrap_or(duckpipe_core::types::GroupMode::BgWorker),
                    );
                }
                None
            });

            if group_mode.is_none() {
                ereport!(
                    ERROR,
                    PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT,
                    format!("Sync group '{}' not found", name)
                );
            }

            if group_mode == Some(duckpipe_core::types::GroupMode::Daemon) {
                warning!(
                    "Group '{}' has mode 'daemon' — use the duckpipe daemon binary instead of start_worker()",
                    name
                );
                return;
            }

            // Start a specific group's worker
            if is_group_worker_running(name) {
                notice!("Background worker already running for group '{}'", name);
                return;
            }
            if !launch_worker(name) {
                ereport!(
                    ERROR,
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    format!("Failed to start background worker for group '{}'", name)
                );
            }
            notice!("Background worker started for group '{}'", name);
        }
        None => {
            // Start workers for all enabled bgworker-mode groups
            let groups: Vec<String> = Spi::connect(|client| {
                let mut names = Vec::new();
                let result = client.select(
                    "SELECT g.name FROM duckpipe.sync_groups g \
                     WHERE g.enabled = true AND g.mode = 'bgworker' \
                     ORDER BY g.name",
                    None,
                    &[],
                );
                if let Ok(tuptable) = result {
                    for row in tuptable {
                        if let Some(name) = row.get::<String>(1).unwrap() {
                            names.push(name);
                        }
                    }
                }
                names
            });

            let mut started = 0;
            for name in &groups {
                if !is_group_worker_running(name) {
                    if launch_worker(name) {
                        started += 1;
                    }
                }
            }
            notice!("Started {} background worker(s)", started);
        }
    }
}

#[pg_extern(sql = "
CREATE FUNCTION duckpipe.stop_worker(group_name TEXT DEFAULT NULL) RETURNS void
AS 'MODULE_PATHNAME', '@FUNCTION_NAME@'
LANGUAGE C;
REVOKE ALL ON FUNCTION duckpipe.stop_worker(TEXT) FROM PUBLIC;
")]
fn stop_worker(group_name: Option<&str>) {
    // Wake the worker(s) from any long poll_interval sleep so they can check
    // ShutdownRequestPending promptly after receiving SIGTERM.
    Spi::connect_mut(|client| {
        match group_name {
            Some(name) => {
                let channel = listen::wakeup_channel(name);
                let _ = client.update(&format!("NOTIFY {}", channel), None, &[]);
            }
            None => {
                // Wake all group workers — query sync_groups for all names
                let result = client.select("SELECT name FROM duckpipe.sync_groups", None, &[]);
                if let Ok(rows) = result {
                    for r in rows {
                        if let Some(name) = r.get::<String>(1).unwrap() {
                            let channel = listen::wakeup_channel(&name);
                            let _ = client.update(&format!("NOTIFY {}", channel), None, &[]);
                        }
                    }
                }
            }
        }
    });

    let (sql, check_fn): (String, Box<dyn Fn() -> bool>) = match group_name {
        Some(name) => {
            let backend_type = format!("pg_duckpipe:{}", name);
            let n = name.to_string();
            (
                format!(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE backend_type = {}",
                    quote_literal(&backend_type)
                ),
                Box::new(move || is_group_worker_running(&n)),
            )
        }
        None => (
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE backend_type LIKE 'pg_duckpipe:%'"
                .to_string(),
            Box::new(|| is_any_worker_running()),
        ),
    };

    let terminated: i64 = Spi::connect_mut(|client| {
        let result = client.update(&sql, None, &[]);
        match result {
            Ok(status) => status.len() as i64,
            Err(_) => 0,
        }
    });

    if terminated > 0 {
        // Wait for the worker(s) to actually exit
        for _ in 0..50 {
            if !check_fn() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        notice!("Terminated {} worker(s)", terminated);
    } else {
        notice!("No workers found to terminate");
    }
}

/// Terminate a specific group's worker (helper for drop_group).
fn terminate_group_worker(group_name: &str) {
    let backend_type = format!("pg_duckpipe:{}", group_name);
    let sql = format!(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE backend_type = {}",
        quote_literal(&backend_type)
    );
    let terminated: i64 = Spi::connect_mut(|client| match client.update(&sql, None, &[]) {
        Ok(status) => status.len() as i64,
        Err(_) => 0,
    });
    if terminated > 0 {
        for _ in 0..50 {
            if !is_group_worker_running(group_name) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }
}

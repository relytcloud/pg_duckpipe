use pgrx::datum::{DatumWithOid, TimestampWithTimeZone};
use pgrx::prelude::*;

use crate::DATA_INLINING_ROW_LIMIT;

/// Execute a SQL statement via raw SPI without marking the transaction as mutable.
/// This is needed for pg_create_logical_replication_slot() which cannot run in a
/// transaction that already has a TransactionId assigned.
unsafe fn spi_exec_raw(sql: &str) -> i32 {
    let c_sql = std::ffi::CString::new(sql).unwrap();
    pg_sys::SPI_execute(c_sql.as_ptr(), false, 0)
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

/// Quote a literal for SQL
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

/// Check if a pg_duckpipe worker is running for the current database
fn is_worker_running() -> bool {
    let result =
        Spi::get_one::<i64>("SELECT 1 FROM pg_stat_activity WHERE backend_type = 'pg_duckpipe'");
    matches!(result, Ok(Some(_)))
}

/// Register and start a dynamic background worker
fn launch_worker() -> bool {
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

        // Set worker name
        let name = format!("pg_duckpipe worker [{}]", dbname_str);
        let name_c = std::ffi::CString::new(name.as_str()).unwrap();
        let name_bytes = name_c.as_bytes_with_nul();
        let copy_len = name_bytes.len().min(worker.bgw_name.len());
        for (i, &b) in name_bytes[..copy_len].iter().enumerate() {
            worker.bgw_name[i] = b as std::ffi::c_char;
        }

        // Set worker type
        let type_name = std::ffi::CString::new("pg_duckpipe").unwrap();
        let type_bytes = type_name.as_bytes_with_nul();
        let copy_len = type_bytes.len().min(worker.bgw_type.len());
        for (i, &b) in type_bytes[..copy_len].iter().enumerate() {
            worker.bgw_type[i] = b as std::ffi::c_char;
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
    slot_name TEXT DEFAULT NULL
) RETURNS TEXT
AS 'MODULE_PATHNAME', '@FUNCTION_NAME@'
LANGUAGE C;
REVOKE ALL ON FUNCTION duckpipe.create_group(TEXT, TEXT, TEXT) FROM PUBLIC;
")]
fn create_group(name: &str, publication: Option<&str>, slot_name: Option<&str>) -> String {
    let pub_name = publication
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("duckpipe_pub_{}", name));
    let slot = slot_name
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("duckpipe_slot_{}", name));

    Spi::connect_mut(|client| {
        // 1. Create replication slot (MUST be first — before any writes assign a txid)
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

        // 2. Create publication
        let sql = format!("CREATE PUBLICATION {}", quote_ident(&pub_name));
        client.update(&sql, None, &[]).unwrap_or_else(|e| {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                format!("Failed to create publication {}: {}", pub_name, e)
            );
        });

        // 3. Insert into sync_groups
        let args = unsafe {
            [
                DatumWithOid::new(name, PgBuiltInOids::TEXTOID.value()),
                DatumWithOid::new(pub_name.clone(), PgBuiltInOids::TEXTOID.value()),
                DatumWithOid::new(slot.as_str(), PgBuiltInOids::TEXTOID.value()),
            ]
        };
        client
            .update(
                "INSERT INTO duckpipe.sync_groups (name, publication, slot_name) VALUES ($1, $2, $3)",
                None,
                &args,
            )
            .unwrap_or_else(|e| {
                ereport!(
                    ERROR,
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    format!("Failed to insert into sync_groups: {}", e)
                );
            });
    });

    name.to_string()
}

#[pg_extern(sql = "
CREATE FUNCTION duckpipe.drop_group(
    name TEXT,
    drop_slot BOOLEAN DEFAULT true
) RETURNS void
AS 'MODULE_PATHNAME', '@FUNCTION_NAME@'
LANGUAGE C STRICT;
REVOKE ALL ON FUNCTION duckpipe.drop_group(TEXT, BOOLEAN) FROM PUBLIC;
")]
fn drop_group(name: &str, drop_slot: bool) {
    Spi::connect_mut(|client| {
        // Get publication and slot_name
        let args = unsafe { [DatumWithOid::new(name, PgBuiltInOids::TEXTOID.value())] };
        let row = client
            .select(
                "SELECT publication, slot_name FROM duckpipe.sync_groups WHERE name = $1",
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

        let mut pub_name = String::new();
        let mut slot = String::new();
        let mut found = false;

        for r in row {
            pub_name = r.get::<String>(1).unwrap().unwrap();
            slot = r.get::<String>(2).unwrap().unwrap();
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

        // Drop slot if requested
        if drop_slot {
            let sql = format!("SELECT pg_drop_replication_slot({})", quote_literal(&slot));
            let _ = client.update(&sql, None, &[]);
        }

        // Drop publication
        let sql = format!("DROP PUBLICATION IF EXISTS {}", quote_ident(&pub_name));
        let _ = client.update(&sql, None, &[]);

        // Delete from sync_groups
        let args = unsafe { [DatumWithOid::new(name, PgBuiltInOids::TEXTOID.value())] };
        client
            .update(
                "DELETE FROM duckpipe.sync_groups WHERE name = $1",
                None,
                &args,
            )
            .unwrap();
    });
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

    Spi::connect_mut(|client| {
        // 1. Get publication and slot name for this group
        let args = unsafe { [DatumWithOid::new(group, PgBuiltInOids::TEXTOID.value())] };
        let result = client
            .select(
                "SELECT publication, slot_name FROM duckpipe.sync_groups WHERE name = $1",
                Some(1),
                &args,
            )
            .unwrap();

        let mut publication = String::new();
        let mut slot_name_val = String::new();
        let mut found = false;
        for r in result {
            publication = r.get::<String>(1).unwrap().unwrap();
            slot_name_val = r.get::<String>(2).unwrap().unwrap();
            found = true;
            break;
        }

        if !found {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT,
                format!("Sync group '{}' not found", group)
            );
        }

        // 2. Check if publication exists
        let pub_exists = {
            let args = unsafe {
                [DatumWithOid::new(
                    publication.as_str(),
                    PgBuiltInOids::TEXTOID.value(),
                )]
            };
            let result = client.select(
                "SELECT 1 FROM pg_publication WHERE pubname = $1",
                Some(1),
                &args,
            );
            matches!(result, Ok(t) if t.len() > 0)
        };

        // 3. Create or alter publication
        if !pub_exists {
            // Create replication slot FIRST (before any writes assign a txid)
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

            // Create publication with this table
            let sql = format!(
                "CREATE PUBLICATION {} FOR TABLE {}.{}",
                quote_ident(&publication),
                quote_ident(&schema),
                quote_ident(&table)
            );
            client.update(&sql, None, &[]).unwrap();
        } else {
            // Add table to existing publication
            let sql = format!(
                "ALTER PUBLICATION {} ADD TABLE {}.{}",
                quote_ident(&publication),
                quote_ident(&schema),
                quote_ident(&table)
            );
            client.update(&sql, None, &[]).unwrap();
        }

        // 4. Auto-create target schema if needed
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

        // 5. Auto-create target table
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {}.{} (LIKE {}.{}) USING ducklake",
            quote_ident(&t_schema),
            quote_ident(&t_table),
            quote_ident(&schema),
            quote_ident(&table)
        );
        client.update(&sql, None, &[]).unwrap();

        // 6. Set data inlining if configured
        let inlining_limit = DATA_INLINING_ROW_LIMIT.get();
        if inlining_limit > 0 {
            let sql = format!(
                "CALL ducklake.set_option('data_inlining_row_limit', {})",
                inlining_limit
            );
            let _ = client.update(&sql, None, &[]);
        }

        // 7. Enforce REPLICA IDENTITY FULL on the source table so pgoutput
        //    always includes every column value in UPDATE WAL records.
        {
            let sql = format!(
                "ALTER TABLE {}.{} REPLICA IDENTITY FULL",
                quote_ident(&schema),
                quote_ident(&table)
            );
            client.update(&sql, None, &[]).unwrap();
        }

        // 8. Insert table mapping (with source OID for rename-safe matching)
        let initial_state = if copy_data { "SNAPSHOT" } else { "STREAMING" };
        let args = unsafe {
            [
                DatumWithOid::new(schema.as_str(), PgBuiltInOids::TEXTOID.value()),
                DatumWithOid::new(table.as_str(), PgBuiltInOids::TEXTOID.value()),
                DatumWithOid::new(t_schema.as_str(), PgBuiltInOids::TEXTOID.value()),
                DatumWithOid::new(t_table.as_str(), PgBuiltInOids::TEXTOID.value()),
                DatumWithOid::new(initial_state, PgBuiltInOids::TEXTOID.value()),
                DatumWithOid::new(group, PgBuiltInOids::TEXTOID.value()),
            ]
        };
        client
            .update(
                "INSERT INTO duckpipe.table_mappings (group_id, source_schema, source_table, \
                 target_schema, target_table, state, source_oid) \
                 SELECT sg.id, $1, $2, $3, $4, $5, c.oid::bigint \
                 FROM duckpipe.sync_groups sg, \
                      pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE sg.name = $6 AND n.nspname = $1 AND c.relname = $2",
                None,
                &args,
            )
            .unwrap();

        // 9. Auto-start background worker if not running
        if !is_worker_running() {
            launch_worker();
        }
    });
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

    Spi::connect_mut(|client| {
        // Get publication and target info
        let args = unsafe {
            [
                DatumWithOid::new(schema.as_str(), PgBuiltInOids::TEXTOID.value()),
                DatumWithOid::new(table.as_str(), PgBuiltInOids::TEXTOID.value()),
            ]
        };
        let result = client
            .select(
                "SELECT g.publication, m.target_schema, m.target_table \
                 FROM duckpipe.table_mappings m \
                 JOIN duckpipe.sync_groups g ON m.group_id = g.id \
                 WHERE m.source_schema = $1 AND m.source_table = $2",
                Some(1),
                &args,
            )
            .unwrap();

        let mut publication = None;
        let mut t_schema = None;
        let mut t_table = None;

        for r in result {
            publication = Some(r.get::<String>(1).unwrap().unwrap());
            t_schema = Some(r.get::<String>(2).unwrap().unwrap());
            t_table = Some(r.get::<String>(3).unwrap().unwrap());
            break;
        }

        // Drop from publication if exists
        if let Some(ref pub_name) = publication {
            let sql = format!(
                "SELECT 1 FROM pg_publication_tables WHERE pubname = {} AND tablename = {}",
                quote_literal(pub_name),
                quote_literal(&table),
            );
            let result = client.select(&sql as &str, Some(1), &[]);
            if matches!(result, Ok(t) if t.len() > 0) {
                let sql = format!(
                    "ALTER PUBLICATION {} DROP TABLE {}.{}",
                    quote_ident(pub_name),
                    quote_ident(&schema),
                    quote_ident(&table)
                );
                let _ = client.update(&sql, None, &[]);
            }
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
        // Get target table info
        let args = unsafe {
            [
                DatumWithOid::new(schema.as_str(), PgBuiltInOids::TEXTOID.value()),
                DatumWithOid::new(table.as_str(), PgBuiltInOids::TEXTOID.value()),
            ]
        };
        let result = client
            .select(
                "SELECT target_schema, target_table FROM duckpipe.table_mappings \
                 WHERE source_schema = $1 AND source_table = $2",
                Some(1),
                &args,
            )
            .unwrap();

        let mut t_schema = String::new();
        let mut t_table = String::new();
        let mut found = false;

        for r in result {
            t_schema = r.get::<String>(1).unwrap().unwrap();
            t_table = r.get::<String>(2).unwrap().unwrap();
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
                 consecutive_failures = 0, retry_at = NULL \
                 WHERE source_schema = $1 AND source_table = $2",
                None,
                &args,
            )
            .unwrap();
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
    table_count INTEGER,
    last_sync TIMESTAMPTZ
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
        name!(table_count, i32),
        name!(last_sync, Option<TimestampWithTimeZone>),
    ),
> {
    let mut rows = Vec::new();

    Spi::connect(|client| {
        let result = client.select(
            "SELECT g.name, g.publication, g.slot_name, g.enabled, \
             (SELECT count(*) FROM duckpipe.table_mappings m WHERE m.group_id = g.id)::int4 as table_count, \
             g.last_sync_at \
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
                let table_count: i32 = row.get(5).unwrap().unwrap();
                let last_sync: Option<TimestampWithTimeZone> = row.get(6).unwrap();

                rows.push((
                    name,
                    publication,
                    slot_name,
                    enabled,
                    table_count,
                    last_sync,
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
    applied_lsn TEXT
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
    ),
> {
    let mut rows = Vec::new();

    Spi::connect(|client| {
        let result = client.select(
            "SELECT g.name as sync_group, \
             m.source_schema || '.' || m.source_table as source_table, \
             m.target_schema || '.' || m.target_table as target_table, \
             m.state, m.enabled, m.rows_synced, m.queued_changes, m.last_sync_at, \
             m.error_message, m.consecutive_failures, m.retry_at, m.applied_lsn::text \
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
                let queued_changes: i64 = row.get::<i64>(7).unwrap().unwrap_or(0);
                let last_sync: Option<TimestampWithTimeZone> = row.get(8).unwrap();
                let error_message: Option<String> = row.get(9).unwrap();
                let consecutive_failures: i32 = row.get::<i32>(10).unwrap().unwrap_or(0);
                let retry_at: Option<TimestampWithTimeZone> = row.get(11).unwrap();
                let applied_lsn: Option<String> = row.get(12).unwrap();

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
                ));
            }
        }
    });

    TableIterator::new(rows)
}

#[pg_extern(sql = "
CREATE FUNCTION duckpipe.worker_status() RETURNS TABLE(
    total_queued_changes BIGINT,
    is_backpressured BOOLEAN,
    updated_at TIMESTAMPTZ
)
AS 'MODULE_PATHNAME', '@FUNCTION_NAME@'
LANGUAGE C STRICT;
")]
fn worker_status() -> TableIterator<
    'static,
    (
        name!(total_queued_changes, i64),
        name!(is_backpressured, bool),
        name!(updated_at, Option<TimestampWithTimeZone>),
    ),
> {
    let mut rows = Vec::new();

    Spi::connect(|client| {
        let result = client.select(
            "SELECT total_queued_changes, is_backpressured, updated_at \
             FROM duckpipe.worker_state WHERE id = 1",
            None,
            &[],
        );

        if let Ok(tuptable) = result {
            for row in tuptable {
                let total_queued_changes: i64 = row.get::<i64>(1).unwrap().unwrap_or(0);
                let is_backpressured: bool = row.get::<bool>(2).unwrap().unwrap_or(false);
                let updated_at: Option<TimestampWithTimeZone> = row.get(3).unwrap();

                rows.push((total_queued_changes, is_backpressured, updated_at));
            }
        }
    });

    TableIterator::new(rows)
}

#[pg_extern(sql = "
CREATE FUNCTION duckpipe.start_worker() RETURNS void
AS 'MODULE_PATHNAME', '@FUNCTION_NAME@'
LANGUAGE C;
REVOKE ALL ON FUNCTION duckpipe.start_worker() FROM PUBLIC;
")]
fn start_worker() {
    let dbname: String = Spi::get_one("SELECT current_database()::text")
        .unwrap()
        .unwrap_or_else(|| "unknown".to_string());

    if is_worker_running() {
        notice!("Background worker started for database {}", dbname);
        return;
    }

    if !launch_worker() {
        ereport!(
            ERROR,
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            format!("Failed to start background worker for database {}", dbname)
        );
    }

    notice!("Background worker started for database {}", dbname);
}

#[pg_extern(sql = "
CREATE FUNCTION duckpipe.stop_worker() RETURNS void
AS 'MODULE_PATHNAME', '@FUNCTION_NAME@'
LANGUAGE C;
REVOKE ALL ON FUNCTION duckpipe.stop_worker() FROM PUBLIC;
")]
fn stop_worker() {
    let terminated: i64 = Spi::connect_mut(|client| {
        let result = client.update(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE backend_type = 'pg_duckpipe'",
            None,
            &[],
        );
        match result {
            Ok(status) => status.len() as i64,
            Err(_) => 0,
        }
    });

    if terminated > 0 {
        // Wait for the worker to actually exit so subsequent start_worker() calls succeed
        for _ in 0..50 {
            if !is_worker_running() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        notice!("Terminated {} worker(s)", terminated);
    } else {
        notice!("No workers found to terminate");
    }
}

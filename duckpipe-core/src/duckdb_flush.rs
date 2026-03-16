//! DuckDB-based flush path: buffer changes in embedded DuckDB, compact, apply to DuckLake.
//!
//! Each target table gets a persistent `FlushWorker` holding a long-lived `duckdb::Connection`.
//! The expensive one-time setup (INSTALL+LOAD+ATTACH) happens once at creation.
//!
//! The buffer lifecycle is split into three phases:
//! 1. `append_to_buffer()` — lazy-creates the buffer table, loads changes via DuckDB Appender
//! 2. `flush_buffer()` — compacts (dedup by PK), applies DELETE+INSERT to DuckLake, drops buffer
//! 3. `clear_buffer()` — drops the buffer table without flushing (shutdown/error)

use std::time::Instant;

use duckdb::{Config, Connection};

use crate::types::{fixed_bytes_for_oid, Change, ChangeType, Value};

/// Push a Value into a row Vec for the DuckDB Appender.
/// Text values are auto-cast by DuckDB to the buffer table's declared column type.
fn push_value_to_row(row: &mut Vec<Box<dyn duckdb::ToSql>>, val: &Value) {
    match val {
        Value::Null => row.push(Box::new(Option::<String>::None)),
        Value::Bool(b) => row.push(Box::new(*b)),
        Value::Int16(i) => row.push(Box::new(*i as i32)),
        Value::Int32(i) => row.push(Box::new(*i)),
        Value::Int64(i) => row.push(Box::new(*i)),
        Value::Float32(f) => row.push(Box::new(*f)),
        Value::Float64(f) => row.push(Box::new(*f)),
        Value::Text(s) => row.push(Box::new(s.clone())),
    }
}

/// Discovered DuckLake table metadata from information_schema.
pub struct LakeTableInfo {
    /// Actual schema name inside DuckLake (may differ from PG schema after ATTACH)
    lake_schema: String,
    /// Column types in ordinal order (DuckDB type strings: INTEGER, VARCHAR, etc.)
    column_types: Vec<String>,
}

/// Query the DuckLake catalog via information_schema to discover the actual schema
/// name and column types for a target table.
///
/// This solves two problems:
/// 1. Schema name may differ after ATTACH (e.g., PG "public" might map differently)
/// 2. Column types are needed so the buffer table uses real types, avoiding
///    VARCHAR/BOOLEAN type mismatches in the MERGE CASE expression.
fn discover_lake_table_info(
    db: &Connection,
    target_schema: &str,
    target_table: &str,
    expected_attnames: &[String],
) -> Result<LakeTableInfo, String> {
    // Find the schema by querying DuckDB's information_schema filtered to the 'lake' catalog.
    // DuckLake-attached databases don't have their own information_schema; instead,
    // their tables appear in the global information_schema with table_catalog = 'lake'.
    let schema_sql = format!(
        "SELECT table_schema FROM information_schema.tables \
         WHERE table_catalog = 'lake' AND table_name = '{}'",
        target_table.replace('\'', "''")
    );
    let mut schema_stmt = db
        .prepare(&schema_sql)
        .map_err(|e| format!("discover schema prepare: {}", e))?;
    let schema_rows: Vec<String> = schema_stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|e| format!("discover schema query: {}", e))?
        .filter_map(|r| r.ok())
        .collect();

    if schema_rows.is_empty() {
        return Err(format!(
            "table '{}' not found in DuckLake catalog",
            target_table
        ));
    }

    // Prefer exact match on target_schema, fall back to first result
    let lake_schema = schema_rows
        .iter()
        .find(|s| s.as_str() == target_schema)
        .cloned()
        .unwrap_or_else(|| schema_rows[0].clone());

    // Query column types ordered by ordinal_position
    let cols_sql = format!(
        "SELECT column_name, data_type FROM information_schema.columns \
         WHERE table_catalog = 'lake' AND table_schema = '{}' AND table_name = '{}' \
         ORDER BY ordinal_position",
        lake_schema.replace('\'', "''"),
        target_table.replace('\'', "''")
    );
    let mut cols_stmt = db
        .prepare(&cols_sql)
        .map_err(|e| format!("discover columns prepare: {}", e))?;
    let col_rows: Vec<(String, String)> = cols_stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|e| format!("discover columns query: {}", e))?
        .filter_map(|r| r.ok())
        .collect();

    if col_rows.is_empty() {
        return Err(format!(
            "no columns found for {}.{} in DuckLake catalog",
            lake_schema, target_table
        ));
    }

    // Build column_types aligned to expected_attnames order.
    // The DuckLake catalog columns should match pgoutput attnames.
    let lake_col_map: std::collections::HashMap<String, String> = col_rows
        .into_iter()
        .map(|(name, dtype)| (name.to_lowercase(), dtype))
        .collect();

    let mut column_types = Vec::with_capacity(expected_attnames.len());
    for name in expected_attnames {
        let dtype = lake_col_map.get(&name.to_lowercase()).ok_or_else(|| {
            format!(
                "column '{}' not found in DuckLake table {}.{}",
                name, lake_schema, target_table
            )
        })?;
        column_types.push(dtype.clone());
    }

    Ok(LakeTableInfo {
        lake_schema,
        column_types,
    })
}

/// Parse a human-readable size string (e.g. "1.2MB", "512.0KB", "0 bytes") to bytes.
/// DuckDB formats sizes as "0 bytes", "123 bytes", "1.2KB", "5.0MB", etc.
/// Returns 0 on parse failure.
fn parse_memory_usage(s: &str) -> i64 {
    let s = s.trim();
    // Find where the numeric part ends and the unit begins
    let unit_start = s.find(|c: char| c.is_ascii_alphabetic()).unwrap_or(s.len());
    let (num_str, unit) = s.split_at(unit_start);
    let num: f64 = match num_str.trim().parse() {
        Ok(n) if n >= 0.0 => n,
        _ => return 0,
    };
    let multiplier: f64 = match unit.trim().to_uppercase().as_str() {
        "" | "B" | "BYTES" => 1.0,
        "KB" | "KIB" => 1024.0,
        "MB" | "MIB" => 1024.0 * 1024.0,
        "GB" | "GIB" => 1024.0 * 1024.0 * 1024.0,
        "TB" | "TIB" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        _ => return 0,
    };
    (num * multiplier) as i64
}

/// Query DuckDB's buffer manager memory usage via pragma_database_size().
/// Returns 0 if the query fails or no rows match.
/// Sums memory_usage across all attached databases (memory + lake).
fn query_memory_usage(db: &Connection) -> i64 {
    let mut stmt = match db.prepare("SELECT memory_usage FROM pragma_database_size()") {
        Ok(s) => s,
        Err(_) => return 0,
    };
    let rows: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map(|iter| iter.filter_map(|r| r.ok()).collect())
        .unwrap_or_default();
    rows.iter().map(|s| parse_memory_usage(s)).sum()
}

/// Parse a "schema.table" target_key into (schema, table).
fn parse_target_key(target_key: &str) -> Result<(&str, &str), String> {
    target_key
        .split_once('.')
        .ok_or_else(|| format!("invalid target_key: {}", target_key))
}

/// Flush worker for a single target table.
///
/// Holds a DuckDB connection with INSTALL+LOAD+ATTACH done at creation.
/// The connection is recreated after each flush cycle to release memory back to the OS.
/// The buffer table is lazily created and persists across append calls within a flush window,
/// allowing DuckDB to spill to disk for large batches.
pub struct FlushWorker {
    db: Connection,
    lake_info: Option<LakeTableInfo>,
    /// True while a `buffer` table exists in the DuckDB instance.
    buffer_exists: bool,
    /// Tracked during `append_to_buffer()`, reset when buffer is cleared/flushed.
    has_non_inserts: bool,
    /// Cached from `ensure_buffer()` — the table name portion of target_key.
    target_table: Option<String>,
    /// Cached from `ensure_buffer()` — quoted PK column identifiers.
    cached_pk_cols: Option<Vec<String>>,
    /// Cached from `ensure_buffer()` — quoted all column identifiers.
    cached_all_cols: Option<Vec<String>>,
    /// True while this worker may encounter duplicate PKs between the lake and an
    /// incoming pure-insert batch (e.g., during WAL replay after initial snapshot
    /// or after a resync).  The DELETE step is always run while this flag is set.
    ///
    /// Cleared to `false` after the first pure-insert batch (no UPDATEs/DELETEs)
    /// that returns zero rows deleted from the lake — that outcome proves no
    /// conflicting PKs exist and the WAL-replay window is over.  All subsequent
    /// pure-insert batches can safely skip the O(lake_size) Parquet DELETE scan.
    ///
    /// Reset to `true` when the worker is recreated (after error or resync) so
    /// the invariant is always established conservatively.
    may_have_conflicts: bool,
    /// DuckDB memory limit string for the flush phase (e.g. "512MB").
    /// Stored so `flush_buffer()` can raise the limit before compaction.
    flush_memory_limit: String,
}

impl FlushWorker {
    /// Create a new FlushWorker with a fresh DuckDB connection.
    /// Performs setup: open in-memory DB, INSTALL+LOAD ducklake, ATTACH to PG.
    /// The initial memory limit is set to `buffer_memory_mb` (low, for 100+ concurrent tables).
    /// During `flush_buffer()`, it's raised to `flush_memory_mb` (high, for compaction + DuckLake writes).
    /// The worker is dropped after each flush cycle to release memory back to the OS.
    pub fn new(
        pg_connstr: &str,
        ducklake_schema: &str,
        buffer_memory_mb: i32,
        flush_memory_mb: i32,
    ) -> Result<Self, String> {
        let config = Config::default()
            .allow_unsigned_extensions()
            .map_err(|e| format!("duckdb config: {}", e))?;
        let db = Connection::open_in_memory_with_flags(config)
            .map_err(|e| format!("duckdb open: {}", e))?;

        db.execute_batch("INSTALL ducklake; LOAD ducklake;")
            .map_err(|e| format!("duckdb install ducklake: {}", e))?;

        let attach_sql = format!(
            "ATTACH 'ducklake:postgres:{}' AS lake (METADATA_SCHEMA '{}')",
            pg_connstr.replace('\'', "''"),
            ducklake_schema.replace('\'', "''")
        );
        db.execute_batch(&attach_sql)
            .map_err(|e| format!("duckdb attach: {}", e))?;

        db.execute_batch(
            "SET ducklake_retry_wait_ms = 100; \
             SET ducklake_retry_backoff = 2.0; \
             SET ducklake_max_retry_count = 10;",
        )
        .map_err(|e| format!("duckdb set retry: {}", e))?;

        let buffer_memory_limit = format!("{}MB", buffer_memory_mb);
        let flush_memory_limit = format!("{}MB", flush_memory_mb);

        // Set initial memory limit to buffer phase (low limit for spilling)
        db.execute_batch(&format!("SET memory_limit = '{}'", buffer_memory_limit))
            .map_err(|e| format!("duckdb set memory_limit: {}", e))?;

        Ok(FlushWorker {
            db,
            lake_info: None,
            buffer_exists: false,
            has_non_inserts: false,
            target_table: None,
            cached_pk_cols: None,
            cached_all_cols: None,
            may_have_conflicts: true,
            flush_memory_limit,
        })
    }

    /// Lazy-create the buffer table if it doesn't exist yet.
    ///
    /// Discovers `lake_info` if not cached, then creates:
    /// `CREATE TABLE buffer (_seq INTEGER, _op_type INTEGER, col1 TYPE1, ...)`
    ///
    /// Also caches `target_table`, `pk_cols`, and `all_cols` for use by `flush_buffer()`.
    /// No-op if buffer already exists.
    fn ensure_buffer(
        &mut self,
        target_key: &str,
        attnames: &[String],
        key_attrs: &[usize],
    ) -> Result<(), String> {
        if self.buffer_exists {
            return Ok(());
        }

        let (target_schema, target_table) = parse_target_key(target_key)?;

        // Discover lake table info on first call or when cache is empty
        if self.lake_info.is_none() {
            self.lake_info = Some(discover_lake_table_info(
                &self.db,
                target_schema,
                target_table,
                attnames,
            )?);
        }
        let lake_info = self.lake_info.as_ref().unwrap();

        // Cache parsed/formatted values for flush_buffer()
        self.target_table = Some(target_table.to_string());
        self.cached_pk_cols = Some(
            key_attrs
                .iter()
                .map(|&i| format!("\"{}\"", attnames[i].replace('"', "\"\"")))
                .collect(),
        );
        self.cached_all_cols = Some(
            attnames
                .iter()
                .map(|n| format!("\"{}\"", n.replace('"', "\"\"")))
                .collect(),
        );

        // Build buffer table schema
        let mut buf_cols = Vec::new();
        buf_cols.push("_seq INTEGER".to_string());
        buf_cols.push("_op_type INTEGER".to_string());
        for (i, name) in attnames.iter().enumerate() {
            buf_cols.push(format!(
                "\"{}\" {}",
                name.replace('"', "\"\""),
                lake_info.column_types[i]
            ));
        }

        let create_buf = format!("CREATE TABLE buffer ({})", buf_cols.join(", "));
        self.db
            .execute_batch(&create_buf)
            .map_err(|e| format!("duckdb create buffer: {}", e))?;

        self.buffer_exists = true;
        self.has_non_inserts = false;
        Ok(())
    }

    /// Append changes to the DuckDB buffer table.
    ///
    /// Calls `ensure_buffer` first, then opens a DuckDB Appender, loads changes
    /// with incrementing `_seq` starting from `seq_start`. Returns the next seq value.
    ///
    /// Also checks REPLICA IDENTITY FULL (col_unchanged).
    pub fn append_to_buffer(
        &mut self,
        changes: &[Change],
        target_key: &str,
        attnames: &[String],
        key_attrs: &[usize],
        atttypes: &[u32],
        seq_start: i32,
    ) -> Result<(i32, i64), String> {
        if changes.is_empty() {
            return Ok((seq_start, 0));
        }

        self.ensure_buffer(target_key, attnames, key_attrs)?;

        // Track non-inserts (short-circuits if already true)
        if !self.has_non_inserts {
            self.has_non_inserts = changes
                .iter()
                .any(|c| !matches!(c.change_type, ChangeType::Insert));
        }

        // Enforce REPLICA IDENTITY FULL
        if changes.iter().any(|c| c.col_unchanged.iter().any(|&u| u)) {
            return Err(
                "TOAST unchanged column detected in WAL — source table must have \
                 REPLICA IDENTITY FULL. Run: ALTER TABLE <name> REPLICA IDENTITY FULL"
                    .to_string(),
            );
        }

        let ncols = attnames.len();
        let mut seq = seq_start;
        let fixed_row_bytes: usize = atttypes.iter().map(|&oid| fixed_bytes_for_oid(oid)).sum();
        let mut var_total: usize = 0;

        {
            let mut appender = self
                .db
                .appender("buffer")
                .map_err(|e| format!("duckdb appender: {}", e))?;

            for change in changes {
                seq += 1;
                let op_type: i32 = match change.change_type {
                    ChangeType::Insert => 0,
                    ChangeType::Update => 1,
                    ChangeType::Delete => 2,
                };

                let mut row: Vec<Box<dyn duckdb::ToSql>> = Vec::with_capacity(2 + ncols);
                row.push(Box::new(seq));
                row.push(Box::new(op_type));

                match change.change_type {
                    ChangeType::Insert | ChangeType::Update => {
                        for i in 0..ncols {
                            let val = change.col_values.get(i).unwrap_or(&Value::Null);
                            var_total += val.var_bytes();
                            push_value_to_row(&mut row, val);
                        }
                    }
                    ChangeType::Delete => {
                        for i in 0..ncols {
                            if let Some(ki) = key_attrs.iter().position(|&k| k == i) {
                                let val = change.key_values.get(ki).unwrap_or(&Value::Null);
                                var_total += val.var_bytes();
                                push_value_to_row(&mut row, val);
                            } else {
                                row.push(Box::new(Option::<String>::None));
                            }
                        }
                    }
                }

                let refs: Vec<&dyn duckdb::ToSql> = row.iter().map(|b| b.as_ref()).collect();
                appender
                    .append_row(refs.as_slice())
                    .map_err(|e| format!("duckdb append row at seq {}: {}", seq, e))?;
            }

            appender
                .flush()
                .map_err(|e| format!("duckdb appender flush: {}", e))?;
        }

        let batch_bytes = (changes.len() * fixed_row_bytes + var_total) as i64;
        Ok((seq, batch_bytes))
    }

    /// Compact the buffer (dedup by PK), apply DELETE+INSERT to DuckLake in a
    /// transaction, then drop the buffer table.
    ///
    /// Returns the flush result with timing and memory metrics.
    pub fn flush_buffer(
        &mut self,
        target_key: &str,
        mapping_id: i32,
        applied_count: i64,
        buffered_bytes: i64,
    ) -> Result<DuckDbFlushResult, String> {
        if !self.buffer_exists {
            return Ok(DuckDbFlushResult {
                target_key: target_key.to_string(),
                mapping_id,
                applied_count: 0,
                memory_bytes: 0,
                flush_duration_ms: 0,
                buffered_bytes: 0,
            });
        }

        let flush_start = Instant::now();

        let target_table = self
            .target_table
            .as_ref()
            .ok_or("target_table not cached — ensure_buffer should have been called")?;
        let lake_info = self.lake_info.as_ref().ok_or_else(|| {
            "lake_info not available — ensure_buffer should have been called".to_string()
        })?;
        let pk_cols = self
            .cached_pk_cols
            .as_ref()
            .ok_or("pk_cols not cached — ensure_buffer should have been called")?;
        let all_cols = self
            .cached_all_cols
            .as_ref()
            .ok_or("all_cols not cached — ensure_buffer should have been called")?;
        let has_non_inserts = self.has_non_inserts;

        // Raise memory limit for flush phase (compaction + DuckLake writes)
        self.db
            .execute_batch(&format!("SET memory_limit = '{}'", self.flush_memory_limit))
            .map_err(|e| format!("duckdb raise memory_limit: {}", e))?;

        // Step 1: Compact — deduplicate by PK, keep last operation (highest seq).
        let t_phase = Instant::now();
        let compact_sql = format!(
            "CREATE TEMP TABLE compacted AS \
             SELECT * EXCLUDE (_rn) FROM ( \
                 SELECT *, ROW_NUMBER() OVER (PARTITION BY {} ORDER BY _seq DESC) AS _rn \
                 FROM buffer \
             ) sub WHERE _rn = 1",
            pk_cols.join(", ")
        );
        self.db
            .execute_batch(&compact_sql)
            .map_err(|e| format!("duckdb compact: {}", e))?;
        let t_compact_ms = t_phase.elapsed().as_secs_f64() * 1000.0;

        // Step 2: Apply changes to DuckLake target
        let target_ref = format!(
            "lake.\"{}\".\"{}\"",
            lake_info.lake_schema.replace('"', "\"\""),
            target_table.replace('"', "\"\"")
        );

        // Wrap DELETE+INSERT in a transaction for atomicity.
        let t_phase = Instant::now();
        self.db
            .execute_batch("BEGIN")
            .map_err(|e| format!("duckdb begin: {}", e))?;
        let t_begin_ms = t_phase.elapsed().as_secs_f64() * 1000.0;

        // Step 2b: DELETE
        let skip_delete = !has_non_inserts && !self.may_have_conflicts;

        let pk_where: Vec<String> = pk_cols
            .iter()
            .map(|c| format!("{target_ref}.{c} = compacted.{c}"))
            .collect();
        let delete_sql = format!(
            "DELETE FROM {target_ref} WHERE EXISTS ( \
                 SELECT 1 FROM compacted WHERE {pk_match} \
             )",
            target_ref = target_ref,
            pk_match = pk_where.join(" AND ")
        );

        let t_phase = Instant::now();
        let deleted_count: usize = if skip_delete {
            0
        } else {
            self.db.execute(&delete_sql, []).map_err(|e| {
                let _ = self.db.execute_batch("ROLLBACK");
                format!("duckdb delete from {}: {}", target_key, e)
            })?
        };
        let t_delete_ms = t_phase.elapsed().as_secs_f64() * 1000.0;

        if !has_non_inserts && deleted_count == 0 {
            self.may_have_conflicts = false;
        }

        // Step 2c: INSERT
        let t_phase = Instant::now();
        let insert_sql = format!(
            "INSERT INTO {target_ref} ({cols}) \
             SELECT {cols} FROM compacted WHERE _op_type IN (0, 1)",
            target_ref = target_ref,
            cols = all_cols.join(", ")
        );
        self.db.execute_batch(&insert_sql).map_err(|e| {
            let _ = self.db.execute_batch("ROLLBACK");
            format!("duckdb insert into {}: {}", target_key, e)
        })?;
        let t_insert_ms = t_phase.elapsed().as_secs_f64() * 1000.0;

        let t_phase = Instant::now();
        self.db
            .execute_batch("COMMIT")
            .map_err(|e| format!("duckdb commit: {}", e))?;
        let t_commit_ms = t_phase.elapsed().as_secs_f64() * 1000.0;

        // Cleanup
        let t_phase = Instant::now();
        self.db
            .execute_batch("DROP TABLE IF EXISTS compacted; DROP TABLE IF EXISTS buffer;")
            .map_err(|e| format!("duckdb cleanup: {}", e))?;
        self.buffer_exists = false;
        self.has_non_inserts = false;
        let t_cleanup_ms = t_phase.elapsed().as_secs_f64() * 1000.0;

        // No need to restore buffer-phase memory limit — the worker is dropped
        // after each flush cycle and recreated with buffer_memory_limit on next use.

        tracing::debug!(
            "DuckPipe perf: action=duckdb_flush target={} rows={} \
             compact_ms={:.1} begin_ms={:.1} delete_ms={:.1} insert_ms={:.1} \
             commit_ms={:.1} cleanup_ms={:.1} total_ms={:.1}",
            target_key,
            applied_count,
            t_compact_ms,
            t_begin_ms,
            t_delete_ms,
            t_insert_ms,
            t_commit_ms,
            t_cleanup_ms,
            flush_start.elapsed().as_secs_f64() * 1000.0,
        );
        tracing::info!(
            "DuckPipe timing: action=duckdb_flush target={} rows={} has_non_inserts={} \
             skip_delete={} deleted={} may_have_conflicts={} elapsed_ms={:.1}",
            target_key,
            applied_count,
            has_non_inserts,
            skip_delete,
            deleted_count,
            self.may_have_conflicts,
            flush_start.elapsed().as_secs_f64() * 1000.0,
        );

        let memory_bytes = query_memory_usage(&self.db);
        let flush_duration_ms = flush_start.elapsed().as_millis() as i64;

        Ok(DuckDbFlushResult {
            target_key: target_key.to_string(),
            mapping_id,
            applied_count,
            memory_bytes,
            flush_duration_ms,
            buffered_bytes,
        })
    }

    /// Drop the buffer table if it exists (used on shutdown/error).
    pub fn clear_buffer(&mut self) {
        if self.buffer_exists {
            let _ = self
                .db
                .execute_batch("DROP TABLE IF EXISTS compacted; DROP TABLE IF EXISTS buffer;");
            self.buffer_exists = false;
            self.has_non_inserts = false;
        }
    }
}

/// Result of a DuckDB-based flush.
#[derive(Debug)]
pub struct DuckDbFlushResult {
    pub target_key: String,
    pub mapping_id: i32,
    pub applied_count: i64,
    /// DuckDB buffer manager memory usage in bytes (from pragma_database_size).
    pub memory_bytes: i64,
    /// Wall-clock duration of the flush in milliseconds.
    pub flush_duration_ms: i64,
    /// Estimated total bytes of the flushed batch (fixed + variable).
    pub buffered_bytes: i64,
}

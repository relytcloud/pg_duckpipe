use serde::{Deserialize, Serialize};

/// Shared validation for config key-value pairs against a `&[(&str, &str)]` table.
fn validate_config_value(
    valid_keys: &[(&str, &str)],
    label: &str,
    key: &str,
    value: &str,
) -> Result<(), String> {
    let entry = valid_keys.iter().find(|(k, _)| *k == key).ok_or_else(|| {
        format!(
            "unknown {} key: '{}'. Valid keys: {}",
            label,
            key,
            valid_keys
                .iter()
                .map(|(k, _)| *k)
                .collect::<Vec<_>>()
                .join(", ")
        )
    })?;
    match entry.1 {
        "int" => {
            let v = value
                .parse::<i32>()
                .map_err(|_| format!("'{}' must be an integer, got '{}'", key, value))?;
            if v <= 0 {
                return Err(format!("'{}' must be positive, got {}", key, v));
            }
        }
        "bool" => {
            value
                .parse::<bool>()
                .map_err(|_| format!("'{}' must be true or false, got '{}'", key, value))?;
        }
        _ => {}
    }
    Ok(())
}

/// Known config keys with their validation rules.
const VALID_CONFIG_KEYS: &[(&str, &str)] = &[
    ("drain_poll_ms", "int"),
    ("duckdb_buffer_memory_mb", "int"),
    ("duckdb_flush_memory_mb", "int"),
    ("duckdb_threads", "int"),
    ("flush_interval_ms", "int"),
    ("flush_batch_threshold", "int"),
    ("max_concurrent_flushes", "int"),
    ("max_queued_changes", "int"),
];

/// Partial config — Option fields. Used for per-group overrides in JSONB.
/// Also used to represent global_config rows (populated from key-value table).
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct GroupConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub drain_poll_ms: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duckdb_buffer_memory_mb: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duckdb_flush_memory_mb: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duckdb_threads: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flush_interval_ms: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flush_batch_threshold: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_concurrent_flushes: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_queued_changes: Option<i32>,
}

impl GroupConfig {
    pub fn from_json_str(s: &str) -> Result<Self, String> {
        serde_json::from_str(s).map_err(|e| format!("invalid config JSON: {}", e))
    }

    pub fn to_json_string(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".to_string())
    }

    /// Build a GroupConfig from key-value rows (e.g., from the global_config table).
    pub fn from_kv_rows(rows: &[(String, String)]) -> Self {
        let mut config = GroupConfig::default();
        for (key, value) in rows {
            let _ = config.set_key(key, value);
        }
        config
    }

    /// Set a single key. Returns Err if the key is unknown or the value is invalid.
    pub fn set_key(&mut self, key: &str, value: &str) -> Result<(), String> {
        Self::validate_key(key, value)?;
        match key {
            "drain_poll_ms" => self.drain_poll_ms = Some(value.parse::<i32>().unwrap()),
            "duckdb_buffer_memory_mb" => {
                self.duckdb_buffer_memory_mb = Some(value.parse::<i32>().unwrap())
            }
            "duckdb_flush_memory_mb" => {
                self.duckdb_flush_memory_mb = Some(value.parse::<i32>().unwrap())
            }
            "duckdb_threads" => self.duckdb_threads = Some(value.parse::<i32>().unwrap()),
            "flush_interval_ms" => self.flush_interval_ms = Some(value.parse::<i32>().unwrap()),
            "flush_batch_threshold" => {
                self.flush_batch_threshold = Some(value.parse::<i32>().unwrap())
            }
            "max_concurrent_flushes" => {
                self.max_concurrent_flushes = Some(value.parse::<i32>().unwrap())
            }
            "max_queued_changes" => self.max_queued_changes = Some(value.parse::<i32>().unwrap()),
            _ => return Err(format!("unknown config key: '{}'", key)),
        }
        Ok(())
    }

    /// Get a single key's value as a string. Returns None if unset or unknown.
    pub fn get_key(&self, key: &str) -> Option<String> {
        match key {
            "drain_poll_ms" => self.drain_poll_ms.map(|v| v.to_string()),
            "duckdb_buffer_memory_mb" => self.duckdb_buffer_memory_mb.map(|v| v.to_string()),
            "duckdb_flush_memory_mb" => self.duckdb_flush_memory_mb.map(|v| v.to_string()),
            "duckdb_threads" => self.duckdb_threads.map(|v| v.to_string()),
            "flush_interval_ms" => self.flush_interval_ms.map(|v| v.to_string()),
            "flush_batch_threshold" => self.flush_batch_threshold.map(|v| v.to_string()),
            "max_concurrent_flushes" => self.max_concurrent_flushes.map(|v| v.to_string()),
            "max_queued_changes" => self.max_queued_changes.map(|v| v.to_string()),
            _ => None,
        }
    }

    pub fn validate_key(key: &str, value: &str) -> Result<(), String> {
        validate_config_value(VALID_CONFIG_KEYS, "config", key, value)
    }

    pub fn is_known_key(key: &str) -> bool {
        VALID_CONFIG_KEYS.iter().any(|(k, _)| *k == key)
    }
}

/// Known per-table config keys with their validation rules.
const VALID_TABLE_CONFIG_KEYS: &[(&str, &str)] = &[
    ("duckdb_flush_memory_mb", "int"),
    ("duckdb_threads", "int"),
    ("flush_batch_threshold", "int"),
    ("flush_interval_ms", "int"),
    ("routing_enabled", "bool"),
];

/// Per-table config overrides (stored in table_mappings.config JSONB).
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct TableConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub routing_enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flush_interval_ms: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flush_batch_threshold: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duckdb_threads: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duckdb_flush_memory_mb: Option<i32>,
}

impl TableConfig {
    pub fn from_json_str(s: &str) -> Result<Self, String> {
        serde_json::from_str(s).map_err(|e| format!("invalid table config JSON: {}", e))
    }

    pub fn to_json_string(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".to_string())
    }

    /// Set a single key. Returns Err if the key is unknown or the value is invalid.
    pub fn set_key(&mut self, key: &str, value: &str) -> Result<(), String> {
        Self::validate_key(key, value)?;
        match key {
            "routing_enabled" => {
                self.routing_enabled = Some(value.parse::<bool>().unwrap());
            }
            "flush_interval_ms" => {
                self.flush_interval_ms = Some(value.parse::<i32>().unwrap());
            }
            "flush_batch_threshold" => {
                self.flush_batch_threshold = Some(value.parse::<i32>().unwrap());
            }
            "duckdb_threads" => {
                self.duckdb_threads = Some(value.parse::<i32>().unwrap());
            }
            "duckdb_flush_memory_mb" => {
                self.duckdb_flush_memory_mb = Some(value.parse::<i32>().unwrap());
            }
            _ => return Err(format!("unknown table config key: '{}'", key)),
        }
        Ok(())
    }

    /// Get a single key's value as a string. Returns None if unset or unknown.
    pub fn get_key(&self, key: &str) -> Option<String> {
        match key {
            "routing_enabled" => self.routing_enabled.map(|v| v.to_string()),
            "flush_interval_ms" => self.flush_interval_ms.map(|v| v.to_string()),
            "flush_batch_threshold" => self.flush_batch_threshold.map(|v| v.to_string()),
            "duckdb_threads" => self.duckdb_threads.map(|v| v.to_string()),
            "duckdb_flush_memory_mb" => self.duckdb_flush_memory_mb.map(|v| v.to_string()),
            _ => None,
        }
    }

    pub fn validate_key(key: &str, value: &str) -> Result<(), String> {
        validate_config_value(VALID_TABLE_CONFIG_KEYS, "table config", key, value)
    }

    pub fn is_known_key(key: &str) -> bool {
        VALID_TABLE_CONFIG_KEYS.iter().any(|(k, _)| *k == key)
    }
}

/// Fully resolved config — no Options. All values guaranteed present.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedConfig {
    pub drain_poll_ms: i32,
    pub duckdb_buffer_memory_mb: i32,
    pub duckdb_flush_memory_mb: i32,
    pub duckdb_threads: i32,
    pub flush_interval_ms: i32,
    pub flush_batch_threshold: i32,
    pub max_concurrent_flushes: i32,
    pub max_queued_changes: i32,
}

impl Default for ResolvedConfig {
    fn default() -> Self {
        ResolvedConfig {
            drain_poll_ms: 100,
            duckdb_buffer_memory_mb: 16,
            duckdb_flush_memory_mb: 512,
            duckdb_threads: 1,
            flush_interval_ms: 5000,
            flush_batch_threshold: 50000,
            max_concurrent_flushes: 4,
            max_queued_changes: 500000,
        }
    }
}

impl ResolvedConfig {
    /// Convert to a GroupConfig with all fields set (for JSON serialization).
    pub fn to_group_config(&self) -> GroupConfig {
        GroupConfig {
            drain_poll_ms: Some(self.drain_poll_ms),
            duckdb_buffer_memory_mb: Some(self.duckdb_buffer_memory_mb),
            duckdb_flush_memory_mb: Some(self.duckdb_flush_memory_mb),
            duckdb_threads: Some(self.duckdb_threads),
            flush_interval_ms: Some(self.flush_interval_ms),
            flush_batch_threshold: Some(self.flush_batch_threshold),
            max_concurrent_flushes: Some(self.max_concurrent_flushes),
            max_queued_changes: Some(self.max_queued_changes),
        }
    }

    /// Get a single key's value as a string.
    pub fn get_key(&self, key: &str) -> Option<String> {
        match key {
            "drain_poll_ms" => Some(self.drain_poll_ms.to_string()),
            "duckdb_buffer_memory_mb" => Some(self.duckdb_buffer_memory_mb.to_string()),
            "duckdb_flush_memory_mb" => Some(self.duckdb_flush_memory_mb.to_string()),
            "duckdb_threads" => Some(self.duckdb_threads.to_string()),
            "flush_interval_ms" => Some(self.flush_interval_ms.to_string()),
            "flush_batch_threshold" => Some(self.flush_batch_threshold.to_string()),
            "max_concurrent_flushes" => Some(self.max_concurrent_flushes.to_string()),
            "max_queued_changes" => Some(self.max_queued_changes.to_string()),
            _ => None,
        }
    }

    /// Apply per-table overrides on top of an already-resolved group config.
    /// Only keys present in TableConfig are overridable at the table level.
    pub fn resolve_for_table(&self, table: &TableConfig) -> Self {
        ResolvedConfig {
            flush_interval_ms: table.flush_interval_ms.unwrap_or(self.flush_interval_ms),
            flush_batch_threshold: table
                .flush_batch_threshold
                .unwrap_or(self.flush_batch_threshold),
            duckdb_threads: table.duckdb_threads.unwrap_or(self.duckdb_threads),
            duckdb_flush_memory_mb: table
                .duckdb_flush_memory_mb
                .unwrap_or(self.duckdb_flush_memory_mb),
            // Non-overridable fields pass through unchanged
            drain_poll_ms: self.drain_poll_ms,
            duckdb_buffer_memory_mb: self.duckdb_buffer_memory_mb,
            max_concurrent_flushes: self.max_concurrent_flushes,
            max_queued_changes: self.max_queued_changes,
        }
    }

    /// Resolve: hardcoded defaults ← global config ← per-group config.
    pub fn resolve(global: &GroupConfig, group: &GroupConfig) -> Self {
        let defaults = ResolvedConfig::default();
        ResolvedConfig {
            drain_poll_ms: group
                .drain_poll_ms
                .or(global.drain_poll_ms)
                .unwrap_or(defaults.drain_poll_ms),
            duckdb_buffer_memory_mb: group
                .duckdb_buffer_memory_mb
                .or(global.duckdb_buffer_memory_mb)
                .unwrap_or(defaults.duckdb_buffer_memory_mb),
            duckdb_flush_memory_mb: group
                .duckdb_flush_memory_mb
                .or(global.duckdb_flush_memory_mb)
                .unwrap_or(defaults.duckdb_flush_memory_mb),
            duckdb_threads: group
                .duckdb_threads
                .or(global.duckdb_threads)
                .unwrap_or(defaults.duckdb_threads),
            flush_interval_ms: group
                .flush_interval_ms
                .or(global.flush_interval_ms)
                .unwrap_or(defaults.flush_interval_ms),
            flush_batch_threshold: group
                .flush_batch_threshold
                .or(global.flush_batch_threshold)
                .unwrap_or(defaults.flush_batch_threshold),
            max_concurrent_flushes: group
                .max_concurrent_flushes
                .or(global.max_concurrent_flushes)
                .unwrap_or(defaults.max_concurrent_flushes),
            max_queued_changes: group
                .max_queued_changes
                .or(global.max_queued_changes)
                .unwrap_or(defaults.max_queued_changes),
        }
    }
}

/// Column value with type information.
/// Parsed from pgoutput text representation using the RELATION message's type_oid.
/// Unrecognized types fall back to Text(String), which DuckDB auto-casts.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Text(String),
}

impl Value {
    /// Returns byte size only for variable-length values (Text).
    /// Fixed-size types return 0 — their cost is precomputed from schema.
    #[inline]
    pub fn var_bytes(&self) -> usize {
        match self {
            Value::Text(s) => s.len(),
            _ => 0,
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Int16(i) => write!(f, "{}", i),
            Value::Int32(i) => write!(f, "{}", i),
            Value::Int64(i) => write!(f, "{}", i),
            Value::Float32(v) => write!(f, "{}", v),
            Value::Float64(v) => write!(f, "{}", v),
            Value::Text(s) => write!(f, "{}", s),
        }
    }
}

/// Operation type for CDC changes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeType {
    Insert,
    Delete,
    /// UPDATE that preserves unchanged TOAST columns via SQL UPDATE SET
    Update,
}

/// A decoded change
#[derive(Debug, Clone)]
pub struct Change {
    pub change_type: ChangeType,
    pub lsn: u64,
    pub col_values: Vec<Value>,
    pub key_values: Vec<Value>,
    /// For Update changes: true for columns that were unchanged (TOAST)
    pub col_unchanged: Vec<bool>,
}

/// Relation cache entry (decoded from pgoutput RELATION messages)
#[derive(Debug, Clone)]
pub struct RelCacheEntry {
    pub nspname: String,
    pub relname: String,
    pub attnames: Vec<String>,
    pub attkeys: Vec<usize>,
    /// PostgreSQL type OIDs from RELATION message (one per column)
    pub atttypes: Vec<u32>,
}

/// Sync group mode: bgworker (PG background worker) or daemon (external binary).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupMode {
    BgWorker,
    Daemon,
}

impl GroupMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            GroupMode::BgWorker => "bgworker",
            GroupMode::Daemon => "daemon",
        }
    }
}

impl std::str::FromStr for GroupMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "bgworker" => Ok(GroupMode::BgWorker),
            "daemon" => Ok(GroupMode::Daemon),
            other => Err(format!(
                "invalid mode '{}': must be 'bgworker' or 'daemon'",
                other
            )),
        }
    }
}

/// Sync group metadata
#[derive(Debug, Clone)]
pub struct SyncGroup {
    pub id: i32,
    pub name: String,
    pub publication: String,
    pub slot_name: String,
    pub pending_lsn: u64,
    pub confirmed_lsn: u64,
    /// Remote PG connection string (NULL = local group).
    pub conninfo: Option<String>,
    pub mode: GroupMode,
    /// Per-group config overrides (from sync_groups.config JSONB column).
    pub config: GroupConfig,
}

/// Table mapping metadata
#[derive(Debug, Clone)]
pub struct TableMapping {
    pub id: i32,
    pub source_schema: String,
    pub source_table: String,
    pub target_schema: String,
    pub target_table: String,
    pub state: String,
    pub snapshot_lsn: u64,
    pub applied_lsn: u64,
    pub enabled: bool,
    pub source_oid: Option<i64>,
    pub target_oid: i64,
    pub error_message: Option<String>,
    pub source_label: String,
    pub sync_mode: String,
    /// Per-table config overrides (from table_mappings.config JSONB column).
    pub config: TableConfig,
}

/// Returns true if `name` is a duckpipe-managed system column (case-insensitive).
/// These columns are injected by duckpipe and should be excluded when aligning
/// source/pgoutput columns with DuckLake catalog columns.
pub fn is_duckpipe_system_column(name: &str) -> bool {
    let lower = name.to_lowercase();
    lower == "_duckpipe_source" || lower == "_duckpipe_op" || lower == "_duckpipe_lsn"
}

/// Fixed byte size for a column type OID. Returns 0 for variable-length types (text, jsonb, etc).
pub fn fixed_bytes_for_oid(oid: u32) -> usize {
    // Only list types that parse_text_value() decodes into fixed Value variants.
    // Types decoded as Value::Text (date, timestamp, uuid, etc.) are sized via
    // Value::var_bytes() instead — listing them here would double-count.
    match oid {
        16 => 1,  // bool
        21 => 2,  // int2
        23 => 4,  // int4
        20 => 8,  // int8
        700 => 4, // float4
        701 => 8, // float8
        26 => 4,  // oid (parsed as int4)
        _ => 0,   // text, varchar, jsonb, date, timestamp, uuid, etc — variable
    }
}

/// Map PostgreSQL type OID to a human-readable type name for error messages.
pub fn pg_oid_to_type_name(oid: u32) -> String {
    match oid {
        16 => "boolean",
        20 => "bigint",
        21 => "smallint",
        23 => "integer",
        25 => "text",
        700 => "real",
        701 => "double precision",
        1043 => "varchar",
        1700 => "numeric",
        1114 => "timestamp",
        1184 => "timestamptz",
        1082 => "date",
        2950 => "uuid",
        114 => "json",
        3802 => "jsonb",
        _ => return format!("oid:{}", oid),
    }
    .to_string()
}

/// Map PostgreSQL type name to DuckDB-compatible type for DDL statements.
pub fn map_pg_type_for_duckdb(pg_type: &str) -> String {
    match pg_type {
        "jsonb" | "json" => "JSON".to_string(),
        other => other.to_string(),
    }
}

/// Parse pg_lsn string (e.g., "0/1C463F8") to u64
pub fn parse_lsn(s: &str) -> u64 {
    let parts: Vec<&str> = s.split('/').collect();
    if parts.len() == 2 {
        let hi = u64::from_str_radix(parts[0], 16).unwrap_or(0);
        let lo = u64::from_str_radix(parts[1], 16).unwrap_or(0);
        (hi << 32) | lo
    } else {
        0
    }
}

/// Format u64 LSN to pg_lsn string (e.g., "0/1C463F8")
pub fn format_lsn(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFFFFFF)
}

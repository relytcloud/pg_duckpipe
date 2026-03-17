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
    pub error_message: Option<String>,
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

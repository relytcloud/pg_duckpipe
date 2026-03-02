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

//! Load DuckDB secrets from PostgreSQL's FDW catalogs (pg_duckdb integration).
//!
//! pg_duckdb stores S3/GCS/R2/Azure credentials in standard PostgreSQL FDW objects
//! (`pg_foreign_server` + `pg_user_mapping` for the `duckdb` FDW). This module reads
//! those catalog entries and builds `CREATE SECRET` SQL strings that can be executed
//! in any DuckDB connection to enable cloud storage access.
//!
//! Both bgworker (SPI) and daemon (tokio-postgres) modes use the same query —
//! only the execution path differs.

/// SQL query that returns complete `CREATE SECRET` statements from pg_duckdb's FDW catalogs.
///
/// Returns one text row per secret: a ready-to-execute DuckDB `CREATE SECRET` statement.
/// Excludes `motherduck` servers (MotherDuck auth is handled separately by pg_duckdb).
///
/// Uses `pg_user_mappings` VIEW (not the raw `pg_user_mapping` catalog) because
/// PG 17+ restricts direct access to `pg_user_mapping.umoptions` via column-level ACL.
/// The view applies proper access control: shows options when the current user is the
/// mapped user with USAGE on the server, or is a superuser.
///
/// The query builds the SQL entirely server-side using `unnest()` + `string_agg()`,
/// so callers only need to read simple text strings (no text[] handling required).
pub const SECRET_QUERY: &str = r#"
SELECT 'CREATE SECRET pgduckdb_secret_' || fs.srvname || ' (TYPE ' || fs.srvtype
    || COALESCE((
        SELECT ', ' || string_agg(
            split_part(opt, '=', 1) || ' ' || quote_literal(substr(opt, strpos(opt, '=') + 1)),
            ', '
        )
        FROM unnest(fs.srvoptions) AS opt
    ), '')
    || COALESCE((
        SELECT ', ' || string_agg(
            split_part(opt, '=', 1) || ' ' || quote_literal(substr(opt, strpos(opt, '=') + 1)),
            ', '
        )
        FROM unnest(um.umoptions) AS opt
    ), '')
    || ')' AS secret_sql
FROM pg_foreign_server fs
JOIN pg_foreign_data_wrapper fdw ON fdw.oid = fs.srvfdw
LEFT JOIN LATERAL (
    SELECT umoptions FROM pg_user_mappings
    WHERE srvid = fs.oid
      AND umuser IN (
          (SELECT oid FROM pg_roles WHERE rolname = current_user),
          0
      )
    ORDER BY umuser DESC  -- prefer user-specific (nonzero) over PUBLIC (0)
    LIMIT 1
) um ON true
WHERE fdw.fdwname = 'duckdb'
  AND COALESCE(fs.srvtype, '') != 'motherduck'
"#;

/// Build a `CREATE SECRET` SQL string from raw FDW catalog data.
///
/// `server_opts` and `user_opts` are `key=value` strings as stored in
/// `pg_foreign_server.srvoptions` and `pg_user_mapping.umoptions`.
///
/// Output format matches pg_duckdb's `MakeDuckDBCreateSecretQuery()`:
/// ```sql
/// CREATE SECRET pgduckdb_secret_<name> (TYPE <type>, region 'us-east-1', KEY_ID '...', ...)
/// ```
///
/// Test-only: production uses `SECRET_QUERY` which builds the SQL server-side.
#[cfg(test)]
fn build_create_secret_sql(
    server_name: &str,
    server_type: &str,
    server_opts: &[String],
    user_opts: &[String],
) -> String {
    let mut sql = format!(
        "CREATE SECRET pgduckdb_secret_{} (TYPE {}",
        server_name, server_type
    );

    for opt in server_opts.iter().chain(user_opts.iter()) {
        if let Some(pos) = opt.find('=') {
            let key = &opt[..pos];
            let value = &opt[pos + 1..];
            sql.push_str(&format!(", {} '{}'", key, value.replace('\'', "''")));
        }
    }

    sql.push(')');
    sql
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_options() {
        let sql = build_create_secret_sql("my_server", "s3", &[], &[]);
        assert_eq!(sql, "CREATE SECRET pgduckdb_secret_my_server (TYPE s3)");
    }

    #[test]
    fn test_server_options_only() {
        let server_opts = vec![
            "region=us-east-1".to_string(),
            "endpoint=https://s3.example.com".to_string(),
        ];
        let sql = build_create_secret_sql("my_s3", "s3", &server_opts, &[]);
        assert_eq!(
            sql,
            "CREATE SECRET pgduckdb_secret_my_s3 (TYPE s3, \
             region 'us-east-1', endpoint 'https://s3.example.com')"
        );
    }

    #[test]
    fn test_server_and_user_options() {
        let server_opts = vec!["region=us-west-2".to_string()];
        let user_opts = vec![
            "KEY_ID=AKIAIOSFODNN7EXAMPLE".to_string(),
            "SECRET=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
        ];
        let sql = build_create_secret_sql("prod_s3", "s3", &server_opts, &user_opts);
        assert_eq!(
            sql,
            "CREATE SECRET pgduckdb_secret_prod_s3 (TYPE s3, \
             region 'us-west-2', \
             KEY_ID 'AKIAIOSFODNN7EXAMPLE', \
             SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY')"
        );
    }

    #[test]
    fn test_value_with_single_quote() {
        let user_opts = vec!["SECRET=it's=complex".to_string()];
        let sql = build_create_secret_sql("test", "s3", &[], &user_opts);
        assert_eq!(
            sql,
            "CREATE SECRET pgduckdb_secret_test (TYPE s3, SECRET 'it''s=complex')"
        );
    }

    #[test]
    fn test_value_with_equals() {
        // Value contains '=' (e.g., base64-encoded secrets)
        let user_opts = vec!["SECRET=abc123+def456==".to_string()];
        let sql = build_create_secret_sql("test", "s3", &[], &user_opts);
        assert_eq!(
            sql,
            "CREATE SECRET pgduckdb_secret_test (TYPE s3, SECRET 'abc123+def456==')"
        );
    }

    #[test]
    fn test_gcs_type() {
        let server_opts = vec!["provider=credential_chain".to_string()];
        let user_opts = vec!["KEY_ID=my_key".to_string(), "SECRET=my_secret".to_string()];
        let sql = build_create_secret_sql("gcs_server", "gcs", &server_opts, &user_opts);
        assert_eq!(
            sql,
            "CREATE SECRET pgduckdb_secret_gcs_server (TYPE gcs, \
             provider 'credential_chain', KEY_ID 'my_key', SECRET 'my_secret')"
        );
    }
}

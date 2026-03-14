-- Ensure pg_duckpipe is in shared_preload_libraries via ALTER SYSTEM.
-- ALTER SYSTEM writes to postgresql.auto.conf which is processed AFTER
-- postgresql.conf and any conf.d/ includes, so it always wins.
-- The change takes effect when the Docker entrypoint restarts PostgreSQL
-- after running init scripts.
ALTER SYSTEM SET shared_preload_libraries = 'pg_duckdb,pg_ducklake,pg_duckpipe';

CREATE EXTENSION IF NOT EXISTS pg_duckpipe;

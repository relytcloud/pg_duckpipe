-- Test per-group configuration system

-- 1. Verify global_config table has defaults
SELECT key, value FROM duckpipe.global_config ORDER BY key;

-- 2. get_config() returns all defaults as JSON
SELECT duckpipe.get_config();

-- 3. get_config(key) returns single value
SELECT duckpipe.get_config('duckdb_buffer_memory_mb');

-- 4. set_config() persists
SELECT duckpipe.set_config('duckdb_buffer_memory_mb', '32');
SELECT duckpipe.get_config('duckdb_buffer_memory_mb');

-- 5. set_config() updates existing
SELECT duckpipe.set_config('duckdb_threads', '4');
SELECT duckpipe.get_config('duckdb_threads');

-- 6. Invalid key errors
SELECT duckpipe.set_config('nonexistent_key', '123');
SELECT duckpipe.get_config('nonexistent_key');

-- 7. Invalid value errors
SELECT duckpipe.set_config('duckdb_threads', 'not_a_number');
SELECT duckpipe.set_config('duckdb_threads', '-1');

-- 8. Per-group: get_group_config defaults (inherits from global)
SELECT duckpipe.get_group_config('default');

-- 9. Per-group: get single key (resolved from global)
SELECT duckpipe.get_group_config('default', 'duckdb_buffer_memory_mb');
SELECT duckpipe.get_group_config('default', 'duckdb_threads');

-- 10. Per-group: set_group_config overrides
SELECT duckpipe.set_group_config('default', 'duckdb_buffer_memory_mb', '64');
SELECT duckpipe.get_group_config('default', 'duckdb_buffer_memory_mb');

-- 11. Per-group: resolved JSON shows override + inherited values
SELECT duckpipe.get_group_config('default');

-- 12. Per-group: nonexistent group errors
SELECT duckpipe.set_group_config('nonexistent', 'duckdb_threads', '2');
SELECT duckpipe.get_group_config('nonexistent');

-- 13. Per-group: invalid key errors
SELECT duckpipe.set_group_config('default', 'bad_key', '123');

-- 14. Verify sync_groups.config column has the override
SELECT config FROM duckpipe.sync_groups WHERE name = 'default';

-- 15. Cleanup: reset global config back to defaults
SELECT duckpipe.set_config('duckdb_buffer_memory_mb', '16');
SELECT duckpipe.set_config('duckdb_threads', '1');

-- 16. Cleanup: reset group config
UPDATE duckpipe.sync_groups SET config = '{}'::jsonb WHERE name = 'default';

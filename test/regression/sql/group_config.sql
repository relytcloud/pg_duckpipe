-- Test per-group configuration system
-- Tests config CRUD mechanics, not the exact set of config keys.

-- 1. Verify global_config table has rows (don't list all — new keys shouldn't break this test)
SELECT count(*) > 0 AS has_defaults FROM duckpipe.global_config;

-- 2. get_config() returns valid JSON containing known keys
SELECT duckpipe.get_config()::jsonb ? 'flush_interval_ms' AS has_flush_interval;

-- 3. get_config(key) returns single value
SELECT duckpipe.get_config('duckdb_buffer_memory_mb');

-- 4. set_config() persists
SELECT duckpipe.set_config('duckdb_buffer_memory_mb', '32');
SELECT duckpipe.get_config('duckdb_buffer_memory_mb');

-- 5. set_config() updates existing
SELECT duckpipe.set_config('duckdb_threads', '4');
SELECT duckpipe.get_config('duckdb_threads');

-- 6. Invalid key errors (catch to avoid matching full key list in error message)
DO $$ BEGIN PERFORM duckpipe.set_config('nonexistent_key', '123'); EXCEPTION WHEN OTHERS THEN RAISE NOTICE 'set_config: got expected error'; END $$;
SELECT duckpipe.get_config('nonexistent_key');

-- 7. Invalid value errors
SELECT duckpipe.set_config('duckdb_threads', 'not_a_number');
SELECT duckpipe.set_config('duckdb_threads', '-1');

-- 8. Per-group: get_group_config returns JSON with known keys
SELECT duckpipe.get_group_config('default')::jsonb ? 'flush_interval_ms' AS has_flush_interval;

-- 9. Per-group: get single key (resolved from global)
SELECT duckpipe.get_group_config('default', 'duckdb_buffer_memory_mb');
SELECT duckpipe.get_group_config('default', 'duckdb_threads');

-- 10. Per-group: set_group_config overrides
SELECT duckpipe.set_group_config('default', 'duckdb_buffer_memory_mb', '64');
SELECT duckpipe.get_group_config('default', 'duckdb_buffer_memory_mb');

-- 11. Per-group: override visible in resolved JSON
SELECT duckpipe.get_group_config('default')::jsonb->>'duckdb_buffer_memory_mb' AS buf_mb;

-- 12. Per-group: nonexistent group errors
SELECT duckpipe.set_group_config('nonexistent', 'duckdb_threads', '2');
SELECT duckpipe.get_group_config('nonexistent');

-- 13. Per-group: invalid key errors (just check it errors, don't match full key list)
DO $$ BEGIN PERFORM duckpipe.set_group_config('default', 'bad_key', '123'); EXCEPTION WHEN OTHERS THEN RAISE NOTICE 'got expected error'; END $$;

-- 14. Verify sync_groups.config column has the override
SELECT config FROM duckpipe.sync_groups WHERE name = 'default';

-- 15. Cleanup: reset global config back to defaults
SELECT duckpipe.set_config('duckdb_buffer_memory_mb', '16');
SELECT duckpipe.set_config('duckdb_threads', '1');

-- 16. Cleanup: reset group config
UPDATE duckpipe.sync_groups SET config = '{}'::jsonb WHERE name = 'default';

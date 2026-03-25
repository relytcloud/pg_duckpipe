-- Test per-table configuration system
-- Tests config CRUD, validation, and 4-tier resolution (defaults ← global ← group ← table).
-- Requires a table already added to duckpipe (uses the default sync group).

-- Setup: create and add a test table
CREATE TABLE tc_orders (id INT PRIMARY KEY, amount INT);
SELECT duckpipe.add_table('tc_orders');
SELECT pg_sleep(0.5);

-- 1. get_table_config() returns JSON with all table config keys
SELECT duckpipe.get_table_config('tc_orders')::jsonb ? 'routing_enabled' AS has_routing;
SELECT duckpipe.get_table_config('tc_orders')::jsonb ? 'flush_interval_ms' AS has_flush_interval;

-- 2. get_table_config(key) returns resolved values
SELECT duckpipe.get_table_config('tc_orders', 'routing_enabled');
-- flush_interval_ms should match the global config (may differ from hardcoded default)
SELECT duckpipe.get_table_config('tc_orders', 'flush_interval_ms') = duckpipe.get_config('flush_interval_ms') AS flush_matches_global;
SELECT duckpipe.get_table_config('tc_orders', 'duckdb_threads') = duckpipe.get_config('duckdb_threads') AS threads_matches_global;

-- 3. set_table_config() persists int key
SELECT duckpipe.set_table_config('tc_orders', 'flush_interval_ms', '2000');
SELECT duckpipe.get_table_config('tc_orders', 'flush_interval_ms');

-- 4. set_table_config() persists bool key
SELECT duckpipe.set_table_config('tc_orders', 'routing_enabled', 'false');
SELECT duckpipe.get_table_config('tc_orders', 'routing_enabled');

-- 5. set_routing() backward compat writes to config JSONB
SELECT duckpipe.set_routing('tc_orders', true);
SELECT duckpipe.get_table_config('tc_orders', 'routing_enabled');

-- 6. Verify tables() shows routing_enabled from config
SELECT source_table, routing_enabled FROM duckpipe.tables()
    WHERE source_table = 'public.tc_orders';

-- 7. Verify status() shows routing_enabled from config
SELECT source_table, routing_enabled FROM duckpipe.status()
    WHERE source_table = 'public.tc_orders';

-- 8. Group override flows through to table (set group, verify table sees it)
SELECT duckpipe.set_group_config('default', 'duckdb_threads', '8');
SELECT duckpipe.get_table_config('tc_orders', 'duckdb_threads');

-- 9. Table override takes precedence over group
SELECT duckpipe.set_table_config('tc_orders', 'duckdb_threads', '2');
SELECT duckpipe.get_table_config('tc_orders', 'duckdb_threads');

-- 10. Verify config column has the overrides
SELECT config FROM duckpipe.table_mappings
    WHERE source_table = 'tc_orders';

-- 11. Invalid key errors
DO $$ BEGIN PERFORM duckpipe.set_table_config('tc_orders', 'nonexistent_key', '123'); EXCEPTION WHEN OTHERS THEN RAISE NOTICE 'set_table_config: got expected error'; END $$;

-- 12. Invalid value errors
SELECT duckpipe.set_table_config('tc_orders', 'flush_interval_ms', 'not_a_number');
SELECT duckpipe.set_table_config('tc_orders', 'flush_interval_ms', '-1');
SELECT duckpipe.set_table_config('tc_orders', 'routing_enabled', 'maybe');

-- 13. Nonexistent table errors
DO $$ BEGIN PERFORM duckpipe.set_table_config('nonexistent_table', 'duckdb_threads', '2'); EXCEPTION WHEN OTHERS THEN RAISE NOTICE 'set_table_config: got expected error for nonexistent'; END $$;

-- 14. Resolved JSON shows all keys with correct resolution
SELECT duckpipe.get_table_config('tc_orders')::jsonb->>'duckdb_threads' AS threads;
SELECT duckpipe.get_table_config('tc_orders')::jsonb->>'flush_interval_ms' AS flush_ms;

-- Cleanup
SELECT duckpipe.remove_table('tc_orders');
SELECT pg_sleep(0.5);
UPDATE duckpipe.sync_groups SET config = '{}'::jsonb WHERE name = 'default';
DROP TABLE tc_orders CASCADE;

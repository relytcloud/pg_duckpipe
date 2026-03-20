-- Test: Transparent analytical query routing
-- Verifies the planner hook rewrites SELECT queries on synced source tables
-- to their DuckLake counterparts based on duckpipe.query_routing GUC.

-- Create a dedicated group for this test to avoid slot interference
SET client_min_messages = warning;
SELECT duckpipe.create_group('qr_test');
SELECT duckpipe.start_worker('qr_test');
RESET client_min_messages;

-- Create source tables
CREATE TABLE qr_orders (
    id      int PRIMARY KEY,
    status  text,
    total   int
);
CREATE TABLE qr_customers (
    id      int PRIMARY KEY,
    name    text,
    order_id int
);

-- Use copy_data=false so tables start directly in STREAMING state.
-- Data is inserted AFTER add_table so it flows through WAL replication.
SELECT duckpipe.add_table('public.qr_orders', NULL, 'qr_test', false);
SELECT duckpipe.add_table('public.qr_customers', NULL, 'qr_test', false);

INSERT INTO qr_orders VALUES (1, 'pending', 100);
INSERT INTO qr_orders VALUES (2, 'shipped', 250);
INSERT INTO qr_orders VALUES (3, 'pending', 75);

INSERT INTO qr_customers VALUES (10, 'alice', 1);
INSERT INTO qr_customers VALUES (20, 'bob', 2);

-- Wait for WAL to be consumed and flushed
SELECT pg_sleep(8);

-- Verify data is synced
SELECT count(*) AS synced_orders FROM public.qr_orders_ducklake;
SELECT count(*) AS synced_customers FROM public.qr_customers_ducklake;

-- =======================================================================
-- 1. Default: routing OFF — no NOTICE emitted
-- =======================================================================
SHOW duckpipe.query_routing;
SET duckpipe.query_routing_log = on;

-- This should NOT emit a routing NOTICE (routing is off)
SELECT count(*) AS off_count FROM qr_orders;

-- =======================================================================
-- 2. ON mode — route ALL SELECTs unconditionally
-- =======================================================================
SET duckpipe.query_routing = 'on';

-- Full scan → routed (NOTICE expected)
SELECT count(*) AS on_count FROM qr_orders;

-- =======================================================================
-- 3. AUTO mode — route analytical, skip point lookups
-- =======================================================================
SET duckpipe.query_routing = 'auto';

-- Analytical: full scan → routed (NOTICE expected)
SELECT count(*) AS auto_full_scan FROM qr_orders;

-- Point lookup on PK → NOT routed (no NOTICE, runs on PG heap)
SELECT id, status FROM qr_orders WHERE id = 1;

-- Aggregation + PK eq → routed (aggregation overrides PK lookup)
SELECT count(*) AS agg_with_pk FROM qr_orders WHERE id = 1;

-- =======================================================================
-- 4. Subquery routing
-- =======================================================================
SET duckpipe.query_routing = 'on';

-- Subquery in FROM clause → DuckDB handles the subquery internally
-- after the outer query is routed (no separate NOTICE for subquery RTEs)
SELECT cnt FROM (SELECT count(*) AS cnt FROM qr_orders) sub;

-- =======================================================================
-- 5. CTE routing
-- =======================================================================

-- CTE with aggregation → DuckDB handles CTE body after routing
WITH order_summary AS (
    SELECT status, count(*) AS cnt FROM qr_orders GROUP BY status
)
SELECT count(*) AS summary_rows FROM order_summary;

-- =======================================================================
-- 6. JOIN between two routed tables
-- =======================================================================

-- JOIN both synced tables → both routed (two NOTICEs expected)
SELECT count(*) AS join_count
FROM qr_orders o
JOIN qr_customers c ON c.order_id = o.id;

-- =======================================================================
-- 7. Per-table routing opt-out
-- =======================================================================
SET duckpipe.query_routing = 'on';

-- Disable routing for this table
SELECT duckpipe.set_routing('public.qr_orders', false);

-- Wait for routing cache to expire (TTL=2s)
SELECT pg_sleep(3);

-- Verify tables() shows routing_enabled = false
SELECT source_table, routing_enabled FROM duckpipe.tables()
WHERE source_table = 'public.qr_orders';

-- Verify status() shows routing_enabled = false
SELECT source_table, routing_enabled FROM duckpipe.status()
WHERE source_table = 'public.qr_orders';

-- This should NOT emit a routing NOTICE (routing disabled for table)
SELECT count(*) AS disabled_count FROM qr_orders;

-- Re-enable routing
SELECT duckpipe.set_routing('public.qr_orders', true);

-- Wait for routing cache to expire
SELECT pg_sleep(3);

-- Should route again (NOTICE expected)
SELECT count(*) AS reenabled_count FROM qr_orders;

-- =======================================================================
-- 8. Global GUC reset disables routing
-- =======================================================================
RESET duckpipe.query_routing;

-- Should NOT route (GUC back to 'off')
SELECT count(*) AS reset_count FROM qr_orders;

-- =======================================================================
-- Cleanup
-- =======================================================================
SELECT duckpipe.remove_table('public.qr_customers', false);
SELECT duckpipe.remove_table('public.qr_orders', false);
DROP TABLE IF EXISTS public.qr_customers_ducklake;
DROP TABLE qr_customers;
DROP TABLE IF EXISTS public.qr_orders_ducklake;
DROP TABLE qr_orders;

SET client_min_messages = warning;
SELECT duckpipe.stop_worker('qr_test');
SELECT duckpipe.drop_group('qr_test');
RESET client_min_messages;
RESET duckpipe.query_routing_log;

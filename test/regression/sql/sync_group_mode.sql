-- Test sync_group mode column (bgworker vs daemon)

-- Default group should have mode = 'bgworker'
SELECT name, mode FROM duckpipe.sync_groups WHERE name = 'default';

-- groups() SRF should include mode column
SELECT name, mode FROM duckpipe.groups() WHERE name = 'default';

-- Create a daemon-mode group
SELECT duckpipe.create_group('daemon_test', mode => 'daemon');
SELECT name, mode FROM duckpipe.sync_groups WHERE name = 'daemon_test';

-- Create a bgworker-mode group (explicit)
SELECT duckpipe.create_group('bgworker_test', mode => 'bgworker');
SELECT name, mode FROM duckpipe.sync_groups WHERE name = 'bgworker_test';

-- Default mode should be bgworker when not specified
SELECT duckpipe.create_group('default_mode_test');
SELECT name, mode FROM duckpipe.sync_groups WHERE name = 'default_mode_test';

-- Invalid mode should be rejected
SELECT duckpipe.create_group('bad_mode', mode => 'invalid');

-- add_table on daemon group should NOT start bgworker
CREATE TABLE t_daemon_mode (id int primary key, val text);
SELECT duckpipe.add_table('public.t_daemon_mode', NULL, 'daemon_test', false);

-- Verify no bgworker is running for the daemon group
SELECT count(*) FROM pg_stat_activity
WHERE backend_type = 'bg worker'
  AND query LIKE '%daemon_test%';

-- start_worker on daemon group should warn and skip
SELECT duckpipe.start_worker('daemon_test');

-- Cleanup
SELECT duckpipe.remove_table('public.t_daemon_mode', true);
DROP TABLE IF EXISTS t_daemon_mode CASCADE;
DROP TABLE IF EXISTS t_daemon_mode_ducklake CASCADE;
SELECT duckpipe.drop_group('daemon_test');
SELECT duckpipe.drop_group('bgworker_test');
SELECT duckpipe.drop_group('default_mode_test');

-- Test LISTEN/NOTIFY wakeup: worker detects add_table() immediately
-- even with a very long poll_interval.

-- Start the worker with default poll_interval (1s) so it stabilizes quickly.
SELECT duckpipe.start_worker();

-- Wait until the worker is visible in pg_stat_activity.
DO $$
DECLARE
  i int := 0;
BEGIN
  WHILE i < 50 LOOP
    IF EXISTS (SELECT 1 FROM pg_stat_activity WHERE backend_type = 'pg_duckpipe') THEN
      RETURN;
    END IF;
    PERFORM pg_sleep(0.1);
    i := i + 1;
  END LOOP;
  RAISE EXCEPTION 'worker did not appear in pg_stat_activity';
END;
$$;

-- Wait for the worker to drain any WAL backlog from previous tests.
-- The worker must complete its sync cycles before we can rely on
-- poll_interval timing. Poll worker_status until queued_changes = 0
-- and the worker has had at least two idle cycles (updated_at advances).
DO $$
DECLARE
  i int := 0;
  prev_ts timestamptz;
  cur_ts  timestamptz;
  q       bigint;
BEGIN
  WHILE i < 100 LOOP
    SELECT total_queued_changes, updated_at INTO q, cur_ts
      FROM duckpipe.worker_status();
    -- Worker is idle when queued_changes = 0 and updated_at has
    -- changed at least once (meaning a cycle completed).
    IF q = 0 AND prev_ts IS NOT NULL AND cur_ts IS DISTINCT FROM prev_ts THEN
      RETURN;
    END IF;
    prev_ts := cur_ts;
    PERFORM pg_sleep(0.2);
    i := i + 1;
  END LOOP;
  RAISE EXCEPTION 'worker did not become idle within 20s';
END;
$$;

-- Now switch to a 30s poll_interval. The worker will pick this up
-- on its next SIGHUP cycle (within ~1s of the current poll interval).
ALTER SYSTEM SET duckpipe.poll_interval = 30000;
SELECT pg_reload_conf();

-- Wait for the worker to reload config and enter the 30s sleep.
-- With default poll_interval=1s, the worker finishes its current
-- sleep within 1s, processes SIGHUP, runs one more cycle, and enters
-- the 30s sleep. 3s is plenty for this.
SELECT pg_sleep(3);

CREATE TABLE notify_src (id int primary key, val text);
INSERT INTO notify_src VALUES (1, 'one'), (2, 'two');

-- add_table fires NOTIFY duckpipe_wakeup → worker wakes immediately
-- from its 30s sleep.
SELECT duckpipe.add_table('public.notify_src');

-- 8s is plenty for snapshot + state transitions when the worker wakes
-- instantly. Without NOTIFY this would need 30s+ (poll_interval).
SELECT pg_sleep(8);

-- Verify data was copied (proves the worker woke up and processed the
-- snapshot within 8s, which would be impossible with 30s poll_interval).
SELECT * FROM public.notify_src_ducklake ORDER BY id;

-- Cleanup: restore poll_interval before stopping.
ALTER SYSTEM RESET duckpipe.poll_interval;
SELECT pg_reload_conf();

SELECT duckpipe.remove_table('public.notify_src', false);
DROP TABLE public.notify_src_ducklake;
DROP TABLE notify_src;

SET client_min_messages = warning;
SELECT duckpipe.stop_worker();
RESET client_min_messages;

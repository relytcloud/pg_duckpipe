-- Test partitioned source table support
-- Verifies: auto-detection, publish_via_partition_root, REPLICA IDENTITY on partitions,
-- streaming across partitions into single DuckLake target.

SET client_min_messages = warning;
SELECT duckpipe.start_worker();
RESET client_min_messages;

-- Create range-partitioned table with 3 partitions
CREATE TABLE part_logs (
    id serial,
    log_date date NOT NULL,
    message text,
    PRIMARY KEY (id, log_date)
) PARTITION BY RANGE (log_date);

CREATE TABLE part_logs_q1 PARTITION OF part_logs
    FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');
CREATE TABLE part_logs_q2 PARTITION OF part_logs
    FOR VALUES FROM ('2024-04-01') TO ('2024-07-01');
CREATE TABLE part_logs_q3 PARTITION OF part_logs
    FOR VALUES FROM ('2024-07-01') TO ('2024-10-01');

-- Add partitioned table (streaming only, skip snapshot)
SELECT duckpipe.add_table('public.part_logs', NULL, 'default', false);

-- Verify REPLICA IDENTITY FULL was set on all partitions
SELECT relname,
       CASE relreplident WHEN 'f' THEN 'FULL' ELSE relreplident::text END AS replica_identity
FROM pg_class
WHERE relname IN ('part_logs', 'part_logs_q1', 'part_logs_q2', 'part_logs_q3')
ORDER BY relname;

-- Verify publish_via_partition_root is set on publication
SELECT pubviaroot FROM pg_publication
WHERE pubname = (SELECT publication FROM duckpipe.sync_groups WHERE name = 'default');

-- Insert into different partitions
INSERT INTO part_logs (log_date, message) VALUES ('2024-01-15', 'Q1 entry');
INSERT INTO part_logs (log_date, message) VALUES ('2024-05-20', 'Q2 entry');
INSERT INTO part_logs (log_date, message) VALUES ('2024-08-10', 'Q3 entry');

SELECT pg_sleep(8);

-- All 3 rows should appear in single DuckLake target
SELECT count(*) AS row_count FROM public.part_logs_ducklake;

-- Verify data from all partitions
SELECT log_date, message FROM public.part_logs_ducklake ORDER BY log_date;

-- Add a new partition after add_table() — auto-inherits publication membership
CREATE TABLE part_logs_q4 PARTITION OF part_logs
    FOR VALUES FROM ('2024-10-01') TO ('2025-01-01');

-- New partitions need REPLICA IDENTITY FULL set manually
ALTER TABLE part_logs_q4 REPLICA IDENTITY FULL;

INSERT INTO part_logs (log_date, message) VALUES ('2024-11-05', 'Q4 entry');

SELECT pg_sleep(8);

-- Q4 data should appear
SELECT count(*) AS row_count_with_q4 FROM public.part_logs_ducklake;

-- Cleanup
SELECT duckpipe.remove_table('public.part_logs');
DROP TABLE public.part_logs_ducklake;
DROP TABLE part_logs;

SET client_min_messages = warning;
SELECT duckpipe.stop_worker();
RESET client_min_messages;

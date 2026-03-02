-- Test snapshot with special characters that stress CSV parsing:
-- commas, quotes, newlines, backslashes, NULLs, empty strings, Unicode.
SELECT duckpipe.start_worker();

CREATE TABLE special_chars (id int primary key, val text);

INSERT INTO special_chars VALUES
    (1, 'hello, world'),                     -- comma
    (2, 'say "hi"'),                         -- double quotes
    (3, E'line1\nline2'),                     -- embedded newline
    (4, E'line1\nline2\nline3'),              -- multiple embedded newlines
    (5, E'comma,\nand "quotes"'),             -- comma + newline + quotes together
    (6, 'backslash \ here'),                  -- backslash
    (7, E'tab\there'),                        -- tab
    (8, E'carriage\rreturn'),                 -- carriage return
    (9, ''),                                  -- empty string
    (10, NULL),                               -- NULL
    (11, ' leading and trailing spaces '),    -- whitespace
    (12, E'escaped ''single quotes'''),       -- single quotes (PG escaping)
    (13, E'mixed\n"csv,escaping"\ntest'),     -- newline + quote + comma
    (14, repeat('x', 1000)),                   -- long string
    (15, E'\u00e9\u00e8\u00ea\u00eb'),        -- accented chars (UTF-8)
    (16, E'\u4f60\u597d\u4e16\u754c'),        -- CJK characters
    (17, E'\\N');                              -- literal \N (must not become NULL)

SELECT duckpipe.add_table('public.special_chars');

SELECT pg_sleep(3);

-- Verify all rows arrived with correct values (use left() for long strings)
SELECT id, CASE WHEN length(val) > 100 THEN left(val, 20) || '...(' || length(val) || ' chars)' ELSE val END AS val
FROM public.special_chars_ducklake ORDER BY id;

-- Verify row count
SELECT count(*) FROM public.special_chars_ducklake;

-- Verify specific tricky values match source exactly
SELECT s.id, s.val = d.val AS match
FROM special_chars s
JOIN special_chars_ducklake d ON s.id = d.id
ORDER BY s.id;

-- Verify NULL is NULL
SELECT id, val IS NULL AS is_null FROM special_chars_ducklake WHERE id = 10;

-- Verify empty string is empty (not NULL)
SELECT id, val = '' AS is_empty, val IS NULL AS is_null FROM special_chars_ducklake WHERE id = 9;

-- Verify long string preserved
SELECT id, length(val) FROM special_chars_ducklake WHERE id = 14;

-- Verify literal \N is not confused with NULL
SELECT id, val, val IS NULL AS is_null FROM special_chars_ducklake WHERE id = 17;

SELECT duckpipe.remove_table('public.special_chars', false);
DROP TABLE public.special_chars_ducklake;
DROP TABLE special_chars;

SET client_min_messages = warning;
SELECT duckpipe.stop_worker();
RESET client_min_messages;

-- Test that add_table on a nonexistent table reports "does not exist"
-- (not the misleading "no primary key" error)

-- Should fail with "table does not exist", not "no primary key"
SELECT duckpipe.add_table('public.this_table_does_not_exist');

-- Schema-qualified nonexistent table
SELECT duckpipe.add_table('nosuchschema.notable');

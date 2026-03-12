#!/usr/bin/sysbench
-- oltp_insert_heavy: 90% INSERT, 10% UPDATE per transaction
--
-- Each transaction does:
--   9 x INSERT (new rows, auto-increment)
--   1 x UPDATE (random existing row, index column)
--
-- This produces a realistic mixed workload where inserts dominate
-- but the CDC pipeline must also handle updates correctly.

require("oltp_common")

sysbench.cmdline.commands.prepare = {
   function ()
      if (not sysbench.opt.auto_inc) then
         sysbench.opt.table_size=0
      end
      cmd_prepare()
   end,
   sysbench.cmdline.PARALLEL_COMMAND
}

function prepare_statements()
   prepare_index_updates()
end

function event()
   local table_name = "sbtest" .. sysbench.rand.uniform(1, sysbench.opt.tables)

   -- 9 INSERTs (new rows)
   for i = 1, 9 do
      local k_val = sysbench.rand.default(1, sysbench.opt.table_size)
      local c_val = get_c_value()
      local pad_val = get_pad_value()

      con:query(string.format("INSERT INTO %s (k, c, pad) VALUES (%d, '%s', '%s')",
                              table_name, k_val, c_val, pad_val))
   end

   -- 1 UPDATE (index column on random existing row)
   execute_index_updates()
end

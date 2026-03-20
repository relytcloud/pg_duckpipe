//! Planner hook for transparent analytical query routing.
//!
//! When `duckpipe.query_routing` is `on` or `auto`, rewrites RangeTblEntry OIDs
//! in SELECT queries so that synced source tables are redirected to their
//! DuckLake targets.  After rewriting, pg_ducklake/pg_duckdb see the DuckLake
//! table and handle execution in DuckDB automatically.

use std::collections::HashMap;
use std::time::Instant;

use pgrx::pg_sys;
use pgrx::prelude::*;

use crate::{QueryRoutingMode, QUERY_ROUTING, QUERY_ROUTING_LOG};

// ---------------------------------------------------------------------------
// GUC access helpers (bypass pgrx thread-safety check)
//
// The planner hook may fire in bgworker flush threads (via SPI → planner).
// `GucSetting::get()` calls `check_active_thread()` which panics on non-main
// threads.  Reading the raw Cell value is safe: PG GUCs are per-process
// globals and the planner runs single-threaded within a backend.
// ---------------------------------------------------------------------------

fn get_routing_mode() -> QueryRoutingMode {
    let raw = unsafe { *QUERY_ROUTING.as_ptr() };
    // Ordinals match the PostgresGucEnum derive order in lib.rs:
    // Off=0, On=1, Auto=2
    match raw {
        1 => QueryRoutingMode::On,
        2 => QueryRoutingMode::Auto,
        _ => QueryRoutingMode::Off,
    }
}

fn get_routing_log() -> bool {
    unsafe { *QUERY_ROUTING_LOG.as_ptr() }
}

// ---------------------------------------------------------------------------
// Hook installation
// ---------------------------------------------------------------------------

static mut PREV_PLANNER_HOOK: pg_sys::planner_hook_type = None;

/// Re-entrancy guard: prevents infinite recursion when the planner hook's
/// own SPI cache-refresh query triggers the planner again.  Uses RAII so
/// the flag is always reset, even if a PostgreSQL ERROR (longjmp) unwinds
/// through `#[pg_guard]`.
static mut IN_ROUTING_HOOK: bool = false;

struct ReentrancyGuard;

impl ReentrancyGuard {
    /// Returns `Some(guard)` if not already inside the hook, `None` otherwise.
    /// The guard resets `IN_ROUTING_HOOK` on drop (including during unwind).
    unsafe fn try_enter() -> Option<Self> {
        if IN_ROUTING_HOOK {
            None
        } else {
            IN_ROUTING_HOOK = true;
            Some(Self)
        }
    }
}

impl Drop for ReentrancyGuard {
    fn drop(&mut self) {
        unsafe {
            IN_ROUTING_HOOK = false;
        }
    }
}

/// Save the current planner_hook as prev and install ours.
/// Called once from `_PG_init`.
pub fn install_planner_hook() {
    unsafe {
        PREV_PLANNER_HOOK = pg_sys::planner_hook;
        pg_sys::planner_hook = Some(duckpipe_planner_hook);
    }
}

/// Planner hook entry point.  Matches the `planner_hook_type` signature.
#[pg_guard]
unsafe extern "C-unwind" fn duckpipe_planner_hook(
    parse: *mut pg_sys::Query,
    query_string: *const ::core::ffi::c_char,
    cursor_options: ::core::ffi::c_int,
    bound_params: pg_sys::ParamListInfo,
) -> *mut pg_sys::PlannedStmt {
    // Re-entrancy guard: skip when we're already inside the routing hook
    // (e.g. SPI cache refresh queries trigger the planner recursively).
    // RAII ensures the flag is reset even on PostgreSQL ERROR (longjmp).
    let _guard = match ReentrancyGuard::try_enter() {
        Some(g) => g,
        None => return call_next_hook(parse, query_string, cursor_options, bound_params),
    };

    // Skip routing if not loaded via shared_preload_libraries.
    // The planner hook chain (duckpipe → ducklake → duckdb) and the
    // SPI cache queries against duckpipe.table_mappings both require
    // the extension to be fully initialized in the postmaster.
    if !crate::shmem_available() {
        return call_next_hook(parse, query_string, cursor_options, bound_params);
    }

    let mode = get_routing_mode();

    // Fast path: routing disabled
    if matches!(mode, QueryRoutingMode::Off) {
        return call_next_hook(parse, query_string, cursor_options, bound_params);
    }

    // Only rewrite SELECT queries
    if (*parse).commandType != pg_sys::CmdType::CMD_SELECT {
        return call_next_hook(parse, query_string, cursor_options, bound_params);
    }

    // Skip utility statements (e.g. EXPLAIN wrapping)
    if !(*parse).utilityStmt.is_null() {
        return call_next_hook(parse, query_string, cursor_options, bound_params);
    }

    try_rewrite_query(parse, mode);

    call_next_hook(parse, query_string, cursor_options, bound_params)
}

/// Chain to previous hook or standard_planner.
unsafe fn call_next_hook(
    parse: *mut pg_sys::Query,
    query_string: *const ::core::ffi::c_char,
    cursor_options: ::core::ffi::c_int,
    bound_params: pg_sys::ParamListInfo,
) -> *mut pg_sys::PlannedStmt {
    match PREV_PLANNER_HOOK {
        Some(prev) => prev(parse, query_string, cursor_options, bound_params),
        None => pg_sys::standard_planner(parse, query_string, cursor_options, bound_params),
    }
}

// ---------------------------------------------------------------------------
// Query rewriting
// ---------------------------------------------------------------------------

/// Top-level entry: fetch cache, then delegate to shared rewrite logic.
#[allow(static_mut_refs)]
unsafe fn try_rewrite_query(parse: *mut pg_sys::Query, mode: QueryRoutingMode) {
    let cache = ROUTING_CACHE.get_or_refresh();
    rewrite_query_with_cache(parse, mode, cache);
}

/// Rewrite a query's RTEs using the provided cache.  Used for both the
/// top-level query and recursive subquery/CTE rewriting.
unsafe fn rewrite_query_with_cache(
    parse: *mut pg_sys::Query,
    mode: QueryRoutingMode,
    cache: &RoutingCacheData,
) {
    let rtable = (*parse).rtable;
    if rtable.is_null() || cache.entries.is_empty() {
        return;
    }

    // Single pass: collect routed RTEs with their varno (1-based index).
    let len = (*rtable).length;
    let mut routed: Vec<(pg_sys::Oid, i32)> = Vec::new(); // (source_oid, varno)

    for i in 0..len {
        let rte = list_nth_rte(rtable, i);
        if (*rte).rtekind == pg_sys::RTEKind::RTE_RELATION
            && cache.entries.contains_key(&(*rte).relid)
        {
            routed.push(((*rte).relid, i + 1)); // varno is 1-based
        }
    }

    if routed.is_empty() {
        return;
    }

    // Auto mode: check if this is a point lookup (skip routing)
    if matches!(mode, QueryRoutingMode::Auto) && is_point_lookup(parse, &routed, cache) {
        pgrx::debug1!("duckpipe: skipping routing (point lookup detected)");
        return;
    }

    let log_notices = get_routing_log();

    // Rewrite matching RTEs
    for i in 0..len {
        let rte = list_nth_rte(rtable, i);
        if (*rte).rtekind != pg_sys::RTEKind::RTE_RELATION {
            continue;
        }
        if let Some(entry) = cache.entries.get(&(*rte).relid) {
            let source_oid = (*rte).relid;
            let target_oid = entry.target_oid;

            let src_name = oid_to_qualified_name(source_oid);
            let tgt_name = oid_to_qualified_name(target_oid);
            if log_notices {
                pgrx::notice!("duckpipe: routing {} \u{2192} {}", src_name, tgt_name);
            }
            pgrx::debug1!("duckpipe: routing {} -> {}", src_name, tgt_name);

            // Rewrite the RTE.  Both source (heap) and target (DuckLake via
            // custom TAM) have relkind='r', so no relkind update needed.
            (*rte).relid = target_oid;

            // Update RTEPermissionInfo (PG16+)
            rewrite_perminfo(parse, rte, target_oid);
        }
    }

    // Recurse into subqueries and CTEs
    rewrite_subqueries(parse, mode, cache);
}

/// Update the RTEPermissionInfo entry for a rewritten RTE.
///
/// PG16 extracted per-RTE permission fields (requiredPerms, checkAsUser,
/// selectedCols, etc.) from RangeTblEntry into a separate RTEPermissionInfo
/// struct, indexed via `rte.perminfoindex` into `query.rteperminfos`.
/// After rewriting `rte.relid`, we must also update the corresponding
/// RTEPermissionInfo.relid so PG's ACL checks target the correct table.
///
/// On PG14/PG15 these fields live directly on the RTE and are covered by
/// the `rte.relid` rewrite — no separate update needed.
#[cfg(any(feature = "pg16", feature = "pg17", feature = "pg18"))]
unsafe fn rewrite_perminfo(
    parse: *mut pg_sys::Query,
    rte: *mut pg_sys::RangeTblEntry,
    target_oid: pg_sys::Oid,
) {
    let perminfoindex = (*rte).perminfoindex;
    if perminfoindex > 0 {
        let perminfos = (*parse).rteperminfos;
        if !perminfos.is_null() && (perminfoindex as i32) <= (*perminfos).length {
            let pi_cell = &*(*perminfos).elements.add((perminfoindex - 1) as usize);
            let pi = pi_cell.ptr_value as *mut pg_sys::RTEPermissionInfo;
            if !pi.is_null() {
                (*pi).relid = target_oid;
            }
        }
    }
}

/// PG14/PG15: permissions live directly on the RTE — no separate update needed.
#[cfg(any(feature = "pg14", feature = "pg15"))]
unsafe fn rewrite_perminfo(
    _parse: *mut pg_sys::Query,
    _rte: *mut pg_sys::RangeTblEntry,
    _target_oid: pg_sys::Oid,
) {
}

/// Recurse into subquery RTEs and CTEs to rewrite nested queries.
unsafe fn rewrite_subqueries(
    parse: *mut pg_sys::Query,
    mode: QueryRoutingMode,
    cache: &RoutingCacheData,
) {
    let rtable = (*parse).rtable;
    if !rtable.is_null() {
        for i in 0..(*rtable).length {
            let rte = list_nth_rte(rtable, i);
            if (*rte).rtekind == pg_sys::RTEKind::RTE_SUBQUERY && !(*rte).subquery.is_null() {
                let subquery = (*rte).subquery;
                if (*subquery).commandType == pg_sys::CmdType::CMD_SELECT {
                    rewrite_query_with_cache(subquery, mode, cache);
                }
            }
        }
    }

    // CTEs
    let cte_list = (*parse).cteList;
    if !cte_list.is_null() {
        for i in 0..(*cte_list).length {
            let cell = &*(*cte_list).elements.add(i as usize);
            let cte = cell.ptr_value as *mut pg_sys::CommonTableExpr;
            if !cte.is_null() && !(*cte).ctequery.is_null() {
                let cte_node = (*cte).ctequery;
                if node_tag(cte_node) == pg_sys::NodeTag::T_Query {
                    let cte_query = cte_node as *mut pg_sys::Query;
                    if (*cte_query).commandType == pg_sys::CmdType::CMD_SELECT {
                        rewrite_query_with_cache(cte_query, mode, cache);
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Auto-mode: point lookup detection
// ---------------------------------------------------------------------------

/// Returns true if the query is a simple point lookup on PK columns of
/// a routed table — i.e. it should NOT be routed to DuckLake.
///
/// A point lookup is detected when ALL of:
/// 1. WHERE clause exists with equality on all PK columns of a routed table
/// 2. No aggregation, GROUP BY, DISTINCT, or window functions
///
/// `routed` contains `(source_oid, varno)` pairs from the first pass.
unsafe fn is_point_lookup(
    parse: *mut pg_sys::Query,
    routed: &[(pg_sys::Oid, i32)],
    cache: &RoutingCacheData,
) -> bool {
    // If query has aggregation, GROUP BY, DISTINCT, or window functions → not a point lookup
    if (*parse).hasAggs || (*parse).hasWindowFuncs {
        return false;
    }
    if !(*parse).groupClause.is_null() && (*(*parse).groupClause).length > 0 {
        return false;
    }
    if !(*parse).distinctClause.is_null() && (*(*parse).distinctClause).length > 0 {
        return false;
    }

    // Need a WHERE clause
    let jointree = (*parse).jointree;
    if jointree.is_null() || (*jointree).quals.is_null() {
        return false;
    }

    // For each routed table, check if all PK columns have equality constraints
    for &(source_oid, varno) in routed {
        if let Some(entry) = cache.entries.get(&source_oid) {
            if entry.pk_attnum.is_empty() {
                continue;
            }

            // Check that all PK columns have equality in quals
            let mut pk_covered: Vec<bool> = vec![false; entry.pk_attnum.len()];
            collect_eq_attnums((*jointree).quals, varno, &entry.pk_attnum, &mut pk_covered);

            if pk_covered.iter().all(|&c| c) {
                return true;
            }
        }
    }

    false
}

/// Walk a qual expression tree to find equality ops on specific (varno, attnum) pairs.
/// Marks the corresponding pk_covered entry as true.
unsafe fn collect_eq_attnums(
    node: *mut pg_sys::Node,
    target_varno: i32,
    pk_attnums: &[i16],
    pk_covered: &mut [bool],
) {
    if node.is_null() {
        return;
    }

    let tag = node_tag(node);

    match tag {
        pg_sys::NodeTag::T_OpExpr => {
            let op = node as *mut pg_sys::OpExpr;
            if is_equality_op((*op).opno) {
                let args = (*op).args;
                if !args.is_null() && (*args).length == 2 {
                    let left = (*(*args).elements.add(0)).ptr_value as *mut pg_sys::Node;
                    let right = (*(*args).elements.add(1)).ptr_value as *mut pg_sys::Node;

                    // Check if either side is a Var referencing our table's PK column
                    check_var_for_pk(left, target_varno, pk_attnums, pk_covered);
                    check_var_for_pk(right, target_varno, pk_attnums, pk_covered);
                }
            }
        }
        pg_sys::NodeTag::T_BoolExpr => {
            let boolexpr = node as *mut pg_sys::BoolExpr;
            // Only AND expressions propagate PK coverage
            if (*boolexpr).boolop == pg_sys::BoolExprType::AND_EXPR {
                let args = (*boolexpr).args;
                if !args.is_null() {
                    for i in 0..(*args).length {
                        let arg =
                            (*(*args).elements.add(i as usize)).ptr_value as *mut pg_sys::Node;
                        collect_eq_attnums(arg, target_varno, pk_attnums, pk_covered);
                    }
                }
            }
        }
        pg_sys::NodeTag::T_ScalarArrayOpExpr => {
            // Handle `col = ANY(ARRAY[...])` or `col IN (...)` — treat as equality
            // on the Var side for PK detection purposes.
            let saop = node as *const pg_sys::ScalarArrayOpExpr;
            if (*saop).useOr && is_equality_op((*saop).opno) {
                let args = (*saop).args;
                if !args.is_null() && (*args).length == 2 {
                    let left = (*(*args).elements.add(0)).ptr_value as *mut pg_sys::Node;
                    check_var_for_pk(left, target_varno, pk_attnums, pk_covered);
                }
            }
        }
        _ => {}
    }
}

/// If `node` is a Var referencing (target_varno, pk_attnum), mark it covered.
unsafe fn check_var_for_pk(
    node: *mut pg_sys::Node,
    target_varno: i32,
    pk_attnums: &[i16],
    pk_covered: &mut [bool],
) {
    if node.is_null() || node_tag(node) != pg_sys::NodeTag::T_Var {
        return;
    }
    let var = node as *mut pg_sys::Var;
    if (*var).varno as i32 == target_varno {
        let attno = (*var).varattno;
        for (idx, &pk_att) in pk_attnums.iter().enumerate() {
            if attno == pk_att {
                pk_covered[idx] = true;
            }
        }
    }
}

/// Check if an operator OID is an equality operator via pg catalog.
unsafe fn is_equality_op(opno: pg_sys::Oid) -> bool {
    let name_ptr = pg_sys::get_opname(opno);
    if name_ptr.is_null() {
        return false;
    }
    let name = std::ffi::CStr::from_ptr(name_ptr);
    name.to_bytes() == b"="
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Get the NodeTag from a Node pointer.
unsafe fn node_tag(node: *mut pg_sys::Node) -> pg_sys::NodeTag {
    (*node).type_
}

/// Get the i-th RangeTblEntry from a pg_sys::List (0-based index).
unsafe fn list_nth_rte(list: *mut pg_sys::List, i: i32) -> *mut pg_sys::RangeTblEntry {
    let cell = &*(*list).elements.add(i as usize);
    cell.ptr_value as *mut pg_sys::RangeTblEntry
}

/// Convert an OID to a qualified "schema.table" name for logging.
unsafe fn oid_to_qualified_name(oid: pg_sys::Oid) -> String {
    let ns_oid = pg_sys::get_rel_namespace(oid);
    let ns_name = pg_sys::get_namespace_name(ns_oid);
    let rel_name = pg_sys::get_rel_name(oid);

    let schema = if ns_name.is_null() {
        "??".to_string()
    } else {
        std::ffi::CStr::from_ptr(ns_name)
            .to_string_lossy()
            .to_string()
    };
    let table = if rel_name.is_null() {
        "??".to_string()
    } else {
        std::ffi::CStr::from_ptr(rel_name)
            .to_string_lossy()
            .to_string()
    };

    format!("{}.{}", schema, table)
}

// ---------------------------------------------------------------------------
// Routing cache
// ---------------------------------------------------------------------------

struct RoutingCache {
    data: Option<RoutingCacheData>,
    last_refresh: Option<Instant>,
}

struct RoutingCacheData {
    entries: HashMap<pg_sys::Oid, RoutingEntry>,
}

struct RoutingEntry {
    target_oid: pg_sys::Oid,
    pk_attnum: Vec<i16>, // AttrNumber values for PK columns
}

/// Cache TTL in seconds.  The SPI refresh is cheap (~1ms catalog lookup)
/// but we avoid running it on every query.  5s is a good balance between
/// responsiveness (set_routing takes effect within 5s) and overhead.
const CACHE_TTL_SECS: u64 = 5;

static mut ROUTING_CACHE: RoutingCache = RoutingCache {
    data: None,
    last_refresh: None,
};

impl RoutingCache {
    /// Return cached data, refreshing via SPI if stale or empty.
    fn get_or_refresh(&mut self) -> &RoutingCacheData {
        let needs_refresh = match self.last_refresh {
            Some(t) => t.elapsed().as_secs() >= CACHE_TTL_SECS,
            None => true,
        };

        if needs_refresh {
            self.refresh();
        }

        self.data.as_ref().unwrap()
    }

    fn refresh(&mut self) {
        let mut entries = HashMap::new();

        // Single SPI query: OID mappings + PK columns via LEFT JOIN.
        // Fetches STREAMING, enabled, routing_enabled tables with their PK attnums.
        let rows: Vec<(i64, i64, Option<i16>)> = Spi::connect(|client| {
            let mut result_rows = Vec::new();
            let result = client.select(
                "SELECT tm.source_oid, c.oid::int8 AS target_oid, \
                        pk.attnum::int2 AS pk_attnum \
                 FROM duckpipe.table_mappings tm \
                 JOIN pg_class c ON c.relname = tm.target_table \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                      AND n.nspname = tm.target_schema \
                 LEFT JOIN LATERAL ( \
                     SELECT unnest(i.indkey) AS attnum \
                     FROM pg_index i \
                     WHERE i.indisprimary AND i.indrelid = tm.source_oid::oid \
                 ) pk ON true \
                 WHERE tm.state = 'STREAMING' AND tm.enabled AND tm.routing_enabled \
                   AND tm.source_oid IS NOT NULL",
                None,
                &[],
            );
            if let Ok(tuptable) = result {
                for row in tuptable {
                    if let (Some(src), Some(tgt)) = (
                        row.get::<i64>(1).unwrap_or(None),
                        row.get::<i64>(2).unwrap_or(None),
                    ) {
                        let pk_att = row.get::<i16>(3).unwrap_or(None);
                        result_rows.push((src, tgt, pk_att));
                    }
                }
            }
            result_rows
        });

        // Build entries from joined result
        for &(src, tgt, pk_att) in &rows {
            let source_oid = pg_sys::Oid::from_u32(src as u32);
            let target_oid = pg_sys::Oid::from_u32(tgt as u32);
            let entry = entries.entry(source_oid).or_insert_with(|| RoutingEntry {
                target_oid,
                pk_attnum: Vec::new(),
            });
            if let Some(attnum) = pk_att {
                if !entry.pk_attnum.contains(&attnum) {
                    entry.pk_attnum.push(attnum);
                }
            }
        }

        self.data = Some(RoutingCacheData { entries });
        self.last_refresh = Some(Instant::now());
    }
}

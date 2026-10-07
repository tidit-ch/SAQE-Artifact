//! Runs one of the 14 official-numbering queries (q1_sq..q17_sq, minus the
//! tdwithin-gap q6/q10/q16 - see BENCHMARK.md) N times against all three
//! SAQE backends (CSV, Parquet, Postgres pushdown) - the consolidated
//! counterpart to the old per-query bench_saqe_vs_mobilitydb_timed_qN.rs
//! binaries (see build/mobilitydb/NOTES.md, "Timing methodology" for why
//! this isn't Criterion), parameterized by query number instead of one
//! binary per query.
//!
//! Two modes:
//!   cargo bench --bench bench_saqe_vs_mobilitydb_timed_all -- <query_num> <scale> <trials>
//!     Normal timed mode - same load->query->clean, N-trial cycle every
//!     other timed binary in this project uses.
//!   cargo bench --bench bench_saqe_vs_mobilitydb_timed_all -- <query_num> <scale> --check
//!     Correctness-gate mode: runs the query once against each backend and
//!     prints only its row count (`RESULT: csv=X parquet=Y postgres=Z`) -
//!     used by run_benchmark_comparison_17.sh to verify all backends agree
//!     (and return more than 0 rows) before any timing is trusted.

mod saqe_vs_mobilitydb;

use datafusion::prelude::SessionContext;
use saqe_vs_mobilitydb::{
    legacy_tdwithin_gap::{q10_sq, q16_sq, q6_sq},
    q11_sq, q12_sq, q13_sq, q14_sq, q15_sq, q17_sq, q1_sq, q21_sq, q22_sq, q2_sq, q3_sq, q4_sq,
    q5_sq, q7_sq, q8_sq, q9_sq, TripsSecondHalf,
};

/// One query's full description: its SQL text, the *base* Postgres tables
/// load_postgres()/clean_postgres() must load/clean (never a sample-view
/// name - periods1/regions1/etc. are plain Postgres VIEWS over their base
/// table, not writable tables), and the full catalog list (base tables +
/// sample views) every query context (CSV/Parquet/Postgres) needs
/// registered, since the SQL text itself references the sample-view names
/// directly.
/// How a query is executed.
///
/// `Standard` runs against all three SAQE backends in turn (CSV, Parquet, Postgres) - the 14
/// official-numbering queries. `Mixed` is a single run whose trips table is split across two
/// stores (q21/q22), so there is no per-backend fan-out and no MobilityDB counterpart.
enum QueryKind {
    Standard,
    Mixed(TripsSecondHalf),
}

struct QueryInfo {
    sql: &'static str,
    kind: QueryKind,

    // required base tables
    load_tables: &'static [&'static str],

    // exact names the SQL text references (base table or sample view)
    catalog_tables: &'static [&'static str],
}

fn query_registry(n: u32) -> QueryInfo {
    match n {
        1 => QueryInfo {
            sql: q1_sq::QUERY_Q1_SQ,
            kind: QueryKind::Standard,
            load_tables: &["datamcar", "licences"],
            catalog_tables: &["datamcar", "licences"],
        },
        2 => QueryInfo {
            sql: q2_sq::QUERY_Q2_SQ,
            kind: QueryKind::Standard,
            load_tables: &["datamcar"],
            catalog_tables: &["datamcar"],
        },
        3 => QueryInfo {
            sql: q3_sq::QUERY_Q3_SQ,
            kind: QueryKind::Standard,
            load_tables: &["trips", "instants", "licences", "datamcar"],
            catalog_tables: &["trips", "instants1", "licences1", "datamcar"],
        },
        4 => QueryInfo {
            sql: q4_sq::QUERY_Q4_SQ,
            kind: QueryKind::Standard,
            load_tables: &["trips", "datamcar", "points"],
            catalog_tables: &["trips", "datamcar", "points"],
        },
        5 => QueryInfo {
            sql: q5_sq::QUERY_Q5_SQ,
            kind: QueryKind::Standard,
            load_tables: &["trips", "licences", "datamcar"],
            catalog_tables: &["trips", "licences1", "licences2", "datamcar"],
        },
        7 => QueryInfo {
            sql: q7_sq::QUERY_Q7_SQ,
            kind: QueryKind::Standard,
            load_tables: &["trips", "datamcar", "points"],
            catalog_tables: &["trips", "datamcar", "points1"],
        },
        8 => QueryInfo {
            sql: q8_sq::QUERY_Q8_SQ,
            kind: QueryKind::Standard,
            load_tables: &["trips", "licences", "periods", "datamcar"],
            catalog_tables: &["trips", "licences1", "periods1", "datamcar"],
        },
        9 => QueryInfo {
            sql: q9_sq::QUERY_Q9_SQ,
            kind: QueryKind::Standard,
            load_tables: &["trips", "periods"],
            catalog_tables: &["trips", "periods"],
        },
        11 => QueryInfo {
            sql: q11_sq::QUERY_Q11_SQ,
            kind: QueryKind::Standard,
            load_tables: &["trips", "datamcar", "points", "instants"],
            catalog_tables: &["trips", "datamcar", "points1", "instants1"],
        },
        12 => QueryInfo {
            sql: q12_sq::QUERY_Q12_SQ,
            kind: QueryKind::Standard,
            load_tables: &["trips", "datamcar", "points", "instants"],
            catalog_tables: &["trips", "datamcar", "points1", "instants1"],
        },
        13 => QueryInfo {
            sql: q13_sq::QUERY_Q13_SQ,
            kind: QueryKind::Standard,
            load_tables: &["trips", "datamcar", "periods", "regions"],
            catalog_tables: &["trips", "datamcar", "periods1", "regions1"],
        },
        14 => QueryInfo {
            sql: q14_sq::QUERY_Q14_SQ,
            kind: QueryKind::Standard,
            load_tables: &["trips", "datamcar", "instants", "regions"],
            catalog_tables: &["trips", "datamcar", "instants1", "regions1"],
        },
        15 => QueryInfo {
            sql: q15_sq::QUERY_Q15_SQ,
            kind: QueryKind::Standard,
            load_tables: &["trips", "datamcar", "periods", "points"],
            catalog_tables: &["trips", "datamcar", "periods1", "points1"],
        },
        17 => QueryInfo {
            sql: q17_sq::QUERY_Q17_SQ,
            kind: QueryKind::Standard,
            load_tables: &["trips", "points"],
            catalog_tables: &["trips", "points"],
        },
        // q6/q10/q16: the tdwithin-capability-gap set (see
        // benches/saqe_vs_mobilitydb/legacy_tdwithin_gap/ and
        // build/mobilitydb/benchmarks/sql_scripts/queries/legacy_tdwithin_gap/).
        // Excluded from the 14-query comparable set's correctness gate since
        // SAQE's output isn't expected to byte-match MobilityDB's for these
        // (documented, approximate capability gap, not a bug) - but still
        // registered here so run_benchmark_comparison_tdwithin_gap.sh can
        // record row counts and time them the same way as the other 14.
        6 => QueryInfo {
            sql: q6_sq::QUERY_Q6_SQ,
            kind: QueryKind::Standard,
            load_tables: &["trips", "datamcar"],
            catalog_tables: &["trips", "datamcar"],
        },
        10 => QueryInfo {
            sql: q10_sq::QUERY_Q10_SQ,
            kind: QueryKind::Standard,
            load_tables: &["trips", "licences", "datamcar"],
            catalog_tables: &["trips", "licences1", "licences2", "datamcar"],
        },
        16 => QueryInfo {
            sql: q16_sq::QUERY_Q16_SQ,
            kind: QueryKind::Standard,
            load_tables: &["trips", "licences", "datamcar", "periods", "regions"],
            catalog_tables: &[
                "trips",
                "licences1",
                "licences2",
                "datamcar",
                "periods1",
                "regions1",
            ],
        },
        // q21/q22: q13 with its trips split across two stores. load_tables omits trips - the
        // Postgres half lives in its own table (pg.public.trips_half), created and dropped by
        // setup_mixed_trips()/clean_mixed_trips() rather than by load_postgres().
        21 => QueryInfo {
            sql: q21_sq::QUERY_Q21_SQ,
            kind: QueryKind::Mixed(TripsSecondHalf::Parquet),
            load_tables: &["datamcar", "periods", "regions"],
            catalog_tables: &["trips", "datamcar", "periods1", "regions1"],
        },
        22 => QueryInfo {
            sql: q22_sq::QUERY_Q22_SQ,
            kind: QueryKind::Mixed(TripsSecondHalf::Postgres),
            load_tables: &["datamcar", "periods", "regions"],
            catalog_tables: &["trips", "datamcar", "periods1", "regions1"],
        },
        other => panic!("q{other} is not a valid query number (expected 1-17, 21 or 22)"),
    }
}

async fn row_count(ctx: &SessionContext, sql: &str) -> usize {
    let df = ctx.sql(sql).await.expect("query failed to plan");
    let batches = df.collect().await.expect("query failed to execute");
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Correctness-gate run for a `Mixed` query: one context, one row count. Prints
/// `RESULT: mixed=X` - run_benchmark_comparison_17.sh compares it against q13's, which is the
/// real property being checked (same query and data, only the trips placement differs).
async fn run_check_mixed(info: &QueryInfo, scale: &str, second_half: TripsSecondHalf) {
    std::env::set_var("BERLINMOD_SCALE", scale);
    let (half_csv, _) = saqe_vs_mobilitydb::setup_mixed_trips(scale, second_half).await;
    let ctx = saqe_vs_mobilitydb::create_mixed_ctx(scale, &half_csv, second_half).await;
    saqe_vs_mobilitydb::print_plans(&ctx, info.sql, "mixed").await;
    let rows = row_count(&ctx, info.sql).await;
    saqe_vs_mobilitydb::clean_mixed_trips(scale, second_half).await;
    println!("RESULT: mixed={rows}");
}

/// Timed run for a `Mixed` query. Same load -> query -> clean shape as the other backends, with
/// "load" being the materialisation of both halves.
async fn run_timed_mixed(info: &QueryInfo, scale: &str, trials: u32, second_half: TripsSecondHalf) {
    std::env::set_var("BERLINMOD_SCALE", scale);
    let mut load_times = Vec::with_capacity(trials as usize);
    let mut query_times = Vec::with_capacity(trials as usize);
    let mut total_times = Vec::with_capacity(trials as usize);

    for trial in 1..=trials {
        let start = std::time::Instant::now();
        let (half_csv, _) = saqe_vs_mobilitydb::setup_mixed_trips(scale, second_half).await;
        let ctx = saqe_vs_mobilitydb::create_mixed_ctx(scale, &half_csv, second_half).await;
        let after_load = std::time::Instant::now();

        let df = ctx.sql(info.sql).await.expect("query failed to plan");
        df.collect().await.expect("query failed to execute");
        let after_query = std::time::Instant::now();

        // After the timed span closes, and before clean_mixed_trips() removes the halves the
        // plan is built against.
        if trial == 1 {
            saqe_vs_mobilitydb::print_plans(&ctx, info.sql, "mixed").await;
        }

        saqe_vs_mobilitydb::clean_mixed_trips(scale, second_half).await;

        let load_time = after_load - start;
        let query_time = after_query - after_load;
        let total_time = after_query - start;
        println!(
            "trial {trial}: load={:.6}s query={:.6}s total={:.6}s",
            load_time.as_secs_f64(),
            query_time.as_secs_f64(),
            total_time.as_secs_f64()
        );
        load_times.push(load_time);
        query_times.push(query_time);
        total_times.push(total_time);
    }

    let mean = |values: &[std::time::Duration]| -> std::time::Duration {
        values.iter().sum::<std::time::Duration>() / values.len() as u32
    };
    println!("--- mean over {trials} trials ---");
    println!("load:  {:.6}s", mean(&load_times).as_secs_f64());
    println!("query: {:.6}s", mean(&query_times).as_secs_f64());
    println!("total: {:.6}s", mean(&total_times).as_secs_f64());
}

async fn run_check(query_num: u32, scale: &str) {
    let info = query_registry(query_num);
    std::env::set_var("BERLINMOD_SCALE", scale);

    if let QueryKind::Mixed(second_half) = info.kind {
        run_check_mixed(&info, scale, second_half).await;
        return;
    }

    let csv_ctx = saqe_vs_mobilitydb::create_ctx().await;
    saqe_vs_mobilitydb::print_plans(&csv_ctx, info.sql, "csv").await;
    let csv_rows = row_count(&csv_ctx, info.sql).await;

    saqe_vs_mobilitydb::load_parquet(scale, info.catalog_tables).await;
    let parquet_ctx =
        saqe_vs_mobilitydb::create_parquet_query_ctx(scale, info.catalog_tables).await;
    let parquet_sql = info.sql.replace("csv.berlinmod.", "");
    saqe_vs_mobilitydb::print_plans(&parquet_ctx, &parquet_sql, "parquet").await;
    let parquet_rows = row_count(&parquet_ctx, &parquet_sql).await;
    saqe_vs_mobilitydb::clean_parquet(scale).await;

    saqe_vs_mobilitydb::clean_postgres(info.load_tables).await;
    saqe_vs_mobilitydb::load_postgres(info.load_tables).await;
    let postgres_ctx = saqe_vs_mobilitydb::create_postgres_query_ctx(info.catalog_tables).await;
    let postgres_sql = info.sql.replace("csv.berlinmod.", "postgres.berlinmod.");
    saqe_vs_mobilitydb::print_plans(&postgres_ctx, &postgres_sql, "postgres").await;
    let postgres_rows = row_count(&postgres_ctx, &postgres_sql).await;
    saqe_vs_mobilitydb::clean_postgres(info.load_tables).await;

    // RESULT: prefix so run_benchmark_comparison_17.sh can grep this exact
    // line out of cargo's own build/run noise reliably, rather than
    // assuming it's the last line of output.
    println!("RESULT: csv={csv_rows} parquet={parquet_rows} postgres={postgres_rows}");
}

async fn run_timed_all(query_num: u32, scale: &str, trials: u32) {
    let info = query_registry(query_num);
    std::env::set_var("BERLINMOD_SCALE", scale);

    if let QueryKind::Mixed(second_half) = info.kind {
        println!("--- SAQE: mixed trips source ---");
        run_timed_mixed(&info, scale, trials, second_half).await;
        return;
    }

    println!("--- SAQE: CSV ---");
    saqe_vs_mobilitydb::run_timed(info.sql, trials).await;

    println!("--- SAQE: Parquet ---");
    saqe_vs_mobilitydb::run_timed_parquet(info.sql, trials, info.catalog_tables).await;

    println!("--- SAQE: Postgres ---");
    // Not using the shared run_timed_postgres() helper here: it takes a
    // single `tables` list for both load_postgres()/clean_postgres() (which
    // must only ever see base tables - periods1/regions1/etc. are plain
    // Postgres VIEWS, not writable tables) and create_postgres_query_ctx()
    // (which needs the sample-view names too, since the query text
    // references them directly). The old bespoke q1-q4 queries never used
    // sample views, so that helper's single-list design never surfaced
    // this; most of the 14 official queries do, so load_tables and
    // catalog_tables are threaded through separately here instead.
    saqe_vs_mobilitydb::clean_postgres(info.load_tables).await;
    let mut load_times = Vec::with_capacity(trials as usize);
    let mut query_times = Vec::with_capacity(trials as usize);
    let mut total_times = Vec::with_capacity(trials as usize);
    for trial in 1..=trials {
        let start = std::time::Instant::now();
        saqe_vs_mobilitydb::load_postgres(info.load_tables).await;
        let after_load = std::time::Instant::now();

        let ctx = saqe_vs_mobilitydb::create_postgres_query_ctx(info.catalog_tables).await;
        let sql = info.sql.replace("csv.berlinmod.", "postgres.berlinmod.");
        let df = ctx.sql(&sql).await.expect("query failed to plan");
        df.collect().await.expect("query failed to execute");
        let after_query = std::time::Instant::now();

        // After the timed span closes, and before clean_postgres() empties the tables the plan
        // is built against.
        if trial == 1 {
            saqe_vs_mobilitydb::print_plans(&ctx, &sql, "postgres").await;
        }

        saqe_vs_mobilitydb::clean_postgres(info.load_tables).await;

        let load_time = after_load - start;
        let query_time = after_query - after_load;
        let total_time = after_query - start;
        println!(
            "trial {trial}: load={:.6}s query={:.6}s total={:.6}s",
            load_time.as_secs_f64(),
            query_time.as_secs_f64(),
            total_time.as_secs_f64()
        );
        load_times.push(load_time);
        query_times.push(query_time);
        total_times.push(total_time);
    }

    let mean = |values: &[std::time::Duration]| -> std::time::Duration {
        values.iter().sum::<std::time::Duration>() / values.len() as u32
    };
    println!("--- mean over {trials} trials ---");
    println!("load:  {:.6}s", mean(&load_times).as_secs_f64());
    println!("query: {:.6}s", mean(&query_times).as_secs_f64());
    println!("total: {:.6}s", mean(&total_times).as_secs_f64());
}

#[tokio::main]
async fn main() {
    const USAGE: &str =
        "usage: bench_saqe_vs_mobilitydb_timed_all -- <query_num> <scale> <trials|--check>";
    let query_num: u32 = std::env::args()
        .nth(1)
        .expect(USAGE)
        .parse()
        .expect("query_num must be a number"); // get the query number from the command-line arguments
    let scale = std::env::args().nth(2).expect(USAGE); // get the scale from the command-line arguments
    let mode = std::env::args().nth(3).expect(USAGE); // get the mode from the command-line arguments

    if mode == "--check" {
        run_check(query_num, &scale).await;
    } else {
        // if mode is not --check then it is expected to contain the trials
        let trials: u32 = mode.parse().expect("trials must be a number or --check");
        run_timed_all(query_num, &scale, trials).await;
    }
}

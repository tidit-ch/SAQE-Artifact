use criterion::{criterion_group, BenchmarkId, Criterion};
use datafusion::prelude::SessionContext;
use tokio::runtime::Runtime;

// Official BerlinMOD-MobilityDB Query 3 (docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html)
// Vehicle positions at specific times. Mirrors
// build/mobilitydb/benchmarks/sql_scripts/queries/q3_mb.sql, including its
// exact SELECT list (Licence, InstantId, Instant, Pos - no trip/vehicle id
// column, unlike an earlier version of this query) and ORDER BY. Note: this
// differs from benches/csv_benchmarks/q3.rs (which reads the full
// `instants` table, not the `instants1` 10-row sample) - that file predates
// this query set and doesn't match the official docs' own Q3 (which uses
// Instants1); fixed here to match both the docs and q3_mb.sql.
//
// DISTINCT is required here (q3_mb.sql already has it) - querylicences.csv
// has duplicate licence-plate strings under different sample ids (e.g.
// "B-YI 65"/"B-UB 100" both appear twice within the first 10 rows), so
// without it, every match found via a duplicated plate is double-counted.
// This only became visible once tested against real (non-empty) matches -
// see BENCHMARK.md's querylicences.csv finding for why scale 0.2 never
// exercised this (zero overlap there masked it as a trivial 0=0 "match").
//
// Joins trips.moid directly to licences1.moid, matching the official docs'
// query text (T.VehId = L.VehId) - licences1/licences2 carry a moid column
// joined in once at view-creation time (create_ctx()), not per query, the
// same way MobilityDB's Licences.VehId is populated once at load time.
pub const QUERY_Q3_SQ: &str = r#"
                        SELECT DISTINCT licences1.licence, instants1.instant_id, instant, point_at_timestamp(polyline, instant) as pos
                        FROM csv.berlinmod.trips, csv.berlinmod.licences1, csv.berlinmod.instants1
                        WHERE trips.moid = licences1.moid AND present(polyline, instant)
                        ORDER BY licences1.licence, instants1.instant_id
                        "#;

async fn run_query(ctx: &SessionContext) {
    let df = ctx.sql(QUERY_Q3_SQ).await.unwrap();
    df.collect().await.unwrap();
}

fn benchmark_sql_query(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let ctx = rt.block_on(super::create_ctx());

    *c = Criterion::default()
        .measurement_time(std::time::Duration::new(10, 0))
        .sample_size(10);

    c.bench_with_input(
        BenchmarkId::new("saqe_vs_mobilitydb", "q3_sq"),
        &ctx,
        |b, ctx| {
            b.to_async(&rt).iter(|| async {
                run_query(ctx).await;
            });
        },
    );
}

criterion_group!(bench_q3_sq, benchmark_sql_query);

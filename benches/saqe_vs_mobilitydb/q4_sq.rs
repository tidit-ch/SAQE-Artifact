use criterion::{criterion_group, BenchmarkId, Criterion};
use datafusion::prelude::SessionContext;
use tokio::runtime::Runtime;

// Official BerlinMOD-MobilityDB Query 4 (docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html)
// Vehicles passing points. Mirrors
// build/mobilitydb/benchmarks/sql_scripts/queries/q4_mb.sql, including its
// FROM/WHERE order and ORDER BY. Note: adds p.point_id to the DISTINCT list
// (csv_benchmarks/q4.rs and the docs both select PointId too) -
// querypoints.csv has a few coordinates that repeat under different point
// ids, so grouping by point value alone silently collapses those into fewer
// rows (see saqe_vs_mobilitydb/q2.rs's older, differently-numbered version
// of this same query, which documents the same issue). q4_mb.sql's bbox
// pre-filter (T.Trip && stbox(P.Geom)) has no SAQE equivalent - passes_point
// does its own segment-distance check directly, with no separate bbox
// shortcut - so it's omitted rather than approximated.
pub const QUERY_Q4_SQ: &str = r#"
                        SELECT DISTINCT p.point_id, p.point, d.licence
                        FROM csv.berlinmod.trips t, csv.berlinmod.datamcar d, csv.berlinmod.points p
                        WHERE t.moid = d.moid AND passes_point(t.polyline, p.point, 0.0000)
                        ORDER BY p.point_id, d.licence
                        "#;

async fn run_query(ctx: &SessionContext) {
    let df = ctx.sql(QUERY_Q4_SQ).await.unwrap();
    df.collect().await.unwrap();
}

fn benchmark_sql_query(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let ctx = rt.block_on(super::create_ctx());

    *c = Criterion::default()
        .measurement_time(std::time::Duration::new(10, 0))
        .sample_size(10);

    c.bench_with_input(
        BenchmarkId::new("saqe_vs_mobilitydb", "q4_sq"),
        &ctx,
        |b, ctx| {
            b.to_async(&rt).iter(|| async {
                run_query(ctx).await;
            });
        },
    );
}

criterion_group!(bench_q4_sq, benchmark_sql_query);

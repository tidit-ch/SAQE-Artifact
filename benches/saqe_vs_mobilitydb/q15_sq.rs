use criterion::{criterion_group, BenchmarkId, Criterion};
use datafusion::prelude::SessionContext;
use tokio::runtime::Runtime;

// Official BerlinMOD-MobilityDB Query 15 (docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html)
// Vehicles passing points during periods. Mirrors
// build/mobilitydb/benchmarks/sql_scripts/queries/q15_mb.sql's FROM order
// (Trips, Vehicles, Points1, Periods1 - already matched here), WHERE order
// (join first), and ORDER BY. q15_mb.sql's bbox pre-filter
// (T.Trip && STBOX(PO.Geom, PR.Period)) is a pure prefilter, logically
// implied by the exact ST_Intersects check that follows - same reasoning
// as q11_sq.rs's header comment - omitted rather than approximated.
pub const QUERY_Q15_SQ: &str = r#"
                        SELECT DISTINCT po.point_id, po.point, pr.period_id, pr.start_period, pr.end_period, c.licence
                        FROM csv.berlinmod.trips t, csv.berlinmod.datamcar c, csv.berlinmod.points1 po, csv.berlinmod.periods1 pr
                        WHERE t.moid = c.moid AND passes_point(subpolyline_between(t.polyline, pr.start_period, pr.end_period), po.point, 0.0)
                        ORDER BY po.point_id, pr.period_id, c.licence
                        "#;

async fn run_query(ctx: &SessionContext) {
    let df = ctx.sql(QUERY_Q15_SQ).await.unwrap();
    df.collect().await.unwrap();
}

fn benchmark_sql_query(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let ctx = rt.block_on(super::create_ctx());

    *c = Criterion::default()
        .measurement_time(std::time::Duration::new(10, 0))
        .sample_size(10);

    c.bench_with_input(
        BenchmarkId::new("saqe_vs_mobilitydb", "q15_sq"),
        &ctx,
        |b, ctx| {
            b.to_async(&rt).iter(|| async {
                run_query(ctx).await;
            });
        },
    );
}

criterion_group!(bench_q15_sq, benchmark_sql_query);

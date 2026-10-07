use criterion::{criterion_group, BenchmarkId, Criterion};
use datafusion::prelude::SessionContext;
use tokio::runtime::Runtime;

// Official BerlinMOD-MobilityDB Query 13 (docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html)
// Vehicles in regions during periods. Mirrors
// build/mobilitydb/benchmarks/sql_scripts/queries/q13_mb.sql's FROM order
// (Trips, Vehicles, Regions1, Periods1), WHERE order (join first), and
// ORDER BY. q13_mb.sql's bbox pre-filter (T.trip && STBOX(R.Geom, P.Period))
// is a pure prefilter, logically implied by the exact ST_Intersects check
// that follows - same reasoning as q11_sq.rs's header comment - omitted
// rather than approximated.
pub const QUERY_Q13_SQ: &str = r#"
                        SELECT DISTINCT r.polygon_id, p.period_id, p.start_period, p.end_period, c.licence
                        FROM csv.berlinmod.trips t, csv.berlinmod.datamcar c, csv.berlinmod.regions1 r, csv.berlinmod.periods1 p
                        WHERE t.moid = c.moid AND st_intersects(subpolyline_between(t.polyline, p.start_period, p.end_period), r.polygon)
                        ORDER BY r.polygon_id, p.period_id, c.licence
                        "#;

async fn run_query(ctx: &SessionContext) {
    let df = ctx.sql(QUERY_Q13_SQ).await.unwrap();
    df.collect().await.unwrap();
}

fn benchmark_sql_query(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let ctx = rt.block_on(super::create_ctx());

    *c = Criterion::default()
        .measurement_time(std::time::Duration::new(10, 0))
        .sample_size(10);

    c.bench_with_input(
        BenchmarkId::new("saqe_vs_mobilitydb", "q13_sq"),
        &ctx,
        |b, ctx| {
            b.to_async(&rt).iter(|| async {
                run_query(ctx).await;
            });
        },
    );
}

criterion_group!(bench_q13_sq, benchmark_sql_query);

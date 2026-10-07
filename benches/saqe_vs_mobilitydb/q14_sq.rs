use criterion::{criterion_group, BenchmarkId, Criterion};
use datafusion::prelude::SessionContext;
use tokio::runtime::Runtime;

// Official BerlinMOD-MobilityDB Query 14 (docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html)
// Vehicles in regions at instants. Mirrors
// build/mobilitydb/benchmarks/sql_scripts/queries/q14_mb.sql's FROM order
// (Trips, Vehicles, Regions1, Instants1), WHERE order (join first), and
// ORDER BY. Uses st_contains(r.polygon, point) rather than st_intersects,
// matching q14_mb.sql's ST_Contains(R.Geom, valueAtTimestamp(...)) exactly
// (st_contains registered in src/core/udf/mod.rs alongside st_intersects -
// same geodatafusion crate, wasn't wired in before). ST_Contains and
// ST_Intersects differ at the polygon boundary (Contains excludes points
// exactly on the edge, Intersects includes them), so this is a real
// semantic fix, not just a rename. q14_mb.sql's bbox pre-filter
// (T.Trip && STBOX(R.Geom, I.Instant)) is a pure prefilter, logically
// implied by the exact ST_Contains check that follows - same reasoning as
// q11_sq.rs's header comment - omitted rather than approximated.
pub const QUERY_Q14_SQ: &str = r#"
                        SELECT DISTINCT r.polygon_id, i.instant_id, i.instant, c.licence
                        FROM csv.berlinmod.trips t, csv.berlinmod.datamcar c, csv.berlinmod.regions1 r, csv.berlinmod.instants1 i
                        WHERE t.moid = c.moid AND st_contains(r.polygon, point_at_timestamp(t.polyline, i.instant))
                        ORDER BY r.polygon_id, i.instant_id, c.licence
                        "#;

async fn run_query(ctx: &SessionContext) {
    let df = ctx.sql(QUERY_Q14_SQ).await.unwrap();
    df.collect().await.unwrap();
}

fn benchmark_sql_query(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let ctx = rt.block_on(super::create_ctx());

    *c = Criterion::default()
        .measurement_time(std::time::Duration::new(10, 0))
        .sample_size(10);

    c.bench_with_input(
        BenchmarkId::new("saqe_vs_mobilitydb", "q14_sq"),
        &ctx,
        |b, ctx| {
            b.to_async(&rt).iter(|| async {
                run_query(ctx).await;
            });
        },
    );
}

criterion_group!(bench_q14_sq, benchmark_sql_query);

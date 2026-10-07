use criterion::{criterion_group, BenchmarkId, Criterion};
use datafusion::prelude::SessionContext;
use tokio::runtime::Runtime;

// Official BerlinMOD-MobilityDB Query 11 (docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html)
// Vehicles at point-instant combinations. Mirrors
// build/mobilitydb/benchmarks/sql_scripts/queries/q11_mb.sql's flat
// FROM/WHERE structure (no WITH clause in the docs either), SELECT
// columns, and ORDER BY. q11_mb.sql's bbox pre-filter
// (T.Trip @> STBOX(P.Geom, I.Instant)) has no SAQE equivalent - same
// omission as q4_sq.rs's bbox pre-filter.
pub const QUERY_Q11_SQ: &str = r#"
                        SELECT p.point_id, p.point, i.instant_id, i.instant, c.licence
                        FROM csv.berlinmod.trips t, csv.berlinmod.datamcar c, csv.berlinmod.points1 p, csv.berlinmod.instants1 i
                        WHERE t.moid = c.moid AND point_at_timestamp(t.polyline, i.instant) = p.point
                        ORDER BY p.point_id, i.instant_id, c.licence
                        "#;

async fn run_query(ctx: &SessionContext) {
    let df = ctx.sql(QUERY_Q11_SQ).await.unwrap();
    df.collect().await.unwrap();
}

fn benchmark_sql_query(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let ctx = rt.block_on(super::create_ctx());

    *c = Criterion::default()
        .measurement_time(std::time::Duration::new(10, 0))
        .sample_size(10);

    c.bench_with_input(
        BenchmarkId::new("saqe_vs_mobilitydb", "q11_sq"),
        &ctx,
        |b, ctx| {
            b.to_async(&rt).iter(|| async {
                run_query(ctx).await;
            });
        },
    );
}

criterion_group!(bench_q11_sq, benchmark_sql_query);

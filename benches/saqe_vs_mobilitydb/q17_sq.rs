use criterion::{criterion_group, BenchmarkId, Criterion};
use datafusion::prelude::SessionContext;
use tokio::runtime::Runtime;

// Official BerlinMOD-MobilityDB Query 17 (docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html)
// Most-visited points. Mirrors
// build/mobilitydb/benchmarks/sql_scripts/queries/q17_mb.sql.
pub const QUERY_Q17_SQ: &str = r#"
                        WITH pointCount AS (
                            SELECT p.point_id, COUNT(DISTINCT t.moid) as hits
                            FROM csv.berlinmod.trips t, csv.berlinmod.points p
                            WHERE passes_point(t.polyline, p.point, 0.0000)
                            GROUP BY p.point_id
                        )
                        SELECT pc.point_id, pc.hits
                        FROM pointCount pc
                        WHERE pc.hits = (SELECT MAX(hits) FROM pointCount)
                        "#;

async fn run_query(ctx: &SessionContext) {
    let df = ctx.sql(QUERY_Q17_SQ).await.unwrap();
    df.collect().await.unwrap();
}

fn benchmark_sql_query(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let ctx = rt.block_on(super::create_ctx());

    *c = Criterion::default()
        .measurement_time(std::time::Duration::new(10, 0))
        .sample_size(10);

    c.bench_with_input(
        BenchmarkId::new("saqe_vs_mobilitydb", "q17_sq"),
        &ctx,
        |b, ctx| {
            b.to_async(&rt).iter(|| async {
                run_query(ctx).await;
            });
        },
    );
}

criterion_group!(bench_q17_sq, benchmark_sql_query);

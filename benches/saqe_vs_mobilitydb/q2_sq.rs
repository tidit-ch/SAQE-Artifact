use criterion::{criterion_group, BenchmarkId, Criterion};
use datafusion::prelude::SessionContext;
use tokio::runtime::Runtime;

// Official BerlinMOD-MobilityDB Query 2 (docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html)
// Passenger car count. Mirrors
// build/mobilitydb/benchmarks/sql_scripts/queries/q2_mb.sql.
pub const QUERY_Q2_SQ: &str =
    "SELECT COUNT(licence) FROM csv.berlinmod.datamcar WHERE type = 'passenger';";

async fn run_query(ctx: &SessionContext) {
    let df = ctx.sql(QUERY_Q2_SQ).await.unwrap();
    df.collect().await.unwrap();
}

fn benchmark_sql_query(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let ctx = rt.block_on(super::create_ctx());

    *c = Criterion::default()
        .measurement_time(std::time::Duration::new(10, 0))
        .sample_size(10);

    c.bench_with_input(
        BenchmarkId::new("saqe_vs_mobilitydb", "q2_sq"),
        &ctx,
        |b, ctx| {
            b.to_async(&rt).iter(|| async {
                run_query(ctx).await;
            });
        },
    );
}

criterion_group!(bench_q2_sq, benchmark_sql_query);

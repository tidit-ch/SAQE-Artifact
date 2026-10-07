use criterion::{criterion_group, BenchmarkId, Criterion};
use datafusion::prelude::SessionContext;
use tokio::runtime::Runtime;

// Official BerlinMOD-MobilityDB Query 1 (docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html)
// Vehicle models and licence plates. Mirrors
// build/mobilitydb/benchmarks/sql_scripts/queries/q1_mb.sql.
pub const QUERY_Q1_SQ: &str =
    "SELECT DISTINCT l.licence as licence, c.model as model FROM csv.berlinmod.datamcar c, csv.berlinmod.licences l WHERE c.licence = l.licence;";

async fn run_query(ctx: &SessionContext) {
    let df = ctx.sql(QUERY_Q1_SQ).await.unwrap();
    df.collect().await.unwrap();
}

fn benchmark_sql_query(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let ctx = rt.block_on(super::create_ctx());

    *c = Criterion::default()
        .measurement_time(std::time::Duration::new(10, 0))
        .sample_size(10);

    c.bench_with_input(
        BenchmarkId::new("saqe_vs_mobilitydb", "q1_sq"),
        &ctx,
        |b, ctx| {
            b.to_async(&rt).iter(|| async {
                run_query(ctx).await;
            });
        },
    );
}

criterion_group!(bench_q1_sq, benchmark_sql_query);

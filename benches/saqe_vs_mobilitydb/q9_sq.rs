use criterion::{criterion_group, BenchmarkId, Criterion};
use datafusion::prelude::SessionContext;
use tokio::runtime::Runtime;

// Official BerlinMOD-MobilityDB Query 9 (docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html)
// Maximum distance travelled per period, over all vehicles. Mirrors
// build/mobilitydb/benchmarks/sql_scripts/queries/q9_mb.sql, including its
// plain comma-join FROM style (q9_mb.sql's `FROM Trips T, Periods P` is an
// implicit cross product filtered by WHERE, not an explicit CROSS JOIN
// keyword - same thing, matched literally here). Note: same overlap-vs-
// during fix as q8_sq.rs (see its comment) - and, matching the docs, this
// uses the full `periods` table, not the periods1 sample.
pub const QUERY_Q9_SQ: &str = r#"
                        WITH distances AS (
                            SELECT p.period_id, p.start_period, p.end_period, t.moid,
                                SUM(st_length(subpolyline_between(t.polyline, p.start_period, p.end_period))) As dist
                            FROM csv.berlinmod.trips t, csv.berlinmod.periods p
                            WHERE array_length(subpolyline_between(t.polyline, p.start_period, p.end_period)) > 0
                            GROUP BY p.period_id, p.start_period, p.end_period, t.moid
                        )
                        SELECT period_id, start_period, end_period, Max(dist) As max_distance
                        FROM distances
                        GROUP BY period_id, start_period, end_period
                        ORDER BY period_id
                        "#;

async fn run_query(ctx: &SessionContext) {
    let df = ctx.sql(QUERY_Q9_SQ).await.unwrap();
    df.collect().await.unwrap();
}

fn benchmark_sql_query(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let ctx = rt.block_on(super::create_ctx());

    *c = Criterion::default()
        .measurement_time(std::time::Duration::new(10, 0))
        .sample_size(10);

    c.bench_with_input(
        BenchmarkId::new("saqe_vs_mobilitydb", "q9_sq"),
        &ctx,
        |b, ctx| {
            b.to_async(&rt).iter(|| async {
                run_query(ctx).await;
            });
        },
    );
}

criterion_group!(bench_q9_sq, benchmark_sql_query);

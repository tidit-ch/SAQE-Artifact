use criterion::{criterion_group, BenchmarkId, Criterion};
use datafusion::prelude::SessionContext;
use tokio::runtime::Runtime;

// Official BerlinMOD-MobilityDB Query 5 (docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html)
// Minimum distance between vehicle pairs. Mirrors
// build/mobilitydb/benchmarks/sql_scripts/queries/q5_mb.sql's SELECT
// columns and ORDER BY, but deliberately does NOT mirror its flat 4-table
// FROM list (Trips T1, Licences1 L1, Trips T2, Licences2 L2) - flattening
// this one was tried and measured getting OOM-killed at scale 1.0 (>500GB
// RSS on a 503GB host) after passing fine at every smaller scale tested so
// far (0.005/0.2), where the flat cross join's actual cardinality never got
// large enough to expose the problem. Same root cause as q12_sq.rs's
// documented exception: a flat comma-join self-joining trips against
// itself gives the planner no forced order to filter down to the small
// licences1/licences2-matched subset before attempting the t1.moid <
// t2.moid self-join, so it can end up materializing something close to a
// full trips x trips cross product first. The two CTEs below pre-filter
// trips down to only the rows matching the 10-row licences1/licences2
// samples *before* the self-join, keeping the self-join's actual input
// small regardless of how big the full trips table is. Same result, same
// precedent as q7_sq.rs/q12_sq.rs keeping their own pre-filtered structure
// over the docs' literal flat form.
pub const QUERY_Q5_SQ: &str = r#"
                        WITH matched1 AS (
                            SELECT t.moid, t.polyline, l1.licence AS licence1
                            FROM csv.berlinmod.trips t, csv.berlinmod.licences1 l1
                            WHERE t.moid = l1.moid
                        ),
                        matched2 AS (
                            SELECT t.moid, t.polyline, l2.licence AS licence2
                            FROM csv.berlinmod.trips t, csv.berlinmod.licences2 l2
                            WHERE t.moid = l2.moid
                        )
                        SELECT m1.licence1 AS licence1, m2.licence2 AS licence2,
                            MIN(st_distance(m1.polyline, m2.polyline)) AS min_dist
                        FROM matched1 m1, matched2 m2
                        WHERE m1.moid < m2.moid
                        GROUP BY m1.licence1, m2.licence2
                        ORDER BY m1.licence1, m2.licence2
                        "#;

async fn run_query(ctx: &SessionContext) {
    let df = ctx.sql(QUERY_Q5_SQ).await.unwrap();
    df.collect().await.unwrap();
}

fn benchmark_sql_query(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let ctx = rt.block_on(super::create_ctx());

    *c = Criterion::default()
        .measurement_time(std::time::Duration::new(10, 0))
        .sample_size(10);

    c.bench_with_input(
        BenchmarkId::new("saqe_vs_mobilitydb", "q5_sq"),
        &ctx,
        |b, ctx| {
            b.to_async(&rt).iter(|| async {
                run_query(ctx).await;
            });
        },
    );
}

criterion_group!(bench_q5_sq, benchmark_sql_query);

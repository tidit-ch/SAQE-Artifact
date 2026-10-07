use criterion::{criterion_group, BenchmarkId, Criterion};
use datafusion::prelude::SessionContext;
use tokio::runtime::Runtime;

// Official BerlinMOD-MobilityDB Query 16 (docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html)
// Vehicle pairs sharing region presence within a period without ever being
// simultaneously present. Mirrors
// build/mobilitydb/benchmarks/sql_scripts/queries/q16_mb.sql.
//
// Note: the docs' final predicate,
// `tintersects(atPeriod(T1.Trip,P.Period), atPeriod(T2.Trip,P.Period)) %= FALSE`,
// is spatiotemporal - same place AND same time, ever, during the clipped
// period. benches/csv_benchmarks/q16.rs instead used
// `NOT st_intersects(t1.p1, t2.p2)`, which is spatial-only (do the two
// clipped paths cross in space at all, regardless of when each vehicle was
// at the crossing point) - not equivalent, and would wrongly exclude pairs
// whose paths cross the same location at different times. Approximated
// here with `NOT tdwithin(t1.p1, t2.p2, 0.0, 'second')` (same place, same
// second) instead - closer to the docs' spatiotemporal intent, but still
// an approximation: SAQE has no direct equivalent of a temporal-boolean
// "always/ever" operator pair (%=/?=) to test tightly against.
// Joins trips.moid directly to licences1/2.moid, matching the official
// docs' query text (T.VehId = L.VehId) - see q3_sq.rs's header comment for
// the moid column note.
pub const QUERY_Q16_SQ: &str = r#"
                        WITH t1_trips AS (
                            SELECT
                                t1.moid, l1.licence, p.period_id, r.polygon_id,
                                subpolyline_between(t1.polyline, p.start_period, p.end_period) AS p1
                            FROM csv.berlinmod.trips t1
                            JOIN csv.berlinmod.licences1 l1 ON t1.moid = l1.moid
                            JOIN csv.berlinmod.periods1 p ON true
                            JOIN csv.berlinmod.regions1 r
                            ON st_intersects(
                                subpolyline_between(t1.polyline, p.start_period, p.end_period),
                                r.polygon
                            )
                        ),
                        t2_trips AS (
                            SELECT
                            t2.moid, l2.licence, p.period_id, r.polygon_id,
                            subpolyline_between(t2.polyline, p.start_period, p.end_period) AS p2
                            FROM csv.berlinmod.trips t2
                            JOIN csv.berlinmod.licences2 l2 ON t2.moid = l2.moid
                            JOIN csv.berlinmod.periods1 p ON true
                            JOIN csv.berlinmod.regions1 r
                            ON st_intersects(
                                subpolyline_between(t2.polyline, p.start_period, p.end_period),
                                r.polygon
                            )
                        )
                        SELECT t1.period_id, t1.polygon_id, t1.licence AS licence1, t2.licence AS licence2
                        FROM t1_trips t1
                        JOIN t2_trips t2 ON t1.moid < t2.moid
                        AND t1.polygon_id = t2.polygon_id
                        AND t1.period_id = t2.period_id
                        WHERE NOT tdwithin(t1.p1, t2.p2, 0.0, 'second');
                        "#;

async fn run_query(ctx: &SessionContext) {
    let df = ctx.sql(QUERY_Q16_SQ).await.unwrap();
    df.collect().await.unwrap();
}

fn benchmark_sql_query(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let ctx = rt.block_on(super::super::create_ctx());

    *c = Criterion::default()
        .measurement_time(std::time::Duration::new(10, 0))
        .sample_size(10);

    c.bench_with_input(
        BenchmarkId::new("saqe_vs_mobilitydb", "q16_sq"),
        &ctx,
        |b, ctx| {
            b.to_async(&rt).iter(|| async {
                run_query(ctx).await;
            });
        },
    );
}

criterion_group!(bench_q16_sq, benchmark_sql_query);

use criterion::{criterion_group, BenchmarkId, Criterion};
use datafusion::prelude::SessionContext;
use tokio::runtime::Runtime;

// Official BerlinMOD-MobilityDB Query 7 (docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html)
// First passenger cars reaching each point. Mirrors
// build/mobilitydb/benchmarks/sql_scripts/queries/q7_mb.sql's SELECT
// DISTINCT, FROM/WHERE/GROUP BY order, SELECT columns, and ORDER BY.
// q7_mb.sql finds the earliest arrival per point via a correlated `<= ALL`
// subquery; kept as a min-then-join here instead (same result, DataFusion
// has no efficient correlated-subquery pushdown for this shape) - the
// CTE-based structure itself is therefore not a literal mirror, but every
// clause's keywords/column order/content now is. Was missing p.point/
// r.instant in the SELECT lists entirely (q7_mb.sql selects Geom and
// Instant, not just Licence/PointId).
pub const QUERY_Q7_SQ: &str = r#"
                        WITH reached_points AS (
                        SELECT DISTINCT c.licence, p.point_id, p.point, MIN(timestamp_at_position(t.polyline, p.point)) AS instant
                        FROM csv.berlinmod.trips t, csv.berlinmod.datamcar c, csv.berlinmod.points1 p
                        WHERE t.moid = c.moid AND c.type = 'passenger' AND passes_point(t.polyline, p.point, 0.0)
                        GROUP BY c.licence, p.point_id, p.point
                        ),
                        first_arrivals AS (
                            SELECT point_id, MIN(instant) AS earliest_instant
                            FROM reached_points
                            GROUP BY point_id)
                        SELECT r.licence, r.point_id, r.point, r.instant
                        FROM reached_points r
                        JOIN first_arrivals f ON r.point_id = f.point_id AND r.instant = f.earliest_instant
                        ORDER BY r.point_id, r.licence
                        "#;

async fn run_query(ctx: &SessionContext) {
    let df = ctx.sql(QUERY_Q7_SQ).await.unwrap();
    df.collect().await.unwrap();
}

fn benchmark_sql_query(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let ctx = rt.block_on(super::create_ctx());

    *c = Criterion::default()
        .measurement_time(std::time::Duration::new(10, 0))
        .sample_size(10);

    c.bench_with_input(
        BenchmarkId::new("saqe_vs_mobilitydb", "q7_sq"),
        &ctx,
        |b, ctx| {
            b.to_async(&rt).iter(|| async {
                run_query(ctx).await;
            });
        },
    );
}

criterion_group!(bench_q7_sq, benchmark_sql_query);

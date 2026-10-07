use criterion::{criterion_group, BenchmarkId, Criterion};
use datafusion::prelude::SessionContext;
use tokio::runtime::Runtime;

// Official BerlinMOD-MobilityDB Query 12 (docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html)
// Vehicle pairs meeting at exact point-instant. Mirrors
// build/mobilitydb/benchmarks/sql_scripts/queries/q12_mb.sql's SELECT
// columns and ORDER BY, but deliberately keeps the CTE structure rather
// than mirroring q12_mb.sql's flat 6-table FROM list (Trips T1, Vehicles
// C1, Trips T2, Vehicles C2, Points1 P, Instants1 I) - flattening this one
// was tried and measured hanging (>13s CPU with zero output at scale
// 0.005, where every other query finishes in under a second): the flat
// form forces a full trips-self-join cross product (trips x trips x
// points1 x instants1) before any filtering, whereas MobilityDB's bbox
// pre-filters (T1/T2.Trip @> STBOX(P.Geom, I.Instant) - themselves pure
// prefilters, logically implied by the exact valueAtTimestamp checks that
// follow, see q11_sq.rs's header comment) let Postgres index-prune that
// cross product before it materializes, which SAQE has no equivalent for.
// The CTE computes the small "passes" match-set (trips x points1 x
// instants1, filtered down to actual matches) first, then only self-joins
// that already-small result - same result, tractable runtime. Same
// precedent as q7_sq.rs keeping its min-then-join structure over the docs'
// correlated-subquery form.
pub const QUERY_Q12_SQ: &str = r#"
                        WITH passes AS (
                            SELECT t.moid, p.point_id, p.point, i.instant_id, i.instant
                            FROM csv.berlinmod.trips t, csv.berlinmod.points1 p, csv.berlinmod.instants1 i
                            WHERE point_at_timestamp(t.polyline, i.instant) = p.point
                        )
                        SELECT DISTINCT p1.point_id, p1.point, p1.instant_id, p1.instant, c1.licence AS licence1, c2.licence AS licence2
                        FROM passes p1
                        JOIN passes p2 ON p1.point_id = p2.point_id AND p1.instant_id = p2.instant_id AND p1.moid < p2.moid
                        JOIN csv.berlinmod.datamcar c1 ON p1.moid = c1.moid
                        JOIN csv.berlinmod.datamcar c2 ON p2.moid = c2.moid
                        ORDER BY p1.point_id, p1.instant_id, licence1, licence2
                        "#;

async fn run_query(ctx: &SessionContext) {
    let df = ctx.sql(QUERY_Q12_SQ).await.unwrap();
    df.collect().await.unwrap();
}

fn benchmark_sql_query(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let ctx = rt.block_on(super::create_ctx());

    *c = Criterion::default()
        .measurement_time(std::time::Duration::new(10, 0))
        .sample_size(10);

    c.bench_with_input(
        BenchmarkId::new("saqe_vs_mobilitydb", "q12_sq"),
        &ctx,
        |b, ctx| {
            b.to_async(&rt).iter(|| async {
                run_query(ctx).await;
            });
        },
    );
}

criterion_group!(bench_q12_sq, benchmark_sql_query);

use criterion::{criterion_group, BenchmarkId, Criterion};
use datafusion::prelude::SessionContext;
use tokio::runtime::Runtime;

// Official BerlinMOD-MobilityDB Query 8 (docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html)
// Travelled distances by vehicle and period. Mirrors
// build/mobilitydb/benchmarks/sql_scripts/queries/q8_mb.sql's FROM/WHERE/
// GROUP BY/ORDER BY order - flattened from JOIN/CROSS JOIN syntax into the
// same flat FROM-list style. Note: unlike benches/csv_benchmarks/q8.rs
// (which uses `during` - entirely-contained - as its filter and sums the
// *whole* trip's length for matching trips), the official docs sum
// length(atPeriod(trip, period)) over any trip that merely *overlaps* the
// period (T.Trip && P.Period) - not a bbox pre-filter shortcut here, but
// the actual overlap semantics the docs specify - not just ones fully
// inside it. SAQE has no standalone "overlaps" predicate, so the overlap
// filter and the length-of-clipped-portion are both expressed via
// subpolyline_between: a trip contributes only the portion of itself that
// falls inside the period, and rows with no overlap (empty clipped result)
// are dropped so they don't appear as spurious dist=0 rows.
//
// Joins trips.moid directly to licences1.moid, matching the official docs'
// query text (T.VehId = L.VehId) - see q3_sq.rs's header comment for the
// moid column note.
pub const QUERY_Q8_SQ: &str = r#"
                        SELECT l.licence, p.period_id, p.start_period, p.end_period,
                            SUM(st_length(subpolyline_between(t.polyline, p.start_period, p.end_period))) AS dist
                        FROM csv.berlinmod.trips t, csv.berlinmod.licences1 l, csv.berlinmod.periods1 p
                        WHERE t.moid = l.moid AND array_length(subpolyline_between(t.polyline, p.start_period, p.end_period)) > 0
                        GROUP BY l.licence, p.period_id, p.start_period, p.end_period
                        ORDER BY l.licence, p.period_id
                        "#;

async fn run_query(ctx: &SessionContext) {
    let df = ctx.sql(QUERY_Q8_SQ).await.unwrap();
    df.collect().await.unwrap();
}

fn benchmark_sql_query(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let ctx = rt.block_on(super::create_ctx());

    *c = Criterion::default()
        .measurement_time(std::time::Duration::new(10, 0))
        .sample_size(10);

    c.bench_with_input(
        BenchmarkId::new("saqe_vs_mobilitydb", "q8_sq"),
        &ctx,
        |b, ctx| {
            b.to_async(&rt).iter(|| async {
                run_query(ctx).await;
            });
        },
    );
}

criterion_group!(bench_q8_sq, benchmark_sql_query);

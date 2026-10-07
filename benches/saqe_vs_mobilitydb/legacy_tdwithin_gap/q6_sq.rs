use criterion::{criterion_group, BenchmarkId, Criterion};
use datafusion::prelude::SessionContext;
use tokio::runtime::Runtime;

// Official BerlinMOD-MobilityDB Query 6 (docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html)
// Closely spaced truck pairs (within 10m during the same second). Mirrors
// build/mobilitydb/benchmarks/sql_scripts/queries/q6_mb.sql. Note: unlike
// q6_mb.sql, this query has no direct control over MobilityDB's `expandSpatial`
// bbox pre-filter equivalent - tdwithin()'s "same second" time-granularity
// check plays that role here instead. Also note: unlike
// benches/csv_benchmarks/q6.rs, this doesn't filter trucks down to the
// licences1 10-row sample - the official docs' Query 6 has no Licences1/2
// reference at all, it queries Vehicles directly (matching q6_mb.sql).
//
// Confirmed capability gap, not a bug: against real BerlinMOD scale 0.2
// data this returns 8 pairs vs. q6_mb.sql's 820. Root cause checked
// directly - moving trips have ~2.1s average gap between GPS pings
// (unsynchronized between vehicles: one truck's pings rarely land in the
// same calendar second as another's), while `tdwithin`'s 'second' argument
// is already its finest available granularity (no finer option exists).
// MobilityDB instead computes distance continuously via linear
// interpolation between each trajectory's instants, so it catches close
// encounters that fall between two vehicles' discrete, unsynchronized
// pings entirely - SAQE's discrete same-second-bucket check structurally
// cannot.
pub const QUERY_Q6_SQ: &str = r#"
                        WITH trucks AS (
                            SELECT moid, licence
                            FROM csv.berlinmod.datamcar
                            WHERE type = 'truck'
                        ),
                        trip_trucks AS (
                            SELECT t.moid, t.polyline, c.licence
                            FROM csv.berlinmod.trips t
                            JOIN trucks c ON t.moid = c.moid
                        )
                        SELECT DISTINCT tt1.licence AS licence1, tt2.licence AS licence2
                        FROM trip_trucks tt1
                        JOIN trip_trucks tt2 ON tt1.moid < tt2.moid
                        WHERE tdwithin(tt1.polyline, tt2.polyline, 10, 'second')
                        "#;

async fn run_query(ctx: &SessionContext) {
    let df = ctx.sql(QUERY_Q6_SQ).await.unwrap();
    df.collect().await.unwrap();
}

fn benchmark_sql_query(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let ctx = rt.block_on(super::super::create_ctx());

    *c = Criterion::default()
        .measurement_time(std::time::Duration::new(10, 0))
        .sample_size(10);

    c.bench_with_input(
        BenchmarkId::new("saqe_vs_mobilitydb", "q6_sq"),
        &ctx,
        |b, ctx| {
            b.to_async(&rt).iter(|| async {
                run_query(ctx).await;
            });
        },
    );
}

criterion_group!(bench_q6_sq, benchmark_sql_query);

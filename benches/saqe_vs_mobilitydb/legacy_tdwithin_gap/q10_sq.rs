use criterion::{criterion_group, BenchmarkId, Criterion};
use datafusion::prelude::SessionContext;
use tokio::runtime::Runtime;

// Official BerlinMOD-MobilityDB Query 10 (docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html)
// Records interactions between vehicle pairs within 3m proximity. Mirrors
// build/mobilitydb/benchmarks/sql_scripts/queries/q10_mb.sql - but only
// approximately, see below.
//
// This is a genuine SAQE capability gap, not just a translation choice.
// The official Query 10 (and q10_mb.sql) returns, per matching vehicle
// pair, the actual array of trajectory segments/positions during which the
// two vehicles were within 3m of each other
// (atPeriodSet(T1.Trip, gettime(atvalues(tdwithin(...), TRUE)))) - a
// temporal-boolean value is built, restricted to its TRUE periods, and
// those periods are used to clip T1's own trajectory back out.
//
// SAQE's `tdwithin` (src/core/udf/temporal_filters/tdwithin.rs) only
// returns a single Boolean for the whole trajectory pair ("were they ever
// within tolerance during some shared time granularity") - it has no
// equivalent of MobilityDB's temporal-boolean type, so there is no way to
// recover *which* periods/positions satisfied the predicate, only *whether*
// any did. This query is therefore reduced to reporting which vehicle
// pairs ever came within 3m of each other (mirroring q6_sq.rs's pattern,
// with Licences1/Licences2 in place of the truck filter, and a 3.0m/
// 'second' tolerance) rather than the full position-list the docs ask for.
// A byte-for-byte match against q10_mb.sql's output is not possible with
// SAQE's current UDF set.
// Joins trips.moid directly to licences1/2.moid, matching the official
// docs' query text (T.VehId = L.VehId) - see q3_sq.rs's header comment for
// the moid column note.
pub const QUERY_Q10_SQ: &str = r#"
                        WITH l1_trips AS (
                            SELECT t.moid, t.polyline, l.licence
                            FROM csv.berlinmod.trips t
                            JOIN csv.berlinmod.licences1 l ON t.moid = l.moid
                        ),
                        l2_trips AS (
                            SELECT t.moid, t.polyline, l.licence
                            FROM csv.berlinmod.trips t
                            JOIN csv.berlinmod.licences2 l ON t.moid = l.moid
                        )
                        SELECT DISTINCT lt1.licence AS query_licence, lt2.licence AS other_licence
                        FROM l1_trips lt1
                        JOIN l2_trips lt2 ON lt1.moid < lt2.moid
                        WHERE tdwithin(lt1.polyline, lt2.polyline, 3.0, 'second')
                        "#;

async fn run_query(ctx: &SessionContext) {
    let df = ctx.sql(QUERY_Q10_SQ).await.unwrap();
    df.collect().await.unwrap();
}

fn benchmark_sql_query(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let ctx = rt.block_on(super::super::create_ctx());

    *c = Criterion::default()
        .measurement_time(std::time::Duration::new(10, 0))
        .sample_size(10);

    c.bench_with_input(
        BenchmarkId::new("saqe_vs_mobilitydb", "q10_sq"),
        &ctx,
        |b, ctx| {
            b.to_async(&rt).iter(|| async {
                run_query(ctx).await;
            });
        },
    );
}

criterion_group!(bench_q10_sq, benchmark_sql_query);

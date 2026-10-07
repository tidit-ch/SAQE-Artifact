//! Ad hoc, one-off tuple-level correctness check (not a real benchmark) for
//! the official-numbering query set (q1_sq..q17_sq / q1_mb..q17_mb) - see
//! BENCHMARK.md, "The official-numbering query set". The earlier pass only
//! compared row counts; this checks actual id-tuple identity (sorted,
//! set-equality) for the queries where a row-count match alone doesn't
//! already prove it: q4, q7, q9, q13, q14, q15. The rest of the 14 "match"
//! queries are all-zero-row on both sides (q1, q3, q5, q8, q11, q12 - an
//! empty set trivially equals an empty set) or a single-row aggregate
//! already directly compared as one number (q2, q17).
//!
//! Never compares geometry or raw timestamp/period text - same
//! methodology bench_saqe_vs_mobilitydb_compare.rs already established
//! (reduced id-tuple projections only), since the two systems represent
//! geometry differently and format timestamp text differently even for
//! identical instants.
//!
//! Requires the MobilityDB container up, already loaded at BERLINMOD_SCALE
//! with the sample views built (sample_views.sql) - does not (re)load
//! MobilityDB itself, unlike bench_saqe_vs_mobilitydb_compare.rs, since this
//! is meant to run against the state already verified in this session.

mod saqe_vs_mobilitydb;

use datafusion::arrow::array::AsArray;
use datafusion::prelude::SessionContext;
use tokio_postgres::NoTls;

async fn fetch_saqe_rows(ctx: &SessionContext, sql: &str) -> Vec<Vec<String>> {
    let df = ctx.sql(sql).await.expect("SAQE query failed to plan");
    let batches = df.collect().await.expect("SAQE query failed to execute");

    let mut rows = Vec::new();
    for batch in &batches {
        for r in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(batch.num_columns());
            for c in 0..batch.num_columns() {
                let col = batch.column(c);
                let s = datafusion::arrow::util::display::array_value_to_string(col, r)
                    .unwrap_or_else(|_| "<err>".to_string());
                row.push(s);
            }
            rows.push(row);
        }
    }
    rows
}

async fn fetch_mobilitydb_rows(client: &tokio_postgres::Client, sql: &str) -> Vec<Vec<String>> {
    let rows = client
        .query(sql, &[])
        .await
        .expect("MobilityDB query failed");
    rows.iter()
        .map(|row| {
            (0..row.len())
                .map(|i| {
                    let v: String = row.get(i);
                    v
                })
                .collect()
        })
        .collect()
}

fn report(label: &str, mut saqe_rows: Vec<Vec<String>>, mut mobilitydb_rows: Vec<Vec<String>>) {
    saqe_rows.sort();
    mobilitydb_rows.sort();

    println!(
        "{label}: SAQE {} rows, MobilityDB {} rows",
        saqe_rows.len(),
        mobilitydb_rows.len()
    );

    if saqe_rows == mobilitydb_rows {
        println!("{label}: TUPLES MATCH ({} rows).", saqe_rows.len());
        return;
    }

    println!("{label}: MISMATCH.");
    let only_in_saqe: Vec<_> = saqe_rows
        .iter()
        .filter(|r| !mobilitydb_rows.contains(r))
        .collect();
    let only_in_mobilitydb: Vec<_> = mobilitydb_rows
        .iter()
        .filter(|r| !saqe_rows.contains(r))
        .collect();
    println!(
        "  only in SAQE ({}): {:?}",
        only_in_saqe.len(),
        &only_in_saqe[..only_in_saqe.len().min(10)]
    );
    println!(
        "  only in MobilityDB ({}): {:?}",
        only_in_mobilitydb.len(),
        &only_in_mobilitydb[..only_in_mobilitydb.len().min(10)]
    );
}

#[tokio::main]
async fn main() {
    let saqe_ctx = saqe_vs_mobilitydb::create_ctx().await;

    let (pg_client, pg_connection) = tokio_postgres::connect(
        "host=localhost port=25432 user=docker password=docker dbname=mobilitydb",
        NoTls,
    )
    .await
    .expect("failed to connect to MobilityDB - is the container up and loaded?");
    tokio::spawn(async move {
        if let Err(e) = pg_connection.await {
            eprintln!("MobilityDB connection error: {e}");
        }
    });

    // q4: which vehicles passed the points in Points (full table, not the
    // *1 sample - matches q4_mb.sql/q4_sq.rs). Reduced to (point_id, licence).
    let saqe_q4 = "SELECT DISTINCT p.point_id, d.licence FROM csv.berlinmod.trips t, csv.berlinmod.points p, csv.berlinmod.datamcar d WHERE passes_point(t.polyline, p.point, 0.0000) AND t.moid = d.moid";
    let mb_q4 = "SELECT DISTINCT P.PointId::text, C.Licence FROM Trips T, Vehicles C, Points P WHERE T.Moid = C.Moid AND T.Trip && stbox(P.Geom) AND ST_Intersects(trajectory(T.Trip), P.Geom)";
    report(
        "q4",
        fetch_saqe_rows(&saqe_ctx, saqe_q4).await,
        fetch_mobilitydb_rows(&pg_client, mb_q4).await,
    );

    // q7: first passenger cars reaching each point (Points1 sample). Already
    // (licence, point_id) with no geometry/timestamp in its own final SELECT.
    report(
        "q7",
        fetch_saqe_rows(&saqe_ctx, saqe_vs_mobilitydb::q7_sq::QUERY_Q7_SQ).await,
        fetch_mobilitydb_rows(
            &pg_client,
            "WITH Timestamps AS (
                SELECT DISTINCT C.Licence, P.PointId, P.Geom,
                  MIN(startTimestamp(atvalues(T.Trip,P.Geom))) AS Instant
                FROM Trips T, Vehicles C, Points1 P
                WHERE T.Moid = C.Moid AND C.Type = 'passenger' AND
                  T.Trip && stbox(P.Geom) AND ST_Intersects(trajectory(T.Trip), P.Geom)
                GROUP BY C.Licence, P.PointId, P.Geom )
             SELECT T1.Licence, T1.PointId::text
             FROM Timestamps T1
             WHERE T1.Instant <= ALL (
               SELECT T2.Instant FROM Timestamps T2 WHERE T1.PointId = T2.PointId )",
        )
        .await,
    );

    // q9: max distance per period. Compared as (period_id, max_distance)
    // with a small float tolerance, not exact string equality - floating
    // point summation order can differ between the two systems.
    {
        let df = saqe_ctx
            .sql(saqe_vs_mobilitydb::q9_sq::QUERY_Q9_SQ)
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let mut saqe_q9: Vec<(i64, f64)> = Vec::new();
        for batch in &batches {
            let period_id = batch
                .column(0)
                .as_primitive::<datafusion::arrow::datatypes::Int64Type>();
            let max_dist = batch
                .column(3)
                .as_primitive::<datafusion::arrow::datatypes::Float64Type>();
            for i in 0..batch.num_rows() {
                saqe_q9.push((period_id.value(i), max_dist.value(i)));
            }
        }
        let pg_rows = pg_client
            .query(
                "WITH Distances AS (
                    SELECT P.PeriodId, T.Moid, SUM(length(attime(T.Trip, P.Period))) AS Dist
                    FROM Trips T, Periods P
                    WHERE T.Trip && P.Period
                    GROUP BY P.PeriodId, T.Moid )
                 SELECT PeriodId, MAX(Dist) AS MaxDist FROM Distances GROUP BY PeriodId",
                &[],
            )
            .await
            .unwrap();
        let mut mb_q9: Vec<(i64, f64)> = pg_rows
            .iter()
            .map(|r| (r.get::<_, i32>(0) as i64, r.get::<_, f64>(1)))
            .collect();
        saqe_q9.sort_by_key(|(id, _)| *id);
        mb_q9.sort_by_key(|(id, _)| *id);
        println!(
            "q9: SAQE {} rows, MobilityDB {} rows",
            saqe_q9.len(),
            mb_q9.len()
        );
        let mut mismatches = 0;
        for ((sid, sdist), (mid, mdist)) in saqe_q9.iter().zip(mb_q9.iter()) {
            if sid != mid || (sdist - mdist).abs() > 1e-6 {
                mismatches += 1;
                if mismatches <= 10 {
                    println!("  MISMATCH period {sid}/{mid}: saqe={sdist} mb={mdist}");
                }
            }
        }
        if mismatches == 0 {
            println!(
                "q9: TUPLES MATCH ({} periods, max_distance within 1e-6).",
                saqe_q9.len()
            );
        } else {
            println!("q9: {mismatches} mismatched periods.");
        }
    }

    // q13: vehicles in regions during periods. Reduced to (polygon_id,
    // period_id, licence).
    report(
        "q13",
        fetch_saqe_rows(
            &saqe_ctx,
            "SELECT DISTINCT r.polygon_id, p.period_id, c.licence
             FROM csv.berlinmod.trips t, csv.berlinmod.periods1 p, csv.berlinmod.regions1 r, csv.berlinmod.datamcar c
             WHERE st_intersects(subpolyline_between(t.polyline, p.start_period, p.end_period), r.polygon) AND t.moid = c.moid",
        )
        .await,
        fetch_mobilitydb_rows(
            &pg_client,
            "SELECT DISTINCT R.RegionId::text, P.PeriodId::text, C.Licence
             FROM Trips T, Vehicles C, Regions1 R, Periods1 P
             WHERE T.Moid = C.Moid AND T.trip && STBOX(R.Geom, P.Period) AND
               ST_Intersects(trajectory(attime(T.Trip, P.Period)), R.Geom)",
        )
        .await,
    );

    // q14: vehicles in regions at instants. Reduced to (polygon_id,
    // instant_id, licence).
    report(
        "q14",
        fetch_saqe_rows(
            &saqe_ctx,
            "SELECT DISTINCT r.polygon_id, i.instant_id, c.licence
             FROM csv.berlinmod.trips t, csv.berlinmod.instants1 i, csv.berlinmod.regions1 r, csv.berlinmod.datamcar c
             WHERE st_intersects(point_at_timestamp(t.polyline, i.instant), r.polygon) AND t.moid = c.moid",
        )
        .await,
        fetch_mobilitydb_rows(
            &pg_client,
            "SELECT DISTINCT R.RegionId::text, I.InstantId::text, C.Licence
             FROM Trips T, Vehicles C, Regions1 R, Instants1 I
             WHERE T.Moid = C.Moid AND T.Trip && STBOX(R.Geom, I.Instant) AND
               ST_Contains(R.Geom, valueAtTimestamp(T.Trip, I.Instant))",
        )
        .await,
    );

    // q15: vehicles passing points during periods. Reduced to (point_id,
    // period_id, licence).
    report(
        "q15",
        fetch_saqe_rows(
            &saqe_ctx,
            "SELECT DISTINCT po.point_id, pr.period_id, c.licence
             FROM csv.berlinmod.trips t, csv.berlinmod.datamcar c, csv.berlinmod.points1 po, csv.berlinmod.periods1 pr
             WHERE passes_point(subpolyline_between(t.polyline, pr.start_period, pr.end_period), po.point, 0.0) AND t.moid = c.moid",
        )
        .await,
        fetch_mobilitydb_rows(
            &pg_client,
            "SELECT DISTINCT PO.PointId::text, PR.PeriodId::text, C.Licence
             FROM Trips T, Vehicles C, Points1 PO, Periods1 PR
             WHERE T.Moid = C.Moid AND T.Trip && STBOX(PO.Geom, PR.Period) AND
               ST_Intersects(trajectory(attime(T.Trip, PR.Period)),PO.Geom)",
        )
        .await,
    );
}

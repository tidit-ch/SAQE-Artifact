// q13 with its trips split across two stores: the first half in CSV, the second in Parquet,
// UNION ALLed back together. Every other relation is CSV, exactly as q13_sq. Not part of the
// official BerlinMOD numbering - q21/q22 exist to measure what a mixed-source trips table costs,
// so they are DataFusion-only and have no MobilityDB counterpart.
//
// The row set must match q13's: same query, same data, only the placement differs.
pub const QUERY_Q21_SQ: &str = r#"
                        SELECT DISTINCT r.polygon_id, p.period_id, p.start_period, p.end_period, c.licence
                        FROM (SELECT trip_id, moid, polyline FROM csv.berlinmod.trips
                              UNION ALL
                              SELECT trip_id, moid, polyline FROM trips_half_parquet) t,
                             csv.berlinmod.datamcar c, csv.berlinmod.regions1 r, csv.berlinmod.periods1 p
                        WHERE t.moid = c.moid AND st_intersects(subpolyline_between(t.polyline, p.start_period, p.end_period), r.polygon)
                        ORDER BY r.polygon_id, p.period_id, c.licence
                        "#;

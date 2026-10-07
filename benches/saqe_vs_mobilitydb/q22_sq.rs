// q13 with its trips split across two stores: the first half in CSV, the second in Postgres,
// UNION ALLed back together. Every other relation is CSV, exactly as q13_sq. See q21_sq for why
// these two are DataFusion-only.
//
// The Postgres half is reached through the `pg` dynamic catalog, which exposes the geometry
// column as Binary EWKB, hence wkb_to_trajectory - `postgres.berlinmod.*` has no entry for
// trips_half (src/core/postgres/schema_provider.rs resolves names through a fixed match).
pub const QUERY_Q22_SQ: &str = r#"
                        SELECT DISTINCT r.polygon_id, p.period_id, p.start_period, p.end_period, c.licence
                        FROM (SELECT trip_id, moid, polyline FROM csv.berlinmod.trips
                              UNION ALL
                              SELECT trip_id, moid, wkb_to_trajectory(polyline) AS polyline FROM pg.public.trips_half) t,
                             csv.berlinmod.datamcar c, csv.berlinmod.regions1 r, csv.berlinmod.periods1 p
                        WHERE t.moid = c.moid AND st_intersects(subpolyline_between(t.polyline, p.start_period, p.end_period), r.polygon)
                        ORDER BY r.polygon_id, p.period_id, c.licence
                        "#;

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE datamcar (
  moid INTEGER PRIMARY KEY,
  licence VARCHAR(32) NOT NULL,
  type VARCHAR(32),
  model VARCHAR(32)
);
CREATE INDEX idx_datamcar_type ON datamcar(type);
CREATE INDEX idx_datamcar_licence ON datamcar(licence);

CREATE TABLE trips (
  trip_id INTEGER PRIMARY KEY,
  moid INTEGER,
  polyline geometry(LINESTRINGM, 4326)
);
CREATE INDEX idx_trips_moid ON trips(moid);
CREATE INDEX idx_trips_polyline_gist ON trips USING gist(polyline);
-- The two indexes above exist here so the live server (this schema is
-- shared with it, not just the benchmark harness - see
-- build/docker-compose.dev.yml) always has them, regardless of whether a
-- benchmark ever runs. For benchmark timing fairness, the SAME two indexes
-- are ALSO dropped and rebuilt every load cycle by
-- benches/saqe_vs_mobilitydb/mod.rs's load_postgres()/clean_postgres() -
-- Trips scales with the benchmark's data size, so its index-build cost
-- needs to be part of the timed "load" phase every cycle, matching
-- MobilityDB's own create_indexes-gated per-trial index build
-- (build/mobilitydb/benchmarks/sql_scripts/load_data/trips/optimized.sql,
-- "CONDITIONAL INDEX CREATION") - these two just start out already
-- present (from this file) rather than absent, which changes nothing
-- about the per-cycle drop-then-rebuild behavior itself.

CREATE TABLE points (
  point_id INTEGER PRIMARY KEY,
  point geometry(Point, 4326)
);
CREATE INDEX idx_points_point_gist ON points USING gist(point);

CREATE TABLE instants (
  instant_id INTEGER PRIMARY KEY,
  instant TIMESTAMP
);

-- moid is populated after data load (see benches/saqe_vs_mobilitydb/
-- mod.rs's load_postgres()) by joining back to datamcar on the licence
-- string, mirroring MobilityDB's own Licences.VehId - lets queries join
-- trips.moid = licences1.moid directly instead of bridging through
-- datamcar, matching the official docs' query text (T.VehId = L.VehId).
CREATE TABLE licences (
  licence_id INTEGER PRIMARY KEY,
  licence VARCHAR(32),
  moid INTEGER
);
CREATE INDEX idx_licences_moid ON licences(moid);

CREATE TABLE periods (
  period_id INTEGER PRIMARY KEY,
  start_period TIMESTAMP NOT NULL,
  end_period TIMESTAMP NOT NULL
);
CREATE INDEX idx_periods_range ON periods USING btree(start_period, end_period);

CREATE TABLE regions (
  polygon_id INTEGER PRIMARY KEY,
  polygon geometry(POLYGON, 4326) NOT NULL
);
CREATE INDEX idx_regions_polygon_gist ON regions USING gist(polygon);

-- Create Views for berlinmod
-- ORDER BY on every one of these is required, not cosmetic: a bare LIMIT
-- over a table with more rows than the limit has no guaranteed result set in
-- Postgres. Confirmed empirically non-deterministic for two of these six
-- (reran the same reload+query cycle repeatedly, got different row sets
-- each time): regions1 (e.g. polygon_id [1..10] vs [1,33,48,49,...] -
-- `regions` is built from large, deeply-nested merged polygon geometries,
-- up to ~80 vertices per polygon, rather than flat rows) and
-- licences1/licences2 (`licences` gets a bulk UPDATE after load to populate
-- `moid`, which relocates rows via Postgres's MVCC, scrambling whatever
-- order the original COPY preserved). points1 tested clean (4/4 identical
-- reruns) and instants1/periods1 were never observed failing either, but
-- since all six tables are ~100 rows, ordering by primary key costs nothing
-- measurable - so all six get it uniformly rather than leaving untested ones
-- as latent risk. This only matters for these small reference-table sample
-- views; `trips` has no equivalent `*1` view (queries always filter it with
-- real predicates, never blind-sample it) so it's unaffected.
CREATE VIEW licences1 AS SELECT * FROM licences ORDER BY licence_id LIMIT 10;
CREATE VIEW licences2 AS SELECT * FROM licences ORDER BY licence_id LIMIT 10 OFFSET 10;
CREATE VIEW points1 AS SELECT * FROM points ORDER BY point_id LIMIT 10;
CREATE VIEW regions1 AS SELECT * FROM regions ORDER BY polygon_id LIMIT 10;
CREATE VIEW instants1 AS SELECT * FROM instants ORDER BY instant_id LIMIT 10;
CREATE VIEW periods1 AS SELECT * FROM periods ORDER BY period_id LIMIT 10;

-- Create user-defined functions for berlinmod
-- Uses ST_Intersects directly (matching MobilityDB's own q2.sql/q2_linestring.sql
-- exactly - build/mobilitydb/benchmarks/sql_scripts/queries/) rather than
-- ST_DWithin - both give identical results (verified: 732 of 732 pairs match
-- for the current dataset), but ST_Intersects is the literal function
-- MobilityDB calls, so this makes the two systems' pushdown SQL call the
-- same underlying operation, not just an equivalent one. tolerance is kept
-- in the signature only for pushdown-call compatibility with the existing
-- passes_point(polyline, point, tolerance) call in QUERY_Q2 - it is unused.
CREATE OR REPLACE FUNCTION passes_point(
    polyline geometry,
    point geometry,
    tolerance double precision
)
RETURNS boolean AS
$$
BEGIN
    RETURN ST_Intersects(polyline, point);
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION present(
    polyline geometry,
    instant timestamp
)
RETURNS boolean AS
$$
DECLARE
    start_m DOUBLE PRECISION;
    end_m DOUBLE PRECISION;
BEGIN
    IF GeometryType(polyline) NOT LIKE 'LINESTRING%' THEN
        RAISE EXCEPTION 'Invalid geometry type: expected LINESTRINGM';
    END IF;

    -- Get M-values for first and last point of the polyline
    start_m := ST_M(ST_PointN(polyline, 1));
    end_m := ST_M(ST_PointN(polyline, ST_NPoints(polyline)));

    IF start_m IS NULL OR end_m IS NULL THEN
        RAISE EXCEPTION 'Polyline does not contain valid M values';
    END IF;

    -- Check if instant is between start and end m value
    RETURN extract(epoch FROM instant) * 1000 BETWEEN start_m AND end_m;
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION distance(
    polyline1 geometry,
    polyline2 geometry
)
RETURNS DOUBLE PRECISION AS
$$
BEGIN
    RETURN ST_Distance(polyline1, polyline2);
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION intersects(line geometry, geom geometry)
RETURNS boolean
AS $$
BEGIN
  RETURN ST_Intersects(line, geom);
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION length(line geometry)
RETURNS double precision
AS $$
BEGIN
  RETURN ST_Length(line::geometry);
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION during(
    polyline geometry,
    start timestamp,
    "end" timestamp
)
RETURNS boolean AS
$$
DECLARE
    start_m DOUBLE PRECISION;
    end_m DOUBLE PRECISION;
    start_ms DOUBLE PRECISION;
    end_ms DOUBLE PRECISION;
BEGIN
    IF GeometryType(polyline) NOT LIKE 'LINESTRING%' THEN
        RAISE EXCEPTION 'Invalid geometry type: expected LINESTRINGM';
    END IF;

    start_m := ST_M(ST_PointN(polyline, 1));
    end_m := ST_M(ST_PointN(polyline, ST_NPoints(polyline)));

    IF start_m IS NULL OR end_m IS NULL THEN
        RAISE EXCEPTION 'Polyline does not contain valid M values';
    END IF;

    start_ms := extract(epoch FROM start) * 1000;
    end_ms := extract(epoch FROM "end") * 1000;

    RETURN start_m >= start_ms AND end_m <= end_ms;
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION tdwithin(
  traj1 geometry,
  traj2 geometry,
  tolerance double precision,
  time_precision text
)
RETURNS boolean
IMMUTABLE
PARALLEL SAFE
LANGUAGE plpgsql
AS $$
DECLARE
  p1 geometry;
  p2 geometry;
  m1 double precision;
  m2 double precision;
  t1 timestamptz;
  t2 timestamptz;
  deg_tolerance double precision;
BEGIN
  FOR i IN 1..ST_NPoints(traj1) LOOP
    p1 := ST_PointN(traj1, i);
    m1 := ST_M(p1);
    t1 := to_timestamp(m1 / 1000.0);
    deg_tolerance := tolerance / (111320 * cos(radians(52.52)));

    FOR j IN 1..ST_NPoints(traj2) LOOP
      p2 := ST_PointN(traj2, j);
      m2 := ST_M(p2);
      t2 := to_timestamp(m2 / 1000.0);

      IF (
        (time_precision = 'day' AND date_trunc('day', t1) = date_trunc('day', t2)) OR
        (time_precision = 'hour' AND date_trunc('hour', t1) = date_trunc('hour', t2)) OR
        (time_precision = 'minute' AND date_trunc('minute', t1) = date_trunc('minute', t2)) OR
        (time_precision = 'second' AND date_trunc('second', t1) = date_trunc('second', t2))
      ) THEN
        IF ST_Distance(ST_Force2D(p1), ST_Force2D(p2)) <= deg_tolerance THEN
          RETURN TRUE;
        END IF;
      END IF;
    END LOOP;
  END LOOP;

  RETURN FALSE;
END;
$$;


CREATE OR REPLACE FUNCTION subpolyline_between(
    polyline geometry,
    period_start timestamp,
    period_end timestamp
)
RETURNS geometry AS $$
BEGIN
    IF GeometryType(polyline) NOT LIKE 'LINESTRING%' THEN
        RAISE EXCEPTION 'Input must be LINESTRINGM';
    END IF;
    RETURN ST_LocateBetween(
        polyline,
        EXTRACT(EPOCH FROM period_start) * 1000,
        EXTRACT(EPOCH FROM period_end) * 1000
    );
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION subpolyline_at(polyline geometry, instant timestamp)
RETURNS geometry AS $$
BEGIN
    IF GeometryType(polyline) NOT LIKE 'LINESTRING%' THEN
        RAISE EXCEPTION 'Input must be LINESTRINGM';
    END IF;
    RETURN ST_LocateBetween(
        polyline,
        EXTRACT(EPOCH FROM instant) * 1000,
        EXTRACT(EPOCH FROM instant) * 1000
    );
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION timestamp_at_position(polyline geometry, pt geometry)
RETURNS timestamp
IMMUTABLE
PARALLEL SAFE
LANGUAGE sql
AS $$
    SELECT
        CASE
            WHEN ST_DWithin(polyline, pt, 1e-6)
            THEN to_timestamp(ST_InterpolatePoint(polyline, pt) / 1000)
            ELSE NULL
        END
$$;


CREATE OR REPLACE FUNCTION point_at_timestamp(
    polyline geometry,
    instant timestamptz
)
RETURNS geometry AS $$
DECLARE
    instant_m DOUBLE PRECISION := EXTRACT(EPOCH FROM instant) * 1000;
BEGIN
    RETURN ST_Force2D(ST_GeometryN(ST_LocateAlong(polyline, instant_m), 1));
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION duration(geom geometry)
RETURNS interval AS $$
DECLARE
    m_start double precision;
    m_end double precision;
    seconds double precision;
BEGIN
    IF GeometryType(geom) != 'LINESTRINGM' THEN
        RAISE EXCEPTION 'Expected LINESTRINGM, got %', GeometryType(geom);
    END IF;

    m_start := ST_M(ST_PointN(geom, 1));
    m_end := ST_M(ST_PointN(geom, ST_NumPoints(geom)));
    seconds := (m_end - m_start) / 1000.0;

    RETURN make_interval(secs := seconds);
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;


-- Overload of Postgres's own built-in array_length(anyarray, integer) -
-- distinguished by argument type/count, no conflict. Needed so the
-- official-numbering SAQE query set's q8/q9/q13/q15
-- (benches/saqe_vs_mobilitydb/q{8,9,13,15}_sq.rs) can push down unchanged:
-- those queries use `array_length(subpolyline_between(...)) > 0` as an
-- overlap filter (subpolyline_between()'s DataFusion UDF returns a
-- List<Struct> there, not a real Postgres array), so calling the same
-- literal SQL against Postgres needs array_length to also accept a
-- geometry - subpolyline_between() here returns a geometry (a clipped
-- LINESTRINGM), and ST_LocateBetween (which it wraps) already returns an
-- empty geometry for a non-overlapping range rather than raising, so this
-- reduces to a straightforward emptiness/point-count check.
CREATE OR REPLACE FUNCTION array_length(geom geometry)
RETURNS integer AS $$
BEGIN
    IF geom IS NULL OR ST_IsEmpty(geom) THEN
        RETURN 0;
    ELSE
        RETURN ST_NPoints(geom);
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;

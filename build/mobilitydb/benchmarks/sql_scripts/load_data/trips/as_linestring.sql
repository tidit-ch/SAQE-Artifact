

-- ============================================================================
-- 1. MANDATORY FLAG CHECKING
-- ============================================================================

-- indicates whether we have a query that joins Trips with Vehicles on Moid
\if :{?join_vehicles}
\else
  \echo '\n[ERROR] Missing flag: -v join_vehicles=true|false\n'
  SELECT 'ERROR: join_vehicles is required'::integer; \q
\endif

-- indicates whether we want to create the indexes on Moid and Trip column
\if :{?create_indexes}
\else
  \echo '\n[ERROR] Missing flag: -v create_indexes=true|false\n'
  SELECT 'ERROR: create_indexes is required'::integer; \q
\endif

-- indicates whether to explicitly run ANALYZE after data/index loading
\if :{?analyze_table}
\else
  \echo '\n[ERROR] Missing flag: -v analyze_table=true|false\n'
  SELECT 'ERROR: analyze_table is required'::integer; \q
\endif

-- path (inside the container) to the trips.csv of the scale factor to load
\if :{?trips_csv}
\else
  \echo '\n[ERROR] Missing flag: -v trips_csv=/home/mobilitydb/BerlinMOD/<scale>/trips.csv\n'
  SELECT 'ERROR: trips_csv is required'::integer; \q
\endif

-- Boolean Type Validation (Server-side)
SELECT
  :join_vehicles::boolean AS join_vehicles_valid,
  :create_indexes::boolean AS create_indexes_valid,
  :analyze_table::boolean AS analyze_table_valid
WHERE false;

-- ============================================================================
-- 2. DATA LOADING (no Geom column, no unrolling step, no intermediate table)
-- ============================================================================

-- creating the table into which we load the data
CREATE TABLE TripsInput(
  Moid    integer,
  Tripid  integer,
  Tstart  timestamp without time zone,
  Tend    timestamp without time zone,
  Xstart  double precision,
  Ystart  double precision,
  Xend    double precision,
  Yend    double precision
);

-- load the data from the CSV into the table TripsInput
COPY TripsInput(Moid, Tripid, Tstart, Tend, Xstart, Ystart, Xend, Yend)
FROM :'trips_csv' DELIMITER ',' CSV HEADER;

-- ============================================================================
-- 3. FINAL TABLE CREATION & POPULATION (single pass, no persisted instants)
-- ============================================================================

-- Builds Trip as a plain LINESTRINGM (M = epoch millis) instead of
-- MobilityDB's native tgeompoint - the same datatype SAQE's Postgres-
-- pushdown backend uses for Trips.polyline (config/init.sql). Column stays
-- named Trip (not e.g. polyline) so queries written against the tgeompoint
-- version keep referencing T.Trip unchanged. See
-- build/mobilitydb/q4_temporal_overhead_finding.md, "converting MobilityDB's
-- own data in place", for why this variant exists.

CREATE TABLE Trips (
  Moid integer NOT NULL,
  Tripid integer NOT NULL,
  Trip geometry(LINESTRINGM, 0)
  \if :join_vehicles
    , PRIMARY KEY (Moid, Tripid)
    , FOREIGN KEY (Moid) REFERENCES Vehicles(Moid)
  \endif
);

INSERT INTO Trips (Moid, Tripid, Trip)
WITH augmented AS MATERIALIZED (
  SELECT Moid, Tripid, Tstart, Tend, Xstart, Ystart, Xend, Yend,
    (Tend = max(Tend) OVER (PARTITION BY Moid, Tripid)) AS is_last
  FROM TripsInput
)
SELECT Moid, Tripid,
  ST_MakeLine(pt ORDER BY t)
FROM (
  SELECT Moid, Tripid, Tstart AS t,
    ST_SetSRID(ST_MakePointM(Xstart, Ystart, extract(epoch FROM Tstart) * 1000), 0) AS pt
  FROM augmented
  UNION ALL
  SELECT Moid, Tripid, Tend AS t,
    ST_SetSRID(ST_MakePointM(Xend, Yend, extract(epoch FROM Tend) * 1000), 0) AS pt
  FROM augmented
  WHERE is_last
) points
GROUP BY Moid, Tripid;

-- ============================================================================
-- 4. CONDITIONAL INDEX CREATION
-- ============================================================================

-- Check whether we have to create indexes
\if :create_indexes
  -- Create B-Tree on Moid only if we run queries/joins on vehicle ID
  \if :join_vehicles
    CREATE INDEX Trips_Moid_idx ON Trips USING btree(Moid);
  \endif

  -- Always create spatial GiST index on the trajectory (no temporal
  -- dimension here, unlike the tgeompoint version's Trips_gist_idx)
  CREATE INDEX Trips_gist_idx ON Trips USING gist(Trip);
\endif

-- This was not included in the documentation of MobilityDB.
-- ============================================================================
-- 5. CONDITIONAL TABLE ANALYSIS
-- ============================================================================

\if :analyze_table
  ANALYZE Trips;
\endif

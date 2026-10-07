-- Optimized counterpart to base.sql for loading the trips data.
-- Same end result as base.sql (a Trips table with one tgeompoint per
-- (Moid, Tripid)), built without the avoidable write overhead present in
-- the documented approach:
-- * No persisted TripsInputInstants table — the "unroll segments into
--   instants" step is done inline in the final SELECT via a MATERIALIZED
--   CTE, instead of a separate CREATE TABLE that gets written to disk.
-- * No separate ALTER TABLE / UPDATE pass to add the "inst" column — the
--   tgeompoint instant is constructed directly in the same SELECT.
-- * TripsInput has no Geom column at all — base.sql's TripsInput.Geom is
--   built via an UPDATE but never actually read again by anything
--   downstream (TripsInputInstants recomputes points from Xstart/Ystart/
--   Xend/Yend directly), so it's dead work removed here entirely.
-- This lets you A/B the documented-style load (base.sql) against this one
-- to measure the cost of that avoidable overhead specifically, while both
-- produce the identical Trips table.

-- Same deviations from the docs as base.sql:
-- * We are not projecting the coordinates into a different coordinate system
--   but instead simply interpret them in a a standard 2D Cartesian coordinate plane like SAQE.
-- * For the final table we removed the Traj column, because in SAQE the trips table provider
--   also doesn't have an additional Traj column.
-- * It is neccessary to pass different flags to this script to define
--   how the loading of the trips data shall happen:
--    ** [join_vehicles] indicates whether we have a query that joins Trips with Vehicles on Moid
--       -> in that case we want to add the foreign key  on Moid
--       -> we also want to construct the index on Moid column of the Trips table
--    ** [create_indexes] indicates whether we want to create the indexes on Moid and Trip column
--    ** [analyze_table] indicates whether to explicitly run ANALYZE after data/index loading


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

-- Removed the Traj column from [CREATE TABLE TRIPS], because SAQE's table provider
-- for the BerlinMOD trips table doesn't have an additional
-- Traj column.

CREATE TABLE Trips (
  Moid integer NOT NULL,
  Tripid integer NOT NULL,
  Trip tgeompoint
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
  tgeompointseq(array_agg(pt ORDER BY t))
FROM (
  SELECT Moid, Tripid, Tstart AS t,
    tgeompoint(ST_SetSRID(ST_MakePoint(Xstart, Ystart), 0), Tstart) AS pt
  FROM augmented
  UNION ALL
  SELECT Moid, Tripid, Tend AS t,
    tgeompoint(ST_SetSRID(ST_MakePoint(Xend, Yend), 0), Tend) AS pt
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

  -- Always create spatio-temporal GiST index on trajectory sequence
  CREATE INDEX Trips_gist_idx ON Trips USING gist(Trip);
\endif

-- This was not included in the documentation of MobilityDB.
-- ============================================================================
-- 5. CONDITIONAL TABLE ANALYSIS
-- ============================================================================

\if :analyze_table
  ANALYZE Trips;
\endif

-- Follows the documentation of MobilityDB for loading the trips data
-- https://docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03.html#idp8

-- However this script is slightly different to match SAQE correctly:
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
-- 2. DATA LOADING & UNROLLING
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
  Yend    double precision,
  Geom geometry(LineString, 0)
  --> Geom is different to the docs: we don't project them into a different coordinate
  -- system. We simply interpret them in a a standard 2D Cartesian coordinate plane like SAQE.
);

-- load the data from the CSV into the table TripsInput
COPY TripsInput(Moid, Tripid, Tstart, Tend, Xstart, Ystart, Xend, Yend)
FROM :'trips_csv' DELIMITER ',' CSV HEADER;

-- fill the Geom columns with the corresponding line segment for each row
UPDATE TripsInput
SET Geom = ST_SetSRID(ST_MakeLine(ARRAY[ST_MakePoint(XStart, YStart),
  ST_MakePoint(XEnd, YEnd)]), 0);

-- unroll segments into discrete point instances
CREATE TABLE TripsInputInstants AS (
-- start point of every segment
SELECT Moid, Tripid, Tstart, Xstart, Ystart, 
  ST_SetSRID(ST_MakePoint(XStart, YStart), 0) as Geom
FROM TripsInput
UNION ALL
-- end points of the final segment for each trip
SELECT T1.Moid, T1.Tripid, T1.Tend, T1.Xend, T1.Yend, 
  ST_SetSRID(ST_MakePoint(T1.XEnd, T1.YEnd), 0) as Geom
FROM TripsInput T1 INNER JOIN (
  SELECT Moid, Tripid, max(Tend) as MaxTend
  FROM TripsInput 
  GROUP BY Moid, Tripid
) T2 ON T1.Moid = T2.Moid AND T1.Tripid = T2.Tripid AND T1.Tend = T2.MaxTend);
ALTER TABLE TripsInputInstants ADD COLUMN inst tgeompoint;
UPDATE TripsInputInstants
SET inst = tgeompoint(Geom, Tstart);

-- ============================================================================
-- 3. FINAL TABLE CREATION & POPULATION
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
SELECT Moid, Tripid, tgeompointseq(array_agg(inst ORDER BY Tstart))
FROM TripsInputInstants
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
-- Companion to load_all_documented.sql - identical for every table except
-- Trips, which follows the documentation's PARTITIONED loading approach
-- instead: https://docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s02.html
-- ("Loading the Data in Partitioned Tables"), rather than ch03s03's plain
-- Trips. Same two priorities, same order, same DEVIATIONS/RESTORED notes,
-- same per-table load_X flags - see that file's own header comment for the
-- full reasoning; only the Trips-specific differences are called out below.
--
-- Trips-specific notes:
--   * create_partitions_by_date is embedded directly (section 7a) rather
--     than assumed pre-existing (MUST, for priority 1): it's a helper
--     procedure from the MobilityDB-BerlinMOD tutorial's Chapter 1
--     (ch01s03.html), not a built-in MobilityDB function - the docs'
--     partitioned-loading chapter itself only calls it, never defines it.
--     Quoted verbatim from that chapter. Not gated by load_trips - it's a
--     harmless, idempotent (CREATE OR REPLACE) function definition, no
--     reason to make it conditional.
--   * TripDate's window function partitions by TripId alone, exactly as
--     documented (not (Moid, Tripid), the defensive fix
--     benchmarks/sql_scripts/load_data/trips/base_partitioned.sql applies
--     elsewhere in this project): confirmed directly, at every scale this
--     project uses (0.005, 0.2, 1.0), TripId's distinct count already
--     equals distinct (Moid, TripId)'s count (1797, 62510, and 292940
--     respectively) - i.e. TripId is already globally unique in the real
--     data, so the extra Moid has no effect here and the literal
--     documented form is used instead.
--   * Trips_pkey_idx (explicit unique btree on Moid,Tripid,TripDate) is
--     redundant with the PRIMARY KEY's own implicit index, same as
--     Licences_VehId_idx in load_all_documented.sql - kept for literal
--     fidelity to the docs, which declare both.
--
-- Usage: identical flags to load_all_documented.sql (same seven CSV paths
-- plus the seven load_X per-table flags):
--   env PGPASSWORD=docker psql -h localhost -p 25432 -U docker -d mobilitydb \
--     -v points_csv=/home/mobilitydb/BerlinMOD/0.005/querypoints.csv \
--     -v regions_csv=/home/mobilitydb/BerlinMOD/0.005/queryregions.csv \
--     -v instants_csv=/home/mobilitydb/BerlinMOD/0.005/queryinstants.csv \
--     -v periods_csv=/home/mobilitydb/BerlinMOD/0.005/queryperiods.csv \
--     -v vehicles_csv=/home/mobilitydb/BerlinMOD/0.005/datamcar.csv \
--     -v licences_csv=/home/mobilitydb/BerlinMOD/0.005/querylicences.csv \
--     -v trips_csv=/home/mobilitydb/BerlinMOD/0.005/trips.csv \
--     -v load_points=true -v load_regions=true -v load_instants=true \
--     -v load_periods=true -v load_vehicles=true -v load_licences=true \
--     -v load_trips=true \
--     -f build/mobilitydb/benchmarks/load_all_partitioned.sql

-- ============================================================================
-- 0. MANDATORY FLAG CHECKING
-- ============================================================================

\if :{?points_csv}
\else
  \echo '\n[ERROR] Missing flag: -v points_csv=/home/mobilitydb/BerlinMOD/<scale>/querypoints.csv\n'
  SELECT 'ERROR: points_csv is required'::integer; \q
\endif

\if :{?regions_csv}
\else
  \echo '\n[ERROR] Missing flag: -v regions_csv=/home/mobilitydb/BerlinMOD/<scale>/queryregions.csv\n'
  SELECT 'ERROR: regions_csv is required'::integer; \q
\endif

\if :{?instants_csv}
\else
  \echo '\n[ERROR] Missing flag: -v instants_csv=/home/mobilitydb/BerlinMOD/<scale>/queryinstants.csv\n'
  SELECT 'ERROR: instants_csv is required'::integer; \q
\endif

\if :{?periods_csv}
\else
  \echo '\n[ERROR] Missing flag: -v periods_csv=/home/mobilitydb/BerlinMOD/<scale>/queryperiods.csv\n'
  SELECT 'ERROR: periods_csv is required'::integer; \q
\endif

\if :{?vehicles_csv}
\else
  \echo '\n[ERROR] Missing flag: -v vehicles_csv=/home/mobilitydb/BerlinMOD/<scale>/datamcar.csv\n'
  SELECT 'ERROR: vehicles_csv is required'::integer; \q
\endif

\if :{?licences_csv}
\else
  \echo '\n[ERROR] Missing flag: -v licences_csv=/home/mobilitydb/BerlinMOD/<scale>/querylicences.csv\n'
  SELECT 'ERROR: licences_csv is required'::integer; \q
\endif

\if :{?trips_csv}
\else
  \echo '\n[ERROR] Missing flag: -v trips_csv=/home/mobilitydb/BerlinMOD/<scale>/trips.csv\n'
  SELECT 'ERROR: trips_csv is required'::integer; \q
\endif

\if :{?load_points}
\else
  \echo '\n[ERROR] Missing flag: -v load_points=true|false\n'
  SELECT 'ERROR: load_points is required'::integer; \q
\endif

\if :{?load_regions}
\else
  \echo '\n[ERROR] Missing flag: -v load_regions=true|false\n'
  SELECT 'ERROR: load_regions is required'::integer; \q
\endif

\if :{?load_instants}
\else
  \echo '\n[ERROR] Missing flag: -v load_instants=true|false\n'
  SELECT 'ERROR: load_instants is required'::integer; \q
\endif

\if :{?load_periods}
\else
  \echo '\n[ERROR] Missing flag: -v load_periods=true|false\n'
  SELECT 'ERROR: load_periods is required'::integer; \q
\endif

\if :{?load_vehicles}
\else
  \echo '\n[ERROR] Missing flag: -v load_vehicles=true|false\n'
  SELECT 'ERROR: load_vehicles is required'::integer; \q
\endif

\if :{?load_licences}
\else
  \echo '\n[ERROR] Missing flag: -v load_licences=true|false\n'
  SELECT 'ERROR: load_licences is required'::integer; \q
\endif

\if :{?load_trips}
\else
  \echo '\n[ERROR] Missing flag: -v load_trips=true|false\n'
  SELECT 'ERROR: load_trips is required'::integer; \q
\endif

-- Boolean Type Validation (Server-side)
SELECT
  :load_points::boolean AS load_points_valid,
  :load_regions::boolean AS load_regions_valid,
  :load_instants::boolean AS load_instants_valid,
  :load_periods::boolean AS load_periods_valid,
  :load_vehicles::boolean AS load_vehicles_valid,
  :load_licences::boolean AS load_licences_valid,
  :load_trips::boolean AS load_trips_valid
WHERE false;

-- ============================================================================
-- 1. POINTS
-- ============================================================================

\if :load_points

CREATE TABLE Points (
  PointId integer,
  PosX double precision,
  PosY double precision,
  Geom geometry(Point, 0)
);

COPY Points(PointId, PosX, PosY)
FROM :'points_csv' DELIMITER ',' CSV HEADER;

UPDATE Points SET Geom = ST_SetSRID(ST_MakePoint(PosX, PosY), 0);

CREATE INDEX Points_geom_idx ON Points USING gist(Geom);

-- ORDER BY is required, not cosmetic, on every one of these views: rows are
-- mutated post-COPY (UPDATE for Points/Periods/Licences, GROUP BY for
-- Regions), which does not preserve original COPY/insertion order in
-- Postgres - a bare LIMIT has no guaranteed result set once that happens.
-- Confirmed empirically non-deterministic on the SAQE-Postgres side for the
-- equivalent Licences1/Licences2/Regions1 views (see config/init.sql) -
-- same risk applies here since the same UPDATE/GROUP BY pattern is used.
CREATE VIEW Points1(PointId, PosX, PosY, Geom) AS
SELECT PointId, PosX, PosY, Geom
FROM Points
ORDER BY PointId
LIMIT 10;

\endif

-- ============================================================================
-- 2. REGIONS
-- ============================================================================

\if :load_regions

CREATE TABLE RegionsInput (
  RegionId integer,
  SegNo integer,
  XStart double precision,
  YStart double precision,
  XEnd double precision,
  YEnd double precision
);

COPY RegionsInput(RegionId, SegNo, XStart, YStart, XEnd, YEnd)
FROM :'regions_csv' DELIMITER ',' CSV HEADER;

CREATE TABLE Regions (
  RegionId integer,
  Geom Geometry(Polygon, 0)
);

INSERT INTO Regions (RegionId, Geom)
WITH RegionsSegs AS (
  SELECT RegionId, SegNo, ST_SetSRID(ST_MakeLine(
    ST_MakePoint(XStart, YStart), ST_MakePoint(XEnd, YEnd)), 0) AS Geom
  FROM RegionsInput
)
SELECT RegionId, ST_Polygon(ST_LineMerge(ST_Union(Geom ORDER BY SegNo)), 0) AS Geom
FROM RegionsSegs
GROUP BY RegionId;

CREATE INDEX Regions_geom_idx ON Regions USING gist(Geom);

CREATE VIEW Regions1(RegionId, Geom) AS
SELECT RegionId, Geom
FROM Regions
ORDER BY RegionId
LIMIT 10;

DROP TABLE RegionsInput;

\endif

-- ============================================================================
-- 3. INSTANTS
-- ============================================================================

\if :load_instants

CREATE TABLE Instants (
  InstantId integer,
  Instant timestamp
);

COPY Instants(InstantId, Instant)
FROM :'instants_csv' DELIMITER ',' CSV HEADER;

CREATE INDEX Instants_instant_btree_idx ON Instants USING btree(Instant);

CREATE VIEW Instants1(InstantId, Instant) AS
SELECT InstantId, Instant
FROM Instants
ORDER BY InstantId
LIMIT 10;

\endif

-- ============================================================================
-- 4. PERIODS
-- ============================================================================

\if :load_periods

CREATE TABLE Periods (
  PeriodId integer,
  BeginP timestamp,
  EndP timestamp,
  Period tstzspan
);

COPY Periods(PeriodId, BeginP, EndP)
FROM :'periods_csv' DELIMITER ',' CSV HEADER;

UPDATE Periods SET Period = span(BeginP, EndP);

CREATE INDEX Periods_period_idx ON Periods USING gist(Period);

CREATE VIEW Periods1(PeriodId, BeginP, EndP, Period) AS
SELECT PeriodId, BeginP, EndP, Period
FROM Periods
ORDER BY PeriodId
LIMIT 10;

\endif

-- ============================================================================
-- 5. VEHICLES
-- ============================================================================

\if :load_vehicles

CREATE TABLE Vehicles (
  Moid integer PRIMARY KEY,
  Licence varchar(32),
  Type varchar(32),
  Model varchar(32)
);

COPY Vehicles(Moid, Licence, Type, Model)
FROM :'vehicles_csv' DELIMITER ',' CSV HEADER;

\endif

-- ============================================================================
-- 6. LICENCES
-- ============================================================================
-- Docs: Licences(VehId PK, LicenceId, Licence), VehId populated by joining
-- the raw (LicenceId, Licence) rows back to Vehicles on the Licence
-- string, plus a btree index on VehId. Loaded and populated the same way
-- here - LicenceId kept as PRIMARY KEY instead of VehId only because
-- VehId can't be (see load_all_documented.sql's header comment for why,
-- and why deduplicating to make VehId work was tried and reverted there).
-- Requires load_vehicles=true (see that same header comment's
-- Dependencies note).

\if :load_licences

CREATE TABLE Licences (
  Licence varchar(32),
  LicenceId integer PRIMARY KEY,
  VehId integer
);

COPY Licences(Licence, LicenceId)
FROM :'licences_csv' DELIMITER ',' CSV HEADER;

UPDATE Licences L SET VehId = V.Moid FROM Vehicles V WHERE L.Licence = V.Licence;

CREATE INDEX Licences_VehId_idx ON Licences USING btree(VehId);

CREATE VIEW Licences1(LicenceId, Licence, VehId) AS
SELECT LicenceId, Licence, VehId
FROM Licences
ORDER BY LicenceId
LIMIT 10;

CREATE VIEW Licences2(LicenceId, Licence, VehId) AS
SELECT LicenceId, Licence, VehId
FROM Licences
ORDER BY LicenceId
LIMIT 10 OFFSET 10;

\endif

-- ============================================================================
-- 7a. create_partitions_by_date (embedded, see header comment)
-- ============================================================================
-- Quoted verbatim from the MobilityDB-BerlinMOD tutorial, Chapter 1
-- (ch01s03.html) - automatically creates one daily LIST partition per day
-- in [StartDate, EndDate] for the named table.

CREATE OR REPLACE FUNCTION create_partitions_by_date(TableName TEXT, StartDate DATE,
  EndDate DATE)
RETURNS void AS $$
DECLARE
  d DATE;
  PartitionName TEXT;
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_name = lower(TableName))
  THEN
    RAISE EXCEPTION 'Table % does not exist', TableName;
  END IF;
  IF StartDate >= EndDate THEN
    RAISE EXCEPTION 'The start date % must be before the end date %', StartDate, EndDate;
  END IF;
  d = StartDate;
  WHILE d <= EndDate
  LOOP
    PartitionName = TableName || '_' || to_char(d, 'YYYY_MM_DD');
    IF NOT EXISTS (
      SELECT 1
       FROM information_schema.tables
       WHERE  table_name = lower(PartitionName))
    THEN
      EXECUTE format('CREATE TABLE %s PARTITION OF %s FOR VALUES IN (''%s'');',
        PartitionName, TableName, to_char(d, 'YYYY-MM-DD'));
      RAISE NOTICE 'Partition % has been created', PartitionName;
    END IF;
    d = d + '1 day'::interval;
  END LOOP;
  RETURN;
END
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 7b. TRIPS (partitioned by TripDate)
-- ============================================================================
-- Faithful port of the documented partitioned-loading approach
-- (intermediate TripsInputInstants table, not the single-pass
-- MATERIALIZED-CTE optimization
-- benchmarks/sql_scripts/load_data/trips/optimized_partitioned.sql uses -
-- see that pair's own comments for the cost this trades off).
-- Traj restored (dropped in this project's older trips/base_partitioned.sql
-- because SAQE's own trips table has no equivalent column). Requires
-- load_vehicles=true (see load_all_documented.sql's header comment's
-- Dependencies note).

\if :load_trips

CREATE TABLE TripsInput(
  Moid    integer,
  Tripid  integer,
  TripDate date,
  Tstart  timestamp without time zone,
  Tend    timestamp without time zone,
  Xstart  double precision,
  Ystart  double precision,
  Xend    double precision,
  Yend    double precision,
  Geom geometry(LineString, 0)
);

COPY TripsInput(Moid, Tripid, Tstart, Tend, Xstart, Ystart, Xend, Yend)
FROM :'trips_csv' DELIMITER ',' CSV HEADER;

UPDATE TripsInput
SET Geom = ST_SetSRID(ST_MakeLine(ARRAY[ST_MakePoint(XStart, YStart),
  ST_MakePoint(XEnd, YEnd)]), 0);

UPDATE TripsInput T1
SET TripDate = T2.TripDate
FROM (SELECT DISTINCT Tripid, date_trunc('day', MIN(Tstart) OVER
  (PARTITION BY Tripid)) AS TripDate FROM TripsInput) T2
WHERE T1.Tripid = T2.Tripid;

CREATE TABLE TripsInputInstants AS (
  SELECT Moid, Tripid, TripDate, Tstart, Xstart, Ystart,
    ST_SetSRID(ST_MakePoint(XStart, YStart), 0) as Geom
  FROM TripsInput
  UNION ALL
  SELECT T1.Moid, T1.Tripid, T1.TripDate, T1.Tend, T1.Xend, T1.Yend,
    ST_SetSRID(ST_MakePoint(T1.XEnd, T1.YEnd), 0) as Geom
  FROM TripsInput T1 INNER JOIN (
    SELECT Moid, Tripid, max(Tend) as MaxTend
    FROM TripsInput
    GROUP BY Moid, Tripid
  ) T2 ON T1.Moid = T2.Moid AND T1.Tripid = T2.Tripid AND T1.Tend = T2.MaxTend
);
ALTER TABLE TripsInputInstants ADD COLUMN inst tgeompoint;
UPDATE TripsInputInstants SET inst = tgeompoint(Geom, Tstart);

CREATE TABLE Trips (
  Moid integer NOT NULL,
  Tripid integer NOT NULL,
  TripDate date,
  Trip tgeompoint,
  Traj geometry,
  PRIMARY KEY (Moid, Tripid, TripDate)
  \if :load_vehicles
    , FOREIGN KEY (Moid) REFERENCES Vehicles(Moid)
  \endif
) PARTITION BY LIST(TripDate);

DO $$
DECLARE
  mindate date;
  maxdate date;
BEGIN
  SELECT MIN(TripDate), MAX(TripDate) INTO mindate, maxdate FROM TripsInputInstants;
  PERFORM create_partitions_by_date('Trips', mindate, maxdate);
END $$;

INSERT INTO Trips (Moid, Tripid, TripDate, Trip)
SELECT Moid, Tripid, TripDate, tgeompointseq(array_agg(inst ORDER BY Tstart))
FROM TripsInputInstants
GROUP BY Moid, Tripid, TripDate;

UPDATE Trips SET Traj = trajectory(Trip);

CREATE INDEX Trips_Moid_idx ON Trips USING btree(Moid);
CREATE UNIQUE INDEX Trips_pkey_idx ON Trips USING btree(Moid, Tripid, TripDate);
CREATE INDEX Trips_gist_idx ON Trips USING gist(Trip);

-- Trips1 (the docs' LIMIT 100 sample view over the large Trips table)
-- deliberately not created here - not referenced by any of the 14
-- comparable queries, and unlike the small reference-table sample views,
-- it isn't needed for schema fidelity either since nothing reads it.
DROP TABLE TripsInput;
DROP TABLE TripsInputInstants;

\endif

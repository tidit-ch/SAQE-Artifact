-- Time-to-insight pipeline: raw CSV -> queryable Trips table -> query result.
-- Run fresh each trial (drops and rebuilds everything) to match the
-- load-once-query-once "first look at a new dataset" scenario.
-- Invoke with: psql ... -f q_during_periods.sql
-- \timing reports wall-clock time for every statement below.

\timing on

DROP TABLE IF EXISTS Trips CASCADE;
DROP TABLE IF EXISTS Periods1 CASCADE;
DROP TABLE IF EXISTS PeriodsInput CASCADE;
DROP TABLE IF EXISTS BerlinMODInput CASCADE;

-- 1. Staging table for the raw trips.csv
CREATE TABLE BerlinMODInput(
  Moid    integer,
  Tripid  integer,
  Tstart  timestamp,
  Tend    timestamp,
  Xstart  float,
  Ystart  float,
  Xend    float,
  Yend    float
);

COPY BerlinMODInput(Moid, Tripid, Tstart, Tend, Xstart, Ystart, Xend, Yend)
FROM '/home/mobilitydb/BerlinMOD/0.005/trips.csv' DELIMITER ',' CSV HEADER;

-- 2. Build the trajectories (single-pass, no persisted intermediate table,
-- SRID 4326 throughout to match SAQE's no-reprojection approach)
CREATE TABLE Trips(Moid, Tripid, Trip) AS
WITH augmented AS MATERIALIZED (
  SELECT Moid, Tripid, Tstart, Tend, Xstart, Ystart, Xend, Yend,
    (Tend = max(Tend) OVER (PARTITION BY Tripid)) AS is_last
  FROM BerlinMODInput
)
SELECT Moid, Tripid,
  tgeompointseq(array_agg(pt ORDER BY t))
FROM (
  SELECT Moid, Tripid, Tstart AS t,
    tgeompoint(ST_SetSRID(ST_MakePoint(Xstart, Ystart), 4326), Tstart AT TIME ZONE 'UTC') AS pt
  FROM augmented
  UNION ALL
  SELECT Moid, Tripid, Tend AS t,
    tgeompoint(ST_SetSRID(ST_MakePoint(Xend, Yend), 4326), Tend AT TIME ZONE 'UTC') AS pt
  FROM augmented
  WHERE is_last
) points
GROUP BY Moid, Tripid;

-- 3. Load Periods1 (the query-period table needed by this query)
CREATE TABLE PeriodsInput(
  Id     integer,
  Begin  timestamp,
  "End"  timestamp
);

COPY PeriodsInput(Id, Begin, "End")
FROM '/home/mobilitydb/BerlinMOD/0.005/queryperiods.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE Periods1(PeriodId, Period) AS
SELECT Id, span(Begin AT TIME ZONE 'UTC', "End" AT TIME ZONE 'UTC')
FROM PeriodsInput;

-- 4. The actual query: which trips took place entirely within one of the
-- periods from Periods1?
SELECT T.Moid, T.Tripid
FROM Trips T, Periods1 P
WHERE timespan(T.Trip) <@ P.Period;

\timing off

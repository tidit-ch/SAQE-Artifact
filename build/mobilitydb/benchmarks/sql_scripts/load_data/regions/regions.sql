-- Follows the documentation of MobilityDB for loading the regions data
-- https://docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03.html#idp8

-- However this script is slightly different to facilitate the benchmarking process:
-- * We are not projecting the coordinates into a different coordinate system 
--   but instead simply interpret them in a a standard 2D Cartesian coordinate plane like SAQE.
-- * It is neccessary to pass different flags to this script to define
--   how the loading of the points data shall happen
-- * RegionId is declared PRIMARY KEY, which the original documentation does not do.
--   These reference tables are always small (independent of scale factor), so the
--   cost is negligible, and it protects against duplicate/corrupted ids in the source CSV.

-- indicates whether we want to create the index on RegionId
\if :{?create_indexes}
\else
  SELECT 'ERROR: create_indexes is required'::integer; \q
\endif

-- indicates whether to explicitly run ANALYZE after data loading
\if :{?analyze_table}
\else
  SELECT 'ERROR: analyze_table is required'::integer; \q
\endif

-- path (inside the container) to the queryregions.csv of the scale factor to load
\if :{?regions_csv}
\else
  \echo '\n[ERROR] Missing flag: -v regions_csv=/home/mobilitydb/BerlinMOD/<scale>/queryregions.csv\n'
  SELECT 'ERROR: regions_csv is required'::integer; \q
\endif

-- Boolean Type Validation (Server-side)
SELECT
  :create_indexes::boolean AS create_indexes_valid,
  :analyze_table::boolean AS analyze_table_valid
WHERE false;


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
  RegionId integer PRIMARY KEY,
  Geom Geometry(Polygon, 0) );

INSERT INTO Regions (RegionId, Geom)
WITH RegionsSegs AS (
  SELECT RegionId, SegNo, ST_SetSRID(St_MakeLine(
    ST_MakePoint(XStart, YStart), ST_MakePoint(XEnd, YEnd)), 0) AS Geom
  FROM RegionsInput
)

SELECT RegionId, ST_Polygon(ST_LineMerge(ST_Union(Geom ORDER BY SegNo)), 0) AS Geom
FROM RegionsSegs
GROUP BY RegionId;

\if :create_indexes
  CREATE INDEX Regions_geom_idx ON Regions USING gist(Geom);
\endif

\if :analyze_table
  ANALYZE Regions;
\endif

-- 10-row sample view, matching SAQE's Postgres-side regions1 view
-- (config/init.sql) - both systems draw the same "first 10 rows" sample so
-- the two are comparable, not the official docs' random SAMPLESIZE=100
-- methodology (deliberately deferred, see build/mobilitydb/NOTES.md).
CREATE VIEW Regions1 AS SELECT * FROM Regions LIMIT 10;
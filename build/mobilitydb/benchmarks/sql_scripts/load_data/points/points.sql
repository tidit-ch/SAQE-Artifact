-- Follows the documentation of MobilityDB for loading the points data
-- https://docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03.html#idp8

-- However this script is slightly different to facilitate the benchmarking process:
-- * We are not projecting the coordinates into a different coordinate system 
--   but instead simply interpret them in a a standard 2D Cartesian coordinate plane like SAQE.
-- * It is neccessary to pass different flags to this script to define
--   how the loading of the points data shall happen
-- * PointId is declared PRIMARY KEY, which the original documentation does not do.
--   These reference tables are always small (independent of scale factor), so the
--   cost is negligible, and it protects against duplicate/corrupted ids in the source CSV.

-- indicates whether we want to create the index on PointId
\if :{?create_indexes}
\else
  SELECT 'ERROR: create_indexes is required'::integer; \q
\endif

-- indicates whether to explicitly run ANALYZE after data loading
\if :{?analyze_table}
\else
  SELECT 'ERROR: analyze_table is required'::integer; \q
\endif

-- path (inside the container) to the querypoints.csv of the scale factor to load
\if :{?points_csv}
\else
  \echo '\n[ERROR] Missing flag: -v points_csv=/home/mobilitydb/BerlinMOD/<scale>/querypoints.csv\n'
  SELECT 'ERROR: points_csv is required'::integer; \q
\endif

-- Boolean Type Validation (Server-side)
SELECT
  :create_indexes::boolean AS create_indexes_valid,
  :analyze_table::boolean AS analyze_table_valid
WHERE false;


CREATE TABLE Points (
  PointId integer PRIMARY KEY,
  PosX double precision,
  PosY double precision,
  Geom geometry(Point, 0)
);

COPY Points(PointId, PosX, PosY)
FROM :'points_csv' DELIMITER ',' CSV HEADER;

UPDATE Points
  SET Geom = ST_SetSRID(ST_MakePoint(PosX, PosY), 0);

\if :create_indexes
  CREATE INDEX Points_geom_idx ON Points USING gist(Geom);
\endif

\if :analyze_table
  ANALYZE Points;
\endif

-- 10-row sample view, matching SAQE's Postgres-side points1 view
-- (config/init.sql) - both systems draw the same "first 10 rows" sample so
-- the two are comparable, not the official docs' random SAMPLESIZE=100
-- methodology (deliberately deferred, see build/mobilitydb/NOTES.md).
CREATE VIEW Points1 AS SELECT * FROM Points LIMIT 10;
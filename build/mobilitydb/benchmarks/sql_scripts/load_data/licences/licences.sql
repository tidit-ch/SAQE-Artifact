-- Follows the documentation of MobilityDB for loading the licences data
-- https://docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03.html#idp8

-- However this script is slightly different to facilitate the benchmarking process:
-- * It is neccessary to pass different flags to this script to define
--   how the loading of the licences data shall happen
-- * The id column is named LicenceId (not Id as in querylicences.csv's own
--   header), matching this project's PascalCase-Id convention used
--   everywhere else (PointId, RegionId, PeriodId, InstantId).
-- * LicenceId is declared PRIMARY KEY, which the original documentation does not do.
--   These reference tables are always small (independent of scale factor), so the
--   cost is negligible, and it protects against duplicate/corrupted ids in the source CSV.
-- * This is a plain sample-index table (Licence plate -> arbitrary sample id),
--   not a per-vehicle table - it has no Moid/VehId column. Queries join it to
--   Vehicles via the Licence text value, not an id (see Licences1/Licences2
--   join in q1_mb.sql/q3_mb.sql etc.), matching how this project's SAQE side
--   already does it (config/init.sql's licences table, same shape).

-- indicates whether to explicitly run ANALYZE after data loading
\if :{?analyze_table}
\else
  \echo '\n[ERROR] Missing flag: -v analyze_table=true|false\n'
  SELECT 'ERROR: analyze_table is required'::integer; \q
\endif

-- path (inside the container) to the querylicences.csv of the scale factor to load
\if :{?licences_csv}
\else
  \echo '\n[ERROR] Missing flag: -v licences_csv=/home/mobilitydb/BerlinMOD/<scale>/querylicences.csv\n'
  SELECT 'ERROR: licences_csv is required'::integer; \q
\endif

-- Boolean Type Validation (Server-side)
SELECT :analyze_table::boolean AS analyze_table_valid
WHERE false;

CREATE TABLE Licences (
  Licence varchar(32),
  LicenceId integer PRIMARY KEY
);

COPY Licences(Licence, LicenceId)
FROM :'licences_csv' DELIMITER ',' CSV HEADER;

\if :analyze_table
  ANALYZE Licences;
\endif

-- 10-row sample views, matching SAQE's Postgres-side licences1/licences2
-- views (config/init.sql) - both systems draw the same "first 10 rows"/
-- "next 10 rows" sample so the two are comparable, not the official docs'
-- random SAMPLESIZE=100 methodology (deliberately deferred, see
-- build/mobilitydb/NOTES.md).
CREATE VIEW Licences1 AS SELECT * FROM Licences LIMIT 10;
CREATE VIEW Licences2 AS SELECT * FROM Licences LIMIT 10 OFFSET 10;

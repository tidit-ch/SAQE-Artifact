-- Follows the documentation of MobilityDB for loading the instants data
-- https://docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03.html#idp8

-- However this script is slightly different to facilitate the benchmarking process:
-- * It is neccessary to pass different flags to this script to define
--   how the loading of the instants data shall happen
-- * The id column is named InstantId (not Id as in queryinstants.csv's own
--   header), matching this project's PascalCase-Id convention used
--   everywhere else (PointId, RegionId, PeriodId, LicenceId).
-- * InstantId is declared PRIMARY KEY, which the original documentation does not do.
--   These reference tables are always small (independent of scale factor), so the
--   cost is negligible, and it protects against duplicate/corrupted ids in the source CSV.
-- * No timezone conversion (naive/local timestamps), matching every other
--   loader in this project (see build/mobilitydb/NOTES.md, "Deviations").

-- indicates whether we want to create the B-tree index on Instant
\if :{?create_indexes}
\else
  \echo '\n[ERROR] Missing flag: -v create_indexes=true|false\n'
  SELECT 'ERROR: create_indexes is required'::integer; \q
\endif

-- indicates whether to explicitly run ANALYZE after data loading
\if :{?analyze_table}
\else
  \echo '\n[ERROR] Missing flag: -v analyze_table=true|false\n'
  SELECT 'ERROR: analyze_table is required'::integer; \q
\endif

-- path (inside the container) to the queryinstants.csv of the scale factor to load
\if :{?instants_csv}
\else
  \echo '\n[ERROR] Missing flag: -v instants_csv=/home/mobilitydb/BerlinMOD/<scale>/queryinstants.csv\n'
  SELECT 'ERROR: instants_csv is required'::integer; \q
\endif

-- Boolean Type Validation (Server-side)
SELECT
  :create_indexes::boolean AS create_indexes_valid,
  :analyze_table::boolean AS analyze_table_valid
WHERE false;

CREATE TABLE Instants (
  InstantId integer PRIMARY KEY,
  Instant timestamp
);

COPY Instants(InstantId, Instant)
FROM :'instants_csv' DELIMITER ',' CSV HEADER;

-- Matches the documentation exactly (Instants_instant_btree_idx, btree on
-- Instant) - https://docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03.html.
-- Previously missing entirely; InstantId's PRIMARY KEY only indexes
-- InstantId, not the Instant timestamp column itself.
\if :create_indexes
  CREATE INDEX Instants_instant_btree_idx ON Instants USING btree(Instant);
\endif

\if :analyze_table
  ANALYZE Instants;
\endif

-- 10-row sample view, matching SAQE's Postgres-side instants1 view
-- (config/init.sql) - both systems draw the same "first 10 rows" sample so
-- the two are comparable, not the official docs' random SAMPLESIZE=100
-- methodology (deliberately deferred, see build/mobilitydb/NOTES.md).
CREATE VIEW Instants1 AS SELECT * FROM Instants LIMIT 10;

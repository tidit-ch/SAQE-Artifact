-- Follows the documentation of MobilityDB for loading the periods data
-- https://docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03.html#idp8

-- However this script is slightly different to facilitate the benchmarking process:
-- * It is neccessary to pass different flags to this script to define
--   how the loading of the periods data shall happen
-- * PeriodId is declared PRIMARY KEY, which the original documentation does not do.
--   These reference tables are always small (independent of scale factor), so the
--   cost is negligible, and it protects against duplicate/corrupted ids in the source CSV.

-- indicates whether we want to create the index on PeriodId
\if :{?create_indexes}
\else
  SELECT 'ERROR: create_indexes is required'::integer; \q
\endif

-- indicates whether to explicitly run ANALYZE after data loading
\if :{?analyze_table}
\else
  SELECT 'ERROR: analyze_table is required'::integer; \q
\endif

-- path (inside the container) to the queryperiods.csv of the scale factor to load
\if :{?periods_csv}
\else
  \echo '\n[ERROR] Missing flag: -v periods_csv=/home/mobilitydb/BerlinMOD/<scale>/queryperiods.csv\n'
  SELECT 'ERROR: periods_csv is required'::integer; \q
\endif

-- Boolean Type Validation (Server-side)
SELECT
  :create_indexes::boolean AS create_indexes_valid,
  :analyze_table::boolean AS analyze_table_valid
WHERE false;


CREATE TABLE Periods (
  PeriodId integer PRIMARY KEY,
  BeginP timestamp,
  EndP timestamp,
  Period tstzspan
);

COPY Periods(PeriodId, BeginP, EndP)
FROM :'periods_csv' DELIMITER ',' CSV HEADER;

UPDATE Periods
SET Period = span(BeginP, EndP);

\if :create_indexes
  -- GiST index on period column for range containment operations (<@)
  CREATE INDEX Periods_period_idx ON Periods USING gist(Period);
\endif

\if :analyze_table
  ANALYZE Periods;
\endif

-- 10-row sample view, matching SAQE's Postgres-side periods1 view
-- (config/init.sql) - both systems draw the same "first 10 rows" sample so
-- the two are comparable, not the official docs' random SAMPLESIZE=100
-- methodology (deliberately deferred, see build/mobilitydb/NOTES.md).
CREATE VIEW Periods1 AS SELECT * FROM Periods LIMIT 10;
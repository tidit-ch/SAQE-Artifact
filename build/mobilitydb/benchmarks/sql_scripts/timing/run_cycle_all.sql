-- One full "clean state -> load -> query -> report elapsed" cycle for the
-- official-numbering 14-query set, using one of the two consolidated
-- documentation-following loaders
-- (build/mobilitydb/benchmarks/load_all_documented.sql or
-- load_all_partitioned.sql) instead of the older per-table loaders
-- run_cycle.sql composes - see those two scripts' own header comments for
-- why they exist and what they restore relative to the per-table loaders.
--
-- Same load+query bracketing rationale as run_cycle.sql (SAQE has no
-- persistent load step, so MobilityDB is timed load+query together too -
-- see build/mobilitydb/NOTES.md, "Timing methodology"). Run in a loop via
-- run_timed_cycles_all.sh for multiple trials.
--
-- Required flags:
--   -v points_csv=/home/mobilitydb/BerlinMOD/<scale>/querypoints.csv
--   -v regions_csv=/home/mobilitydb/BerlinMOD/<scale>/queryregions.csv
--   -v instants_csv=/home/mobilitydb/BerlinMOD/<scale>/queryinstants.csv
--   -v periods_csv=/home/mobilitydb/BerlinMOD/<scale>/queryperiods.csv
--   -v vehicles_csv=/home/mobilitydb/BerlinMOD/<scale>/datamcar.csv
--   -v licences_csv=/home/mobilitydb/BerlinMOD/<scale>/querylicences.csv
--   -v trips_csv=/home/mobilitydb/BerlinMOD/<scale>/trips.csv
--   -v load_points=true|false     -v load_regions=true|false
--   -v load_instants=true|false   -v load_periods=true|false
--   -v load_vehicles=true|false   -v load_licences=true|false
--   -v load_trips=true|false
--      (passed straight through to loader_script - only load what the
--      query being timed actually reads, the same "don't pay for what you
--      don't read" convention run_cycle.sql's load_X flags already use;
--      see load_all_documented.sql's header comment for the
--      load_licences/load_trips => load_vehicles dependency)
--   -v loader_script=build/mobilitydb/benchmarks/load_all_documented.sql
--      (or load_all_partitioned.sql - resolved via \i relative to the repo
--      root, like query_file below)
--   -v query_file=build/mobilitydb/benchmarks/sql_scripts/queries/qN_mb.sql

\if :{?points_csv}
\else
  \echo '\n[ERROR] Missing flag: -v points_csv=...\n'
  \q
\endif
\if :{?regions_csv}
\else
  \echo '\n[ERROR] Missing flag: -v regions_csv=...\n'
  \q
\endif
\if :{?instants_csv}
\else
  \echo '\n[ERROR] Missing flag: -v instants_csv=...\n'
  \q
\endif
\if :{?periods_csv}
\else
  \echo '\n[ERROR] Missing flag: -v periods_csv=...\n'
  \q
\endif
\if :{?vehicles_csv}
\else
  \echo '\n[ERROR] Missing flag: -v vehicles_csv=...\n'
  \q
\endif
\if :{?licences_csv}
\else
  \echo '\n[ERROR] Missing flag: -v licences_csv=...\n'
  \q
\endif
\if :{?trips_csv}
\else
  \echo '\n[ERROR] Missing flag: -v trips_csv=...\n'
  \q
\endif
\if :{?loader_script}
\else
  \echo '\n[ERROR] Missing flag: -v loader_script=build/mobilitydb/benchmarks/load_all_documented.sql\n'
  \q
\endif
\if :{?query_file}
\else
  \echo '\n[ERROR] Missing flag: -v query_file=/path/to/queries/qN_mb.sql\n'
  \q
\endif

-- Clean slate - safe even if nothing exists yet (first run). CASCADE drops
-- Trips' partitions automatically too when loader_script is
-- load_all_partitioned.sql (they're child tables of Trips). Not dropping
-- create_partitions_by_date: CREATE OR REPLACE in that loader makes
-- re-running it across cycles idempotent, so leaving the function in place
-- between cycles is harmless.
DROP VIEW IF EXISTS Trips1, Points1, Regions1, Instants1, Periods1, Licences1, Licences2 CASCADE;
DROP TABLE IF EXISTS Trips, Vehicles, Licences, Points, Regions, Periods, Instants,
  RegionsInput, TripsInput, TripsInputInstants CASCADE;

CREATE TEMP TABLE _cycle_timing (phase text PRIMARY KEY, ts timestamptz);
INSERT INTO _cycle_timing VALUES ('start', clock_timestamp());

\i :loader_script

INSERT INTO _cycle_timing VALUES ('after_load', clock_timestamp());

\i :query_file

INSERT INTO _cycle_timing VALUES ('after_query', clock_timestamp());

SELECT
  extract(epoch FROM (l.ts - s.ts)) AS load_seconds,
  extract(epoch FROM (q.ts - l.ts)) AS query_seconds,
  extract(epoch FROM (q.ts - s.ts)) AS total_seconds
FROM
  (SELECT ts FROM _cycle_timing WHERE phase = 'start') s,
  (SELECT ts FROM _cycle_timing WHERE phase = 'after_load') l,
  (SELECT ts FROM _cycle_timing WHERE phase = 'after_query') q;

-- Leave the database clean for the next cycle.
DROP VIEW IF EXISTS Trips1, Points1, Regions1, Instants1, Periods1, Licences1, Licences2 CASCADE;
DROP TABLE IF EXISTS Trips, Vehicles, Licences, Points, Regions, Periods, Instants,
  RegionsInput, TripsInput, TripsInputInstants CASCADE;
DROP TABLE _cycle_timing;

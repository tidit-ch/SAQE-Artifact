-- One full "clean state -> load -> query -> report elapsed" cycle.
--
-- Why load is included in the timed interval: SAQE has no persistent load
-- step of its own - it re-scans its CSVs directly on every query execution,
-- so there is no equivalent of MobilityDB's warm, already-indexed, persisted
-- table state. Timing MobilityDB as "query-only against warm data" while
-- SAQE is inherently "load+query every time" would not be a fair comparison.
-- This script brackets load+query together so both systems are timed the
-- same way. Run it in a loop (see run_timed_cycles.sh) for multiple trials.
--
-- Reuses the existing per-table loaders unchanged via \i - this script does
-- not duplicate their logic, only sequences them and brackets timestamps
-- around load vs. query.
--
-- Required flags (CSV paths are container-internal, same as
-- benchmarks/load_data/* - see build/mobilitydb/README.md step 6):
--   -v vehicles_csv=/home/mobilitydb/BerlinMOD/<scale>/datamcar.csv
--   -v regions_csv=/home/mobilitydb/BerlinMOD/<scale>/queryregions.csv
--   -v periods_csv=/home/mobilitydb/BerlinMOD/<scale>/queryperiods.csv
--   -v points_csv=/home/mobilitydb/BerlinMOD/<scale>/querypoints.csv
--   -v trips_csv=/home/mobilitydb/BerlinMOD/<scale>/trips.csv
--   -v create_indexes=true|false
--   -v join_vehicles=true|false
--   -v analyze_table=true|false
--   -v load_vehicles=true|false (Trips itself always loads - every query needs it)
--   -v load_regions=true|false
--   -v load_periods=true|false
--   -v load_points=true|false
--   -v query_file=build/mobilitydb/benchmarks/sql_scripts/queries/qN.sql
--      (run via \i - resolved by psql relative to the repo root, like every
--      other -f/-i path in this project; unlike the CSV paths above, which
--      are container-internal, since COPY runs server-side)
--   -v trips_loader=build/mobilitydb/benchmarks/sql_scripts/load_data/trips/optimized.sql
--      (which Trips-loading strategy this cycle uses - e.g. optimized.sql
--      (tgeompoint, the default), as_linestring.sql (plain LINESTRINGM, see
--      build/mobilitydb/q4_temporal_overhead_finding.md), base_partitioned.sql,
--      optimized_partitioned.sql - resolved via \i the same way as query_file)

\if :{?vehicles_csv}
\else
  \echo '\n[ERROR] Missing flag: -v vehicles_csv=...\n'
  \q
\endif
\if :{?regions_csv}
\else
  \echo '\n[ERROR] Missing flag: -v regions_csv=...\n'
  \q
\endif
\if :{?periods_csv}
\else
  \echo '\n[ERROR] Missing flag: -v periods_csv=...\n'
  \q
\endif
\if :{?points_csv}
\else
  \echo '\n[ERROR] Missing flag: -v points_csv=...\n'
  \q
\endif
\if :{?trips_csv}
\else
  \echo '\n[ERROR] Missing flag: -v trips_csv=...\n'
  \q
\endif
\if :{?create_indexes}
\else
  \echo '\n[ERROR] Missing flag: -v create_indexes=true|false\n'
  \q
\endif
\if :{?join_vehicles}
\else
  \echo '\n[ERROR] Missing flag: -v join_vehicles=true|false\n'
  \q
\endif
\if :{?analyze_table}
\else
  \echo '\n[ERROR] Missing flag: -v analyze_table=true|false\n'
  \q
\endif
\if :{?load_vehicles}
\else
  \echo '\n[ERROR] Missing flag: -v load_vehicles=true|false\n'
  \q
\endif
\if :{?load_regions}
\else
  \echo '\n[ERROR] Missing flag: -v load_regions=true|false\n'
  \q
\endif
\if :{?load_periods}
\else
  \echo '\n[ERROR] Missing flag: -v load_periods=true|false\n'
  \q
\endif
\if :{?load_points}
\else
  \echo '\n[ERROR] Missing flag: -v load_points=true|false\n'
  \q
\endif
\if :{?query_file}
\else
  \echo '\n[ERROR] Missing flag: -v query_file=/path/to/queries/qN.sql\n'
  \q
\endif
\if :{?trips_loader}
\else
  \echo '\n[ERROR] Missing flag: -v trips_loader=/path/to/load_data/trips/....sql\n'
  \q
\endif

-- Clean slate - safe even if nothing exists yet (first run).
-- RegionsInput/TripsInput are permanent staging tables the loaders never
-- drop themselves (by design - see build/mobilitydb/NOTES.md); a repeated
-- clean-load-query cycle has to drop them too or the next load fails with
-- "relation already exists".
DROP TABLE IF EXISTS Trips, Vehicles, Regions, Periods, Points, RegionsInput, TripsInput CASCADE;

-- temporary table for remembering timestamps at each phase of 
-- the cycle (start, after loading, after the query)
CREATE TEMP TABLE _cycle_timing (phase text PRIMARY KEY, ts timestamptz);
INSERT INTO _cycle_timing VALUES ('start', clock_timestamp());

-- Trips always loads - every query reads it. The other four are gated by
-- their own load_X flag, since not every query touches every table (e.g.
-- q1 only needs Trips; loading the rest too would inflate its "load" time
-- with work the query never uses).
\if :load_vehicles
  \i build/mobilitydb/benchmarks/sql_scripts/load_data/vehicles/vehicles.sql
\endif
\if :load_regions
  \i build/mobilitydb/benchmarks/sql_scripts/load_data/regions/regions.sql
\endif
\if :load_periods
  \i build/mobilitydb/benchmarks/sql_scripts/load_data/periods/periods.sql
\endif
\if :load_points
  \i build/mobilitydb/benchmarks/sql_scripts/load_data/points/points.sql
\endif
\i :trips_loader

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

-- Leave the database clean for the next cycle. IF EXISTS now matters here,
-- not just at the top: whichever of Vehicles/Regions/Periods/Points were
-- skipped via load_X=false were never created this cycle.
DROP TABLE IF EXISTS Trips, Vehicles, Regions, Periods, Points, RegionsInput, TripsInput CASCADE;
DROP TABLE _cycle_timing;

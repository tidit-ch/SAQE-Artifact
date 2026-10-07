-- Smoke test: not a benchmark, just a sanity check that the container,
-- extension, and Trips-loading pipeline all actually work end to end.
-- Loads Trips at the given scale, prints the row count, then drops
-- everything it created so the database is left in a clean state.
--
-- Usage:
--   PGPASSWORD=docker psql -h localhost -p 25432 -U docker -d mobilitydb \
--     -v trips_csv=/home/mobilitydb/BerlinMOD/0.005/trips.csv \
--     -f build/mobilitydb/benchmarks/smoke_test.sql

\if :{?trips_csv}
\else
  \echo '\n[ERROR] Missing flag: -v trips_csv=/home/mobilitydb/BerlinMOD/<scale>/trips.csv\n'
  SELECT 'ERROR: trips_csv is required'::integer; \q
\endif

\set join_vehicles false
\set create_indexes false
\set analyze_table false

\echo 'Loading Trips...'
\i build/mobilitydb/benchmarks/sql_scripts/load_data/trips/optimized.sql

\echo 'Row count:'
SELECT count(*) AS trip_count FROM Trips;

\echo 'Cleaning up...'
DROP TABLE IF EXISTS Trips CASCADE;
DROP TABLE IF EXISTS TripsInput CASCADE;

\echo 'Smoke test passed — database is back to a clean state.'

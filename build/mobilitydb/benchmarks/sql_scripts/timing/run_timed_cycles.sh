#!/usr/bin/env bash
# Runs run_cycle.sql N times (clean -> load -> query -> clean, per trial) and
# reports load/query/total seconds for each trial plus the mean. Run from the
# repo root - see build/mobilitydb/benchmarks/sql_scripts/timing/run_cycle.sql
# for why load time is included.
#
# Usage:
#   build/mobilitydb/benchmarks/sql_scripts/timing/run_timed_cycles.sh \
#     <scale> <query_file> <trials> [create_indexes] [join_vehicles] [analyze_table] \
#     [load_vehicles] [load_regions] [load_periods] [load_points] [trips_loader]
#
# Example:
#   build/mobilitydb/benchmarks/sql_scripts/timing/run_timed_cycles.sh \
#     0.005 build/mobilitydb/benchmarks/sql_scripts/queries/legacy_q1_q4/q2.sql 5

# exit immediately on any command error
set -euo pipefail

# parsing arguments
SCALE="${1:?Usage: $0 <scale> <query_file> <trials> [create_indexes] [join_vehicles] [analyze_table] [load_vehicles] [load_regions] [load_periods] [load_points]}" # if $1 is unset or empty, bash prints message "Usage: ..."
QUERY_FILE="${2:?query_file is required}"
TRIALS="${3:?trials is required}"
CREATE_INDEXES="${4:-true}"
JOIN_VEHICLES="${5:-true}"
ANALYZE_TABLE="${6:-true}"
# Trips always loads regardless (every query needs it) - these four default
# to true so a caller not passing them gets the old "load everything" behavior.
LOAD_VEHICLES="${7:-true}"
LOAD_REGIONS="${8:-true}"
LOAD_PERIODS="${9:-true}"
LOAD_POINTS="${10:-true}"
# Which Trips-loading strategy to use - defaults to the tgeompoint-based
# loader so callers not passing this get the old behavior unchanged. See
# build/mobilitydb/benchmarks/sql_scripts/load_data/trips/ for alternatives
# (e.g. as_linestring.sql - build/mobilitydb/q4_temporal_overhead_finding.md).
TRIPS_LOADER="${11:-build/mobilitydb/benchmarks/sql_scripts/load_data/trips/optimized.sql}"

CSV="/home/mobilitydb/BerlinMOD/$SCALE"
PSQL=(env PGPASSWORD=docker psql -h localhost -p 25432 -U docker -d mobilitydb -t -A -F',')

# declare three separate arrays
declare -a load_times query_times total_times

for trial in $(seq 1 "$TRIALS"); do
  # psql prints DDL status lines (CREATE TABLE, COPY n, DROP TABLE, ...)
  # alongside the final SELECT's data row even with -t -A; the numeric
  # result is always the last, so filter for exactly that shape.
  result=$("${PSQL[@]}" \
    -v vehicles_csv="$CSV/datamcar.csv" \
    -v regions_csv="$CSV/queryregions.csv" \
    -v periods_csv="$CSV/queryperiods.csv" \
    -v points_csv="$CSV/querypoints.csv" \
    -v trips_csv="$CSV/trips.csv" \
    -v create_indexes="$CREATE_INDEXES" \
    -v join_vehicles="$JOIN_VEHICLES" \
    -v analyze_table="$ANALYZE_TABLE" \
    -v load_vehicles="$LOAD_VEHICLES" \
    -v load_regions="$LOAD_REGIONS" \
    -v load_periods="$LOAD_PERIODS" \
    -v load_points="$LOAD_POINTS" \
    -v query_file="$QUERY_FILE" \
    -v trips_loader="$TRIPS_LOADER" \
    -v row_limit="${ROW_LIMIT:-100}" \
    -f build/mobilitydb/benchmarks/sql_scripts/timing/run_cycle.sql \
    | grep -E '^-?[0-9.]+,-?[0-9.]+,-?[0-9.]+$' | tail -n1)
  # "${PSQL[@]}" re-expands the PSQL array into actual words of a command -> run psql with all those connection flags
  # -v something="..." lets us define variables we can pass into an SQL script
  # -f executes the run_cycle.sql script
  # | <-> "take everything the command on the left printed, and feed it as input to the command on the right"
  # grep discards every DDL status line (CREATE TABLE, COPY n, ...) and keeps only the line that's exactly number,number,number.
  # tail -n1 only takes the last line of what came in

  IFS=',' read -r load_s query_s total_s <<< "$result"
  load_times+=("$load_s")
  query_times+=("$query_s")
  total_times+=("$total_s")
  echo "trial $trial: load=${load_s}s query=${query_s}s total=${total_s}s"
done

mean() {
  local sum=0
  for v in "$@"; do sum=$(echo "$sum + $v" | bc -l); done
  # bc -l defaults to 20 decimal places (e.g. 1.59235700000000000000) -
  # printf rounds that down to a sane, fixed 6 decimals for display.
  printf '%.6f' "$(echo "$sum / $#" | bc -l)"
}

echo "--- mean over $TRIALS trials ---"
echo "load:  $(mean "${load_times[@]}")s"
echo "query: $(mean "${query_times[@]}")s"
echo "total: $(mean "${total_times[@]}")s"

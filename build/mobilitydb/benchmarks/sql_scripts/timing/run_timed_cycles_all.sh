#!/usr/bin/env bash
# Runs run_cycle_all.sql N times (clean -> load -> query -> clean, per
# trial) and reports load/query/total seconds for each trial plus the mean -
# the same model as run_timed_cycles.sh, but using one of the two
# consolidated documentation-following loaders (load_all_documented.sql /
# load_all_partitioned.sql) instead of the older per-table loaders under
# benchmarks/sql_scripts/load_data/.
#
# Only the tables passed in <tables> get loaded each cycle (everything else
# defaults to false) - the same "don't pay to load what the query doesn't
# read" fairness reasoning run_cycle.sql's own load_X flags already use, and
# SAQE's own per-query table lists in
# benches/bench_saqe_vs_mobilitydb_timed_all.rs already follow. Whatever
# tables *are* loaded still get the complete, unconditional treatment
# (every index, ANALYZE, every sample view) - see
# load_all_documented.sql's own header comment.
#
# Usage:
#   build/mobilitydb/benchmarks/sql_scripts/timing/run_timed_cycles_all.sh \
#     <scale> <query_file> <trials> <loader_script> <tables>
#   <tables> is a comma-separated subset of:
#     points,regions,instants,periods,vehicles,licences,trips
#   licences/trips both require vehicles too (see load_all_documented.sql's
#   header comment's Dependencies note) - include it explicitly, it is not
#   implied automatically.
#
# Example:
#   build/mobilitydb/benchmarks/sql_scripts/timing/run_timed_cycles_all.sh \
#     0.005 build/mobilitydb/benchmarks/sql_scripts/queries/q13_mb.sql 5 \
#     build/mobilitydb/benchmarks/load_all_documented.sql \
#     trips,vehicles,periods,regions

set -euo pipefail

SCALE="${1:?Usage: $0 <scale> <query_file> <trials> <loader_script> <tables>}"
QUERY_FILE="${2:?query_file is required}"
TRIALS="${3:?trials is required}"
LOADER_SCRIPT="${4:?loader_script is required (build/mobilitydb/benchmarks/load_all_documented.sql or load_all_partitioned.sql)}"
TABLES="${5:?tables is required - comma-separated subset of points,regions,instants,periods,vehicles,licences,trips}"

CSV="/home/mobilitydb/BerlinMOD/$SCALE"
PSQL=(env PGPASSWORD=docker psql -h localhost -p 25432 -U docker -d mobilitydb -t -A -F',')

# Turns the comma-separated <tables> list into one true|false per load_X
# flag - e.g. "trips,vehicles" => load_trips=true, load_vehicles=true,
# everything else load_X=false.
table_flag() {
  local name="$1"
  case ",$TABLES," in
    *",$name,"*) echo true ;;
    *) echo false ;;
  esac
}
LOAD_POINTS=$(table_flag points)
LOAD_REGIONS=$(table_flag regions)
LOAD_INSTANTS=$(table_flag instants)
LOAD_PERIODS=$(table_flag periods)
LOAD_VEHICLES=$(table_flag vehicles)
LOAD_LICENCES=$(table_flag licences)
LOAD_TRIPS=$(table_flag trips)

declare -a load_times query_times total_times

for trial in $(seq 1 "$TRIALS"); do
  # psql prints DDL/NOTICE status lines alongside the final SELECT's data
  # row even with -t -A; the numeric result is always the last, so filter
  # for exactly that shape (same convention as run_timed_cycles.sh).
  result=$("${PSQL[@]}" \
    -v points_csv="$CSV/querypoints.csv" \
    -v regions_csv="$CSV/queryregions.csv" \
    -v instants_csv="$CSV/queryinstants.csv" \
    -v periods_csv="$CSV/queryperiods.csv" \
    -v vehicles_csv="$CSV/datamcar.csv" \
    -v licences_csv="$CSV/querylicences.csv" \
    -v trips_csv="$CSV/trips.csv" \
    -v load_points="$LOAD_POINTS" \
    -v load_regions="$LOAD_REGIONS" \
    -v load_instants="$LOAD_INSTANTS" \
    -v load_periods="$LOAD_PERIODS" \
    -v load_vehicles="$LOAD_VEHICLES" \
    -v load_licences="$LOAD_LICENCES" \
    -v load_trips="$LOAD_TRIPS" \
    -v loader_script="$LOADER_SCRIPT" \
    -v query_file="$QUERY_FILE" \
    -f build/mobilitydb/benchmarks/sql_scripts/timing/run_cycle_all.sql \
    | grep -E '^-?[0-9.]+,-?[0-9.]+,-?[0-9.]+$' | tail -n1)

  IFS=',' read -r load_s query_s total_s <<< "$result"
  load_times+=("$load_s")
  query_times+=("$query_s")
  total_times+=("$total_s")
  echo "trial $trial: load=${load_s}s query=${query_s}s total=${total_s}s"
done

mean() {
  # The (export LC_NUMERIC=C; ...) subshell is required here, not cosmetic:
  # bc always outputs period-decimal numbers regardless of locale, but
  # bash's printf builtin respects the system's LC_NUMERIC - on a machine
  # configured with a comma-decimal locale, feeding bc's period-formatted
  # output into printf fails to parse ("invalid number"), silently
  # truncates to the integer part, and reformats with a comma - confirmed
  # empirically on mond47 (e.g. "7.876623" -> "7,000000"). A plain
  # `LC_NUMERIC=C printf ...` prefix does NOT fix this - bash's printf
  # builtin doesn't re-read a per-command locale override the way an
  # external command would (confirmed empirically too) - it has to be
  # `export`ed into the environment first. Scoped to a subshell so it
  # doesn't leak into the rest of this script.
  local sum=0
  for v in "$@"; do sum=$(echo "$sum + $v" | bc -l); done
  (export LC_NUMERIC=C; printf '%.6f' "$(echo "$sum / $#" | bc -l)")
}

echo "--- mean over $TRIALS trials ---"
echo "load:  $(mean "${load_times[@]}")s"
echo "query: $(mean "${query_times[@]}")s"
echo "total: $(mean "${total_times[@]}")s"

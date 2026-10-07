#!/usr/bin/env bash
# Runs the SAQE-vs-MobilityDB comparison for the 14-query official-numbering
# set (q1_sq/q1_mb .. q17_sq/q17_mb, minus q6/q10/q16 - the tdwithin
# capability gap, see BENCHMARK.md), in two phases:
#
#   Phase 1 - correctness gate. Every query is run once against all five
#   systems (SAQE CSV, SAQE Parquet, SAQE Postgres pushdown, MobilityDB
#   loaded via load_all_documented.sql, MobilityDB loaded via
#   load_all_partitioned.sql) and their row counts compared. If any two
#   systems disagree, the run stops before Phase 2: a timing number is
#   meaningless if the systems being timed aren't even comparing the same
#   thing. A row count of exactly 0 across all five systems is treated as a
#   PASS like any other matching count, not specially flagged - whether
#   that's a legitimate empty result (q11/q12 at small scales - see
#   build/mobilitydb/NOTES.md) or something else is a judgment call left
#   to whoever's reading the results, not something this gate tries to
#   guess at automatically across different scales.
#
#   Both phases load MobilityDB selectively, per query, via
#   mb_tables_for_query() below - a direct translation of SAQE's own
#   per-query table lists (benches/bench_saqe_vs_mobilitydb_timed_all.rs),
#   so neither side ever pays to load a table its query never reads. This
#   matters for Phase 2's timing fairness, but it also just makes Phase 1
#   much faster (no more reloading all 7 tables, including the expensive
#   Trips, for every one of the 14 queries).
#
#   Phase 2 - timing, only reached if Phase 1 passes. Same fixed-trial-count
#   model as run_benchmark_comparison.sh (see build/mobilitydb/NOTES.md,
#   "Timing methodology"): SAQE via bench_saqe_vs_mobilitydb_timed_all (one
#   binary parameterized by query number, not one per query), MobilityDB via
#   run_timed_cycles_all.sh - run twice per query, once loaded via
#   load_all_documented.sql and once via load_all_partitioned.sql, so the
#   two loading approaches are directly comparable too.
#
# Run from the repo root. Requires the MobilityDB container up (see
# build/mobilitydb/README.md), the chameleon_postgis_dev container up (see
# build/docker-compose.dev.yml), and psql on PATH.
#
# Usage: ./run_benchmark_comparison_17.sh --help

set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: ./run_benchmark_comparison_17.sh [--queries N[,N...]] [scale] [trials]
  Defaults: scale=1.0, trials=1.
  --queries restricts the run to the listed query numbers, in the order given,
  for both phases. Without it the whole set runs:
      1 2 3 4 5 7 8 9 11 12 13 14 15 17 21 22
  q6/q10/q16 are excluded by design (tdwithin capability gap) and are rejected
  rather than skipped. q21/q22 are DataFusion-only - q13 with its trips split
  across CSV + Parquet and CSV + Postgres - so they run no MobilityDB step.
  e.g. --queries 13        one query
       --queries 1,3       two queries
       --queries 13,21,22  q13 against its two mixed-source variants
USAGE
}

# --queries may appear anywhere; everything else stays positional, so the
# existing `./run_benchmark_comparison_17.sh 1.0 5` form is unchanged.
QUERIES_ARG=""
POSITIONAL=()
while [ $# -gt 0 ]; do
  case "$1" in
    --queries)
      shift
      [ $# -gt 0 ] || { echo "--queries needs a value, e.g. --queries 1,3" >&2; exit 1; }
      QUERIES_ARG="$1"; shift ;;
    --queries=*) QUERIES_ARG="${1#*=}"; shift ;;
    -h|--help) usage; exit 0 ;;
    --) shift; while [ $# -gt 0 ]; do POSITIONAL+=("$1"); shift; done ;;
    -*) echo "unknown option: $1" >&2; usage >&2; exit 1 ;;
    *) POSITIONAL+=("$1"); shift ;;
  esac
done
set -- ${POSITIONAL[@]+"${POSITIONAL[@]}"}

# ---- Config ----
SCALE="${1:-1.0}"
TRIALS="${2:-1}"
# ----------------

export CONFIG_FILE="${CONFIG_FILE:-config/config_dev.json}"

mkdir -p benchmark_results
RUN_TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="benchmark_results/run17_${RUN_TIMESTAMP}.log"   # unfiltered transcript of everything printed during becnhmarking
SUMMARY_FILE="benchmark_results/summary17_${RUN_TIMESTAMP}.txt"   # summary of the complete benchmark run
exec > >(tee "$LOG_FILE") 2>&1

echo "--- Warming CSV cache (data/berlinmod/$SCALE) ---"
cat "data/berlinmod/$SCALE"/*.csv > /dev/null

CSV="/home/mobilitydb/BerlinMOD/$SCALE"
PSQL=(env PGPASSWORD=docker psql -h localhost -p 25432 -U docker -d mobilitydb -t -A)
DOC_LOADER="build/mobilitydb/benchmarks/load_all_documented.sql"
PART_LOADER="build/mobilitydb/benchmarks/load_all_partitioned.sql"

# Query numbers in the 14-query comparable set. q6/q10/q16 excluded
# (tdwithin capability gap). Each one's MobilityDB query file is a plain
# predictable path (mb_query_file() below) - no lookup table needed, and
# associative arrays (declare -A) aren't available under macOS's default
# Bash 3.2, which this script needs to run under without requiring a newer
# bash on PATH.
ALL_QUERY_NUMS=(1 2 3 4 5 7 8 9 11 12 13 14 15 17 21 22)

# q21/q22 are q13 with its trips split across two stores - first half CSV, second half Parquet
# (21) or Postgres (22). They exist to measure what a mixed-source table costs, so they are
# DataFusion-only: no MobilityDB counterpart exists or is wanted, and both phases skip every
# MobilityDB step for them.
DATAFUSION_ONLY=(21 22)
is_datafusion_only() {
  local n=$1 candidate
  for candidate in "${DATAFUSION_ONLY[@]}"; do
    [ "$candidate" = "$n" ] && return 0
  done
  return 1
}

# --queries narrows the set for both phases. The requested order is kept, so
# `--queries 13,1` runs q13 then q1; duplicates are dropped. Anything outside
# ALL_QUERY_NUMS is rejected rather than silently skipped - asking for q6 is a
# mistake worth reporting, not a no-op.
# Expands --queries into QUERY_NUMS, or takes the whole set when it was not given. Rejects a
# non-number, and a number outside ALL_QUERY_NUMS (q6/q10/q16 are excluded by design - see
# BENCHMARK.md - so asking for one is a mistake worth reporting, not a no-op). Keeps the
# requested order and drops repeats. The `case` globs are membership tests over a space-padded
# list: macOS ships Bash 3.2, which has no associative arrays.
select_queries() {
  QUERY_NUMS=("${ALL_QUERY_NUMS[@]}")
  [ -n "$QUERIES_ARG" ] || return 0

  local known=" ${ALL_QUERY_NUMS[*]} "
  local selected="" requested
  for requested in ${QUERIES_ARG//,/ }; do
    case "$requested" in
      ''|*[!0-9]*) echo "--queries: '$requested' is not a query number" >&2; exit 1 ;;
    esac
    case "$known" in
      *" $requested "*) ;;
      *) echo "--queries: q$requested is not in the comparable set (${ALL_QUERY_NUMS[*]});" >&2
         echo "           q6/q10/q16 are excluded by design - see BENCHMARK.md." >&2
         exit 1 ;;
    esac
    case " $selected " in
      *" $requested "*) ;;                 # already chosen
      *) selected="$selected $requested" ;;
    esac
  done
  QUERY_NUMS=($selected)
}

announce_queries() {
  local scope
  if [ "${#QUERY_NUMS[@]}" -eq "${#ALL_QUERY_NUMS[@]}" ]; then
    scope="all ${#QUERY_NUMS[@]}"
  else
    scope="${#QUERY_NUMS[@]} of ${#ALL_QUERY_NUMS[@]}"
  fi
  echo "--- Queries: $scope (${QUERY_NUMS[*]}) ---"
}

select_queries
announce_queries

mb_query_file() {
  echo "build/mobilitydb/benchmarks/sql_scripts/queries/q$1_mb.sql"
}

# Per-query MobilityDB table list for Phase 2 (fair, selective loading -
# only pay to load what each query actually needs). A direct translation of
# SAQE's own per-query table lists in
# benches/bench_saqe_vs_mobilitydb_timed_all.rs (datamcar -> vehicles,
# regions1 -> regions), made possible because Trips' FOREIGN KEY to
# Vehicles is itself conditional on load_vehicles now (see
# load_all_documented.sql's header comment's Dependencies note) - q9/q17
# genuinely don't need vehicles loaded at all, matching SAQE exactly.
mb_tables_for_query() {
  case "$1" in
    1)  echo "vehicles,licences" ;;
    2)  echo "vehicles" ;;
    3)  echo "trips,instants,licences,vehicles" ;;
    4)  echo "trips,vehicles,points" ;;
    5)  echo "trips,licences,vehicles" ;;
    7)  echo "trips,vehicles,points" ;;
    8)  echo "trips,licences,periods,vehicles" ;;
    9)  echo "trips,periods" ;;
    11) echo "trips,vehicles,points,instants" ;;
    12) echo "trips,vehicles,points,instants" ;;
    13) echo "trips,vehicles,periods,regions" ;;
    14) echo "trips,vehicles,instants,regions" ;;
    15) echo "trips,vehicles,periods,points" ;;
    17) echo "trips,points" ;;
    *) echo "ERROR: no table list for q$1" >&2; exit 1 ;;
  esac
}

# Converts the comma separated table lists from 'mb_tables_for_query'
# into a yes/no flag for a provided table name indicating whether it is included
mb_table_flag() {
  local tables="$1" name="$2"
  case ",$tables," in
    *",$name,"*) echo true ;;
    *) echo false ;;
  esac
}

mobilitydb_load() {
  # Takes the same comma-separated <tables> list mb_tables_for_query()
  # produces (and run_timed_cycles_all.sh already accepts) - only load
  # what the query being checked actually needs. Used in both phases now:
  # Phase 1 gets this for free simply because it's the same underlying
  # per-table load_X flags Phase 2 already relies on, not a separate
  # mechanism - and it happens to make Phase 1 much faster too (no more
  # reloading all 7 tables, including the expensive Trips, for every one
  # of the 14 queries).
  local loader="$1" tables="$2" # loader = "$DOC_LOADER" or "$PART_LOADER"
  # pass the path to the csv file of the corresponding table to the sql script underlying the argument 'loader'
  # compute for each table a flag indicating whether it shall be loaded
  # and pass it to the sql script underlying the argument 'loader'
  "${PSQL[@]}" \
    -v points_csv="$CSV/querypoints.csv" \
    -v regions_csv="$CSV/queryregions.csv" \
    -v instants_csv="$CSV/queryinstants.csv" \
    -v periods_csv="$CSV/queryperiods.csv" \
    -v vehicles_csv="$CSV/datamcar.csv" \
    -v licences_csv="$CSV/querylicences.csv" \
    -v trips_csv="$CSV/trips.csv" \
    -v load_points="$(mb_table_flag "$tables" points)" \
    -v load_regions="$(mb_table_flag "$tables" regions)" \
    -v load_instants="$(mb_table_flag "$tables" instants)" \
    -v load_periods="$(mb_table_flag "$tables" periods)" \
    -v load_vehicles="$(mb_table_flag "$tables" vehicles)" \
    -v load_licences="$(mb_table_flag "$tables" licences)" \
    -v load_trips="$(mb_table_flag "$tables" trips)" \
    -f "$loader" > /dev/null
}

# Bring mobilitydb into a clean state.
mobilitydb_clean() {
  "${PSQL[@]}" -c "
    DROP VIEW IF EXISTS Trips1, Points1, Regions1, Instants1, Periods1, Licences1, Licences2 CASCADE;
    DROP TABLE IF EXISTS Trips, Vehicles, Licences, Points, Regions, Periods, Instants,
      RegionsInput, TripsInput, TripsInputInstants CASCADE;
  " > /dev/null
}

# Row count for one query file against whatever is currently loaded -
# strips header-comment lines and the trailing semicolon, then wraps in a
# count(*) subquery (same technique used throughout BENCHMARK.md's own
# ad hoc investigations this project already relies on).
mobilitydb_row_count() {
  local query_file="$1"
  local qtext
  qtext=$(sed -e '$s/;[[:space:]]*$//' "$query_file" | grep -v '^--')
  "${PSQL[@]}" -c "SELECT count(*) FROM ($qtext) t;"
}

# SKIP_PHASE1=1 skips straight to Phase 2 (timing) - for re-running timing
# alone once Phase 1 has already been verified for a given scale (e.g.
# scale 1.0's only-known mismatch, q4's documented MobilityDB trajectory-
# normalization epsilon confound - see build/mobilitydb/NOTES.md). Phase 1
# itself is left fully intact below rather than removed, so it stays
# available to re-verify correctness from scratch (e.g. after a code
# change, or at a new scale) any time by simply not setting this flag.
SKIP_PHASE1="${SKIP_PHASE1:-0}"

if [ "$SKIP_PHASE1" != "1" ]; then
  echo "=================================================="
  echo "PHASE 1: Correctness gate (scale $SCALE)"
  echo "=================================================="

  GATE_FAILURES=()

  # q21/q22 are checked against q13's row count - same query and data, only the trips
  # placement differs - so q13's count is needed even when it was not itself selected.
  Q13_ROWS=""
  for n in "${QUERY_NUMS[@]}"; do
    if is_datafusion_only "$n" && [ -z "$Q13_ROWS" ]; then
      echo "--- q13 (reference count for q21/q22) ---"
      Q13_ROWS=$(cargo bench --bench bench_saqe_vs_mobilitydb_timed_all -- 13 "$SCALE" --check 2>&1 \
        | grep "^RESULT:" | grep -oE 'csv=[0-9]+' | cut -d= -f2)
      echo "q13 reference: $Q13_ROWS rows"
      break
    fi
  done

  for n in "${QUERY_NUMS[@]}"; do
    echo "--- q$n ---"

    if is_datafusion_only "$n"; then
      mixed_n=$(cargo bench --bench bench_saqe_vs_mobilitydb_timed_all -- "$n" "$SCALE" --check 2>&1 \
        | grep "^RESULT:" | grep -oE 'mixed=[0-9]+' | cut -d= -f2)
      echo "q$n: mixed=$mixed_n (q13 reference=$Q13_ROWS)"
      if [ "$mixed_n" != "$Q13_ROWS" ]; then
        echo "q$n: FAIL - row count differs from q13"
        GATE_FAILURES+=("q$n: mixed=$mixed_n but q13=$Q13_ROWS")
      else
        echo "q$n: PASS ($mixed_n rows)"
      fi
      continue
    fi

    mb_file="$(mb_query_file "$n")"
    mb_tables="$(mb_tables_for_query "$n")"

    saqe_line=$(cargo bench --bench bench_saqe_vs_mobilitydb_timed_all -- "$n" "$SCALE" --check 2>&1 | grep "^RESULT:")
    csv_n=$(echo "$saqe_line" | grep -oE 'csv=[0-9]+' | cut -d= -f2)
    parquet_n=$(echo "$saqe_line" | grep -oE 'parquet=[0-9]+' | cut -d= -f2)
    postgres_n=$(echo "$saqe_line" | grep -oE 'postgres=[0-9]+' | cut -d= -f2)

    mobilitydb_load "$DOC_LOADER" "$mb_tables"
    doc_n=$(mobilitydb_row_count "$mb_file")
    mobilitydb_clean

    mobilitydb_load "$PART_LOADER" "$mb_tables"
    part_n=$(mobilitydb_row_count "$mb_file")
    mobilitydb_clean

    echo "q$n: csv=$csv_n parquet=$parquet_n postgres=$postgres_n mobilitydb_documented=$doc_n mobilitydb_partitioned=$part_n"

    if [[ "$csv_n" != "$parquet_n" || "$csv_n" != "$postgres_n" || "$csv_n" != "$doc_n" || "$csv_n" != "$part_n" ]]; then
      echo "q$n: FAIL - row counts differ across systems"
      GATE_FAILURES+=("q$n: mismatched row counts (csv=$csv_n parquet=$parquet_n postgres=$postgres_n mobilitydb_documented=$doc_n mobilitydb_partitioned=$part_n)")
    else
      echo "q$n: PASS ($csv_n rows)"
    fi
  done

  if [ "${#GATE_FAILURES[@]}" -gt 0 ]; then
    echo "=================================================="
    echo "CORRECTNESS GATE: ${#GATE_FAILURES[@]} of ${#QUERY_NUMS[@]} queries mismatched -"
    echo "proceeding to Phase 2 (timing) anyway. Treat timings for the queries"
    echo "listed below with caution - their systems weren't measuring the same"
    echo "thing. This does not stop the run: a small, understood mismatch (e.g."
    echo "a handful of rows) shouldn't block measuring everything else, but a"
    echo "mismatch is never silently ignored either - it stays visible here and"
    echo "in the q\$n: FAIL lines in the summary below."
    echo "=================================================="
    printf '%s\n' "${GATE_FAILURES[@]}"
  else
    echo "=================================================="
    echo "Correctness gate passed for ${#QUERY_NUMS[@]} queries (${QUERY_NUMS[*]}) - proceeding to timing"
    echo "=================================================="
  fi
else
  echo "=================================================="
  echo "PHASE 1: Correctness gate SKIPPED (SKIP_PHASE1=1)"
  echo "Already verified at scale $SCALE - see benchmark_results/ for that"
  echo "run's log. Proceeding straight to Phase 2 (timing)."
  echo "=================================================="
fi

echo "=================================================="
echo "PHASE 2: Timing (scale $SCALE, $TRIALS trials)"
echo "=================================================="

for n in "${QUERY_NUMS[@]}"; do
  echo "=================================================="
  echo "q$n @ scale $SCALE"
  echo "=================================================="

  echo "--- SAQE ---"
  cargo bench --bench bench_saqe_vs_mobilitydb_timed_all -- "$n" "$SCALE" "$TRIALS"

  if is_datafusion_only "$n"; then
    echo "--- MobilityDB: skipped (q$n is DataFusion-only) ---"
    continue
  fi

  mb_file="$(mb_query_file "$n")"
  mb_tables="$(mb_tables_for_query "$n")"

  echo "--- MobilityDB (load_all_documented) ---"
  build/mobilitydb/benchmarks/sql_scripts/timing/run_timed_cycles_all.sh \
    "$SCALE" "$mb_file" "$TRIALS" "$DOC_LOADER" "$mb_tables"

  echo "--- MobilityDB (load_all_partitioned) ---"
  build/mobilitydb/benchmarks/sql_scripts/timing/run_timed_cycles_all.sh \
    "$SCALE" "$mb_file" "$TRIALS" "$PART_LOADER" "$mb_tables"
done

# Pull just the banners/headers/mean-block lines back out of the full log -
# drops cargo build spam, psql NOTICEs, and per-trial lines.
# The plan dumps stay in the full log only - their "--- <backend> logical plan ---" banners would
# otherwise land in the summary without the plans they head.
grep -E "^==|^q[0-9]+ @ scale|^--- .* ---$|^--- mean|^load:|^query:|^total:|^q[0-9]+: (PASS|FAIL|WARN)" "$LOG_FILE" \
  | grep -v " plan ---$" > "$SUMMARY_FILE"

echo
echo "=================================================="
echo "Full log:     $LOG_FILE"
echo "Summary:      $SUMMARY_FILE"
echo "=================================================="
cat "$SUMMARY_FILE"

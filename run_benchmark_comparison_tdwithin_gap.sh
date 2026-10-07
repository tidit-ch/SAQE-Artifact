#!/usr/bin/env bash
# Runs the SAQE-vs-MobilityDB comparison for the 3 queries excluded from the
# main 14-query comparable set (q6, q10, q16 - the tdwithin capability gap,
# see build/mobilitydb/NOTES.md and
# benches/saqe_vs_mobilitydb/legacy_tdwithin_gap/). Parallel to
# run_benchmark_comparison_17.sh, but deliberately does NOT gate on row-count
# mismatches: SAQE's tdwithin only supports discrete same-second-bucket
# comparison, while MobilityDB computes distance continuously via linear
# interpolation - a documented, expected capability gap, not a correctness
# bug, so a mismatch here is exactly what's expected, not a failure to stop
# the run over. Instead, both phases just record every system's row count so
# the actual size of the gap is visible (e.g. q6's already-documented "8 vs
# 820 pairs" at scale 0.2), then proceed straight to timing regardless.
#
# Whether SAQE's result is a strict subset of MobilityDB's or reports
# genuinely different entries differs per query - see each query's own
# header comment (q6_sq.rs/q10_sq.rs: SAQE under-detects, likely a subset;
# q16_sq.rs: SAQE's "never co-located" approximation is easier to satisfy
# than MobilityDB's continuous check, so SAQE may over-include pairs
# instead - not simply a subset). The row counts recorded here are a
# starting point for checking that, not a proof either way.
#
# Run from the repo root. Requires the MobilityDB container up (see
# build/mobilitydb/README.md), the chameleon_postgis_dev container up (see
# build/docker-compose.dev.yml), and psql on PATH.
#
# Usage: ./run_benchmark_comparison_tdwithin_gap.sh [scale] [trials]
#   Defaults: scale=1.0, trials=1.

set -euo pipefail

# ---- Config ----
SCALE="${1:-1.0}"
TRIALS="${2:-1}"
# ----------------

export CONFIG_FILE="${CONFIG_FILE:-config/config_dev.json}"

mkdir -p benchmark_results
RUN_TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="benchmark_results/run_tdwithin_gap_${RUN_TIMESTAMP}.log"
SUMMARY_FILE="benchmark_results/summary_tdwithin_gap_${RUN_TIMESTAMP}.txt"
exec > >(tee "$LOG_FILE") 2>&1

echo "--- Warming CSV cache (data/berlinmod/$SCALE) ---"
cat "data/berlinmod/$SCALE"/*.csv > /dev/null

CSV="/home/mobilitydb/BerlinMOD/$SCALE"
PSQL=(env PGPASSWORD=docker psql -h localhost -p 25432 -U docker -d mobilitydb -t -A)
DOC_LOADER="build/mobilitydb/benchmarks/load_all_documented.sql"
PART_LOADER="build/mobilitydb/benchmarks/load_all_partitioned.sql"

# The tdwithin-capability-gap set. Same "no associative arrays" constraint
# as run_benchmark_comparison_17.sh (macOS's default Bash 3.2 on PATH).
# Overridable via QUERY_NUMS_OVERRIDE (space-separated, e.g. "10 16") - for
# running a subset without touching this file, e.g. when q6 alone is taking
# far longer than q10/q16 are expected to (q6 self-joins ALL trips of
# type='truck' vehicles, scaling with the real fleet size; q10/q16 both
# join through licences1/licences2, fixed 10-row samples regardless of
# scale factor - so q6's cost is not representative of the other two).
if [ -n "${QUERY_NUMS_OVERRIDE:-}" ]; then
  read -ra QUERY_NUMS <<< "$QUERY_NUMS_OVERRIDE"
else
  QUERY_NUMS=(6 10 16)
fi
mb_query_file() {
  echo "build/mobilitydb/benchmarks/sql_scripts/queries/legacy_tdwithin_gap/q$1_mb.sql"
}

# Per-query MobilityDB table list - same reasoning/convention as
# run_benchmark_comparison_17.sh's own mb_tables_for_query(), direct
# translation of query_registry()'s load_tables in
# benches/bench_saqe_vs_mobilitydb_timed_all.rs.
mb_tables_for_query() {
  case "$1" in
    6)  echo "trips,vehicles" ;;
    10) echo "trips,licences,vehicles" ;;
    16) echo "trips,licences,vehicles,periods,regions" ;;
    *) echo "ERROR: no table list for q$1" >&2; exit 1 ;;
  esac
}

mb_table_flag() {
  local tables="$1" name="$2"
  case ",$tables," in
    *",$name,"*) echo true ;;
    *) echo false ;;
  esac
}

mobilitydb_load() {
  local loader="$1" tables="$2"
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

mobilitydb_clean() {
  "${PSQL[@]}" -c "
    DROP VIEW IF EXISTS Trips1, Points1, Regions1, Instants1, Periods1, Licences1, Licences2 CASCADE;
    DROP TABLE IF EXISTS Trips, Vehicles, Licences, Points, Regions, Periods, Instants,
      RegionsInput, TripsInput, TripsInputInstants CASCADE;
  " > /dev/null
}

mobilitydb_row_count() {
  local query_file="$1"
  local qtext
  qtext=$(sed -e '$s/;[[:space:]]*$//' "$query_file" | grep -v '^--')
  "${PSQL[@]}" -c "SELECT count(*) FROM ($qtext) t;"
}

# SKIP_PHASE1=1 skips straight to Phase 2 (timing) - same convention as
# run_benchmark_comparison_17.sh, for re-running timing alone once the row
# counts have already been recorded for a given scale.
SKIP_PHASE1="${SKIP_PHASE1:-0}"

if [ "$SKIP_PHASE1" != "1" ]; then
  echo "=================================================="
  echo "PHASE 1: Row count comparison (scale $SCALE) - INFORMATIONAL ONLY"
  echo "A mismatch here is expected (documented tdwithin capability gap,"
  echo "not a bug) and never blocks Phase 2 - see this script's own header"
  echo "comment and each query's _sq.rs/_mb.sql header for why."
  echo "=================================================="

  for n in "${QUERY_NUMS[@]}"; do
    mb_file="$(mb_query_file "$n")"
    mb_tables="$(mb_tables_for_query "$n")"
    echo "--- q$n ---"

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
  done

  echo "=================================================="
  echo "Row counts recorded for all ${#QUERY_NUMS[@]} queries - proceeding to timing"
  echo "=================================================="
else
  echo "=================================================="
  echo "PHASE 1: Row count comparison SKIPPED (SKIP_PHASE1=1)"
  echo "Already recorded at scale $SCALE - see benchmark_results/ for that"
  echo "run's log. Proceeding straight to Phase 2 (timing)."
  echo "=================================================="
fi

echo "=================================================="
echo "PHASE 2: Timing (scale $SCALE, $TRIALS trials)"
echo "=================================================="

for n in "${QUERY_NUMS[@]}"; do
  mb_file="$(mb_query_file "$n")"
  mb_tables="$(mb_tables_for_query "$n")"
  echo "=================================================="
  echo "q$n @ scale $SCALE"
  echo "=================================================="

  echo "--- SAQE ---"
  cargo bench --bench bench_saqe_vs_mobilitydb_timed_all -- "$n" "$SCALE" "$TRIALS"

  echo "--- MobilityDB (load_all_documented) ---"
  build/mobilitydb/benchmarks/sql_scripts/timing/run_timed_cycles_all.sh \
    "$SCALE" "$mb_file" "$TRIALS" "$DOC_LOADER" "$mb_tables"

  echo "--- MobilityDB (load_all_partitioned) ---"
  build/mobilitydb/benchmarks/sql_scripts/timing/run_timed_cycles_all.sh \
    "$SCALE" "$mb_file" "$TRIALS" "$PART_LOADER" "$mb_tables"
done

# Pull just the banners/headers/mean-block lines back out of the full log -
# drops cargo build spam, psql NOTICEs, and per-trial lines.
grep -E "^==|^q[0-9]+ @ scale|^--- .* ---$|^--- mean|^load:|^query:|^total:|^q[0-9]+:" "$LOG_FILE" > "$SUMMARY_FILE"

echo
echo "=================================================="
echo "Full log:     $LOG_FILE"
echo "Summary:      $SUMMARY_FILE"
echo "=================================================="
cat "$SUMMARY_FILE"

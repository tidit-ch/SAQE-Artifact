> **Artifact note (October 2026).** The numbers in the EDBT 2027 paper come from
> `run_benchmark_comparison_17.sh` with the 17-query set (`benches/saqe_vs_mobilitydb/`,
> `build/mobilitydb/benchmarks/sql_scripts/queries/q*_mb.sql`); see `README.md` for how to
> run it. The legacy four-query suite (`run_benchmark_comparison.sh`, `bench_saqe_vs_mobilitydb_timed_q1..q4`,
> `legacy_q1_q4/`) and the per-backend criterion benches described below were removed from this
> artifact; their descriptions are kept here as historical context.

# SAQE vs MobilityDB benchmark — entry point

This file exists so someone with **zero prior
context** can understand how the SAQE-vs-MobilityDB benchmark comparison
works, how to run it, and which methodology details matter for reporting
results in a paper/thesis. It's scoped to the **cross-system entry point** —
the orchestration that spans both systems and the SAQE side specifically. For
the MobilityDB side only (schema, loaders, query scripts, Docker setup), see
[`build/mobilitydb/NOTES.md`](build/mobilitydb/NOTES.md) — that file is
explicitly scoped to not duplicate what's here, and vice versa.

This is part of an EDBT thesis comparing SAQE (this repo's DataFusion-based
query engine, `chameleon-datafusion`) against MobilityDB on the BerlinMOD
benchmark. All of this work lives on branch `mobilitydb-vs-saqe`.

> **Scope note (as of 2026-09-23):** the active focus going forward is
> **the official 17-query BerlinMOD set** (`q1_sq.rs`..`q17_sq.rs` /
> `q1_mb.sql`..`q17_mb.sql` - see "The official-numbering query set" below).
> Everything in this file about the older, bespoke four-query set (`q1`-`q4`,
> `run_benchmark_comparison.sh`, the "Confounds investigated"/"Known open
> asymmetries" tables, etc.) is **retained as historical reference only** -
> it's still accurate and still runs, it's just no longer where reporting
> effort goes. New work should build on the 17-query set instead of
> extending the four-query one.

## What's being compared

Four systems/backends, run against the same BerlinMOD data at the same scale:

1. **SAQE / CSV** — DataFusion querying the BerlinMOD CSVs directly, via a
   custom `CsvCatalogProvider`. No load step (lazy metadata registration
   only, zero file I/O until the query itself runs).
2. **SAQE / Parquet** — the same data converted to Parquet first, then
   queried. Has a genuine load phase (the CSV→Parquet conversion), timed
   separately from the query.
3. **SAQE / Postgres full pushdown** — the same query executed as a single
   SQL string sent to Postgres/PostGIS (`chameleon_postgis_dev` container,
   `trips.polyline` stored as plain `LINESTRINGM`, no temporal type), via
   `PushdownOptimizerRule` + `PostgresQueryPlanner`/`PostgresExtensionPlanner`.
4. **MobilityDB** — native `tgeompoint` temporal type, run via its own SQL
   query scripts. Run **twice** per query: once with the documented
   `tgeompoint` schema, once with `Trips` loaded as a plain `LINESTRINGM`
   (same datatype SAQE-Postgres uses) — see "Confounds investigated" below
   for why.

All four read/write BerlinMOD's **trip-based approach (TBA)** representation
(one row per trip, not one row per vehicle) at a configurable scale factor
(`0.005`, `0.2`, `1.0` — same nominal scales MobilityDB's own setup uses,
though the actual CSVs used here are 1797 trips at `0.005`, not the official
project's 1620).

## How to run it

Prerequisites:
- `chameleon_postgis_dev` container up (`build/docker-compose.dev.yml`) —
  SAQE-Postgres backend.
- `mobilitydb` (or `mobilitydb-arm64`) container up
  (`build/mobilitydb/docker-compose.yml`) — see
  [`build/mobilitydb/README.md`](build/mobilitydb/README.md) for first-time
  setup (submodule init, image build).
- `data/berlinmod/<scale>/*.csv` present (same files both systems read).
- `psql` on `PATH`.

Then, from the repo root:

```bash
./run_benchmark_comparison.sh
```

with no arguments. [`run_benchmark_comparison.sh`](run_benchmark_comparison.sh)
is the single control point for a run — edit `SCALE`/`TRIALS`/
`CREATE_INDEXES`/`ANALYZE_TABLE` at the top of the script, and comment out
whichever `run_query` calls at the bottom you don't want. It warms the OS
page cache for every source CSV up front (otherwise whichever phase happens
to touch a CSV first pays a real cold-disk-read cost the rest get for free —
confirmed to cause a ~2x timing swing that had nothing to do with the actual
comparison), then for each of `q1`–`q4` runs:
1. SAQE (`cargo bench --bench bench_saqe_vs_mobilitydb_timed_qN -- <scale>
   <trials>`, which itself runs all three SAQE backends in sequence: CSV,
   Parquet, Postgres),
2. MobilityDB with native `tgeompoint`,
3. MobilityDB with `Trips` as `LINESTRINGM`.

Output goes to `benchmark_results/run_<timestamp>.log` (everything) and
`benchmark_results/summary_<timestamp>.txt` (just the banners and
mean/load/query/total lines, pulled out via `grep`).

## The four queries

| # | What it does | Paper correspondence | Tables touched |
|---|---|---|---|
| `q1` | `SELECT * FROM Trips LIMIT N` — sanity/"feel for the data" query | none | Trips |
| `q2` | Which vehicles passed the points in `Points`? (spatial) | Query 4 (p. 11) | Trips, Vehicles, Points |
| `q3` | Which trips took place entirely within one of the periods in `Periods`? (temporal) | none — added specifically for a purely-temporal comparison point | Trips, Periods |
| `q4` | Which vehicles travelled within one of the regions in `Regions` during one of the periods in `Periods`? (spatio-temporal) | Query 13 (p. 14) | Trips, Vehicles, Regions, Periods |

("Paper" = the original 2007 BerlinMOD paper, Düntgen/Behr/Güting.)

SAQE's query text lives in `benches/saqe_vs_mobilitydb/q{1,2,3,4}.rs`
(`pub const QUERY_QN`/`pub fn query_q1()`), MobilityDB's in
`build/mobilitydb/benchmarks/sql_scripts/queries/q{1,2,3,4}.sql` (and
`q2_linestring.sql`/`q4_linestring.sql`, the `LINESTRINGM`-schema
equivalents used for MobilityDB's second run — q1/q3 have no
datatype-specific predicate, so they reuse the same file for both runs).

**None of the four queries sorts (`ORDER BY`) on either system**, even
though the official docs' `q2`/`q4` do — dropped deliberately for timing
fairness, since SAQE's equivalents don't sort either and `q4`'s result set is
tens of thousands of rows even at the smallest scale. This has no effect on
correctness verification (see below), which sorts both sides itself
independent of return order.

## Where the code lives

```
run_benchmark_comparison.sh                     repo root — the cross-system entry point (this file's companion)

benches/
├── saqe_vs_mobilitydb/
│   ├── mod.rs                                  create_ctx() (CSV), run_timed()/run_timed_parquet()/
│   │                                            run_timed_postgres() (the three timing-cycle functions),
│   │                                            load_postgres()/clean_postgres()/create_postgres_query_ctx()
│   ├── q{1,2,3,4}.rs                            query text (pub) + Criterion benches (SAQE-only perf
│   │                                            tracking, not used for the cross-system comparison)
│   └── fetch_trips.rs                           unrelated Criterion micro-bench, not part of q1-q4
├── bench_saqe_vs_mobilitydb_main.rs             Criterion harness entry point (all q1-q4 + fetch_trips)
├── bench_saqe_vs_mobilitydb_timed_q{1,2,3,4}.rs the actual cross-system timing binaries — fixed-trial
│                                                 loop (not Criterion), run all 3 SAQE backends per query
└── bench_saqe_vs_mobilitydb_compare.rs          correctness-only cross-check, no timing (see below)

build/mobilitydb/                                MobilityDB side — see build/mobilitydb/NOTES.md
├── q4_temporal_overhead_finding.md              citable methodology + result writeup: why SAQE-Postgres
│                                                 beats MobilityDB on q4, confounds ruled out
└── benchmarks/sql_scripts/
    ├── queries/q{1,2,3,4}.sql (+ q{2,4}_linestring.sql)
    └── timing/run_cycle.sql, run_timed_cycles.sh
```

## Timing methodology

- **Fixed trial count, not Criterion**, for the cross-system numbers.
  Criterion's adaptive statistical sampling has no fixed relationship to a
  trial count, which would make the two systems' iteration counts
  incomparable — `run_benchmark_comparison.sh` passes the same `TRIALS` to
  both `bench_saqe_vs_mobilitydb_timed_qN` and
  `run_timed_cycles.sh`, so both systems run exactly the same number of
  times. (The Criterion-based `benches/saqe_vs_mobilitydb/q{1,2,3,4}.rs`
  benches still exist and remain useful for SAQE-only performance tracking —
  they're just not used for the cross-system comparison.)
- **Load and query are bracketed together, every trial, on both sides** —
  deliberately not the official MobilityDB-BerlinMOD benchmark's model
  (load once, run many query-only trials against warm, already-indexed
  data). SAQE-CSV has no persistent load step of its own (it re-scans CSVs
  on every query call), so timing MobilityDB "query-only against warm data"
  while SAQE is inherently "load+query every time" would not be a fair
  comparison.
- **Compare `total` seconds, not `load`/`query` split, across systems.**
  SAQE-CSV's `create_ctx()` does zero file I/O — it only registers CSV paths
  in the catalog — so there's no genuine load phase to separate out the way
  there is for MobilityDB (which really does copy rows and build indexes)
  or SAQE-Parquet (CSV→Parquet conversion is a real cost). An earlier
  version that reported a load/query split for SAQE-CSV was actively
  misleading: its "load" number was a one-time Rust/tokio cold-start
  artifact (~100ms on trial 1, <1ms after), not anything resembling
  MobilityDB's load cost.
- **No memory limit configured on either side.** Previously both had a
  matching "2 GiB" (MobilityDB's container `mem_limit`, SAQE's
  `RuntimeEnvBuilder::with_memory_limit`). Removed from both, deliberately,
  because it was never true apples-to-apples: a container `mem_limit` is a
  hard OS-enforced ceiling on *total* memory, while DataFusion's memory pool
  only ever bounded the portion of SAQE's memory use its execution engine
  explicitly tracks — not the plain Rust heap allocations inside custom
  UDFs (`passes_point`, `st_intersects`, `subpolyline_between`, ...). That
  gap is exactly why `q4` was `SIGKILL`ed at BerlinMOD scale `0.2` despite
  the "2 GiB" cap being in place: most of the memory pressure came from
  UDF-side allocations the pool never saw.
- **Server-side timestamps on both sides** — MobilityDB via
  `clock_timestamp()` bracketing inside the same `psql` session; the SAQE
  timing binaries via `std::time::Instant` around the whole
  `create_ctx()`+`sql()`+`collect()` sequence in the same process. Neither
  is a client-side clock in a separate process/language.

## Confounds investigated (paper-relevant — read before citing any number)

| Confound | Status | Where documented |
|---|---|---|
| **CPU architecture**: `chameleon_postgis_dev` ran `postgis/postgis:latest` (amd64-only image) under QEMU emulation on this arm64 dev host, while `mobilitydb` runs a genuinely native arm64 build. | **Ruled out for PostGIS-heavy query timing** (at the time - measured directly: a real ~35% penalty exists for raw scalar CPU work, but re-running SAQE's exact Postgres schema/functions on native arm64 produced a statistically indistinguishable result, 13.87s vs 14.2s on q4, <3% trial variance) **- and now moot anyway**: `build/postgis-dev.Dockerfile` (added while fixing the GEOS row below) switched `chameleon_postgis_dev` to build from `imresamu/postgis` on arm64 hosts, the same genuinely-native multi-arch fork `mobilitydb` already uses - no more emulation on either side, on this dev machine. | `build/mobilitydb/q4_temporal_overhead_finding.md`, "Follow-up: ruling out the CPU-architecture confound"; fix in `build/docker-compose.dev.yml`/`build/postgis-dev.Dockerfile` |
| **GEOS version mismatch**: `chameleon_postgis_dev` bundled GEOS 3.9.0, `mobilitydb` bundles GEOS 3.14.1. | **Fixed, not just disclosed.** Originally: real, small (0.27%), deliberately-not-fixed gap - GEOS 3.9.0's `ST_Intersects` gives a false negative for a degenerate zero-length `LINESTRING` fully inside a polygon (a fixed upstream GEOS bug), causing SAQE-Postgres's old `q4` to return 130/48658 fewer rows, and the new-numbering `q13` to return 2187 instead of 2192 (see "The official-numbering query set" below). Matching the base image *tag* alone wasn't sufficient - confirmed directly, a fresh pull of the same tag both sides reference still returned an older GEOS than MobilityDB's actual running container, because tags aren't immutable (a registry can republish different content under an unchanged tag) and because MobilityDB's own Dockerfile additionally runs `apt-get install libgeos-dev`, which pulls a newer PGDG-repo GEOS package on top of whatever the base image originally bundled. `build/postgis-dev.Dockerfile` replicates both: pins the exact image digest MobilityDB's own local build was made from, and runs the identical `apt-get install libgeos-dev` step. Both containers now report the identical version string (`GEOS="3.14.1-CAPI-1.20.5" (compiled against GEOS 3.11.1)`), and the `q13` gap is closed (re-verified directly: 2192 rows, exact match). The previously-rejected `ST_DWithin(..., 0.0)` workaround (~23x slower) is no longer needed at all. | same doc, "Follow-up: a separate small row-count gap"; fix in `build/postgis-dev.Dockerfile` |
| **`tgeompoint` normalization**: MobilityDB's sequence constructor silently drops any instant collinear with its neighbors within `MEOS_EPSILON` (~11cm at Berlin's latitude). SAQE operates on the raw, unsimplified polyline. | **Expected semantic divergence, not a bug on either side.** Can cause `q2`-style point-touch predicates to disagree at the individual-trip level; absorbed by `DISTINCT` in the actual `q2` result set (doesn't change row counts there), but would show up in a raw non-distinct match count. | `build/mobilitydb/NOTES.md`, "MobilityDB trajectory normalization" |
| **Index/join-shape mismatch** on `q4`: MobilityDB's canonical query drives off a loose `stbox` GiST index; forcing a plain 2D polygon index (same index *type* SAQE-Postgres uses) recovers part of the gap. | **Real, ~20% of the total q4 gap** — the remaining ~80% is the intrinsic ~2x per-call cost of `tgeompoint`'s `attime()`+`trajectory()` temporal algebra vs SAQE's single `ST_LocateBetween` call, confirmed via an isolated microbenchmark with no index/join involved at all. The index-tuned MobilityDB variant is **not** adopted as the benchmark query (would be one-sided tuning) — reported only as a diagnostic. | same doc, full body |
| **`queryregions.csv` duplication**: the file shipped with its entire ~4923-row content duplicated back-to-back at every scale. | **Fixed.** Harmless for MobilityDB (its `GROUP BY` collapses duplicates), but not for SAQE's streaming per-region loader (produced two `Polygon` rows per id instead of one). Truncated to the correct first pass at all three scales; `.bak` originals kept alongside. | `build/mobilitydb/NOTES.md`, "Database schema" |
| **Postgres/PostGIS version drift**: `mobilitydb` pins `17-3.5`; `chameleon_postgis_dev` (`build/docker-compose.dev.yml`) used to pull `:latest`, a moving target. | **Fixed alongside the GEOS fix.** `chameleon_postgis_dev` now builds from `postgis-dev.Dockerfile`, which pins `BASE_IMAGE` to `postgis/postgis:17-3.5` by default (same tag `mobilitydb`'s own Dockerfile uses), overridable via `POSTGIS_BASE_IMAGE` for arm64 hosts - see the GEOS row above for the arm64-specific digest/PGDG-package detail. | `build/docker-compose.dev.yml`, `build/postgis-dev.Dockerfile` |
| **Docker VM boundary**: on this macOS dev machine, MobilityDB runs in Docker (VM + virtualized bind-mount for CSV reads); SAQE runs natively. | **Open, dev-machine-only caveat**, expected to mostly resolve on the planned Linux/EPYC run (containers near-native there). May currently inflate MobilityDB's "load" numbers specifically. | `build/mobilitydb/NOTES.md`, "Timing methodology" |

## Correctness verification

`benches/bench_saqe_vs_mobilitydb_compare.rs` — a separate, non-timing binary
that loads MobilityDB fresh (via the same `psql` load scripts, subprocessed),
runs both systems' `q2`/`q3`/`q4` (not `q1` — it has no `ORDER BY` and either
system may legitimately return a different row subset, so there's nothing
meaningful to diff), and compares **sorted, reduced id-tuples only** — e.g.
`(point_id, licence)` for `q2` — never geometry columns, since the two
systems represent geometry differently by design. Drops MobilityDB's tables
again at the end regardless of match/mismatch.

## The official-numbering query set (q1_sq.rs..q17_sq.rs / q1_mb.sql..q17_mb.sql)

Separate from the four queries above (which are already wired into
`run_benchmark_comparison.sh`'s timing harness), `benches/saqe_vs_mobilitydb/
q{1..17}_sq.rs` and `build/mobilitydb/benchmarks/sql_scripts/queries/
q{1..17}_mb.sql` implement the MobilityDB-BerlinMOD docs' own 17 numbered
queries (docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html)
verbatim, adapted to each system. `_sq`/`_mb` mirror each other's suffix.
Full adaptation detail (schema renames, MobilityDB 1.3.0 API renames, sample
views, per-query fixes) is in `build/mobilitydb/NOTES.md`'s "The
official-numbering query set" section - not duplicated here.

Verified against real BerlinMOD scale `0.2` data via
`benches/bench_saqe_vs_mobilitydb_dump_all.rs` (row counts, all 17, SAQE
side) plus direct `psql` execution (MobilityDB side), and
`benches/bench_saqe_vs_mobilitydb_tuple_check.rs` (actual sorted id-tuple
identity - not just matching counts - for the six queries where a row-count
match alone wouldn't prove it: q4, q7, q9, q13, q14, q15). Neither is yet
wired into `run_benchmark_comparison.sh` or
`bench_saqe_vs_mobilitydb_compare.rs`'s automated diff, so results below are
from one-off comparison passes, not a repeatable CI-style check.

**14 of 17 match MobilityDB.** q1=0, q3=0, q5=0, q8=0, q11=0, q12=0 match
trivially (empty result set on both sides - nothing more granular to
check). q2=804 and q17 (`point_id`=28, `hits`=145) are single-row
aggregates, already fully verified as the one row each returns. The
remaining six - **q4, q7, q9, q13, q14, q15 - are tuple-level verified**,
not just row-count matches: `bench_saqe_vs_mobilitydb_tuple_check.rs`
sorts and diffs the actual `(point_id, licence)`/`(polygon_id, period_id,
licence)`/etc. id-tuples both sides return (never geometry or raw
timestamp text, which the two systems format differently even for
identical values - same reduced-projection approach
`bench_saqe_vs_mobilitydb_compare.rs` already established), plus q9's
`max_distance` compared as a float within `1e-6` rather than exact string
equality (floating-point summation order can legitimately differ between
the two systems' execution). All six: exact match, zero rows only on one
side.

Two of those 14 needed a real UDF fix first, both confirmed via direct
before/after runs, not just inferred from the row count matching:
- **q14** was crashing outright on the SAQE side (`External error: Invalid
  argument error: Field extension type name missing`). Root cause:
  `point_at_timestamp()`'s output carried no GeoArrow extension metadata,
  so `st_intersects()` (which needs that metadata to interpret its input)
  rejected it whenever fed the UDF's output directly rather than a plain
  table column. Fixed by giving `point_at_timestamp.rs` a
  `return_field_from_args` impl that tags the output field, mirroring the
  fix `subpolyline_between.rs` already had for the identical problem.
- **q15** was returning 89,400 rows vs MobilityDB's 127 - exactly
  `10 points x 10 periods x 894 licences`, the full theoretical ceiling of
  the DISTINCT projection. Root cause, confirmed directly (diagnostic
  query grouping `passes_point()`'s result by whether its trajectory input
  was empty): `passes_point()` returned `true` whenever the trajectory
  input was *empty* (e.g. a trip clipped to a period it doesn't overlap)
  instead of `false` - `euclidean_distance()` degenerates to a spurious
  ~0 distance for empty geometry rather than `None`. Fixed by checking the
  raw trajectory list length explicitly in `passes_point.rs` and
  short-circuiting to `false` when empty, ahead of the distance
  computation.

**q6, q10, q16 are a known, deliberately-deferred SAQE capability gap** -
see below.

### Backend coverage: CSV, Parquet, Postgres full pushdown

Everything above (the row-count pass and the tuple-level pass) only ever
exercised SAQE's **CSV** backend. SAQE has two other backends (see "What's
being compared" above) that hadn't been checked for the 17-query set at all
until this pass:

**Parquet** - verified via `benches/bench_saqe_vs_mobilitydb_dump_all_parquet.rs`,
which converts every source table the 17 queries need (including the
`*1`/`*2` sample views - each materialized as its own independent Parquet
file, since `create_parquet_query_ctx()` has no notion of a SQL view over
another registered table) to Parquet via `load_parquet()`, then runs each
query's own `QUERY_QN_SQ` text with its `csv.berlinmod.` prefix stripped.
Needed one real fix first: `parquet_table_schema()` (the explicit schema
override needed to preserve GeoArrow extension metadata across the CSV ->
Parquet -> Parquet-read round trip - see that function's own comment) only
covered `trips`/`points`/`regions`, not the `points1`/`regions1` sample
views, which carry the same geometry-typed columns and the same metadata-
loss risk. Fixed by extending its match arms to cover both.

**All 14 of the row-count/tuple-verified queries (everything except
q6/q10/q16) match exactly on Parquet too** - q1=0, q2=804, q3=0, q4=666,
q5=0, q7=5, q8=0, q9=100, q11=0, q12=0, q13=2192, q14=195, q15=127, q17=1
(`point_id`=28, `hits`=145) - identical to both the CSV backend and
MobilityDB.

**q6/q10/q16 were deliberately skipped on Parquet** (for the timed/dumped
correctness check specifically - not because Parquet can't run them at
all), for a reason unrelated to their already-documented `tdwithin`
capability gap above.

**Follow-up investigation, since the first attempt's framing turned out to
be wrong**: initially, q6 alone at scale `0.2` looked catastrophically,
non-linearly slower on Parquet (89+ CPU-minutes without finishing, vs. an
assumed-fast CSV baseline inferred indirectly from "the CSV backend's
entire 17-query suite finished in ~35 minutes total"). That inference was
the actual mistake: gathering a real, *isolated* CSV-alone timing (not
inferred from the whole-suite total) showed q6 alone is genuinely very
expensive on CSV too - a direct, isolated run was still going after 28+
minutes without finishing. The whole-suite ~35-minute figure did not mean
q6 was cheap on CSV; it meant the other 16 queries were cheap and q6 ate
most of that total.

With a fair, completed, apples-to-apples comparison (both backends
finished, not one aborted early), the real picture is a **consistent
~2.5x constant-factor overhead specifically for this query shape, not a
non-linear blowup and not a general Parquet weakness**:
- At the tiny `0.005` scale: CSV 4.2s, Parquet 10.9s (~2.6x).
- At scale `0.2`, restricted to a 10-truck subset (real 0.2-scale trip
  density/geometry, just fewer trucks - needed because the full 41-truck
  version doesn't complete on either backend in a practical amount of
  time to test against): CSV 177.9s, Parquet 436.9s (~2.46x).

**Crucial context, checked directly rather than assumed: this is the
opposite of Parquet's usual behavior here.** Timing all 14 of the other
(non-tdwithin) queries at scale `0.2` head to head shows **Parquet faster
on every single one** - from roughly even (q1 0.78x, q17 0.64x) up to
dramatically faster (q11/q12 ~33x faster, q3/q8 ~17x faster). So q6/q10/q16
are a genuine, isolated exception to an otherwise completely consistent
"Parquet wins" pattern, not representative of Parquet-vs-CSV in general -
worth stating plainly given the investigation's early framing
("Parquet is pathologically slow") could otherwise be misread as a general
claim about the Parquet backend.

**Why this one query shape is the exception, isolated by elimination
(not left as a guess):**
- `EXPLAIN` on both backends for the same query shows the physical plans
  are structurally near-identical - same operator tree end to end
  (`AggregateExec` -> `NestedLoopJoinExec` -> `HashJoinExec` -> scan,
  8-way partitioned throughout on both sides). The only two differences:
  (a) CSV repartitions `trips` explicitly after reading it as one stream,
  while Parquet's `DataSourceExec` splits into 8 byte-range file groups
  natively at the scan - different mechanism, same 8-way end result; (b)
  a `DynamicFilterPhysicalExpr [ true ]` sits inertly on Parquet's scan.
  Directly disabling it via
  `datafusion.optimizer.enable_dynamic_filter_pushdown = false` produced
  no change - ruled out.
- A bare scan of `trips` (no join, no UDF) and the same `NestedLoopJoinExec`
  self-join *shape* q6 has but with a cheap `moid < moid` filter instead
  of `tdwithin` are both **dramatically faster on Parquet**, not slower
  (bare scan: CSV 9.26s vs Parquet 0.011s; join-without-UDF: CSV 17.3s vs
  Parquet 0.010s, identical 220,047-row result both times) - consistent
  with the broader 14-query trend above. This rules out scanning,
  decoding, and the join operator itself as the cause; the overhead is
  isolated specifically to `tdwithin`'s own execution.
- Temporarily instrumenting `tdwithin.rs`'s `invoke_with_args` (call
  counter + summed `num_coords1 * num_coords2` per call, removed again
  after this investigation) on a 5-truck subset showed Parquet does
  **provably the same total work as CSV** - 143,641 row-pairs and
  4,655,469,361 point-pairs on both sides, to the byte - since by the
  time data reaches the UDF it's plain Arrow arrays regardless of
  provenance, as it should be. Array-conversion time inside the UDF
  (`from_arrow_array`) was negligible on both (microseconds per call).
  The only structural difference: Parquet does this identical work in 379
  larger calls vs CSV's 1137 smaller ones. Checked whether `tdwithin`'s
  per-row early exit (`continue 'outer` the moment a match is found)
  could mean the naive `P1*P2` product overestimates one side's real
  work more than the other's: irrelevant here specifically, since this
  5-truck test found zero matching pairs on both sides - no early exit
  ever fired, so the measured totals already are the real executed work.
- Directly inspecting the Arrow array structure (`ArrayData::offset()`,
  buffer count, child/grandchild nesting) for the exact same query on
  both backends showed **identical layouts** - zero-offset, unsliced,
  same buffer/child counts at every nesting level. The only difference is
  batch *count*: CSV produced 6 batches (700 total rows, 57-239 rows
  each) for the 10-truck read; Parquet produced 1 batch of all 700 rows
  at once - plausibly because CSV reads `trips` as one stream then
  round-robin repartitions by row count regardless of content, spreading
  matches evenly across all 8 partitions, while Parquet's native
  byte-range partitioning splits by physical file location, which can
  concentrate matches unevenly if the source data has any vehicle/moid
  locality within the file (not confirmed further - a real avenue if this
  needs to be pinned down completely, but not chased here since batch
  count differences didn't explain the CPU-bound work itself, below).

With loading, scanning, decoding, the join operator, total logical work,
array-conversion overhead, and array memory layout all ruled out or
confirmed identical, the one remaining, evidence-consistent explanation
for *this specific query shape* is **CPU cache locality**: `tdwithin` is
the only UDF among all 17 queries doing a genuinely heavy nested
point-by-point loop (comparing every point of one trajectory - trips have
up to ~470 points - against every point of another, per row-pair) -
every other query's predicate (`passes_point`, `subpolyline_between`,
`st_intersects`, etc.) is comparatively cheap per row, which is exactly
where Parquet's decode efficiency dominates and wins big (the 14-query
trend above). Parquet's single much-larger batch for q6 doesn't fit in
cache the way CSV's several smaller batches do, so the same
instruction-for-instruction, byte-for-byte identical nested-loop work
runs slower against a working set that exceeds cache size - plausible
only where the per-batch compute is this heavy, which is why it doesn't
show up anywhere else. This wasn't confirmed with a CPU profiler (`perf`/
flamegraph, which would be needed to see actual cache-miss counts
directly) - that's the one remaining gap, and would be the natural next
step if this ever needs to move from "well-supported by elimination" to
"instrument-confirmed."

Given all three (q6/q10/q16) already carry the same `tdwithin`-granularity
capability gap on CSV regardless (see above) - not a result anyone would
report as correct today either way - skipping them in the Parquet
correctness check doesn't lose any currently-usable coverage. The ~2.5x
overhead itself is a real, modest, and now reasonably well-understood
Parquet-vs-CSV cost difference for this query shape specifically, not an
open mystery.

**Postgres full pushdown** - checked, using the exact same `QUERY_QN_SQ`
text CSV/Parquet use (only rewriting `csv.berlinmod.` -> `postgres.berlinmod.`,
the same rewrite `run_timed_postgres()` already did for the old q1-q4 set) -
no separate per-backend query text, by design. This worked out because
`plan_to_sql()` (the pushdown planner's SQL generator,
`src/federation/postgres_full_pushdown/planner.rs`) is DataFusion's plain
built-in unparser with no function-name translation layer at all - a query
only pushes down successfully if Postgres already has a function with the
exact name DataFusion's UDF uses. Turned out to need less new
infrastructure than first scoped:

- **The `*1`/`*2` sample views already existed** in `config/init.sql`
  (`CREATE VIEW licences1 AS SELECT * FROM licences LIMIT 10`, etc.) -
  the actual gap was `src/core/postgres/schema_provider.rs`'s `table()`
  method, a hardcoded `match name { "trips" => ..., "points" => ..., ... }`
  covering only the 7 base table names. Fixed by adding the six view names
  as additional match patterns on the *same* arms their base tables already
  use (`"points" | "points1" => ...`, `"licences" | "licences1" |
  "licences2" => ...`) - each provider struct already takes its table name
  as a constructor parameter used directly in its own scan SQL, so the
  view's name just needed to reach that constructor.
- **`st_intersects`/`st_distance`/`st_length` needed no changes at all** -
  they resolve directly to PostGIS's own built-in `ST_Intersects`/
  `ST_Distance`/`ST_Length` (Postgres identifiers are case-insensitive).
  Only `array_length` (called on `subpolyline_between()`'s `geometry`
  return value in q8/q9/q13/q15's overlap filter, not a real SQL array)
  needed a new native overload - added
  `array_length(geometry) RETURNS integer` to `config/init.sql`, checking
  `ST_IsEmpty` and returning 0 or the point count (distinguished from
  Postgres's built-in `array_length(anyarray, integer)` by argument
  type/count, no conflict).
- **`regions` needed a different load strategy than the other six base
  tables.** `queryregions.csv` ships with its whole ~100-region content
  duplicated (see "queryregions.csv had its entire content duplicated"
  above) - MobilityDB and the CSV/Parquet-backed `Regions1`/`points1`-style
  sample views tolerate this fine (GROUP BY / LIMIT-prefix-order
  reasoning respectively), but a naive full-table `write_table()` into
  Postgres's `regions` (which has `polygon_id INTEGER PRIMARY KEY`) hits a
  primary-key violation on the second copy of each id. Since none of the
  17 queries ever reference the full `regions` table anyway (only
  `regions1` - see "Regions is the one entity exclusively sampled" above),
  the fix loads only the first 10 (always-correct, see the LIMIT-prefix
  reasoning) rows from `csv.berlinmod.regions1` directly, sidestepping the
  conflict entirely rather than deduplicating the full table.

**Two real bugs found and fixed**, both root-caused by tracing the crash
down to DataFusion's own unparser source
(`datafusion-sql-50.3.0/src/unparser/expr.rs`, `arrow_dtype_to_ast_dtype`):
q3/q11/q12/q14 all crashed identically with `Unsupported DataType:
conversion: List(Struct(...))` - none of them are constant-folding a
literal (confirmed by inspecting the optimized logical plan directly,
which was completely clean), but `present.rs` and `point_at_timestamp.rs`
each hand-rolled their *own* `List<Struct{x,y,m}>` type for their
`Signature::exact` instead of reusing the shared `TRAJECTORY_DATATYPE`
constant (the way `passes_point.rs`/`subpolyline_between.rs` correctly
already do) - and each one's hand-rolled version didn't exactly match the
real `polyline` column's type (differing nullability, and differing from
each other too). `Signature::exact` requires a byte-for-byte match, so
DataFusion's analyzer inserted an implicit `CAST` to reconcile the
mismatch - and `List`/`Struct` types have no SQL `CAST` syntax at all, so
the unparser fails outright the moment it needs to render that cast as
text. CSV/Parquet never noticed since they never unparse to SQL text - the
cast just runs fine as an Arrow compute kernel. Fixed by having both
functions use `TRAJECTORY_DATATYPE.clone()` directly, eliminating the
mismatch (and the unneeded cast) entirely. Confirmed via direct rerun:
q3=0, q11=0, q12=0, q14=195 - all now match CSV/Parquet/MobilityDB exactly.

**Final tally, all confirmed with a clean single-threaded load (a race
between overlapping background test runs briefly produced spurious 0-row
and duplicate-key results during investigation - resolved by never running
more than one load/query cycle against the container at a time):**

| # | Result |
|---|---|
| q1, q2, q3, q4, q5, q7, q8, q9, q11, q12, q13, q14, q15, q17 | Match exactly - identical to CSV/Parquet/MobilityDB |
| q6, q10, q16 | Skipped - the already-documented `tdwithin` capability gap applies identically here |

**q13 originally showed 2187 rows vs 2192 on every other backend - root
cause was the same pre-existing, already-disclosed GEOS confound
documented in `build/mobilitydb/q4_temporal_overhead_finding.md` for the
old q4 query**: `chameleon_postgis_dev` bundled GEOS 3.9.0, whose
`ST_Intersects` has a known false-negative for a *zero-length*
`LINESTRING` fully inside a polygon (fixed in later GEOS, e.g.
MobilityDB's own container's 3.14.1). Confirmed directly for all 5
missing rows at the time - each was a stationary/parked vehicle segment
(two points, identical x/y, only the timestamp differs) that becomes a
zero-length line once clipped to its period; `ST_Intersects` on it
returned `false` while `ST_DWithin(..., 0.0)` correctly returned `true`.

**Now fixed, not just disclosed** - see "GEOS version mismatch" in the
confounds table above for how (`build/postgis-dev.Dockerfile`,
matching `chameleon_postgis_dev`'s GEOS version to MobilityDB's exactly).
Re-ran q13 against `chameleon_postgis_dev` after the fix: **2192 rows,
exact match**, gap fully closed.

## `load_all_documented.sql` and the `querylicences.csv` finding

`build/mobilitydb/benchmarks/load_all_documented.sql` is a single,
consolidated MobilityDB loading script that follows the official
MobilityDB-BerlinMOD documentation as closely as possible - same table
order (Points, Regions, Instants, Periods, Vehicles, Licences, Trips), same
indexes, same sample views - specifically so "we followed the
MobilityDB-BerlinMOD documentation for loading the data" is literally true
and citable, rather than true-with-footnotes spread across the several
separate per-table loaders under `benchmarks/sql_scripts/load_data/`. Its
own header comment tags every deviation as either `MUST` (the script would
not run/produce correct results otherwise) or `KEPT` (a deliberate,
cross-system-comparability choice this whole benchmark depends on).

**`querylicences.csv` at scale `0.2` is a genuine data bug** - not a
property of the dataset, confirmed directly: it is byte-for-byte identical
to the scale `0.005` file (`diff` shows zero differences), so it was never
actually regenerated for the 0.2-scale, 894-vehicle fleet. Against that
fleet its 100 sampled plates match **zero** real vehicles (`SELECT
count(*) FROM licences L JOIN vehicles V ON L.licence = V.licence` → `0`).
At scale `0.005` (67/67 unique plates match) and `1.0` (97/97 match, after
fixing that scale's separate missing-header bug - `querylicences.csv`
there had no `Licence,Id` header row at all) the file is correct. Not yet
fixed for `0.2` - would need regenerating a correct 100-plate sample from
that scale's own `datamcar.csv`.

**Licences' `PRIMARY KEY` cannot literally be `VehId`, as documented**,
tried and reverted: the docs declare `Licences(VehId PK, LicenceId,
Licence)`, populating `VehId` by joining the raw rows back to `Vehicles`
on the `Licence` string. Two problems, both confirmed directly against the
real data rather than assumed:
1. `querylicences.csv` has duplicate plate strings under different
   `LicenceId`s (e.g. `B-YI 65` appears as both `LicenceId` 1 and 2); both
   join to the *same* `Vehicle`, so inserting both raises `duplicate key
   value violates unique constraint` on `VehId`. Deduplicating by `Licence`
   (keeping the lowest `LicenceId` per plate) before the join avoids this
   and lets `VehId` be a real `PRIMARY KEY`.
2. But deduplicating changes *which* plates land in `Licences1`/
   `Licences2` (the first-10/next-10 sample views) - not just removing
   duplicates from an otherwise-identical sample, since fewer rows in the
   dedup'd table means the "first 10" window reaches further into the file
   to fill its 10 slots with different plates than SAQE's own unfiltered
   `Licences` table (`config/init.sql`) would sample. Measured directly at
   scale `0.005`: this changed q3 (80→100), q5 (30→38), and q8 (80→100)
   against SAQE - 3 of the 14 previously-exact-matching queries broke.

**Decision: `LicenceId` stays the `PRIMARY KEY`, `VehId` a plain nullable
column.** No deduplication needed, `Licences` keeps every raw row exactly
like SAQE's own table, and all 14 queries verified matching again. A
one-line schema exception (documented in `load_all_documented.sql`'s own
header) costs nothing; a 3-query result mismatch would actively undermine
what this benchmark is trying to demonstrate.

**Verified end to end**: at scale `0.005`, all 14 of the 17 official
queries (q1, q2, q3, q4, q5, q7, q8, q9, q11, q12, q13, q14, q15, q17)
return **identical row counts** between MobilityDB (loaded via
`load_all_documented.sql`) and SAQE's Postgres pushdown backend. This also
surfaced and fixed a genuine pre-existing bug: `q3_sq.rs`
(`benches/saqe_vs_mobilitydb/q3_sq.rs`) was missing a `DISTINCT` that
`q3_mb.sql` already had - invisible at scale `0.2` only because that
scale's `querylicences.csv` bug above made every licence-based query
trivially return 0=0 there; this was the first time q3 was ever exercised
against real, non-empty data.

## Known SAQE capability gap: continuous-time spatiotemporal proximity (q6, q10, q16)

For a focused, standalone explainer of this specific gap (why it exists,
the exact mechanism, a worked example) - see
`build/mobilitydb/tdwithin_discrete_vs_continuous_time.md`.

One could fairly say **MobilityDB is purpose-built for exactly this class of
question, while SAQE's UDF-over-raw-trajectory design targets a different
set of tradeoffs** - this gap is a real, structural difference in what each
system was built to do well, not a bug to be shrugged off, and not
something either system's *architecture* fundamentally forbids solving
(the raw `(x, y, timestamp)` data has everything needed - the gap is in
SAQE's current `tdwithin` implementation, not in the data model).

**What MobilityDB does**: a trajectory is a continuous function of time -
linear interpolation between consecutive GPS pings. Checking whether two
vehicles were ever within some distance of each other can evaluate that
question at *any* instant, including one where neither vehicle happens to
have an actual GPS ping, by interpolating both trajectories to that instant
and measuring the distance between the two interpolated positions
(`tdwithin(...) ?= true` - "ever true" over a continuously-evaluated
temporal boolean).

**What SAQE's `tdwithin` UDF does instead**: it only compares GPS pings
that fall in the exact same calendar second - not a continuous check.
Checked directly against real BerlinMOD scale `0.2` data: moving trips
have GPS pings roughly every **2.1 seconds on average**, and two different
vehicles' pings are not synchronized with each other, so it's fairly rare
for two vehicles to *both* happen to ping within the same one-second
window even when they are genuinely close together between those pings.
`'second'` is already `tdwithin`'s finest available granularity - there is
no tighter bucket to ask for.

**Measured impact**: q6 ("closely spaced truck pairs within 10m") found 8
pairs on the SAQE side vs. MobilityDB's 820 - not a rounding difference,
almost the entire true result set is missed. q10 and q16 use the same
`tdwithin` primitive for their own proximity checks and inherit the same
gap; q10 additionally has an independent, unrelated capability gap (SAQE's
`tdwithin` only ever returns a single boolean for a whole trajectory pair -
there is no way to recover *which* time periods/positions satisfied the
predicate, which is what the official Query 10 actually asks for, so even
a perfectly accurate boolean wouldn't fully close that query's gap).

**A real fix exists but is a substantial rewrite, not a quick patch**: it
would mean implementing genuine closest-point-of-approach (CPA) distance
math between the two linearly-interpolated trajectories - finding the
minimum distance between two moving line segments over a shared time
window, including where that minimum falls strictly between two GPS pings
- matching MobilityDB's continuous-time semantics rather than sampling at
discrete buckets. A cheaper middle ground (merge both trajectories'
timestamps and check distance at each shared instant, no interpolation
between them) would get closer than today's same-second bucketing but is
still not guaranteed identical to MobilityDB, since it can still miss a
closest approach that falls strictly between two synchronized instants.

**Decision**: documented here as a known, root-caused limitation;
deliberately not fixed for now. The other 14 queries (q1-q5, q7-q9,
q11-q15, q17) don't use `tdwithin` and are unaffected. Revisit if/when the
CPA rewrite becomes worth the engineering investment.

## Known open asymmetries

- **Fixed**: SAQE-Postgres's `trips` table used to carry a **permanent**
  index (`idx_trips_moid`, `idx_trips_polyline_gist`, created once in
  `config/init.sql`, never rebuilt per-cycle), unlike MobilityDB's
  `create_indexes`-gated, per-trial-measured index build
  (`build/mobilitydb/benchmarks/sql_scripts/load_data/trips/optimized.sql`,
  "CONDITIONAL INDEX CREATION") — meaning SAQE-Postgres's "load" time never
  paid for the index cost MobilityDB's did every single cycle. Also,
  `src/core/postgres/write/sink_trips.rs` loaded data via one `INSERT`
  round trip per row, unlike MobilityDB's `COPY`-based bulk loaders.
  **Both fixed together**: `config/init.sql` no longer creates
  `idx_trips_moid`/`idx_trips_polyline_gist` at all — they're now created
  by `benches/saqe_vs_mobilitydb/mod.rs`'s `load_postgres()` right after
  data is loaded (and dropped again by `clean_postgres()` before the next
  cycle), mirroring MobilityDB's "COPY then index" ordering and charging
  the same index-build cost to SAQE-Postgres's load phase every cycle.
  `sink_trips.rs` now bulk-loads via `COPY ... FROM STDIN` (hex-encoded
  EWKB as CSV text) instead of per-row `INSERT`, for the `Append`/
  `Overwrite` case that `load_postgres()` actually uses (the `Replace`/
  upsert case has no `COPY` equivalent, so it still does per-row
  `INSERT ... ON CONFLICT`). **Measured directly** at BerlinMOD scale `0.2`
  (62510 trip rows), same container, same data: old per-row `INSERT` with
  the indexes already present throughout the load — **44.05s**; new bulk
  `COPY` *plus* building both indexes fresh from scratch afterward —
  **21.24s**. About **2.1x faster**, despite the new number now honestly
  including the index-build cost the old number was silently skipping.
- MobilityDB's `Trips` primary key is conditional on `join_vehicles`
  (composite `Moid, Tripid`), while SAQE-Postgres's `trip_id` surrogate PK
  is always present. Not yet resolved.

## Fairness rules established for this project

- **Never hand-tune one system's query to make it look better** just
  because a rewrite happens to be faster. The canonical/idiomatic query per
  system is always the reported benchmark number; any hand-tuned or
  mechanism-matched variant goes in a clearly-labeled diagnostic section
  instead (see the "index/join-shape mismatch" row above).
- When a result looks surprising, don't explain it from first principles —
  get `EXPLAIN (ANALYZE, BUFFERS)` evidence directly against the live
  systems, and when a confound is plausible (index, architecture, load
  path, environment), design a control experiment that isolates it rather
  than asserting it doesn't matter (see "Confounds investigated" above for
  worked examples of this).
- Any ad hoc `psql`/temp-bench-binary investigation should leave the repo
  and both Postgres containers exactly as found afterward (drop test
  databases/tables/functions, delete temp files, revert `Cargo.toml` bench
  entries).

## Starting points for picking this up cold

1. Read this file, then `build/mobilitydb/NOTES.md` (MobilityDB side).
2. Read `build/mobilitydb/q4_temporal_overhead_finding.md` for a worked
   example of the confound-isolation methodology this project uses — it's
   the most thorough single piece of evidence here and a template for how
   any future "why is X faster/slower" question should be investigated.
3. `run_benchmark_comparison.sh` is the actual thing to run; its own header
   comment and the "How to run it" section above cover prerequisites.
4. Check `git status` / recent commits on `mobilitydb-vs-saqe` before
   assuming any of the above is already committed — this area of the repo
   has had uncommitted work in flight.

# MobilityDB setup — current state

This file exists so someone with **zero prior
context** can understand how this
MobilityDB setup works, what state it's actually in, and what's verified vs.
still open. It's part of a benchmark comparing MobilityDB against this
repo's own SAQE/chameleon-datafusion query engine on the BerlinMOD dataset,
but this file is scoped to the **MobilityDB side only** — how it's built,
what's loaded, what the queries do, and what currently works.

## Where things live

- `build/mobilitydb/` (this folder, branch `mobilitydb-vs-saqe`) — the
  active, current MobilityDB setup and benchmark scripts.
- `build/mobilitydb/README.md` — fresh-system setup instructions (clone →
  containers up → data loaded → ready to query), plus a log of Docker/build
  problems hit and fixed along the way.
- `MobilityDB-src` — the MobilityDB source, as a git submodule pinned to tag
  `v1.3.0` (not `master`), for reproducibility.

## Docker / build setup

Two Compose services, selected by profile, sharing the same `container_name`
so only one is ever active at a time:

| Profile | Service | Base image | Arch |
|---|---|---|---|
| `default` | `mobilitydb` | `postgis/postgis:17-3.5` | amd64 |
| `arm64` | `mobilitydb-arm64` | `imresamu/postgis:17-3.5` | arm64 |

Plus an always-on `pgadmin` service for a GUI. The official
`mobilitydb/mobilitydb` Docker Hub image and `postgis/postgis` base are
**amd64-only** — on Apple Silicon, Docker would otherwise run them under
QEMU/Rosetta emulation, which is a real confound for a project whose whole
point is measuring query performance. So instead of pulling a prebuilt image,
`Dockerfile` here **builds MobilityDB from source** (via the pinned
`MobilityDB-src` submodule) against `imresamu/postgis`, a community multi-arch
fork that actually publishes an arm64 manifest, producing a genuinely native
image on either architecture.

Verified working state (checked directly against the running container):
- `docker exec mobilitydb uname -m` → `aarch64` on Apple Silicon (i.e.
  genuinely native, not emulated)
- `SELECT mobilitydb_version();` → `MobilityDB 1.3.0`
- Postgres 17, PostGIS 3.5.2 (amd64) / 3.5.3 (arm64)
- Connection: `localhost:25432`, db `mobilitydb`, user/password `docker`/`docker`
- pgAdmin: `http://localhost:5050`, `admin@admin.com`/`admin`

## Database schema (trip-based approach)

BerlinMOD has two ways to represent the same trip data; this setup uses the
**trip-based approach (TBA)** — one row per individual trip, rather than one
row per vehicle with its whole trajectory collapsed into a single moving
point. This choice matters semantically: a vehicle can appear across
multiple trip rows, so anywhere a query joins back to `Vehicles` for
`Licence`, `DISTINCT` is required to avoid duplicate rows (not an
arbitrary/stylistic choice — the original 2007 BerlinMOD paper says the same
thing explicitly for its own TBA formulation).

Five tables, populated in this order (Vehicles before Trips, since Trips can
be foreign-keyed to it):

- **`Vehicles`** (`Moid PK`, `Licence`, `Type`, `Model`) — one row per
  vehicle, from `datamcar.csv`.
- **`Regions`** (`RegionId PK`, `Geom geometry(Polygon, 0)`) — built by
  merging line segments per region from `queryregions.csv` into closed
  polygons (`ST_Polygon(ST_LineMerge(ST_Union(...)))`).
- **`Periods`** (`PeriodId PK`, `BeginP`, `EndP`, `Period tstzspan`) — from
  `queryperiods.csv`.
- **`Points`** (`PointId PK`, `PosX`, `PosY`, `Geom geometry(Point, 0)`) —
  from `querypoints.csv`.
- **`Trips`** (`Moid`, `Tripid`, `Trip tgeompoint`, optionally `PRIMARY KEY
  (Moid, Tripid)` + `FOREIGN KEY (Moid) REFERENCES Vehicles`) — one moving
  point per trip, built from `trips.csv` (start/end coordinate pairs per
  trip segment, unrolled into a `tgeompointseq` per `(Moid, Tripid)`).

`RegionId`/`PeriodId`/`PointId` are declared `PRIMARY KEY` even though the
official docs don't bother — these tables are small and scale-independent,
so the cost is negligible, and it protects against duplicate/corrupt ids in
the source CSV. `Trips` does **not** get this unconditionally (see below) —
it scales with the dataset, so the same reasoning doesn't apply there.

Coordinates use **SRID 0** (no reprojection), and timestamps are stored and
compared as naive/local values (no `AT TIME ZONE` conversion) — both
deliberate deviations from the official docs (which reproject to SRID
25832/4326 and normalize to UTC) to keep raw coordinate/timestamp semantics
comparable to how the other system (SAQE) in this benchmark treats them.

**`queryregions.csv` had its entire content duplicated** (9846 rows instead
of the correct 4923 — the whole 100-region pass repeated back-to-back, not
per-region duplication) at every scale (`0.005`/`0.2`/`1.0` all shipped the
identical, identically-broken file). This was harmless for MobilityDB's own
`Regions` table, since its `GROUP BY RegionId` in `regions.sql` collapses any
amount of duplicate input to one row per id regardless. It was **not**
harmless for SAQE: SAQE's region loader groups rows into one polygon per id
by detecting adjacent `RegionId` changes in file order (a correct, cheap
streaming approach for a normal file), so a whole-file duplication — where
each id reappears far later in the file, not adjacently — produced two
separate `Polygon` rows per id (200 rows for 100 regions) instead of one
merged one. This was invisible in query results wherever `DISTINCT` happened
to be present (as in the `q4`-equivalent query), but represented real
duplicated work — and since SAQE has no persistent load step and re-parses
its CSVs on every query call, that duplication cost would be paid on *every*
query, not once. Fixed by truncating `queryregions.csv` to its correct first
4923-row pass at all three scales (`.bak` copies of the originals kept
alongside). Verified this changes nothing on the MobilityDB side and no
query result changed on either side.

## Loading scripts

`benchmarks/sql_scripts/load_data/{vehicles,regions,periods,points,trips}/`
— one script per table, run via `psql -v flag=value -f script.sql`. Design
rules used consistently across all of them:
- **Pure loading only** — no querying, no sample-table creation. A script
  either builds its table or errors out; nothing downstream is assumed.
- **CSV path is always a flag** (`-v vehicles_csv=...` etc.), never
  hardcoded, so the same script works across scale factors/directory layouts
  without editing.
- **Every optional behavior is a mandatory, explicitly-typed flag** —
  `create_indexes`, `analyze_table`, and (for `Trips`) `join_vehicles`. Each
  script refuses to run (`\if :{?flag} ... \else SELECT 'ERROR...' \q`) if a
  required flag is missing, rather than silently defaulting, so a benchmark
  run can never accidentally skip/include indexing without the caller
  noticing.

`Trips` has two implementations producing an *identical* final table, kept
side by side specifically to measure the cost difference:

- **`base.sql`** — faithful port of the documented MobilityDB-BerlinMOD
  loading approach: persists an intermediate `TripsInputInstants` table,
  then a separate `ALTER TABLE`/`UPDATE` pass to add the `tgeompoint` instant
  column.
- **`optimized.sql`** — single-pass: the "unroll trip segments into
  instants" step happens inline via a `MATERIALIZED` CTE, with no
  intermediate table ever written to disk.
- `base_partitioned.sql`/`optimized_partitioned.sql` are the same pair, but
  additionally partition `Trips` by date (per the docs' partitioned-loading
  chapter), using a `create_partitions_by_date` procedure sourced from the
  MobilityDB-BerlinMOD docs.

Measured at scale `0.2` (three trials each, wall-clock via `date +%s.%N` —
bash's `time` builtin didn't format usably in this shell):

| Variant | Trial 1 | Trial 2 | Trial 3 | Mean |
|---|---|---|---|---|
| `base.sql` | 208.3s | 215.9s | 243.1s | ≈222.4s |
| `optimized.sql` | 89.5s | 91.7s | 78.2s | ≈86.5s |
| **Speedup** | | | | **≈2.6×** |
| `base_partitioned.sql` | 345.1s | 362.6s | 388.0s | ≈365.2s |
| `optimized_partitioned.sql` | 103.1s | 151.6s | 115.8s | ≈123.5s |
| **Speedup** | | | | **≈3×** |

Why: Postgres's durability spectrum, from most to least durable/costly —
regular (WAL-logged) table > `UNLOGGED` table > `TEMP` table > in-query
`MATERIALIZED` CTE (which stays purely in memory, spilling to non-durable
temp files only past `work_mem`). `base.sql` writes a real WAL-logged
intermediate table to disk; `optimized.sql` never persists that intermediate
step at all. `optimized.sql` is the default choice going forward unless
specifically reproducing the documented approach is the point.

`Trips`' `PRIMARY KEY`/`FOREIGN KEY` and its `Moid` index are conditional on
the `join_vehicles` flag (only added if the query being benchmarked actually
joins to `Vehicles`), and its GiST index on `Trip` is conditional on
`create_indexes` — unlike the small reference tables, this constraint's/
index's cost is not negligible at scale, so it's charged only to the
workload that actually needs it.

`optimized.sql`, `optimized_partitioned.sql`, and `base_partitioned.sql`
previously computed their `is_last`/`TripDate` window functions with
`PARTITION BY Tripid` alone. In the actual BerlinMOD-generated CSVs used here,
`Tripid` happens to already be globally unique (verified: distinct `Tripid`
count equals distinct `(Moid, Tripid)` count at every scale), so this was
never observed to produce a wrong result — it wasn't a live bug. Still
changed to `PARTITION BY Moid, Tripid` (and `base_partitioned.sql`'s join
condition to include `Moid` too) as a defensive correctness fix, since nothing
in the schema/CSV format actually guarantees `Tripid` uniqueness on its own —
it's an accident of how this particular dataset happens to have been
generated, not a documented invariant. `base.sql` was never affected — it
already scoped its equivalent logic to `GROUP BY/ON Moid, Tripid`.

## MobilityDB trajectory normalization (permanent, not a loading bug)

`tgeompointSeq()` — used by every `Trips` loading variant to build the final
per-trip `tgeompoint` — hardcodes `NORMALIZE = true` in its C wrapper
(`Tsequence_constructor`, `mobilitydb/src/temporal/temporal.c`). None of the
three SQL overloads exposes a way to pass `false`. Normalization silently
drops any instant that is spatiotemporally collinear with its neighbors
(under linear interpolation) within `MEOS_EPSILON = 1.0e-06`
(`meos/include/temporal/temporal.h`) — roughly 11cm at Berlin's latitude,
since coordinates here are raw degrees (SRID 0, no reprojection).

Discovered via direct investigation while comparing SAQE's `passes_point`
(exact) against MobilityDB's `ST_Intersects` for the `q2`-equivalent query:
SAQE found 2 raw (trip, point) matches MobilityDB didn't, both at points that
are genuine vertices in the source CSV. Reproduced directly against the
MobilityDB container: constructing `tgeompointSeq()` from one affected trip's
full ~305-instant sequence drops the vertex (306 instants in → 275 vertices
out) even though it isn't part of any stationary/duplicate-position run;
`ST_Distance` from the dropped point to the resulting (simplified) trajectory
measured `8.94e-07` — just under the `1e-06` epsilon.

**Implication:** MobilityDB's stored trajectories are an epsilon-simplified
version of the raw BerlinMOD polylines by design, with no way to disable it
via the SQL functions actually exposed to Postgres. SAQE operates on the raw,
unsimplified polyline instead. Point-touches-trajectory queries (`q2`, the
paper's Query 4) can therefore disagree at the individual-trip level between
the two systems — this is expected divergence from a real semantic
difference, not a correctness bug in either system. It didn't surface in the
final `q2` result set because `DISTINCT` absorbs it whenever a vehicle has
another trip through the same point; a raw, non-distinct match count would
show it.

## The official-numbering query set (q1_mb.sql..q17_mb.sql / q1_sq.rs..q17_sq.rs)

Separate from the four custom `q{1,2,3,4}.sql`/`bench_saqe_vs_mobilitydb_timed_qN`
queries described below, `benchmarks/sql_scripts/queries/q{1..17}_mb.sql`
implements the MobilityDB-BerlinMOD docs' own 17 numbered queries verbatim
(docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html), adapted to
this project's actual schema/MobilityDB 1.3.0 API, with a SAQE counterpart
at `benches/saqe_vs_mobilitydb/q{1..17}_sq.rs` (`_sq` mirrors `_mb`'s
suffix convention). Kept as new files rather than replacing q1-q4 above,
which are already wired into `run_benchmark_comparison.sh`.

Adaptations needed for the MobilityDB (`_mb.sql`) side:
- `VehId` -> `Moid` everywhere (this schema's `Vehicles`/`Trips` PK is
  `Moid`, not `VehId`).
- Renamed for 1.3.0: `atPeriod`/`atPeriodSet` -> `attime` (unified
  restrict-to-time function, polymorphic over timestamp/set/span/spanset),
  `atValue` -> `atvalues`, `expandSpatial` -> `expandspace`.
- `tgeompoint && geometry` no longer exists as an operator (only
  `tgeompoint && stbox/tstzspan/tgeompoint` do) - any docs query using
  `T.Trip && P.Geom` needs the point wrapped: `T.Trip && stbox(P.Geom)`.
- `Licences.VehId` (renamed `Moid` here) is populated once at load time by
  joining back to `Vehicles` on the `Licence` string
  (`load_all_documented.sql`/`load_all_partitioned.sql`'s Licences section;
  `PRIMARY KEY` stays on `LicenceId`, not `VehId`, since `VehId` can be
  `NULL` for a sampled plate with no fleet match - see that script's own
  header comment) - so `Trips.Moid = Licences1.VehId` joins directly,
  exactly as documented, in q3/q5/q8/q10/q16 (and their SAQE-side
  `q{3,5,8,10,16}_sq.rs` counterparts, which populate the equivalent `moid`
  column the same way). Earlier versions of these queries bridged through
  `Vehicles`/`datamcar` on the `Licence` string instead, before `VehId`/
  `moid` existed as a column here - confirmed by direct A/B test to give
  identical results either way, but the direct join now matches the docs'
  own query text.
- **Query 10's own published SQL is broken as printed** on the docs page
  itself (not just a transcription slip here): a missing `AND` before its
  final `dwithin(...)` line, and `dwithin` isn't an actual MobilityDB
  function for two `tgeompoint` arguments (only `tdwithin` is). Fixed using
  the same `tdwithin(...) ?= true` pattern Query 6 already uses.
- The `Regions1`/`Periods1`/`Points1`/`Instants1`/`Licences1`/`Licences2`
  sample views these queries need (10-row samples off the 100-row Query*
  tables, `SAMPLESIZE=100` per the docs) are built by
  `benchmarks/sql_scripts/load_data/sample_views/sample_views.sql` - run
  once after the five base tables are loaded. `LIMIT 10`/`LIMIT 10 OFFSET
  10`, matching SAQE's own equivalent views exactly (see below) so both
  sides sample identically.

All 17 execute successfully against the loaded database at scale `0.2`.
q11/q12 (exact point-at-instant equality) return 0 rows - checked directly:
the `T.Trip @> STBOX(P.Geom, I.Instant)` bbox prefilter alone finds 380
candidates, so this isn't a broken predicate, it's the final exact-equality
check genuinely never holding at this sample size (the 10 sampled points
aren't necessarily trajectory vertices at exactly one of the 10 sampled
instants). q1/q3/q5/q8/q10/q16 return 0 rows for an unrelated, known
reason: `querylicences.csv` has zero overlap with the actual vehicle fleet
at this scale (see "queryregions.csv had its entire content duplicated"
below for the regions half of this same investigation) - accepted as-is
per explicit decision, not something these queries are expected to work
around.

Adaptations/fixes on the SAQE (`_sq.rs`) side, added to
`saqe_vs_mobilitydb/mod.rs`'s `create_ctx()` (the same six sample views as
above, `LIMIT 10`/`LIMIT 10 OFFSET 10`, matching MobilityDB's split
exactly):
- Several of these queries were drafted once already, unused/untested,
  under `benches/csv_benchmarks/q{1..19}.rs` (whose `create_ctx()` points
  at a stale, nonexistent `./data/enormous/...` path - a separate,
  not-yet-done item, see below - so those files could not actually have
  been run recently). Reused as the starting point here, but several had
  real bugs once actually executed against live data, documented in each
  `q{n}_sq.rs`'s own header comment: q3 read the full `instants` table
  instead of the `instants1` sample; q4/q11 dropped `point_id`/`instant_id`
  from their SELECT, which for q4 silently collapses point ids that share
  the same coordinate (querypoints.csv has a few); q6 filtered trucks down
  to the `licences1` sample even though the official Query 6 has no
  Licences reference at all; q8/q9 used `during` (entirely-contained) and
  summed the trip's *whole* length, where the docs sum only the
  *period-clipped* portion of trips that merely *overlap* the period.
- **q8/q9's overlap filter**: SAQE has no standalone "does this trajectory
  overlap this period at all" predicate. Its `overlaps` UDF
  (`src/core/udf/temporal_filters/overlaps.rs`) implements Allen's strict
  interval relation (trajectory starts before the period and ends inside
  it) - not general overlap, so using it would wrongly exclude trips fully
  containing/contained by/starting-after the period. Fixed instead by
  computing `subpolyline_between(...)` (the period-clipped portion) and
  filtering on `array_length(...) > 0`, then summing `st_length` of that
  same clipped result - both the filter and the aggregate come from one
  function, general and correct.
- **q10_sq/q16_sq are genuine SAQE capability gaps, not just translation
  choices** - full detail in each file's own header comment:
  - q10 (docs: `atPeriodSet(T1.Trip, gettime(atvalues(tdwithin(...),
    TRUE)))`) asks for the actual trajectory segments during which two
    vehicles were within 3m of each other. SAQE's `tdwithin` only returns a
    single Boolean for the whole trajectory pair - there's no MobilityDB-style
    temporal-boolean type to extract periods from, so q10_sq is reduced to
    reporting *which* vehicle pairs ever came within 3m (mirroring q6_sq's
    pattern), not the position/period list q10_mb.sql returns. Not
    byte-comparable to q10_mb.sql's output by construction.
  - q16 (docs: `tintersects(atPeriod(T1.Trip,P.Period),
    atPeriod(T2.Trip,P.Period)) %= FALSE` - spatiotemporal, same place AND
    same time) - the original csv_benchmarks/q16.rs used
    `NOT st_intersects(t1.p1, t2.p2)`, spatial-only (crosses in space at
    all, regardless of when each vehicle was at the crossing point), which
    is a real mismatch, not an equivalent. Approximated here with
    `NOT tdwithin(t1.p1, t2.p2, 0.0, 'second')` (same place, same second) -
    closer to the docs' intent, but still an approximation: SAQE has no
    direct analogue of MobilityDB's always/ever temporal-boolean operators
    (`%=`/`?=`) to test tightly against.
- Verified against real BerlinMOD scale `0.2` data via
  `benches/bench_saqe_vs_mobilitydb_dump_all.rs` (an ad hoc runner, not a
  real Criterion benchmark - same non-benchmark-under-`[[bench]]`
  convention `bench_saqe_vs_mobilitydb_compare.rs` already uses), which
  prints each query's row count and a short preview.

## Query scripts

`benchmarks/sql_scripts/queries/legacy_q1_q4/q{1,2,3,4}.sql` — moved into
their own subfolder to separate them from the official-numbering set below
(superseded by it; `q4` here corresponds to `q13_mb.sql`). Each assumes the
data is already loaded (no setup inside these files), and each documents in
its own header comment which query from the original 2007 BerlinMOD paper
(Düntgen/Behr/Güting) it corresponds to, if any:

- **`q1.sql`** — `SELECT * FROM Trips LIMIT :row_limit`. No paper
  equivalent; a "get a feel for the dataset" sanity query. No `ORDER BY`
  deliberately (irrelevant to a fresh-table-per-run methodology).
- **`q2.sql`** — spatial: which vehicles passed the points in `Points`.
  Matches the paper's **Query 4** (p. 11). Joins `Trips`, `Vehicles`,
  `Points`; keeps `DISTINCT` (paper: required for TBA).
- **`q3.sql`** — temporal: which trips took place entirely within one of the
  periods in `Periods`. **No paper equivalent** — none of the original 17
  queries is purely temporal (every one is non-spatiotemporal, purely
  spatial, or spatio-temporal already); this one was added specifically to
  have a purely-temporal comparison point.
- **`q4.sql`** — spatio-temporal: which vehicles travelled within one of the
  regions in `Regions` during one of the periods in `Periods`. Matches the
  paper's **Query 13** (p. 14). Uses the `T.trip && stbox(R.Geom, P.Period)`
  bounding-box operator as a cheap pre-filter ahead of the real
  `ST_Intersects(trajectory(attime(...)), ...)` check; keeps `DISTINCT` for
  the same TBA reason as `q2.sql`.

**None of the four query files has an `ORDER BY`** — the official docs'
versions of `q2.sql`/`q4.sql` do have one (`q1.sql`/`q3.sql` never did, on
either system). Dropped here deliberately for timing fairness: SAQE's
equivalent queries (`benches/saqe_vs_mobilitydb/q{2,4}.rs`) don't sort
either, and for the timed benchmark comparison, an `ORDER BY` MobilityDB
pays and SAQE doesn't would charge a real, non-trivial cost (q4's result set
is tens of thousands of rows even at the smallest scale) to only one side of
the comparison. This has no effect on correctness verification -
`bench_saqe_vs_mobilitydb_compare.rs` sorts both sides itself before
comparing, independent of whatever order either query returns rows in.

Note: `q2.sql`/`q4.sql` currently query the **full** `Points`/`Regions`/
`Periods` tables (~100 rows each), not the paper's 10-row `*1` sample views
(`Points1`, `Regions1`, `Periods1`, etc.) — that sample-view creation step
has been deliberately deferred to whenever a full benchmark-orchestration
layer gets built, so it doesn't exist yet.

## Timing methodology

`benchmarks/sql_scripts/timing/run_cycle.sql` + `run_timed_cycles.sh` run one
query N times, each trial doing a full clean state → load all five tables →
run the query → clean state cycle, timed via `clock_timestamp()` calls
bracketed around the load and query phases (reported separately as
`load_seconds`/`query_seconds`/`total_seconds` per trial, plus the mean over
N). `run_benchmark_comparison.sh` at the repo root (outside this directory,
since it spans both systems) is the actual entry point: for each of `q1`–`q4`
it runs SAQE's side then this MobilityDB cycle script, side by side.

SAQE's side runs via one of the four `bench_saqe_vs_mobilitydb_timed_qN.rs`
binaries (`cargo bench --bench bench_saqe_vs_mobilitydb_timed_qN -- <scale>
<trials>`, one per query rather than a single binary dispatching on a query
number) — a plain fixed-trial-count loop, not Criterion (whose adaptive statistical
sampling has no fixed relationship to a trial count, which would have made
the two systems' iteration counts incomparable). It reuses the exact query
text already defined in `benches/saqe_vs_mobilitydb/q{1,2,3,4}.rs` (exposed
as `pub`) rather than duplicating it, and reports a single
total-seconds-per-trial-plus-mean — deliberately *not* a load/query split
like `run_cycle.sql`'s: `create_ctx()` does zero file I/O (it only registers
CSV paths in the catalog), so there is no genuine load phase to separate out
for SAQE the way there is for MobilityDB (which really does copy rows and
build indexes during load). An earlier version of this tool did split
load/query for SAQE too and it was actively misleading — the "load" number
was just a one-time Rust/tokio cold-start artifact (~100ms on trial 1, <1ms
on every trial after), not anything resembling MobilityDB's load cost, and
invited exactly the wrong comparison. Compare SAQE's total against
MobilityDB's *total* column specifically - that's the only fair
like-for-like point. `run_benchmark_comparison.sh` passes the same
`TRIALS` value to both this tool and `run_timed_cycles.sh`, so both systems
now run exactly the same number of times per comparison. Criterion-based
`benches/saqe_vs_mobilitydb/q{1,2,3,4}.rs` still exist and remain useful for
SAQE-only performance tracking (where Criterion's statistical rigor is the
point) - they're just no longer used for the cross-system comparison.

**How this compares to the official MobilityDB/BerlinMOD benchmark**
(`MobilityDB/MobilityDB-BerlinMOD`, `BerlinMOD/benchmarks/`):

Matches:
- **Explicit trial count.** Their canonical driver
  (`SELECT berlinmod_R_queries(trials, ...)`) runs a fixed N trials; so does
  `run_timed_cycles.sh`.
- **Server-side timing.** They use `EXPLAIN ANALYZE` (executor-internal
  instrumentation); we use `clock_timestamp()` bracketing within the same
  psql session. Not literally the same mechanism, but both are server-
  evaluated timestamps rather than a client-side clock in a different
  process/language, so the intent matches.

Does not match (some deliberately):
- **We bracket load+query together every cycle; they load once and run many
  query-only trials against already-warm, already-indexed data.** This is a
  deliberate choice, not an oversight: SAQE has no persistent load step of
  its own (it re-scans its CSVs on every query call), so timing MobilityDB
  as "query-only against warm data" while SAQE is inherently
  "load+query every time" would not be a fair comparison. It does mean our
  MobilityDB timings aren't directly comparable to the official project's
  own published numbers, which measure something different (query-only).
- **Query set**: 4 custom queries here (`q1`–`q4`, mapped to specific
  original-paper queries where one exists) vs. their canonical 17 R-queries
  from the actual portable SQL files.
- **No index-tier isolation** — they run the same queries across a 4-tier
  matrix (none / GiST / SP-GiST / th3index prefilter) to isolate what
  indexing contributes; this setup has one flat `create_indexes` on/off flag.
- **No correctness gate against a published reference row-count table** —
  they validate each query's row count against known values (e.g. their
  Q13 → 278 rows at scale 0.005); here, correctness is instead checked by
  comparing SAQE against MobilityDB directly (see
  `benches/bench_saqe_vs_mobilitydb_compare.rs`), which is a different kind
  of check with a different purpose.
- **The dataset itself differs even at a matching nominal scale factor** —
  their `0.005` scale reference is 1620 trips; the CSVs used here have 1797
  trips at the same nominal scale. So even a perfectly matching methodology
  wouldn't make these absolute numbers comparable to their published tables.
- **No memory limit on either side.** This was previously a matching "2 GiB"
  configured on both: MobilityDB's container (`docker-compose.yml`'s
  `mem_limit`) and SAQE's `SessionContext` (`RuntimeEnvBuilder::
  with_memory_limit`, via a shared `MEMORY_BUDGET_BYTES` constant). That
  number was removed from both rather than kept, because it was never a
  true apples-to-apples cap in the first place: MobilityDB's container
  `mem_limit` is a hard, OS-enforced ceiling on the container's *total*
  memory use, while DataFusion's memory pool only ever bounded the portion
  of SAQE's memory use that its execution engine explicitly tracks
  (sort/join/aggregate buffers, batches in flight) - not the plain Rust
  heap allocations made inside custom UDFs (`passes_point`, `st_intersects`,
  `subpolyline_between`, the `geo`/`geoarrow` crates, ...).
  That gap is exactly why q4 was `SIGKILL`ed by the OS at BerlinMOD scale
  `0.2` despite the "2 GiB" setting being in place - most of its memory
  pressure came from UDF-side allocations the pool never saw. Since the
  same configured number never meant the same actual ceiling, keeping it
  would have implied a false parity; both sides now use whatever memory the
  host actually has, uncapped.
- **MobilityDB runs in Docker; SAQE runs natively (dev machine only).** On
  this macOS dev machine, Docker Desktop runs containers inside a Linux VM
  (Apple's Virtualization.framework), so MobilityDB pays for a VM boundary
  plus a virtualized bind-mount filesystem for its CSV reads
  (`../../data/berlinmod:...:ro` in `docker-compose.yml`) that SAQE's native
  process doesn't. Since most of MobilityDB's measured "total" time is the
  `COPY`/load step reading from that mount (see the timing numbers this
  produces), this may currently inflate MobilityDB's numbers specifically.
  Considered and deliberately deferred rather than fixed: containerizing
  SAQE too (adds its own deployment-realism deviation, and duplicates work
  that a Linux target machine makes moot), and building MobilityDB natively
  on macOS instead (would re-do the from-source build this `Dockerfile`
  already solves, only to redo it again for Linux). Expected to mostly
  resolve itself on the planned Linux/EPYC benchmarking run, where Docker
  containers run near-native (namespaces/cgroups, no VM layer) — this is a
  dev-machine-only caveat, not expected to hold for final reported numbers.
- **Parallelism/core-count parity — checked directly, found symmetric but
  conservative relative to the actual hardware.** SAQE's DataFusion
  `target_partitions` is hardcoded to `8`
  (`benches/saqe_vs_mobilitydb/mod.rs`'s `create_ctx()`). MobilityDB's own
  container and `chameleon_postgis_dev` (SAQE's Postgres-pushdown backend)
  both run Postgres's untouched upstream defaults - confirmed identical on
  both containers via `pg_settings`, on the local dev machine and on mond47:
  `max_worker_processes=8`, `max_parallel_workers=8`,
  `max_parallel_workers_per_gather=2` (the last one being a *per-query*
  share of the system-wide 8-worker budget, not a separate/lower ceiling -
  Postgres's own conservative default for a database expected to serve many
  concurrent connections, not a single benchmark query running alone).
  mond47 itself has **64** real CPU cores (`nproc`). So: neither system is
  unfairly advantaged over the other (both land on ~8-way parallelism by
  what looks like coincidence, not deliberate design), but neither is
  exploiting anywhere near mond47's actual 64-core capacity either. **Not
  yet resolved:** whether final reported numbers should reflect this
  modest, matched-but-conservative parallelism as-is, or whether both sides
  should be deliberately raised together (SAQE's `target_partitions` *and*
  Postgres's `max_worker_processes`/`max_parallel_workers`/
  `max_parallel_workers_per_gather` on both containers) to show best-case
  performance on the actual target hardware - raising only one side would
  reintroduce exactly the kind of one-sided-tuning confound this project
  otherwise avoids. Also still unverified: whether MobilityDB's own
  spatiotemporal query plans ever actually use parallel workers in practice
  at all (depends on whether its custom C functions are marked
  parallel-safe) - checking `EXPLAIN ANALYZE` output for "Workers Launched"
  on a real `_mb.sql` query would settle this.

## Deviations from the official MobilityDB-BerlinMOD docs (all deliberate)

- **SRID 0, no reprojection** (docs reproject to SRID 25832/4326).
- **No `AT TIME ZONE` conversion** — timestamps compared as naive/local values.
- **Function/type renames** required for MobilityDB 1.3.0 (the docs target
  an older API version): `tgeompoint_inst`→`tgeompoint`,
  `tgeompoint_seq`→`tgeompointseq`, `atPeriod`→`attime`, `period`
  (type/fn)→`tstzspan`/`span()`.
- **`PRIMARY KEY`** added unconditionally to `Regions`/`Periods`/`Points`/
  `Vehicles` (the docs don't bother) — negligible cost on these small,
  scale-independent tables, protects against duplicate/corrupt CSV ids.
- **No `ORDER BY`** on `q2.sql`/`q4.sql` (the docs' versions have one) — kept
  out for timing fairness against SAQE's equivalent queries, which don't
  sort either. See "Query scripts" above for the full reasoning.

## Current state / what's verified vs. still open

**Working end to end:** containers build and run natively on both
architectures; all five tables load successfully via the scripts above; all
four query scripts run without error and return results; `Trips`
base-vs-optimized (and partitioned variants) timing difference has been
measured and matches the expected explanation. Verified at BerlinMOD scale
factors `0.005` and `0.2` only.

**Not yet done:**
- The `*1`/`*2` sample views (`Regions1`, `Periods1`, `Points1`, `Instants1`,
  `Licences1`, `Licences2` — 10-row samples drawn from the 100-row `Query*`
  tables, `SAMPLESIZE=100`) don't exist yet; `q2.sql`/`q4.sql` query the full
  tables instead (see above).
- No run yet at the `1.0` scale end to end through the full pipeline (only
  `0.005` and `0.2` verified so far).
- A reusable multi-trial timing harness now exists for the query scripts on
  both sides, run together via `run_benchmark_comparison.sh` (see "Timing
  methodology" above).
- Postgres/PostGIS version is pinned here (`17-3.5`) but not everywhere else
  in this repo (`build/docker-compose.dev.yml` uses `:latest` for the same
  base images) — worth re-checking before relying on version parity for any
  comparison, since `:latest` can silently drift to a newer major version.
- SAQE's own CSV benchmarks (`benches/csv_benchmarks/mod.rs`,
  `create_ctx()`) don't yet point at `data/berlinmod/<scale>/` — they still
  hardcode a `./data/enormous/...` path that doesn't exist on disk, left
  over from before this data layout was adopted here. Bringing that in line
  is a separate, not-yet-done follow-up on the SAQE side, not something this
  MobilityDB setup depends on.

## File map

```
build/mobilitydb/
├── README.md                          fresh-system setup guide + Docker fix log
├── NOTES.md                          this file
├── Dockerfile                         native-arch build (arg: BASE_IMAGE)
├── docker-compose.yml                 default(amd64)/arm64 profiles + pgadmin
├── MobilityDB-src/                    submodule, pinned v1.3.0
└── benchmarks/
    ├── setup_once.sql                 one-time CREATE EXTENSION, run once per DB
    ├── smoke_test.sql                 loads Trips, prints row count, drops everything — sanity check, not a benchmark
    ├── q_during_periods.sql           early prototype combining load+query in one file (superseded by the split below)
    └── sql_scripts/
        ├── load_data/
        │   ├── vehicles/vehicles.sql
        │   ├── regions/regions.sql
        │   ├── periods/periods.sql
        │   ├── points/points.sql
        │   └── trips/{base,base_partitioned,optimized,optimized_partitioned}.sql
        ├── queries/legacy_q1_q4/q{1,2,3,4}.sql     assume data already loaded; each documents its paper correspondence in its own header comment
        └── timing/
            ├── run_cycle.sql          one clean->load->query->clean cycle, reports load/query/total seconds
            └── run_timed_cycles.sh    runs run_cycle.sql N times, reports per-trial + mean

run_benchmark_comparison.sh             repo root (spans both systems) - the actual entry point tying
                                         this timing harness together with SAQE's side, per query
```

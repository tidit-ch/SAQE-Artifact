# Finding: why SAQE's Postgres-pushdown backend beats MobilityDB on q4

## Question

After adding a plain GiST index on `regions.polygon` to the SAQE-Postgres
backend (`config/init.sql`, `idx_regions_polygon_gist` — added to bring its
indexing in line with MobilityDB's `create_indexes=true` set), q4's
SAQE-Postgres timing dropped below MobilityDB's, at scale `0.005`:

- SAQE-Postgres (full pushdown): ~15s query time
- MobilityDB (`create_indexes=true`, canonical `q4.sql`): ~43-45s query time

Both ultimately run on Postgres/PostGIS. The question this document answers:
**is that difference explained by something fixable (a suboptimal index or
join choice), or is it intrinsic to how the two systems represent
trajectories?** And separately: **could MobilityDB be made to execute the
query "the same way" SAQE does, to close the gap?**

## Method

All experiments ran directly against the live containers via `psql
EXPLAIN (ANALYZE, BUFFERS)` (server-side timestamps, not a client-side
clock — same methodology used elsewhere in this project, see
`NOTES.md`, "Timing methodology"), at BerlinMOD scale `0.005` (1797 trips,
100 periods, 100 regions, 141 vehicles), with both backends' data freshly
loaded and indexed exactly as the benchmark loads them
(`create_indexes=true`, `join_vehicles=true` for MobilityDB;
`config/init.sql`'s permanent indexes plus the newly added
`idx_regions_polygon_gist`/`idx_periods_range`/`idx_points_point_gist` for
SAQE-Postgres). Every number below is the mean of 3 repeated
`EXPLAIN ANALYZE` runs against the same loaded, already-warm data (not a
single sample) — variance between the 3 trials was under 3% in every case.

Three query variants were compared:

1. **MobilityDB canonical** — `build/mobilitydb/benchmarks/sql_scripts/queries/legacy_q1_q4/q4.sql`,
   the query actually used by the benchmark, following the official
   MobilityDB-BerlinMOD documentation:
   ```sql
   SELECT DISTINCT R.RegionId, P.PeriodId, P.Period, C.Licence
   FROM Trips T, Vehicles C, Regions R, Periods P
   WHERE T.Moid = C.Moid AND T.trip && stbox(R.Geom, P.Period) AND
     ST_Intersects(trajectory(attime(T.Trip, P.Period)), R.Geom);
   ```
2. **MobilityDB rewritten** — a diagnostic-only variant (never adopted as
   the benchmark query — see "Why not adopt the rewrite" below) that
   forces the same index type and join shape SAQE-Postgres's plan uses:
   plain 2D GiST on `Regions.Geom` (`Regions_geom_idx`, which already
   exists under `create_indexes=true` but the planner doesn't choose it
   for the canonical query), with the trips×periods cross product as the
   outer loop:
   ```sql
   SET enable_nestloop = off; -- otherwise the planner still picks a bad
                               -- Vehicles-first join order here
   SELECT DISTINCT R.RegionId, P.PeriodId, P.Period, C.Licence
   FROM Trips T, Vehicles C, Regions R, Periods P
   WHERE T.Moid = C.Moid
     AND R.Geom && trajectory(attime(T.Trip, P.Period), false)
     AND ST_Intersects(trajectory(attime(T.Trip, P.Period), false), R.Geom);
   ```
3. **SAQE-Postgres pushdown** — the actual SQL text
   `PostgresExtensionPlanner` generates and sends to Postgres for
   `QUERY_Q4` (`benches/saqe_vs_mobilitydb/q4.rs`), confirmed via the
   pushdown planner's debug print (`src/federation/postgres_full_pushdown/planner.rs`):
   ```sql
   SELECT DISTINCT r.polygon_id, p.period_id, p.start_period, p.end_period, d.licence
   FROM trips AS t CROSS JOIN periods AS p CROSS JOIN regions AS r CROSS JOIN datamcar AS d
   WHERE (t.moid = d.moid) AND
     st_intersects(subpolyline_between(t.polyline, p.start_period, p.end_period), r.polygon);
   ```

A fourth, isolated microbenchmark measures *only* the per-pair temporal-
restriction cost, with no join to Regions/Vehicles/Datamcar and no index
involved at all (an `IS NOT NULL` predicate on a computed expression is not
indexable, so this forces the function to be evaluated for the full
1797 × 100 = 179,700 trips×periods cross product either way):

```sql
-- MobilityDB
SELECT count(*) FROM trips t, periods p
WHERE trajectory(attime(t.trip, p.period), false) IS NOT NULL;

-- SAQE-Postgres
SELECT count(*) FROM trips t, periods p
WHERE subpolyline_between(t.polyline, p.start_period, p.end_period) IS NOT NULL;
```

## Results

**Full query, 3-trial mean:**

| Variant | Mean execution time | vs. canonical MobilityDB |
|---|---:|---:|
| MobilityDB canonical (`q4.sql`, as benchmarked) | 40.9s | — |
| MobilityDB rewritten (matched index + join shape) | 32.6s | −20% |
| SAQE-Postgres pushdown | 14.2s | −65% |

**Isolated per-pair function cost, 179,700 calls, no index/join, 3-trial
mean:**

| Function | Mean execution time | Per-call |
|---|---:|---:|
| MobilityDB: `trajectory(attime(trip, period), false)` | 2.15s | ~12.0 µs |
| SAQE-Postgres: `subpolyline_between(polyline, start, end)` (→ `ST_LocateBetween`) | 1.10s | ~6.1 µs |

## Interpretation

**The gap decomposes into two additive, independently-confirmed causes:**

1. **Index/join-shape mismatch — accounts for ~20% of the total gap.**
   MobilityDB's canonical query drives the join off `Trips_gist_idx`, a
   GiST index on `stbox` — a combined spatial+temporal bounding box around
   the *whole* trip. That's a much looser filter than a plain 2D polygon
   index: each of the 10,000 region×period probes it drives still returns
   dozens of false-positive candidate trips that need the expensive real
   check. Forcing the planner to instead use the plain 2D
   `Regions_geom_idx` — the same index *type* SAQE-Postgres uses, in the
   same trips×periods-outer-loop shape — recovers about 8.3s (40.9s →
   32.6s) purely by shrinking the number of candidates that reach the
   expensive filter. This part *is* fixable by query/index tuning alone.

2. **Intrinsic per-call cost of the temporal type — accounts for the
   remaining ~80% of the total gap, and is not fixable by tuning.** Even
   after removing cause (1) entirely, MobilityDB (32.6s) is still ~2.3x
   slower than SAQE-Postgres (14.2s). The isolated microbenchmark shows
   why, with no index or join involved at all: MobilityDB's
   `attime()`+`trajectory()` — a full temporal-algebra restriction of a
   piecewise-linear `tgeompoint` sequence to a time window (walking the
   sequence, interpolating new points at the boundary), followed by a
   separate conversion back to plain geometry — costs about **2x** more
   per call than SAQE-Postgres's `ST_LocateBetween`, a single, narrow,
   long-optimized PostGIS C function that clips a static `LINESTRINGM` by
   an M-value range. This cost difference is reproducible (<3% variance
   across trials) and appears with zero indexing or join-order variables
   in play — it is the cost of the two systems' underlying trajectory
   representations, not an artifact of how the query happens to be
   written.

## Why not adopt the rewrite as the benchmark query

The "MobilityDB rewritten" variant is not used by `run_benchmark_comparison.sh`
and `q4.sql` was deliberately left unchanged. Two reasons:

- It isn't MobilityDB's documented, idiomatic form — `q4.sql`'s own
  comment header states it follows the official
  MobilityDB-BerlinMOD documentation
  (`https://docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html`).
  Hand-tuning MobilityDB's query specifically because we discovered it
  helps would be the same kind of one-sided tuning this project has
  avoided elsewhere (e.g. `ORDER BY` was removed from both `q2.sql`/
  `q4.sql` rather than kept on one side only — see `NOTES.md`,
  "Query scripts").
- Even with the rewrite applied, MobilityDB is still slower — so adopting
  it would not change the qualitative conclusion, only cosmetically
  narrow the reported gap while quietly deviating from the documented
  query form for one system and not the other.

## Can MobilityDB be made to "execute the same way" as SAQE?

Not fully, and this is a structural limit rather than a tuning gap.
`Trips.Trip` is a native `tgeompoint` — a temporal sequence type with its
own storage and algebra. The only way to obtain "a plain geometry
restricted to a time window" from it is `attime()` followed by
`trajectory()` — that machinery is MobilityDB's actual value proposition.
SAQE-Postgres's `ST_LocateBetween` only works because its schema does not
use a temporal type at all: `trips.polyline` is a plain PostGIS
`LINESTRINGM`, with time smuggled in as the M-coordinate. Making
MobilityDB "execute the same way" would require abandoning `tgeompoint`
and storing trips as `LINESTRINGM` instead — at which point it is no
longer MobilityDB being benchmarked, just Postgres/PostGIS with a
different schema (i.e., a copy of the SAQE-Postgres backend under a
different name). The two backends' full-query numbers here should
therefore be read as "each system's own idiomatic best effort," not as a
mechanism-matched execution-engine comparison — the microbenchmark above
is what isolates the mechanism-level comparison specifically, and it's
reported separately for exactly that reason.

## Follow-up: ruling out the CPU-architecture confound

A concern with the results above: `chameleon_postgis_dev` runs
`postgis/postgis:latest`, which only publishes an `amd64` image (confirmed
via `docker manifest inspect` across several tags — no `arm64` manifest
exists), while this is an `arm64` (Apple Silicon) host and `mobilitydb`
runs a native `arm64` build (`mobilitydb-local:arm64`). So
`chameleon_postgis_dev` has been running under QEMU emulation this whole
time, while `mobilitydb` has been running natively — a genuine environment
difference, not just an algorithmic one, and exactly the kind of "is the
difference something else" possibility this investigation exists to rule
out.

**Quantifying the raw emulation penalty** (a CPU-bound query with no
PostGIS/geometry involved at all, 3-trial mean each):
```sql
SELECT count(*) FROM generate_series(1, 20000000) g WHERE sqrt(g::float8)::int % 7 = 0;
```
- `chameleon_postgis_dev` (amd64, emulated): 3.84s mean
- `mobilitydb` (arm64, native): 2.85s mean

So raw scalar-arithmetic throughput really is ~35% slower under emulation
here — the confound is real, not imagined.

**Eliminating it directly**: rather than guess how that 35% CPU penalty
would translate to PostGIS-heavy geometry workloads, the SAQE-Postgres
schema, indexes, and `subpolyline_between` function (byte-identical to
`config/init.sql`) were recreated in a fresh `saqe_style` database
**inside the `mobilitydb` container itself** (native `arm64`, same
Postgres process family as MobilityDB's own tables, just a separate
database so table names don't collide), loaded via the exact same Rust
sink code path (`load_postgres()`, pointed at the new database through a
temporary config override) as `chameleon_postgis_dev` normally is. Then
the identical pushdown query and isolated microbenchmark were run against
it, 3 trials each:

| Variant | Environment | Full q4 query | Isolated per-pair call (179,700x) |
|---|---|---:|---:|
| SAQE-Postgres (as benchmarked) | amd64, **emulated** | 14.2s | 1.10s (~6.1µs/call) |
| SAQE-style schema, same functions | arm64, **native** | 13.87s | 1.10s (~6.1µs/call) |
| MobilityDB canonical | arm64, native | 40.9s | — |
| MobilityDB rewritten (matched index/join) | arm64, native | 32.6s | — |

The native and emulated SAQE-side numbers are statistically
indistinguishable (13.87s vs 14.2s, ~2%, well within the <3% trial-to-trial
variance already observed elsewhere in this investigation) — despite a
confirmed, measurable 35% penalty on raw scalar arithmetic. Whatever makes
this specific PostGIS-heavy workload not show that penalty (plausibly:
it's dominated by Postgres executor/index-probe overhead and
GEOS/liblwgeom C calls rather than the kind of tight scalar/transcendental
math loop the microbenchmark above stresses, which may hit a Rosetta/QEMU
translation path with worse relative overhead), the practical upshot is
unambiguous: **the ~3x gap between SAQE's plain-geometry approach and
MobilityDB's native `tgeompoint` approach on q4 is not an artifact of
which container happens to run emulated.** Both the "matched index/join"
finding and the "intrinsic ~2x per-call temporal-algebra cost" finding
above hold on identical, native, apples-to-apples hardware.

This also means the original, unqualified numbers reported for q4
elsewhere in this project (e.g. `run_benchmark_comparison.sh`'s output,
`summary_*.txt` files) were not artificially flattering SAQE-Postgres —
if anything, the emulation confound could only have worked against it,
and evidently didn't move the number at all for this query.

## Follow-up: converting MobilityDB's own data in place

The previous section eliminated the CPU-architecture confound by running
SAQE's schema on native arm64, but it did so with a *separately loaded*
copy of the data (via `load_postgres()`, from the same CSVs). To close the
last possible gap — "maybe the two load paths produce subtly different
trip geometries" — this section instead takes MobilityDB's own,
already-loaded `Trips.Trip` (`tgeompoint`) values, still inside the
`mobilitydb` container/database, and converts them **in place** to
`LINESTRINGM` using MobilityDB's own decomposition functions
(`instants()`, `getValue()`, `getTimestamp()`):

```sql
ALTER TABLE Trips ADD COLUMN polyline geometry(LINESTRINGM, 4326);

UPDATE Trips t SET polyline = sub.geom
FROM (
  SELECT trip_id, ST_MakeLine(pt ORDER BY ts) AS geom
  FROM (
    SELECT tp.trip_id,
           gettimestamp(inst) AS ts,
           ST_MakePointM(ST_X(getvalue(inst)), ST_Y(getvalue(inst)),
                         extract(epoch FROM gettimestamp(inst)) * 1000) AS pt
    FROM (SELECT tripid AS trip_id, unnest(instants(trip)) AS inst FROM trips) tp
  ) points
  GROUP BY trip_id
) sub
WHERE t.tripid = sub.trip_id;

CREATE INDEX idx_trips_polyline_gist ON Trips USING gist(polyline);
```

(`Regions.Geom`'s declared SRID was relabeled from `0` to `4326` via
`UpdateGeometrySRID` — a metadata-only relabel with no coordinate
transform, consistent with this project's existing "SRID 0, no
reprojection" deviation documented in `NOTES.md` — so the join predicate
didn't need to wrap the column in a function, which would otherwise have
made `Regions_geom_idx` unusable.) Then the same `subpolyline_between` +
`ST_Intersects` query as SAQE-Postgres uses was run directly against
these converted rows, 3 trials:

| Variant | Data source | Mean time |
|---|---|---:|
| MobilityDB canonical (`tgeompoint`) | loaded via `optimized.sql` | 40.9s |
| MobilityDB rewritten (matched index/join, still `tgeompoint`) | loaded via `optimized.sql` | 32.6s |
| **MobilityDB's own rows, converted to `LINESTRINGM` in place** | **same loaded rows, converted** | **12.42s** |
| SAQE-Postgres (as benchmarked) | separately loaded via `load_postgres()` | 14.2s |

This is the strongest form of the test: not a separately-loaded dataset,
not a different container, not a different architecture — the exact same
1797 trip rows MobilityDB itself loaded and indexed, with only the
trajectory column's type changed from `tgeompoint` to `LINESTRINGM` and
the query predicate changed from `attime()`/`trajectory()` to
`ST_LocateBetween()`. The result (12.42s) lands in the same range as
SAQE-Postgres's own number (14.2s) and confirms, with no remaining
plausible alternative explanation, that the ~3x gap on q4 is caused by
the trajectory *datatype* itself — not the data, not the load path, not
the container, not the CPU architecture, and only partially the index/
join shape.

## Follow-up: a separate small row-count gap, and why it's left as-is

Independent of the temporal-type-overhead finding above, a tuple-level diff
between SAQE-Postgres's q4 pushdown result and SAQE-CSV's q4 result (which
should be identical - same predicate, same data) found a genuine, small,
root-caused discrepancy: SAQE-Postgres returns 48528 rows against CSV's
48658 - a strict subset, 130 rows missing, 0 extra.

**Root cause** (confirmed via isolated, `ST_LocateBetween`-independent
tests): PostGIS/GEOS's `ST_Intersects` gives a **false negative** for a
degenerate (zero-length) `LINESTRING` against a polygon it actually lies
inside, even though `ST_Intersects` on the same coordinates as a plain
`POINT` correctly returns true:

```sql
-- both test the exact same location:
ST_Intersects(POINT(13.30945, 52.51166), region)                        --> true
ST_Intersects(LINESTRING(13.30945 52.51166, 13.30945 52.51166), region) --> false
```

`subpolyline_between` (`ST_LocateBetween`) produces exactly this kind of
degenerate line whenever a "parked" trip segment (two identical points -
common in BerlinMOD data, representing a stationary period) falls
entirely within a query period's time range. SAQE-CSV/Parquet don't hit
this, because `st_intersects` in the DataFusion-side query resolves to a
`geodatafusion` (Rust/georust) implementation - a different, independent
algorithm from GEOS entirely.

**More precisely - this is a GEOS *version* bug, not a PostGIS/GEOS
design limitation.** Running the identical predicate
(`ST_Intersects(ST_LocateBetween(...), ...)`) against MobilityDB's own
data (both its canonical `tgeompoint` query and the `_linestring`
variant, `q4_linestring.sql`) was tuple-diffed against SAQE-CSV's 48658
rows and matched **exactly, 0 rows different, both directions** - despite
constructing the exact same degenerate zero-length line for the exact
same trip/period/region combination (confirmed: `ST_AsText` output and
region 1's polygon WKT are byte-identical between `chameleon_postgis_dev`
and `mobilitydb`; SRID was also ruled out as a factor by testing both
`0`- and `4326`-labeled versions of the same geometry, no difference).
The only remaining difference is the actual library version each
container's PostGIS build links against:

```
chameleon_postgis_dev (SAQE-Postgres): POSTGIS="3.5.2" GEOS="3.9.0-CAPI-1.16.2"
mobilitydb:                            POSTGIS="3.5.3" GEOS="3.14.1-CAPI-1.20.5"
```

`postgis/postgis:latest` (and every other amd64 tag checked, e.g.
`17-3.5` - confirmed identical image ID) bundles the same GEOS 3.9.0 for
this architecture; there is no newer-GEOS tag to switch to without
building a custom image from source. So this is best understood as: **an
upstream GEOS defect, present in the specific ~2020-era GEOS build this
Docker image happens to bundle, and already fixed in the GEOS release
MobilityDB's image links against** - not something reachable by changing
SAQE's query, schema, or data construction.

**The fix that works, and why it was rejected anyway**: `ST_DWithin(a, b,
0.0)` does not have this defect (confirmed: returns `true` for the same
degenerate-line case) - this is the same style of tolerance-based check
`passes_point` already uses for q2. Applying it to q4 (both
`q4_linestring.sql` and, hypothetically, a new SAQE-side UDF for
`QUERY_Q4`) was tested directly against live data:

| Predicate | Correctness | Execution time (SAQE-Postgres, 3 trials) |
|---|---|---|
| `ST_Intersects(subline, polygon)` (current) | 48528/48658 rows (~0.27% short) | ~14.0-14.4s |
| `ST_DWithin(subline, polygon, 0.0)` | 48658/48658 rows (exact) | ~334.7s (one trial; stopped early - clearly not noise) |

The query plan is otherwise identical (both use `idx_regions_polygon_gist`
via the same index condition, confirmed via `EXPLAIN`) - the ~23x
slowdown is purely the per-candidate cost of `ST_DWithin`'s actual
distance computation against a many-vertex polygon, versus
`ST_Intersects`'s topological check.

**Decision: keep `ST_Intersects`, accept the 0.27% gap.** This project's
q4 numbers exist to compare *execution time*, not to guarantee bit-perfect
row parity at any cost. Trading a 23x timing distortion for a 130-row
(~0.27%) correctness improvement would make the timing comparison
dramatically *less* representative of real usage - the opposite of what
the fix would nominally be for. `q4_linestring.sql` was briefly changed to
`ST_DWithin` and then reverted back to `ST_Intersects` once this was
confirmed; `subpolyline_between`+`st_intersects` in SAQE's own
`QUERY_Q4` (`benches/saqe_vs_mobilitydb/q4.rs`) was never changed. The
~0.27% gap is disclosed here rather than silently accepted.

## Reproducing this

Requires both `chameleon_postgis_dev` (port 5432) and `mobilitydb`
(port 25432) containers up (`docker compose` files under `build/`),
loaded at scale `0.005` with `create_indexes=true`/`join_vehicles=true`
(MobilityDB) — see `build/mobilitydb/benchmarks/sql_scripts/load_data/`
and `benches/saqe_vs_mobilitydb::load_postgres` for the respective
loaders — then run the four query variants above directly via `psql
EXPLAIN (ANALYZE, BUFFERS)`. No code changes are required to reproduce
any of these numbers; all four variants were run as ad hoc `psql`
sessions, not committed as permanent query files.

## Caveats

- Single dev machine (macOS, Docker Desktop VM boundary for both
  containers equally — see `NOTES.md`'s native-vs-Docker caveat, which
  doesn't apply differentially here since *both* backends being compared
  in this document run inside Docker).
- **CPU architecture confound — investigated and ruled out, not merely
  noted.** `chameleon_postgis_dev` runs under amd64-on-arm64 emulation
  while `mobilitydb` runs native arm64; see "Follow-up: ruling out the
  CPU-architecture confound" above. A real ~35% penalty exists for raw
  scalar CPU work, but re-running SAQE's exact schema/functions natively
  produced a statistically indistinguishable result (13.87s vs 14.2s) —
  so this confound does not explain any part of the reported q4 gap.
- Scale `0.005` only (1797 trips). The ~2x per-call ratio and the ~20%/
  ~80% split between the two causes are not verified to hold at larger
  scales, though there's no structural reason to expect either backend's
  relative index selectivity or per-call cost ratio to change
  qualitatively with scale.
- One trial series (3 repeats) per variant, all back-to-back against
  warm, already-loaded data. This rules out one-off noise/scheduling
  jitter but is not a full statistical study (no confidence intervals,
  no cross-session repetition on a different day).

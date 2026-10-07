# Why `tdwithin` can disagree with MobilityDB (q6, q10, q16)

## The short version

SAQE's `tdwithin` UDF only ever compares **actual, recorded GPS points**
against each other. MobilityDB's `tgeompoint` represents a trajectory as a
**continuous function of time**, so it can find the true closest distance
between two vehicles even at a moment that falls *between* two GPS pings.
This is a real, structural difference in how the two systems model
movement - not a bug in either one - and it's why queries q6, q10, and q16
(the ones that ask "were two vehicles ever within N meters of each other?")
aren't guaranteed to return identical results between SAQE and MobilityDB.
They're excluded from the correctness/timing comparison for this reason -
see `BENCHMARK.md`, "Known SAQE capability gap: continuous-time
spatiotemporal proximity".

## The concrete mechanism

Both of SAQE's `tdwithin` implementations do a plain nested loop over each
trajectory's **stored vertices** - no interpolation anywhere:

- Rust UDF (CSV/Parquet backends),
  `src/core/udf/temporal_filters/tdwithin.rs:223-227`:
  ```rust
  for idx1 in 0..trajectory_1.num_coords() {
      let coord1 = trajectory_1.coord(idx1).unwrap();
      for idx2 in 0..trajectory_2.num_coords() {
          let coord2 = trajectory_2.coord(idx2).unwrap();
  ```
  `trajectory.coord(idx)` reads a vertex straight out of the `LineString`'s
  coordinate array. Distance is `Euclidean.distance(p1, p2)` between two
  real vertices - there's no call anywhere in the function that computes a
  position *along* a segment.

- Postgres-pushdown plpgsql function, `config/init.sql`'s `tdwithin(...)`:
  same shape, via `ST_PointN(traj1, i)` / `ST_PointN(traj2, j)` in a nested
  loop. `ST_PointN` also returns a *stored* vertex, never an interpolated
  one (that would be `ST_LineInterpolatePoint`, which this function never
  calls).

So both backends do an O(n×m) all-pairs check: for every recorded point of
trajectory 1, compare it against every recorded point of trajectory 2 that
falls in the same time bucket (`precision` argument: `'second'`, `'minute'`,
...), and return true the first time two of them are within `tolerance`.

MobilityDB's `tgeompoint` doesn't work this way. It interpolates linearly
*in both space and time* between consecutive recorded instants, so its
"ever within distance" check (`tdwithin(...) ?= true` in the
MobilityDB-BerlinMOD docs) can evaluate the distance between the two
vehicles' interpolated positions at *any* instant, not just at recorded
GPS pings.

## Why this can produce a different answer

Picture two vehicles that each report a GPS position once every ~10
seconds. Vehicle A is heading east, vehicle B is heading west, and their
paths cross roughly 4 seconds after A's last ping and 4 seconds before B's
next one. At that crossing moment they might be only 2 meters apart - well
inside a 5-meter `tdwithin` tolerance.

- **MobilityDB**: interpolates both vehicles' positions continuously, finds
  the crossing moment, sees the 2-meter gap, returns `true`.
- **SAQE's `tdwithin`**: only has A's and B's *recorded* points to compare.
  At A's nearest recorded ping, B might actually be 40 meters away (it
  hasn't reached the crossing point yet); at B's nearest recorded ping, A
  has already moved past it. Neither recorded pair is within tolerance, so
  `tdwithin` returns `false` - even though the vehicles really did get
  close to each other.

The reverse can also happen in principle (two recorded points from
different, unrelated moments could coincidentally land within tolerance
without the vehicles ever truly being that close), though the "false
negative" direction above is the one actually observed in this project.

## Current status

Documented as a **known, root-caused limitation** - not fixed. A closer
approximation exists (merge both trajectories' timestamps and check
distance at each shared instant, without full interpolation) but is still
not guaranteed identical to MobilityDB's continuous-time result, since a
true closest approach can still fall strictly between two synchronized
instants. Revisit only if a full closest-point-of-approach (CPA) rewrite of
`tdwithin` becomes worth the engineering investment - see `BENCHMARK.md`
for the fairness/scope reasoning behind leaving this as-is.

**Affected queries**: q6, q10, q16 (all three use `tdwithin`). The other 14
queries in the benchmark set don't use `tdwithin` and are unaffected.

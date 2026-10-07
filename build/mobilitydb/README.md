# MobilityDB setup

Docker setup for MobilityDB (PostgreSQL + PostGIS + MobilityDB), built to run
**natively** on both amd64 and arm64 (Apple Silicon) — no emulation — so that
query performance measured here is comparable to `SAQE`
itself.

## Getting started

**1. Fetch the pinned MobilityDB source**

[`MobilityDB-src`](MobilityDB-src) is a git submodule pinned to tag `v1.3.0`
(not `master`), so the exact source this comparison's numbers are built from
is fixed and can't silently drift:
```bash
git submodule update --init --depth 1 build/mobilitydb/MobilityDB-src
```

**2. BerlinMOD CSVs**

Download the BerlinMOD data from
[secondo-database.github.io/BerlinMOD/BerlinMOD.html](https://secondo-database.github.io/BerlinMOD/BerlinMOD.html)
and ensure it's at the right place (`<scale>` is for example `0.005`, `0.2`,
or `1.0`):
```
data/berlinmod/<scale>/trips.csv
data/berlinmod/<scale>/datamcar.csv
data/berlinmod/<scale>/queryregions.csv
data/berlinmod/<scale>/queryperiods.csv
data/berlinmod/<scale>/querypoints.csv
data/berlinmod/<scale>/queryinstants.csv
data/berlinmod/<scale>/querylicences.csv
```

**Note on `queryregions.csv`:** the pregenerated BerlinMOD data ships this
file with its entire content duplicated back-to-back (the whole 100-region
pass repeated once, at every scale) rather than one clean pass. For our
measurements, this duplicate second half was removed.


**3. Bring the MobilityDB container up**

```bash
# Apple Silicon / arm64
docker compose -f build/mobilitydb/docker-compose.yml --profile arm64 up -d --build mobilitydb-arm64 pgadmin

# amd64
docker compose -f build/mobilitydb/docker-compose.yml --profile default up -d --build mobilitydb pgadmin
```
Run in terminal.
First run builds the image from source (a few minutes); afterwards it's
cached.

**4. Verify the extension is live**
```bash
PGPASSWORD=docker psql -h localhost -p 25432 -d mobilitydb -U docker -c "SELECT mobilitydb_version();"
```
Run in terminal.
Should print `MobilityDB 1.3.0`.

**Connection info:**
- Database: `localhost:25432`, db `mobilitydb`, user `docker`, password `docker`
- pgAdmin GUI: http://localhost:5050, login `admin@admin.com` / `admin`

**Add server:**
- host `mobilitydb`
- port `5432`
- username `docker`
- password `docker`

**5. Bring chameleon-datafusion's own Postgres container up too**

The benchmark comparison also measures SAQE's own Postgres-pushdown backend
(one of the systems being compared alongside CSV/Parquet/MobilityDB), which
runs in a *separate* container from MobilityDB's own — bring it up too:
```bash
docker compose -f build/docker-compose.dev.yml up -d chameleon_postgis_dev
```
Run in terminal (targeting just this one service — it won't also start the
unrelated `web_dev`/`chameleon_influxdb_dev` services). On Apple Silicon /
any non-amd64 host, override the base image the same way MobilityDB's own
setup does:
```bash
POSTGIS_BASE_IMAGE=imresamu/postgis:17-3.5 docker compose -f build/docker-compose.dev.yml up -d chameleon_postgis_dev
```
**Connection info:** `localhost:5432`, db `gis`, user `user`, password `password`.

**6. MobilityDB smoke test**

Sanity-check that CSV loading and `tgeompoint` construction actually work
before doing anything else — loads `Trips` at the smallest scale, prints the
row count, then drops everything again, leaving the database clean:
```bash
PGPASSWORD=docker psql -h localhost -p 25432 -U docker -d mobilitydb \
  -v trips_csv=/home/mobilitydb/BerlinMOD/0.005/trips.csv \
  -f build/mobilitydb/benchmarks/smoke_test.sql
```
Run in terminal.
Should end with `Smoke test passed — database is back to a clean state.`

## Running the full benchmark comparison

This is the actual entry point — it drives both systems (SAQE via
CSV/Parquet/Postgres, MobilityDB via both its documented and partitioned
loaders) through all 14 comparable BerlinMOD-R queries, first a correctness
gate (row counts must match across all five systems), then timing.

**The 14 comparable queries** are q1, q2, q3, q4, q5, q7, q8, q9, q11, q12,
q13, q14, q15, q17 - the official BerlinMOD-MobilityDB numbering
([docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html](https://docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html)),
excluding q6, q10, q16. Those three are deliberately left out of this correctness-gated
set: they depend on MobilityDB's continuous-time "ever/always" temporal
operators, which SAQE has no equivalent for (SAQE's own `tdwithin` only
supports discrete same-time-bucket comparison), so SAQE's results for those
three are a genuine, documented capability gap rather than something
expected to match MobilityDB row-for-row.

**Prerequisites beyond the containers above:**
- Rust/Cargo installed natively on the host (the script runs `cargo bench`
  directly, not inside a container).
- `psql` on the host's `PATH`.
  - **Check**: `which psql` (prints a path if it's found) or `psql --version`
    (also confirms it's actually executable, not just that some other
    program shares the name).
  - **If missing**: macOS - `brew install libpq && brew link --force libpq`;
    Debian/Ubuntu - `sudo apt-get install postgresql-client`.
- `CONFIG_FILE` set to the right config for your platform — `config/config_dev.json`
  on macOS (the default if unset), `config/config_dev_linux.json` on Linux
  (uses `localhost` instead of `host.docker.internal` for the Postgres/Influx
  connections).

**Run it** (from the repo root):

**On Linux** 
set this first:
```bash
export CONFIG_FILE=config/config_dev_linux.json
```

Then, on any platform:
```bash
./run_benchmark_comparison_17.sh [scale] [trials]  # defaults: scale=1.0, trials=1
```
e.g. `./run_benchmark_comparison_17.sh 1.0 3` for three timed trials at full scale.

Once Phase 1 (correctness) has already passed for a given scale and you only
want to re-run timing (e.g. after a code change that doesn't affect
correctness), skip straight to Phase 2 rather than redoing the full
correctness gate:
```bash
SKIP_PHASE1=1 ./run_benchmark_comparison_17.sh 1.0 3
```

**Results** land in `benchmark_results/`, at the repo root: a detailed log of all console output during the benchmark
(`run17_<timestamp>.log`) plus, once the whole run completes, a clean
extracted summary (`summary17_<timestamp>.txt`).

### Running at higher parallelism (32 workers, optional)

1. **DataFusion's own partition count** - ensure the three
   `.with_target_partitions(_)` calls in
   `benches/saqe_vs_mobilitydb/mod.rs` (`create_ctx()`,
   `create_parquet_query_ctx()`, `create_postgres_query_ctx()`) are set to `.with_target_partitions(32)`, so
   SAQE's CSV/Parquet/Postgres-pushdown contexts all use the same ceiling as
   the Postgres containers below.

2. **Bring up the benchmark-specific containers instead of the default ones**
   (not alongside - same container names/ports would conflict):
   ```bash
   # SAQE's Postgres backend - instead of chameleon_postgis_dev
   docker compose -f build/docker-compose.benchmark.yml up -d --build chameleon_postgis_benchmark

   # MobilityDB - instead of the production `mobilitydb` container
   docker stop mobilitydb
   docker compose -f build/mobilitydb/docker-compose.benchmark.yml --profile default up -d --build mobilitydb-benchmark
   ```
   (`--profile arm64 ... mobilitydb-benchmark-arm64` on Apple Silicon/arm64)

3. **Point the benchmark at them:**
   ```bash
   export CONFIG_FILE=config/config_benchmark.json
   ```

This setup runs **32 workers** on the DataFusion side (`target_partitions`)
and **32 workers per query** (`max_worker_processes`/`max_parallel_workers`/
`max_parallel_workers_per_gather`) on *both* Postgres backends - SAQE's
`chameleon_postgis_benchmark` and MobilityDB's `mobilitydb-benchmark` -
plus **512MB of `work_mem` per worker** on both

Since these are Postgres startup-time settings, not something a
compose-file edit alone changes on an already-running container, always
confirm a setting actually took effect on the live container after
bringing it up, rather than trusting the file alone:
```bash
PGPASSWORD=password psql -h localhost -p 5434 -U user -d gis -c "SHOW max_parallel_workers_per_gather; SHOW work_mem;"
PGPASSWORD=docker psql -h localhost -p 25432 -U docker -d mobilitydb -c "SHOW max_parallel_workers_per_gather; SHOW work_mem;"
```


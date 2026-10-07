# SAQE: a federated query engine for trajectory data

SAQE (Spatio-temporal Analytics and Query Engine) exposes one SQL interface
with spatio-temporal predicate semantics over trajectory data held in CSV
files, Parquet files, and PostGIS. It is a query broker built on
[Apache DataFusion](https://datafusion.apache.org/) and Apache Arrow: adapters
map every source onto one nested trajectory type, the predicate operators run
inside the broker or are delegated to PostGIS where their semantics can be
defined there, and the same query keeps working as data migrates from raw
files into a database or is spread across several stores.

This repository is the artifact of the paper

> *SAQE: A Federated Query Engine for Trajectory Data across Heterogeneous
> Backends.* Johann Bornholdt, Sachin Kumar, Niels Richter, Theodoros
> Chondrogiannis, Michael Grossniklaus. EDBT 2027 (submitted).

It contains the query processor, the web front-end, the container definitions
for PostGIS and for the MobilityDB baseline, the BerlinMOD loading scripts for
every backend, the query texts of all experiments for both systems, and the
fixed-trial benchmark harness that produced every number in Section 7 of the
paper. The tag `edbt2027-submission` marks the state the paper reports on.

## Repository layout

| Path                                     | Content                                                                               |
| ---------------------------------------- | ------------------------------------------------------------------------------------- |
| `src/`                                   | Query processor (Rust): adapters, UDFs, crop operator, pushdown rules, server         |
| `src/federation/postgres_full_pushdown/` | Whole-query pushdown rule to PostGIS (paper §6.4)                                     |
| `config/init.sql`                        | Database-side definitions of the SAQE UDFs used by pushdown (paper Table 6)           |
| `config/*.json`                          | Server configurations; credentials are the Docker Compose defaults                    |
| `benches/saqe_vs_mobilitydb/`            | The 17 BerlinMOD queries as SAQE SQL (`q*_sq.rs`) and the harness                     |
| `build/mobilitydb/`                      | MobilityDB container, loading scripts, the 17 queries as MobilityDB SQL (`q*_mb.sql`) |
| `build/mobilitydb/MobilityDB-src`        | MobilityDB source, git submodule pinned to tag `v1.3.0`                               |
| `build/docker-compose.benchmark.yml`     | SAQE's PostGIS backend container, benchmark settings                                  |
| `run_benchmark_comparison_17.sh`         | Entry point that produces the Section 7 numbers                                       |
| `BENCHMARK.md`                           | Benchmark design, timing methodology, fairness rules, known asymmetries               |
| `frontend/`                              | Web front-end (React/Leaflet map, query sidebar)                                      |

## Reproducing the evaluation (paper Section 7)

### Prerequisites

- Rust 1.88 or newer (`rustup toolchain install stable`; the committed
  `Cargo.lock` pins dependencies that require it), Docker with Compose, and
  `psql` on `PATH`.
- Disk for the BerlinMOD data at the chosen scale plus the Parquet and
  PostgreSQL copies the harness creates; scale 1.0 holds 292,940 trips.
- Time: the full scale-1.0 run with three trials took about three days on the
  server described below. Start with scale 0.005, which finishes in minutes.

### 1. Clone with the MobilityDB submodule

```bash
git clone --recurse-submodules --branch edbt2027-submission \
    https://github.com/tidit-ch/SAQE-Artifact.git
cd SAQE-Artifact
```

The `--branch` option checks out the tag `edbt2027-submission`, the state the
paper reports on; omit it for the current `main`.

If you cloned without `--recurse-submodules`:

```bash
git submodule update --init --depth 1 build/mobilitydb/MobilityDB-src
```

### 2. BerlinMOD data

Download the pregenerated BerlinMOD datasets from
<https://secondo-database.github.io/BerlinMOD/BerlinMOD.html>. Use the CSV
variant in **geographic coordinates**, `BerlinMOD_<scale>_Geo_CSV.zip`
(longitude/latitude in degrees; the parsers and loaders expect this layout,
and the older `_CSV.zip` lacks `querylicences.csv`). The archive has no
top-level folder; unpack it into `data/berlinmod/<scale>/`:

```bash
mkdir -p data/berlinmod/0.005
unzip BerlinMOD_0_005_Geo_CSV.zip -d data/berlinmod/0.005
```

The seven files the harness reads are `trips.csv`, `datamcar.csv`,
`queryregions.csv`, `queryperiods.csv`, `querypoints.csv`,
`queryinstants.csv`, and `querylicences.csv` (`streets.csv` is unused).

The distributed `queryregions.csv` contains its content twice, back to back.
Remove the duplicate second half before running, as the paper's measurements
did:

```bash
f=data/berlinmod/0.005/queryregions.csv
n=$(wc -l < "$f"); head -n $(( (n - 1) / 2 + 1 )) "$f" > "$f.tmp" && mv "$f.tmp" "$f"
```

### 3. Start the two database containers

SAQE's PostGIS backend, with the benchmark settings of the paper's final
configuration (32 parallel workers, 512 MB `work_mem`, `init.sql` applied on
first start):

```bash
# amd64
docker compose -f build/docker-compose.benchmark.yml up -d
# arm64 / Apple Silicon
POSTGIS_BASE_IMAGE=imresamu/postgis:17-3.5 docker compose -f build/docker-compose.benchmark.yml up -d 
```

The MobilityDB baseline with identical settings, built from the pinned
source (the first build takes a few minutes):

```bash
# amd64
docker compose -f build/mobilitydb/docker-compose.benchmark.yml --profile default up -d --build mobilitydb-benchmark
# arm64 / Apple Silicon
docker compose -f build/mobilitydb/docker-compose.benchmark.yml --profile arm64 up -d --build mobilitydb-benchmark-arm64
```

Verify: `PGPASSWORD=docker psql -h localhost -p 25432 -U docker -d mobilitydb -c "SELECT mobilitydb_version();"`
prints `MobilityDB 1.3.0`.

### 4. Run the harness

```bash
CONFIG_FILE=config/config_benchmark.json ./run_benchmark_comparison_17.sh 0.005 3
```

Note: The compilation of the harness takes some time, roughly 10 minutes or so.

The two positional arguments are the scale factor and the number of trials
(the paper uses `1.0 3`). The script first runs a correctness gate, every
query once on all systems with row counts compared, and only then the timed
phase. `--queries 13,21,22` restricts a run to the federation experiment.
Output lands in `benchmark_results/run_<timestamp>.log` (everything) and
`benchmark_results/summary17_<timestamp>.txt` (row counts and mean
load/query/total times per query and system).

### 5. Mapping the output to the paper

| Paper                                    | Summary file                                                                                                                                                                                                                  |
| ---------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Table 7, one-time costs                  | `load:` lines of SAQE Parquet, SAQE Postgres, and the two MobilityDB loaders                                                                                                                                                  |
| Table 8, query times per level           | `query:` lines (CSV reports `total:` only, as it has no load phase)                                                                                                                                                           |
| Table 9, initial vs. final configuration | Final: this setup. Initial: `build/docker-compose.dev.yml` and `build/mobilitydb/docker-compose.yml` containers, `target_partitions = 8` in `benches/saqe_vs_mobilitydb/mod.rs`; see `BENCHMARK.md`, "Known open asymmetries" |
| Table 10, federation                     | `q13` (all CSV), `q21` (CSV + Parquet), `q22` (CSV + PostGIS); the `load:` of q21/q22 is experiment setup, not reported                                                                                                       |
| Section 7.2, correctness | Phase-1 `PASS`/`FAIL` lines compare row counts across all five systems; `q4` reports 8,930 (SAQE) vs. 8,926 (MobilityDB) rows at scale 1.0 by design. Identifier-tuple identity between SAQE and MobilityDB: `cargo bench --bench bench_saqe_vs_mobilitydb_tuple_check` against a MobilityDB instance loaded at the scale under test (see the file header) |

Queries 6, 10, and 16 are excluded by the script: their proximity predicate
has different semantics in the two systems (paper Section 7.2).

### Software versions used for the paper

DataFusion 50.3.0 and Arrow 56.2.1 (`Cargo.lock`); PostgreSQL 17.5 with
PostGIS 3.5.2 and GEOS 3.9.0 in both containers; MobilityDB 1.3.0 (submodule
tag `v1.3.0`). Hardware: two AMD EPYC 7351 processors (64 threads), 512 GB RAM,
Linux.

## Running SAQE interactively

The server and its development PostGIS container:

```bash
# amd64
POSTGIS_IMAGE=postgis/postgis:latest docker compose -f build/docker-compose.dev.yml up
# arm64
POSTGIS_IMAGE=imresamu/postgis:latest docker compose -f build/docker-compose.dev.yml up
```

The server listens on `http://localhost:3000`. Data sources are registered
through the configuration files in `config/`; CSV datasets are read from
`data/<dataset>/`, for example `data/porto_taxi/` for the Porto Taxi dataset
of the paper's running example (Kaggle, "Taxi Trajectory"). The front-end:

```bash
cd frontend && npm install && npm run dev
```

and open `http://localhost:5173`.

Note that the interactive server registers per-source pushdown only; the
whole-query pushdown rule of Section 6.4 is registered by the benchmark
harness (`benches/saqe_vs_mobilitydb/mod.rs`, `create_postgres_query_ctx`).
An InfluxDB adapter exists in `src/core/influx/` but is not part of the paper.

## License and contact

MIT License, see `LICENSE`. Corresponding author: Johann Bornholdt,
<johann.bornholdt@tidit.ch>.

// Every bench binary that includes this module pulls in the whole of it, but each one uses only
// the part it needs: bench_saqe_vs_mobilitydb_timed_all takes the QUERY_Q*_SQ constants and never
// the Criterion run_query()/benchmark_sql_query() pair each query module also carries, while the
// per-query Criterion benches do the opposite. That asymmetry is by design, so the ~50 dead_code
// warnings it produces are noise that would otherwise bury the timing output.
#![allow(dead_code)]

pub mod fetch_trips;
pub mod legacy_tdwithin_gap;

// Official BerlinMOD-MobilityDB query set (docs.mobilitydb.com/
// MobilityDB-BerlinMOD/master/ch03s03.html), numbered to match
// build/mobilitydb/benchmarks/sql_scripts/queries/q{1..17}_mb.sql exactly -
// `_sq` suffix (SAQE) mirrors that side's `_mb` suffix. This is the query
// set actually used going forward (q6/q10/q16 excluded - known tdwithin
// capability gap, see BENCHMARK.md and legacy_tdwithin_gap above),
// superseding legacy_q1_q4 above (its q4 corresponds to q13_sq here).
pub mod q11_sq;
pub mod q12_sq;
pub mod q13_sq;
pub mod q14_sq;
pub mod q15_sq;
pub mod q17_sq;
pub mod q1_sq;
pub mod q21_sq;
pub mod q22_sq;
pub mod q2_sq;
pub mod q3_sq;
pub mod q4_sq;
pub mod q5_sq;
pub mod q7_sq;
pub mod q8_sq;
pub mod q9_sq;

use chameleon_datafusion::core::csv;
use chameleon_datafusion::core::postgres;
use chameleon_datafusion::core::udf;
use chameleon_datafusion::core::utils::schema::{
    POINT_XY_DATATYPE, POLYGON_DATATYPE, TRAJECTORY_DATATYPE,
};
use chameleon_datafusion::federation::postgres_full_pushdown::optimizer::PushdownOptimizerRule;
use chameleon_datafusion::federation::postgres_full_pushdown::planner::{
    PostgresExtensionPlanner, PostgresQueryPlanner,
};
use chameleon_datafusion::server::utils::config::GLOBAL_CONFIG;

use arrow_schema::extension::{ExtensionType, EXTENSION_TYPE_NAME_KEY};
use arrow_schema::{DataType, Field, Schema};
use datafusion::physical_plan::displayable;
use datafusion::{
    catalog::CatalogProvider,
    dataframe::DataFrameWriteOptions,
    execution::SessionStateBuilder,
    optimizer::OptimizerRule,
    physical_planner::DefaultPhysicalPlanner,
    prelude::{ParquetReadOptions, SessionConfig, SessionContext},
};
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use geoarrow_schema::{LineStringType, PointType, PolygonType};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Overrides the trips file `create_ctx()` registers, for the duration of one call. q21/q22 set
/// it to a half-sized CSV so the other half of the trips can come from Parquet or Postgres - see
/// `create_mixed_ctx()`. `None` restores the scale directory's own trips.csv.
static TRIPS_CSV_OVERRIDE: Mutex<Option<String>> = Mutex::new(None);

fn set_trips_csv_override(path: Option<&str>) {
    *TRIPS_CSV_OVERRIDE
        .lock()
        .expect("trips override lock poisoned") = path.map(str::to_string);
}

fn trips_csv_override() -> Option<String> {
    TRIPS_CSV_OVERRIDE
        .lock()
        .expect("trips override lock poisoned")
        .clone()
}

/// Prints the logical and physical plans for `sql` as planned by `ctx`, so the run log carries
/// the plan behind every SAQE number it reports - the backends differ in how they read the same
/// data, and the plan is what shows it.
///
/// Called once per backend per query, never per trial: the plan does not change between trials
/// and repeating it would bury the timings.
pub async fn print_plans(ctx: &SessionContext, sql: &str, backend: &str) {
    let df = ctx.sql(sql).await.expect("query failed to plan");
    // logical first: create_physical_plan() consumes the DataFrame.
    let logical = df.logical_plan().display_indent().to_string();
    let physical = df
        .create_physical_plan()
        .await
        .expect("physical planning failed");

    println!("--- {backend} logical plan ---");
    println!("{logical}");
    println!("--- {backend} physical plan ---");
    print!("{}", displayable(physical.as_ref()).indent(false));
}

pub async fn create_ctx() -> SessionContext {
    let config = SessionConfig::new()
        .with_batch_size(2048)
        .with_target_partitions(32);
    // No memory limit configured (neither here nor on MobilityDB's
    // container - see build/mobilitydb/docker-compose.yml) - see
    // build/mobilitydb/NOTES.md, "Timing methodology", for why a shared
    // "budget" number was removed rather than kept: MobilityDB's container
    // mem_limit is a hard, total ceiling, while DataFusion's memory pool
    // only ever bounded the portion of SAQE's own memory use that its
    // execution engine explicitly tracks (not UDF-side allocations) - so
    // the same configured number never meant the same actual ceiling on
    // both sides. Both now use whatever the host actually has, uncapped.
    let ctx = SessionContext::new_with_config(config);

    // Init BerlinMod Udfs
    udf::init(&ctx);

    // Points directly at the same flat CSVs MobilityDB loads from via COPY
    // (`data/berlinmod/<scale>/*.csv`, see build/mobilitydb/README.md), so
    // both systems benchmark against identical data. BERLINMOD_SCALE matches
    // MobilityDB's own scale naming (`0.005`, `0.2`, `1.0`).
    let scale = std::env::var("BERLINMOD_SCALE").unwrap_or_else(|_| "0.005".to_string());
    let data_dir = format!("./data/berlinmod/{scale}");

    let trips_csv: (String, String) = (
        "trips".to_string(),
        trips_csv_override().unwrap_or_else(|| format!("{data_dir}/trips.csv")),
    );
    let datamcar_csv: (String, String) =
        ("datamcar".to_string(), format!("{data_dir}/datamcar.csv"));
    let query_points_csv: (String, String) =
        ("points".to_string(), format!("{data_dir}/querypoints.csv"));
    let query_instant_csv: (String, String) = (
        "instants".to_string(),
        format!("{data_dir}/queryinstants.csv"),
    );
    let query_periods_csv: (String, String) = (
        "periods".to_string(),
        format!("{data_dir}/queryperiods.csv"),
    );
    let query_licences_csv: (String, String) = (
        "licences".to_string(),
        format!("{data_dir}/querylicences.csv"),
    );
    let query_regions_csv: (String, String) = (
        "regions".to_string(),
        format!("{data_dir}/queryregions.csv"),
    );

    let custom_catalog: Arc<dyn CatalogProvider> =
        Arc::new(csv::catalog_provider::CsvCatalogProvider::new(
            vec![
                trips_csv,
                query_points_csv,
                datamcar_csv,
                query_instant_csv,
                query_periods_csv,
                query_licences_csv,
                query_regions_csv,
            ],
            vec![],
        ));
    ctx.register_catalog("csv", custom_catalog);

    // Sample views for the official BerlinMOD-MobilityDB query set (q1_sq
    // ..q17_sq, mirroring build/mobilitydb/benchmarks/sql_scripts/queries/
    // q{1..17}_mb.sql) - 10-row samples off the 100-row Query* tables,
    // matching both the official docs' SAMPLESIZE=100 convention and
    // MobilityDB's own Regions1/Periods1/Points1/Instants1/Licences1/
    // Licences2 views (benchmarks/sql_scripts/load_data/sample_views/
    // sample_views.sql) exactly - same LIMIT 10 / LIMIT 10 OFFSET 10 split,
    // so both sides sample identically.
    // moid is joined in here (not at query time) to mirror MobilityDB's own
    // Licences.VehId, populated once at load time via the same Licence-
    // string match - see build/mobilitydb/benchmarks/load_all_documented.sql's
    // Licences section. Lets q3_sq/q5_sq/q8_sq/q10_sq/q16_sq join
    // trips.moid = licences1.moid directly, matching the official docs'
    // query text (T.VehId = L.VehId) instead of bridging through datamcar.
    ctx.sql(
        "CREATE VIEW csv.berlinmod.licences1 AS \
             SELECT l.licence_id, l.licence, d.moid \
             FROM (SELECT * FROM csv.berlinmod.licences LIMIT 10) l \
             LEFT JOIN csv.berlinmod.datamcar d ON l.licence = d.licence",
    )
    .await
    .unwrap();
    ctx.sql(
        "CREATE VIEW csv.berlinmod.licences2 AS \
             SELECT l.licence_id, l.licence, d.moid \
             FROM (SELECT * FROM csv.berlinmod.licences LIMIT 10 OFFSET 10) l \
             LEFT JOIN csv.berlinmod.datamcar d ON l.licence = d.licence",
    )
    .await
    .unwrap();
    ctx.sql("CREATE VIEW csv.berlinmod.points1 AS SELECT * FROM csv.berlinmod.points LIMIT 10")
        .await
        .unwrap();
    ctx.sql("CREATE VIEW csv.berlinmod.regions1 AS SELECT * FROM csv.berlinmod.regions LIMIT 10")
        .await
        .unwrap();
    ctx.sql("CREATE VIEW csv.berlinmod.instants1 AS SELECT * FROM csv.berlinmod.instants LIMIT 10")
        .await
        .unwrap();
    ctx.sql("CREATE VIEW csv.berlinmod.periods1 AS SELECT * FROM csv.berlinmod.periods LIMIT 10")
        .await
        .unwrap();

    ctx
}

fn mean(values: &[Duration]) -> Duration {
    values.iter().sum::<Duration>() / values.len() as u32
}

/// Runs `sql` `trials` times, matching MobilityDB's run_cycle.sql/
/// run_timed_cycles.sh model exactly: a fixed trial count (not Criterion's
/// adaptive statistical sampling - see build/mobilitydb/NOTES.md, "Timing
/// methodology"). Shared by the per-query bench_saqe_vs_mobilitydb_timed_qN
/// binaries so the trial loop/timing logic lives in one place.
///
/// Reports a single total-seconds-per-trial-plus-mean, not a load/query
/// split - see that same NOTES.md section for why.
pub async fn run_timed(sql: &str, trials: u32) {
    let mut total_times = Vec::with_capacity(trials as usize);

    for trial in 1..=trials {
        let start = Instant::now();
        let ctx = create_ctx().await;
        let df = ctx.sql(sql).await.expect("query failed to plan");
        df.collect().await.expect("query failed to execute");
        let total_time = start.elapsed();

        // After the timed span closes: planning the query again to print it is real work and
        // must not land in the measurement.
        if trial == 1 {
            print_plans(&ctx, sql, "csv").await;
        }

        // Always seconds, fixed 6 decimals - {:?} on a Duration switches
        // between ms/s/etc. depending on magnitude, which doesn't match
        // MobilityDB's always-seconds output and makes the two harder to
        // compare side by side in the log/summary.
        println!("trial {trial}: total={:.6}s", total_time.as_secs_f64());
        total_times.push(total_time);
    }

    println!("--- mean over {trials} trials ---");
    println!("total: {:.6}s", mean(&total_times).as_secs_f64());
}

// Directory used for the Parquet load->query->clean cycle - kept separate
// from data/berlinmod (the CSV source of truth, read fresh every cycle) and
// from data/parquet/enormous (the static, pre-baked set benches/
// parquet_benchmarks/ assumes already exists and never times the
// conversion for).
fn parquet_cycle_dir(scale: &str) -> String {
    format!("./data/berlinmod_parquet_cycle/{scale}")
}

/// Reads the given BerlinMOD tables from CSV (via create_ctx()'s own
/// catalog) and writes each one out as its own Parquet file under
/// parquet_cycle_dir(scale). This is the "load" half of the cycle. Unlike
/// create_ctx()'s CSV registration - which is free, lazy metadata, not real
/// I/O - converting to Parquet is a genuine, expensive step, so it's worth
/// timing separately from "query" the same way MobilityDB's run_cycle.sql
/// times load vs. query separately (see build/mobilitydb/NOTES.md, "Timing
/// methodology"). `tables` should list only what the query actually needs
/// (see run_timed_parquet) - converting tables a query never reads would
/// inflate "load" with work that isn't part of what's being measured, the
/// same reasoning behind MobilityDB's own per-table load_vehicles/
/// load_regions/load_periods/load_points flags (run_cycle.sql).
pub async fn load_parquet(scale: &str, tables: &[&str]) {
    let dir = parquet_cycle_dir(scale);
    tokio::fs::create_dir_all(&dir)
        .await
        .expect("failed to create parquet cycle output directory");

    let csv_ctx = create_ctx().await;
    for table in tables {
        let path = format!("{dir}/{table}.parquet");
        csv_ctx
            .sql(&format!("SELECT * FROM csv.berlinmod.{table}"))
            .await
            .unwrap_or_else(|e| panic!("failed to plan read of {table}: {e}"))
            .write_parquet(&path, DataFrameWriteOptions::new(), None)
            .await
            .unwrap_or_else(|e| panic!("failed to write {table}.parquet: {e}"));
    }
}

/// Registers each Parquet file load_parquet() wrote as a bare table name
/// (no catalog/schema prefix - same convention benches/parquet_benchmarks/
/// already uses) for the "query" half of the cycle. No memory limit, same
/// as create_ctx() - see that function's comment for why. `tables` must
/// match what load_parquet() was given.
pub async fn create_parquet_query_ctx(scale: &str, tables: &[&str]) -> SessionContext {
    let config = SessionConfig::new()
        .with_batch_size(2048)
        .with_target_partitions(32);
    let ctx = SessionContext::new_with_config(config);
    udf::init(&ctx);

    let dir = parquet_cycle_dir(scale);
    for table in tables {
        let path = format!("{dir}/{table}.parquet");
        let schema = parquet_table_schema(table);
        let options = match &schema {
            Some(schema) => ParquetReadOptions::default().schema(schema),
            None => ParquetReadOptions::default(),
        };
        ctx.register_parquet(*table, &path, options)
            .await
            .unwrap_or_else(|e| panic!("failed to register {table}.parquet: {e}"));
    }
    ctx
}

fn geoarrow_field(name: &str, data_type: DataType, extension_name: &str) -> Field {
    Field::new(name, data_type, false).with_metadata(
        [(
            EXTENSION_TYPE_NAME_KEY.to_owned(),
            extension_name.to_owned(),
        )]
        .into(),
    )
}

/// Explicit schema override for the three tables carrying a GeoArrow
/// extension-typed column (polyline/point/polygon). Needed because
/// register_parquet()'s default schema inference reads only the physical
/// Parquet file, which has no notion of Arrow's custom field metadata - so
/// the `EXTENSION_TYPE_NAME_KEY` metadata these columns need (set explicitly
/// in each table's own CSV schema.rs, e.g.
/// src/core/csv/berlin_mod_csv/trips/schema.rs) would otherwise silently be
/// lost across the CSV -> Parquet -> Parquet-read round trip. Any UDF that
/// inspects that metadata (passes_point, subpolyline_between,
/// st_intersects, ...) fails without it; UDFs that
/// bypass it (during) happen to work either way, which is why this only
/// surfaced on q2/q4, not q1/q3.
fn parquet_table_schema(table: &str) -> Option<Schema> {
    match table {
        "trips" => Some(Schema::new(vec![
            Field::new("trip_id", DataType::Int64, false),
            Field::new("moid", DataType::Int64, false),
            geoarrow_field(
                "polyline",
                TRAJECTORY_DATATYPE.clone(),
                LineStringType::NAME,
            ),
        ])),
        // points1/regions1 (the *1 sample views) carry the same geometry-typed
        // columns as their full tables - and the same extension-metadata-loss
        // risk across the CSV -> Parquet -> Parquet-read round trip - so they
        // need the same explicit schema override, just under their own table
        // name (load_parquet()/create_parquet_query_ctx() treat each sample
        // view as its own independently materialized Parquet file, not a
        // view over the full table).
        "points" | "points1" => Some(Schema::new(vec![
            Field::new("point_id", DataType::Int64, false),
            geoarrow_field("point", POINT_XY_DATATYPE.clone(), PointType::NAME),
        ])),
        "regions" | "regions1" => Some(Schema::new(vec![
            Field::new("polygon_id", DataType::Int64, false),
            geoarrow_field("polygon", POLYGON_DATATYPE.clone(), PolygonType::NAME),
        ])),
        _ => None,
    }
}

/// Deletes the whole cycle directory - the "clean" half of the cycle,
/// restoring a clean state the same way MobilityDB's run_cycle.sql drops
/// its tables again after each trial.
pub async fn clean_parquet(scale: &str) {
    let dir = parquet_cycle_dir(scale);
    if tokio::fs::try_exists(&dir).await.unwrap_or(false) {
        tokio::fs::remove_dir_all(&dir)
            .await
            .expect("failed to clean up parquet cycle directory");
    }
}

/// The Parquet counterpart to run_timed(): a full clean->load->query->clean
/// cycle per trial, load and query timed separately (plus their sum) -
/// matching MobilityDB's run_cycle.sql model, not run_timed()'s single-total
/// CSV model, since converting to Parquet is a real load cost here, unlike
/// CSV's free lazy registration.
///
/// `sql` should reference the *bare* table names (`trips`, `datamcar`, ...),
/// matching create_parquet_query_ctx()'s registration - so this strips any
/// `csv.berlinmod.` prefix automatically, letting callers reuse the exact
/// same QUERY_Q2/QUERY_Q3/QUERY_Q4/query_q1() constants run_timed() uses,
/// unchanged. `tables` lists every table `sql` actually reads (including
/// `trips` - unlike MobilityDB's loader, nothing here loads it implicitly),
/// so only those get converted to Parquet each cycle.
pub async fn run_timed_parquet(sql: &str, trials: u32, tables: &[&str]) {
    let scale = std::env::var("BERLINMOD_SCALE").unwrap_or_else(|_| "0.005".to_string());
    let sql = sql.replace("csv.berlinmod.", "");

    let mut load_times = Vec::with_capacity(trials as usize);
    let mut query_times = Vec::with_capacity(trials as usize);
    let mut total_times = Vec::with_capacity(trials as usize);

    for trial in 1..=trials {
        let start = Instant::now();
        load_parquet(&scale, tables).await;
        let after_load = Instant::now();

        let ctx = create_parquet_query_ctx(&scale, tables).await;
        let df = ctx.sql(&sql).await.expect("query failed to plan");
        df.collect().await.expect("query failed to execute");
        let after_query = Instant::now();

        // After the timed span closes: planning the query again to print it is real work and
        // must not land in the measurement. Before clean_parquet(), which removes the files the
        // plan is built against.
        if trial == 1 {
            print_plans(&ctx, &sql, "parquet").await;
        }

        clean_parquet(&scale).await;

        let load_time = after_load - start;
        let query_time = after_query - after_load;
        let total_time = after_query - start;

        println!(
            "trial {trial}: load={:.6}s query={:.6}s total={:.6}s",
            load_time.as_secs_f64(),
            query_time.as_secs_f64(),
            total_time.as_secs_f64()
        );
        load_times.push(load_time);
        query_times.push(query_time);
        total_times.push(total_time);
    }

    println!("--- mean over {trials} trials ---");
    println!("load:  {:.6}s", mean(&load_times).as_secs_f64());
    println!("query: {:.6}s", mean(&query_times).as_secs_f64());
    println!("total: {:.6}s", mean(&total_times).as_secs_f64());
}

async fn postgres_pool() -> Arc<PostgresConnectionPool> {
    postgres::build_postgres_pool("localhost").await
}

/// A plain tokio-postgres connection for DDL/DML that has no DataFusion
/// counterpart (index creation, TRUNCATE/DELETE) - shared by
/// clean_postgres() and load_postgres()'s trips-index handling below.
async fn raw_postgres_client() -> tokio_postgres::Client {
    let connection_string = format!(
        "host=localhost port={} user={} password={} dbname={}",
        GLOBAL_CONFIG.get::<String>("postgres.port").unwrap(),
        GLOBAL_CONFIG.get::<String>("postgres.user").unwrap(),
        GLOBAL_CONFIG.get::<String>("postgres.password").unwrap(),
        GLOBAL_CONFIG.get::<String>("postgres.db").unwrap(),
    );
    let (client, connection) = tokio_postgres::connect(&connection_string, tokio_postgres::NoTls)
        .await
        .expect("failed to connect to postgres");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// trips' two indexes are created here, not in config/init.sql - see that
/// file's comment on the CREATE TABLE trips block. Charged to the load
/// phase (called before load_postgres()'s caller records after_load),
/// mirroring MobilityDB's own Trips loader, which creates its indexes
/// immediately after COPY-ing data in, inside the same timed load phase.
async fn create_trips_indexes(client: &tokio_postgres::Client) {
    client
        .batch_execute(
            "CREATE INDEX idx_trips_moid ON trips(moid); \
             CREATE INDEX idx_trips_polyline_gist ON trips USING gist(polyline);",
        )
        .await
        .expect("failed to create trips indexes");
}

/// licences.moid is populated here, not in config/init.sql - see that
/// file's comment on the CREATE TABLE licences block. Mirrors MobilityDB's
/// own Licences.VehId, populated once at load time via the same
/// Licence-string join - lets queries join trips.moid = licences1.moid
/// directly instead of bridging through datamcar.
async fn populate_licences_moid(client: &tokio_postgres::Client) {
    client
        .batch_execute(
            "UPDATE licences l SET moid = d.moid FROM datamcar d WHERE l.licence = d.licence;",
        )
        .await
        .expect("failed to populate licences.moid");
}

/// (CREATE INDEX statement(s), DROP INDEX statement(s)) for every table
/// besides trips (handled separately by create_trips_indexes/its own
/// DROP above - Trips is the one table that scales with the benchmark, so
/// its index cost is the main thing this per-cycle treatment exists to
/// measure fairly). These six are small, fixed-size reference tables,
/// already permanent in config/init.sql for the live server's sake (this
/// schema is shared with it, not just the benchmark harness) - also
/// recreated per cycle here so all 7 tables get the same
/// MobilityDB-comparable index-timing treatment, not just Trips.
fn reference_table_index_sql(table: &str) -> Option<(&'static str, &'static str)> {
    match table {
        "datamcar" => Some((
            "CREATE INDEX idx_datamcar_type ON datamcar(type); \
             CREATE INDEX idx_datamcar_licence ON datamcar(licence);",
            "DROP INDEX IF EXISTS idx_datamcar_type; \
             DROP INDEX IF EXISTS idx_datamcar_licence;",
        )),
        "points" => Some((
            "CREATE INDEX idx_points_point_gist ON points USING gist(point);",
            "DROP INDEX IF EXISTS idx_points_point_gist;",
        )),
        "licences" => Some((
            "CREATE INDEX idx_licences_moid ON licences(moid);",
            "DROP INDEX IF EXISTS idx_licences_moid;",
        )),
        "periods" => Some((
            "CREATE INDEX idx_periods_range ON periods USING btree(start_period, end_period);",
            "DROP INDEX IF EXISTS idx_periods_range;",
        )),
        "regions" => Some((
            "CREATE INDEX idx_regions_polygon_gist ON regions USING gist(polygon);",
            "DROP INDEX IF EXISTS idx_regions_polygon_gist;",
        )),
        _ => None,
    }
}

pub async fn load_postgres(tables: &[&str]) {
    let ctx = create_ctx().await;
    let pg_tables: Vec<String> = tables.iter().map(|t| t.to_string()).collect();
    let pg_catalog: Arc<dyn CatalogProvider> = Arc::new(
        postgres::catalog_provider::PostgresCatalogProvider::new(pg_tables, postgres_pool().await),
    );
    ctx.register_catalog("postgres", pg_catalog);

    for table in tables {
        ctx.table(format!("csv.berlinmod.{table}"))
            .await
            .unwrap_or_else(|e| panic!("failed to reference {table}: {e}"))
            .write_table(
                &format!("postgres.berlinmod.{table}"),
                DataFrameWriteOptions::new(),
            )
            .await
            .unwrap_or_else(|e| panic!("failed to load {table} into postgres: {e}"));
    }

    if tables.contains(&"trips") {
        create_trips_indexes(&raw_postgres_client().await).await;
    }

    if tables.contains(&"licences") && tables.contains(&"datamcar") {
        populate_licences_moid(&raw_postgres_client().await).await;
    }

    // Reference-table indexes, created after populate_licences_moid() so
    // idx_licences_moid is built once moid is actually populated, not on a
    // column full of NULLs.
    let client = raw_postgres_client().await;
    for table in tables {
        if let Some((create_sql, _)) = reference_table_index_sql(table) {
            client
                .batch_execute(create_sql)
                .await
                .unwrap_or_else(|e| panic!("failed to create {table} indexes: {e}"));
        }
    }
}

/// The pushdown-enabled query context - mirrors
/// benches/postgres_full_pushdown_benchmarks/mod.rs's create_ctx() exactly
/// (same optimizer rules, same custom query planner), except only the
/// tables this specific query needs get registered, and only the "postgres"
/// catalog exists at all (no csv catalog registered here) - so
/// PushdownOptimizerRule::all_tables_from_postgres is satisfied and the
/// whole query gets sent to Postgres as one SQL string, executed by
/// Postgres's own engine using the native SQL/PLpgSQL predicate
/// implementations in config/init.sql (passes_point, during,
/// subpolyline_between, ...) - not DataFusion's own UDFs.
pub async fn create_postgres_query_ctx(tables: &[&str]) -> SessionContext {
    let pool = postgres_pool().await;

    let extension_planner = Arc::new(PostgresExtensionPlanner {
        postgres_pool: pool.clone(),
    });
    let query_planner = Arc::new(PostgresQueryPlanner {
        physical_planner: Arc::new(DefaultPhysicalPlanner::with_extension_planners(vec![
            extension_planner,
        ])),
    });
    let mut custom_rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> =
        vec![Arc::new(PushdownOptimizerRule {})];
    custom_rules.extend(datafusion::optimizer::Optimizer::new().rules);
    let config = SessionConfig::new()
        .with_batch_size(2048)
        .with_target_partitions(32);
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_optimizer_rules(custom_rules)
        .with_query_planner(query_planner)
        .with_default_features()
        .build();
    let ctx = SessionContext::new_with_state(state);
    udf::init(&ctx);

    let pg_tables: Vec<String> = tables.iter().map(|t| t.to_string()).collect();
    let pg_catalog: Arc<dyn CatalogProvider> = Arc::new(
        postgres::catalog_provider::PostgresCatalogProvider::new(pg_tables, pool),
    );
    ctx.register_catalog("postgres", pg_catalog);
    ctx
}

/// Empties the given Postgres tables via a plain, direct tokio-postgres
/// connection (not through DataFusion, which has no DELETE/TRUNCATE DML
/// support here) - the "clean" half of the cycle, restoring a clean state
/// the same way MobilityDB's run_cycle.sql drops its tables again after
/// each trial. DELETE rather than DROP/CREATE: these tables (and the native
/// predicate functions in config/init.sql that operate on them) are
/// provisioned once by chameleon_postgis_dev's own init script, not
/// recreated per cycle.
pub async fn clean_postgres(tables: &[&str]) {
    let client = raw_postgres_client().await;

    // trips' indexes are dropped before the DELETE (not just after data is
    // gone) so the delete itself doesn't pay index-maintenance cost that
    // create_trips_indexes() will charge again next cycle anyway, and so a
    // second load_postgres() call never hits "index already exists".
    if tables.contains(&"trips") {
        client
            .batch_execute(
                "DROP INDEX IF EXISTS idx_trips_moid; \
                 DROP INDEX IF EXISTS idx_trips_polyline_gist;",
            )
            .await
            .expect("failed to drop trips indexes");
    }

    // Same DROP-before-DELETE reasoning as trips above, for the six
    // reference-table indexes (see reference_table_index_sql()) - IF
    // EXISTS matters here specifically because these tables' indexes are
    // ALSO permanent in config/init.sql (for the live server's sake), so
    // the very first cycle against a freshly-initialized container drops
    // indexes that came from init.sql, not from a previous cycle - this is
    // what guarantees load_postgres()'s later CREATE INDEX never hits
    // "already exists", on the first cycle or any other.
    for table in tables {
        if let Some((_, drop_sql)) = reference_table_index_sql(table) {
            client
                .batch_execute(drop_sql)
                .await
                .unwrap_or_else(|e| panic!("failed to drop {table} indexes: {e}"));
        }
    }

    for table in tables {
        client
            .batch_execute(&format!("DELETE FROM {table}"))
            .await
            .unwrap_or_else(|e| panic!("failed to clean postgres table {table}: {e}"));
    }
}

/// The Postgres/pushdown counterpart to run_timed()/run_timed_parquet(): a
/// full clean->load->query->clean cycle per trial, load and query timed
/// separately (plus their sum) - matching MobilityDB's run_cycle.sql model.
/// Unlike run_timed_parquet(), the "query" phase here isn't SAQE's own
/// execution engine at all once pushdown fires - see
/// create_postgres_query_ctx()'s comment. `sql` should reference the *bare*
/// table names or the `csv.berlinmod.` form (this rewrites either to
/// `postgres.berlinmod.`), so callers can reuse the same
/// QUERY_Q2/QUERY_Q3/QUERY_Q4/query_q1() constants unchanged. `tables` lists
/// every table `sql` reads, same convention as run_timed_parquet().
pub async fn run_timed_postgres(sql: &str, trials: u32, tables: &[&str]) {
    let sql = sql.replace("csv.berlinmod.", "postgres.berlinmod.");

    // Defensive: chameleon_postgis_dev is a persistent, shared container
    // (unlike the Parquet cycle's own scratch directory, which is
    // guaranteed empty on first use) - clean once up front in case a prior
    // run was interrupted before its own final clean_postgres() ran.
    clean_postgres(tables).await;

    let mut load_times = Vec::with_capacity(trials as usize);
    let mut query_times = Vec::with_capacity(trials as usize);
    let mut total_times = Vec::with_capacity(trials as usize);

    for trial in 1..=trials {
        let start = Instant::now();
        load_postgres(tables).await;
        let after_load = Instant::now();

        let ctx = create_postgres_query_ctx(tables).await;
        let df = ctx.sql(&sql).await.expect("query failed to plan");
        df.collect().await.expect("query failed to execute");
        let after_query = Instant::now();

        clean_postgres(tables).await;

        let load_time = after_load - start;
        let query_time = after_query - after_load;
        let total_time = after_query - start;

        println!(
            "trial {trial}: load={:.6}s query={:.6}s total={:.6}s",
            load_time.as_secs_f64(),
            query_time.as_secs_f64(),
            total_time.as_secs_f64()
        );
        load_times.push(load_time);
        query_times.push(query_time);
        total_times.push(total_time);
    }

    println!("--- mean over {trials} trials ---");
    println!("load:  {:.6}s", mean(&load_times).as_secs_f64());
    println!("query: {:.6}s", mean(&query_times).as_secs_f64());
    println!("total: {:.6}s", mean(&total_times).as_secs_f64());
}

// ---------------------------------------------------------------------------------------
// Mixed-source trips (q21/q22)
//
// q21/q22 run q13 with its trips split across two stores: the first half stays in CSV, the
// second half moves to Parquet (q21) or Postgres (q22), and the query UNION ALLs them back
// together. Both halves are materialised, not filtered at query time - a predicate would leave
// the CSV side scanning the whole file while the other side scanned half, which would bias the
// very number being measured.
// ---------------------------------------------------------------------------------------

fn trips_half_dir(scale: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("chameleon_bench/{scale}"))
}

/// Tripid of one raw trips CSV row. The file is `Moid,Tripid,Tstart,...`, so it is column 1.
fn raw_trip_id(line: &str) -> Option<i64> {
    line.split(',').nth(1)?.trim().parse().ok()
}

/// Splits the trips in two and writes the first half as a CSV the trips provider can read,
/// returning `(path, split_trip_id)`. Every trip with `trip_id <= split_trip_id` is in the file;
/// the callers load the rest into Parquet or Postgres.
///
/// Filters the raw CSV rather than going through DataFusion: the provider's output is assembled
/// trajectories (`trip_id, moid, polyline`), which it could not read back. Splits on Tripid
/// rather than row count - the file holds one row per segment, so a row split would cut a trip
/// across the two sources - and at the median distinct id, so the halves are equal in trips even
/// if the ids are sparse.
pub async fn setup_trips_half_csv(scale: &str) -> (String, i64) {
    use std::io::{BufRead, BufReader, BufWriter, Write};

    let source = format!("./data/berlinmod/{scale}/trips.csv");
    let dir = trips_half_dir(scale);
    tokio::fs::create_dir_all(&dir)
        .await
        .expect("failed to create the trips-half directory");
    let target = dir.join("trips_half.csv");

    let open = || {
        BufReader::new(
            std::fs::File::open(&source).unwrap_or_else(|e| panic!("failed to open {source}: {e}")),
        )
    };

    // Pass 1: the distinct trip ids, to split at the median. Streamed rather than read whole -
    // this file is 29 MB at scale 0.005 and much larger at 1.0.
    let mut ids: Vec<i64> = open()
        .lines()
        .skip(1)
        .filter_map(|line| raw_trip_id(&line.expect("failed to read trips.csv")))
        .collect();
    ids.sort_unstable();
    ids.dedup();
    assert!(!ids.is_empty(), "{source} has no trips");
    let split = ids[ids.len() / 2 - 1];

    // Pass 2: header verbatim, then every row of a first-half trip.
    let mut reader = open().lines();
    let header = reader
        .next()
        .expect("trips.csv is empty")
        .expect("failed to read the trips.csv header");
    let mut writer = BufWriter::new(
        std::fs::File::create(&target).expect("failed to create the trips-half CSV"),
    );
    writeln!(writer, "{header}").unwrap();
    for line in reader {
        let line = line.expect("failed to read trips.csv");
        if raw_trip_id(&line).is_some_and(|id| id <= split) {
            writeln!(writer, "{line}").unwrap();
        }
    }
    writer.flush().unwrap();

    (target.to_string_lossy().to_string(), split)
}

pub async fn clean_trips_half_csv(scale: &str) {
    let dir = trips_half_dir(scale);
    if tokio::fs::try_exists(&dir).await.unwrap_or(false) {
        tokio::fs::remove_dir_all(&dir)
            .await
            .expect("failed to clean up the trips-half directory");
    }
}

/// Writes the second-half trips as `trips_half.parquet` under the parquet cycle dir, and
/// registers nothing - `create_mixed_ctx()` does that. `split` comes from
/// `setup_trips_half_csv()`, so the two halves reconstruct the whole table with no overlap.
pub async fn load_parquet_trips_half(scale: &str, split: i64) {
    let dir = parquet_cycle_dir(scale);
    tokio::fs::create_dir_all(&dir)
        .await
        .expect("failed to create parquet cycle output directory");

    let csv_ctx = create_ctx().await;
    csv_ctx
        .sql(&format!(
            "SELECT trip_id, moid, polyline FROM csv.berlinmod.trips WHERE trip_id > {split}"
        ))
        .await
        .unwrap_or_else(|e| panic!("failed to plan the trips second half: {e}"))
        .write_parquet(
            &format!("{dir}/trips_half.parquet"),
            DataFrameWriteOptions::new(),
            None,
        )
        .await
        .unwrap_or_else(|e| panic!("failed to write trips_half.parquet: {e}"));
}

/// Creates and fills `public.trips_half` with the second-half trips.
///
/// The table is created with plain DDL and reached through the `pg` dynamic catalog, which
/// describes any table against the live database - `postgres.berlinmod.*` resolves names through
/// a fixed match (src/core/postgres/schema_provider.rs) and has no entry for this one. The
/// geometry column therefore arrives as Binary EWKB, so the insert goes through
/// `trajectory_to_wkb` and the query reads it back through `wkb_to_trajectory`.
pub async fn load_postgres_trips_half(split: i64) {
    let client = raw_postgres_client().await;
    client
        .batch_execute(
            "DROP TABLE IF EXISTS trips_half;
             CREATE TABLE trips_half (
               trip_id INTEGER PRIMARY KEY,
               moid INTEGER,
               polyline geometry(LINESTRINGM, 4326));",
        )
        .await
        .expect("failed to create trips_half");

    let ctx = create_ctx().await;
    register_pg_catalog(&ctx).await;
    ctx.sql(&format!(
        "INSERT INTO pg.public.trips_half \
         SELECT trip_id, moid, trajectory_to_wkb(polyline) \
         FROM csv.berlinmod.trips WHERE trip_id > {split}"
    ))
    .await
    .unwrap_or_else(|e| panic!("failed to plan the trips_half insert: {e}"))
    .collect()
    .await
    .unwrap_or_else(|e| panic!("failed to load trips_half: {e}"));
}

pub async fn clean_postgres_trips_half() {
    raw_postgres_client()
        .await
        .batch_execute("DROP TABLE IF EXISTS trips_half;")
        .await
        .expect("failed to drop trips_half");
}

/// Registers the `pg` dynamic catalog on `ctx`, so `pg.public.<table>` resolves any table in the
/// database without a hand-written provider.
async fn register_pg_catalog(ctx: &SessionContext) {
    let pool = postgres_pool().await;
    let catalog: Arc<dyn CatalogProvider> =
        Arc::new(postgres::dynamic::catalog_provider::DynamicPostgresCatalogProvider::new(pool));
    ctx.register_catalog(postgres::dynamic::CATALOG_NAME, catalog);
}

/// Which store holds the second half of the trips.
#[derive(Clone, Copy)]
pub enum TripsSecondHalf {
    Parquet,
    Postgres,
}

/// Context for q21/q22: every table from CSV as usual, except trips, which holds only the first
/// half - plus the second half registered from Parquet or reachable as `pg.public.trips_half`.
pub async fn create_mixed_ctx(
    scale: &str,
    half_csv: &str,
    second_half: TripsSecondHalf,
) -> SessionContext {
    // Scoped to this one call: create_ctx() reads the override, which is cleared straight away
    // so every other context still sees the full trips.csv.
    set_trips_csv_override(Some(half_csv));
    let ctx = create_ctx().await;
    set_trips_csv_override(None);

    match second_half {
        TripsSecondHalf::Parquet => {
            let path = format!("{}/trips_half.parquet", parquet_cycle_dir(scale));
            ctx.register_parquet(
                "trips_half_parquet",
                &path,
                ParquetReadOptions::default().schema(
                    &parquet_table_schema("trips").expect("trips has a fixed parquet schema"),
                ),
            )
            .await
            .unwrap_or_else(|e| panic!("failed to register trips_half.parquet: {e}"));
        }
        TripsSecondHalf::Postgres => register_pg_catalog(&ctx).await,
    }
    ctx
}

/// Materialises both halves and returns the CSV half's path plus the split id. The halves must
/// reconstruct the whole table exactly, which is asserted here rather than left to the caller.
pub async fn setup_mixed_trips(scale: &str, second_half: TripsSecondHalf) -> (String, i64) {
    let (half_csv, split) = setup_trips_half_csv(scale).await;
    match second_half {
        TripsSecondHalf::Parquet => load_parquet_trips_half(scale, split).await,
        TripsSecondHalf::Postgres => load_postgres_trips_half(split).await,
    }

    let full = trip_count(&create_ctx().await, "csv.berlinmod.trips").await;
    let ctx = create_mixed_ctx(scale, &half_csv, second_half).await;
    let first = trip_count(&ctx, "csv.berlinmod.trips").await;
    let second = match second_half {
        TripsSecondHalf::Parquet => trip_count(&ctx, "trips_half_parquet").await,
        TripsSecondHalf::Postgres => trip_count(&ctx, "pg.public.trips_half").await,
    };
    assert_eq!(
        first + second,
        full,
        "the trips halves do not reconstruct the full table (csv={first} second={second} full={full})"
    );

    (half_csv, split)
}

async fn trip_count(ctx: &SessionContext, relation: &str) -> i64 {
    use datafusion::arrow::array::Int64Array;
    let batches = ctx
        .sql(&format!("SELECT count(trip_id) AS n FROM {relation}"))
        .await
        .unwrap_or_else(|e| panic!("failed to plan count over {relation}: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("failed to count {relation}: {e}"));
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count is an Int64")
        .value(0)
}

pub async fn clean_mixed_trips(scale: &str, second_half: TripsSecondHalf) {
    clean_trips_half_csv(scale).await;
    match second_half {
        TripsSecondHalf::Parquet => clean_parquet(scale).await,
        TripsSecondHalf::Postgres => clean_postgres_trips_half().await,
    }
}

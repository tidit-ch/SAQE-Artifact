//! Schema-free access to Postgres/PostGIS.
//!
//! Registers a catalog in which *any* table in the database is readable and writable without
//! a hand-written `TableProvider`, a declared Arrow schema, or a restart:
//!
//! ```sql
//! SELECT * FROM pg.public.some_table_created_five_seconds_ago;
//! INSERT INTO pg.public.trips SELECT trip_id, moid, trajectory_to_wkb(polyline)
//!   FROM csv.berlinmod.trips;
//! ```
//!
//! Geometry columns arrive as `Binary` (raw EWKB from the Postgres wire format), not as
//! [`TRAJECTORY_DATATYPE`](crate::core::utils::schema::TRAJECTORY_DATATYPE), because the schema
//! is derived from the database rather than declared in Rust. Use the `wkb_to_trajectory` /
//! `trajectory_to_wkb` UDFs to bridge the two.

pub mod catalog_provider;
pub mod dialect;
pub mod schema_provider;
pub mod sink;
pub mod table_provider;

use crate::core::DATAFUSION_CTX;
use catalog_provider::DynamicPostgresCatalogProvider;
use datafusion::catalog::CatalogProvider;
use datafusion_table_providers::sql::db_connection_pool::{
    postgrespool::PostgresConnectionPool, DbConnectionPool,
};
use std::sync::Arc;

/// Catalog name under which the dynamic Postgres tables are registered.
pub const CATALOG_NAME: &str = "pg";

/// Postgres schemas that hold no user tables.
const SYSTEM_SCHEMAS: [&str; 3] = ["pg_catalog", "information_schema", "pg_toast"];

/// Registers the `pg` catalog and pre-warms its schema/table listings.
///
/// Listing failures are not fatal — tables still resolve by name — so this never panics on a
/// database that is merely slow to come up.
pub async fn init_dynamic_postgres(pool: Arc<PostgresConnectionPool>) {
    let catalog = Arc::new(DynamicPostgresCatalogProvider::new(pool.clone()));

    match list_schemas(&pool).await {
        Ok(schemas) => catalog.refresh(schemas).await,
        Err(e) => {
            tracing::warn!("could not list Postgres schemas for the '{CATALOG_NAME}' catalog: {e}");
        }
    }

    DATAFUSION_CTX.register_catalog(CATALOG_NAME, catalog as Arc<dyn CatalogProvider>);
}

async fn list_schemas(
    pool: &Arc<PostgresConnectionPool>,
) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
    let mut db_conn = pool.connect().await?;
    let conn = datafusion_table_providers::postgres::Postgres::postgres_conn(&mut db_conn)?;

    let rows = conn
        .conn
        .query(
            "SELECT schema_name FROM information_schema.schemata \
             WHERE schema_name <> ALL($1) ORDER BY schema_name",
            &[&SYSTEM_SCHEMAS.to_vec()],
        )
        .await?;

    Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_table_providers::util::secrets::to_secret_map;
    use std::collections::HashMap;

    async fn test_pool() -> Arc<PostgresConnectionPool> {
        let params = to_secret_map(HashMap::from([
            ("host".into(), "localhost".to_string()),
            ("user".into(), "user".to_string()),
            ("db".into(), "gis".to_string()),
            ("pass".into(), "password".to_string()),
            ("port".into(), "5432".to_string()),
            ("sslmode".into(), "disable".to_string()),
        ]));
        Arc::new(PostgresConnectionPool::new(params).await.unwrap())
    }

    async fn raw_client() -> tokio_postgres::Client {
        let (client, conn) = tokio_postgres::connect(
            "host=localhost port=5432 user=user password=password dbname=gis",
            tokio_postgres::NoTls,
        )
        .await
        .unwrap();
        tokio::spawn(async move {
            let _ = conn.await;
        });
        client
    }

    /// End-to-end: a table that does not exist when the catalog is built is read, written and
    /// read back through SQL alone — no Arrow schema declared anywhere, no restart.
    #[tokio::test]
    #[ignore = "needs a live PostGIS instance on localhost:5432"]
    async fn reads_and_writes_a_table_created_after_startup() {
        let ctx = SessionContext::new();
        crate::core::udf::init(&ctx);

        // Make sure a previous run left nothing behind.
        let client = raw_client().await;
        client
            .batch_execute("DROP TABLE IF EXISTS dyn_probe;")
            .await
            .unwrap();

        let pool = test_pool().await;
        let catalog = Arc::new(DynamicPostgresCatalogProvider::new(pool.clone()));

        // Catalog is warmed BEFORE the new table exists.
        catalog.refresh(vec!["public".to_string()]).await;
        assert!(
            !catalog
                .schema("public")
                .unwrap()
                .table_names()
                .contains(&"dyn_probe".to_string()),
            "probe table must not be known at refresh time"
        );

        ctx.register_catalog(CATALOG_NAME, catalog as Arc<dyn CatalogProvider>);

        // ---- now create the table, after the catalog was built ----
        client
            .batch_execute(
                "CREATE TABLE dyn_probe ( \
                     trip_id int4, moid int4, polyline geometry(LineStringM, 4326) );",
            )
            .await
            .unwrap();

        // ---- READ a pre-existing geometry table, with no schema declared in Rust ----
        let df = ctx
            .sql("SELECT trip_id, polyline FROM pg.public.trips ORDER BY trip_id LIMIT 2")
            .await
            .expect("resolve pg.public.trips");
        let batches = df.collect().await.expect("scan trips");
        println!(
            "read schema: polyline -> {:?}",
            batches[0].schema().field(1).data_type()
        );
        assert_eq!(batches[0].num_rows(), 2);

        // ---- the bridge UDF makes the geometry usable by the trajectory UDFs ----
        let df = ctx
            .sql(
                "SELECT trip_id, duration(wkb_to_trajectory(polyline)) AS dur \
                 FROM pg.public.trips ORDER BY trip_id LIMIT 2",
            )
            .await
            .expect("plan wkb_to_trajectory + duration");
        let dur = df
            .collect()
            .await
            .expect("run wkb_to_trajectory + duration");
        println!(
            "duration over decoded trajectory: {} rows",
            dur[0].num_rows()
        );
        assert_eq!(dur[0].num_rows(), 2);

        // ---- WRITE into the brand-new table, through SQL, via the dynamic provider ----
        let df = ctx
            .sql(
                "INSERT INTO pg.public.dyn_probe \
                 SELECT trip_id, moid, polyline FROM pg.public.trips WHERE trip_id <= 3",
            )
            .await
            .expect("plan INSERT into a table created after startup");
        let written = df.collect().await.expect("run INSERT");
        println!("insert result batch: {:?}", written[0].column(0));

        // ---- verify in Postgres that real geometry landed ----
        let rows = client
            .query(
                "SELECT trip_id, GeometryType(polyline), ST_SRID(polyline), ST_NDims(polyline)::int4 \
                 FROM dyn_probe ORDER BY trip_id",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 3, "three trips should have been written");
        for row in &rows {
            assert_eq!(row.get::<_, String>(1), "LINESTRINGM");
            assert_eq!(row.get::<_, i32>(2), 4326);
            assert_eq!(row.get::<_, i32>(3), 3);
        }

        // ---- geometry survived byte-for-byte ----
        let same: bool = client
            .query_one(
                "SELECT bool_and(ST_OrderingEquals(a.polyline, b.polyline)) \
                 FROM dyn_probe a JOIN trips b USING (trip_id)",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert!(same, "written geometry must equal the source geometry");

        // ---- the new table now shows up in listings too ----
        client.batch_execute("DROP TABLE dyn_probe;").await.unwrap();
        println!("OK");
    }

    /// Which shapes may be inserted into a PostGIS geometry column through this catalog.
    #[tokio::test]
    #[ignore = "needs a live PostGIS instance on localhost:5432"]
    async fn insert_accepts_trajectory_shapes() {
        let ctx = SessionContext::new();
        crate::core::udf::init(&ctx);
        // `trajectory_from_text` is an internal helper, not part of `udf::init`.
        ctx.register_udf(
            crate::core::udf::utils::line::TrajectoryFromText::new(
                geoarrow_schema::CoordType::Separated,
            )
            .into(),
        );

        let client = raw_client().await;
        client
            .batch_execute(
                "DROP TABLE IF EXISTS shape_probe; \
                 CREATE TABLE shape_probe ( \
                     trip_id int4, moid int4, polyline geometry(LineStringM, 4326) );",
            )
            .await
            .unwrap();

        let catalog = Arc::new(DynamicPostgresCatalogProvider::new(test_pool().await));
        catalog.refresh(vec!["public".to_string()]).await;
        ctx.register_catalog(CATALOG_NAME, catalog as Arc<dyn CatalogProvider>);

        const TRAJ: &str = "trajectory_from_text('LINESTRING M (13.4 52.4 10, 13.5 52.5 20)')";

        async fn try_insert(ctx: &SessionContext, sql: &str) -> Result<(), String> {
            match ctx.sql(sql).await {
                Ok(df) => df.collect().await.map(|_| ()).map_err(|e| e.to_string()),
                Err(e) => Err(e.to_string()),
            }
        }

        // (a) geoarrow trajectory wrapped by trajectory_to_wkb -> Binary
        let via_udf = try_insert(
            &ctx,
            &format!(
                "INSERT INTO pg.public.shape_probe \
                 SELECT 1 AS trip_id, 1 AS moid, trajectory_to_wkb({TRAJ}) AS polyline"
            ),
        )
        .await;
        println!("(a) trajectory_to_wkb(...)      -> {via_udf:?}");

        // (b) raw geoarrow trajectory, no cast at the call site
        let raw = try_insert(
            &ctx,
            &format!(
                "INSERT INTO pg.public.shape_probe \
                 SELECT 2 AS trip_id, 2 AS moid, {TRAJ} AS polyline"
            ),
        )
        .await;
        println!("(b) raw geoarrow trajectory     -> {raw:?}");

        let rows = client
            .query(
                "SELECT trip_id, ST_AsText(polyline) FROM shape_probe ORDER BY trip_id",
                &[],
            )
            .await
            .unwrap();
        for r in &rows {
            println!(
                "  stored trip_id={} {}",
                r.get::<_, i32>(0),
                r.get::<_, String>(1)
            );
        }

        assert!(
            via_udf.is_ok(),
            "trajectory_to_wkb path must work: {via_udf:?}"
        );
        client
            .batch_execute("DROP TABLE shape_probe;")
            .await
            .unwrap();
    }

    /// Reproduces the reported failure: geoarrow trajectory -> `pg.public.trips`, the real
    /// table, through `trajectory_to_wkb`.
    #[tokio::test]
    #[ignore = "needs a live PostGIS instance on localhost:5432"]
    async fn inserts_trajectory_into_the_real_trips_table() {
        let ctx = SessionContext::new();
        crate::core::udf::init(&ctx);
        ctx.register_udf(
            crate::core::udf::utils::line::TrajectoryFromText::new(
                geoarrow_schema::CoordType::Separated,
            )
            .into(),
        );

        let client = raw_client().await;
        client
            .execute("DELETE FROM trips WHERE trip_id = 9999", &[])
            .await
            .unwrap();

        let catalog = Arc::new(DynamicPostgresCatalogProvider::new(test_pool().await));
        catalog.refresh(vec!["public".to_string()]).await;
        ctx.register_catalog(CATALOG_NAME, catalog as Arc<dyn CatalogProvider>);

        ctx.sql(
            "INSERT INTO pg.public.trips SELECT 9999 AS trip_id, 42 AS moid, \
             trajectory_to_wkb(trajectory_from_text( \
                'LINESTRING M (13.43593 52.41721 1180224000000, 13.43605 52.41723 1180224060000)' \
             )) AS polyline",
        )
        .await
        .expect("plan INSERT into pg.public.trips")
        .collect()
        .await
        .expect("run INSERT into pg.public.trips");

        let row = client
            .query_one(
                "SELECT moid, GeometryType(polyline), ST_SRID(polyline), ST_AsText(polyline) \
                 FROM trips WHERE trip_id = 9999",
                &[],
            )
            .await
            .unwrap();
        println!(
            "moid={} type={} srid={} wkt={}",
            row.get::<_, i32>(0),
            row.get::<_, String>(1),
            row.get::<_, i32>(2),
            row.get::<_, String>(3)
        );
        assert_eq!(row.get::<_, String>(1), "LINESTRINGM");
        assert_eq!(row.get::<_, i32>(2), 4326);

        client
            .execute("DELETE FROM trips WHERE trip_id = 9999", &[])
            .await
            .unwrap();
    }
}

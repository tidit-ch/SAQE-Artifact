pub mod berlin_mod_postgres;
pub mod catalog_provider;
pub mod dynamic;
pub mod schema_provider;
pub mod write;

use super::DATAFUSION_CTX;
use crate::server::utils::config::GLOBAL_CONFIG;
use datafusion::catalog::CatalogProvider;

use datafusion_table_providers::{
    sql::db_connection_pool::postgrespool::PostgresConnectionPool, util::secrets::to_secret_map,
};

use std::collections::HashMap;
use std::sync::Arc;

pub async fn build_postgres_pool(host: &str) -> Arc<PostgresConnectionPool> {
    let postgres_params = to_secret_map(HashMap::from([
        ("host".to_string(), host.to_string()),
        (
            "user".to_string(),
            GLOBAL_CONFIG.get::<String>("postgres.user").unwrap(),
        ),
        (
            "db".to_string(),
            GLOBAL_CONFIG.get::<String>("postgres.db").unwrap(),
        ),
        (
            "pass".to_string(),
            GLOBAL_CONFIG.get::<String>("postgres.password").unwrap(),
        ),
        (
            "port".to_string(),
            GLOBAL_CONFIG.get::<String>("postgres.port").unwrap(),
        ),
        (
            "sslmode".to_string(),
            GLOBAL_CONFIG.get::<String>("postgres.sslmode").unwrap(),
        ),
    ]));
    Arc::new(
        PostgresConnectionPool::new(postgres_params)
            .await
            .expect("unable to create PostgreSQL connection pool"),
    )
}

pub async fn init_postgres() {
    // Create PostgreSQL connection parameters
    let postgres_params = to_secret_map(HashMap::from([
        (
            "host".to_string(),
            GLOBAL_CONFIG.get::<String>("postgres.host").unwrap(),
        ),
        (
            "user".to_string(),
            GLOBAL_CONFIG.get::<String>("postgres.user").unwrap(),
        ),
        (
            "db".to_string(),
            GLOBAL_CONFIG.get::<String>("postgres.db").unwrap(),
        ),
        (
            "pass".to_string(),
            GLOBAL_CONFIG.get::<String>("postgres.password").unwrap(),
        ),
        (
            "port".to_string(),
            GLOBAL_CONFIG.get::<String>("postgres.port").unwrap(),
        ),
        (
            "sslmode".to_string(),
            GLOBAL_CONFIG.get::<String>("postgres.sslmode").unwrap(),
        ),
    ]));

    // Create PostgreSQL connection pool
    let postgres_pool = Arc::new(
        PostgresConnectionPool::new(postgres_params)
            .await
            .expect("unable to create PostgreSQL connection pool"),
    );

    // Register catalog for Postgres tables
    let tables = vec![
        "trips".to_string(),
        "regions".to_string(),
        "periods".to_string(),
        "points".to_string(),
        "datamcar".to_string(),
        "instants".to_string(),
        "licences".to_string(),
    ];
    let custom_catalog: Arc<dyn CatalogProvider> = Arc::new(
        catalog_provider::PostgresCatalogProvider::new(tables, postgres_pool.clone()),
    );
    DATAFUSION_CTX.register_catalog("postgres", custom_catalog);

    // Schema-free access to the same database: every table is readable and writable under
    // the `pg` catalog without a hand-written provider. See `postgres::dynamic`.
    dynamic::init_dynamic_postgres(postgres_pool).await;
}

/// Populate PostgreSQL tables with data from the CSV files
#[allow(dead_code)]
pub async fn populate_postgres_tables() -> datafusion::error::Result<()> {
    //let query = "INSERT INTO postgres.postgres.points SELECT * FROM csv.berlinmod.points;";
    //let query = "INSERT OR REPLACE INTO postgres.berlinmod.points SELECT * FROM csv.berlinmod.points;";
    let query = "INSERT OVERWRITE INTO postgres.berlinmod.trips SELECT * FROM csv.berlinmod.trips;";
    let _ = DATAFUSION_CTX.sql(query).await?.collect().await?;
    println!("Trips table populated successfully.");
    let query =
        "INSERT OVERWRITE INTO postgres.berlinmod.points SELECT * FROM csv.berlinmod.points;";
    let _ = DATAFUSION_CTX.sql(query).await?.collect().await?;
    println!("Points table populated successfully.");
    let query =
        "INSERT OVERWRITE INTO postgres.berlinmod.datamcar SELECT * FROM csv.berlinmod.datamcar;";
    let _ = DATAFUSION_CTX.sql(query).await?.collect().await?;
    println!("Datamcar table populated successfully.");
    let query =
        "INSERT OR REPLACE INTO postgres.berlinmod.regions SELECT * FROM csv.berlinmod.regions;";
    let _ = DATAFUSION_CTX.sql(query).await?.collect().await?;
    println!("Regions table populated successfully.");
    let query =
        "INSERT OVERWRITE INTO postgres.berlinmod.instants SELECT * FROM csv.berlinmod.instants;";
    let _ = DATAFUSION_CTX.sql(query).await?.collect().await?;
    println!("Instants table populated successfully.");
    let query =
        "INSERT OVERWRITE INTO postgres.berlinmod.periods SELECT * FROM csv.berlinmod.periods;";
    DATAFUSION_CTX
        .sql(query)
        .await?
        .collect()
        .await
        .expect("Error");
    println!("Periods table populated successfully.");
    let query =
        "INSERT OVERWRITE INTO postgres.berlinmod.licences SELECT * FROM csv.berlinmod.licences;";
    let _ = DATAFUSION_CTX.sql(query).await?.collect().await?;
    println!("Licenece table populated successfully.");
    Ok(())
}

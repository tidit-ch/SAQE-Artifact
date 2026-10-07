pub mod berlin_mod_influx;
pub mod catalog_provider;
pub mod schema_provider;
pub mod write;

use super::DATAFUSION_CTX;
use crate::server::utils::config::GLOBAL_CONFIG;
use crate::utils::error::ToDataFusionError;
use datafusion::catalog::CatalogProvider;

use std::collections::HashMap;
use std::sync::Arc;

pub async fn init_influxdb3() {
    let influx_options = HashMap::from([
        (
            "database_name".to_string(),
            GLOBAL_CONFIG.get::<String>("influx.db").unwrap(),
        ),
        (
            "password".to_string(),
            GLOBAL_CONFIG.get::<String>("influx.token").unwrap(),
        ),
        (
            "influx_url".to_string(),
            GLOBAL_CONFIG.get::<String>("influx.url").unwrap(),
        ),
        (
            "precision".to_string(),
            GLOBAL_CONFIG.get::<String>("influx.precision").unwrap(),
        ),
    ]);

    // Register catalog for InfluxDb3 tables
    let influx_tables: Vec<String> = vec![
        "trips".to_string(),
        "datamcar".to_string(),
        "instants".to_string(),
        "periods".to_string(),
        "licences".to_string(),
        "points".to_string(),
        "regions".to_string(),
    ];
    let custom_catalog: Arc<dyn CatalogProvider> = Arc::new(
        catalog_provider::InfluxCatalogProvider::new(influx_tables, influx_options),
    );
    DATAFUSION_CTX.register_catalog("influx", custom_catalog);
    // populate_influx_measurements()
    //     .await
    //     .expect("Error populating InfluxDB3 measurements");
}

/// Populate InfluxDB3 measurements with data from the CSV files
#[allow(dead_code)]
pub async fn populate_influx_measurements() -> datafusion::error::Result<()> {
    let query = "INSERT INTO influx.berlinmod.trips SELECT * FROM csv.berlinmod.trips;";
    let _ = DATAFUSION_CTX.sql(query).await?.collect().await?;
    println!("Trips table populated successfully.");
    let query = "INSERT INTO influx.berlinmod.points SELECT * FROM csv.berlinmod.points;";
    let _ = DATAFUSION_CTX.sql(query).await?.collect().await?;
    println!("Points table populated successfully.");
    let query = "INSERT INTO influx.berlinmod.datamcar SELECT * FROM csv.berlinmod.datamcar;";
    let _ = DATAFUSION_CTX.sql(query).await?.collect().await?;
    println!("Datamcar table populated successfully.");
    let query = "INSERT INTO influx.berlinmod.regions SELECT * FROM csv.berlinmod.regions;";
    let _ = DATAFUSION_CTX
        .sql(query)
        .await
        .to_df_error_ctx("[influx][populate] error populating regions table")?
        .collect()
        .await
        .to_df_error_ctx("[influx][poulate] error fetching record batch")?;
    println!("Regions table populated successfully.");
    let query = "INSERT INTO influx.berlinmod.instants SELECT * FROM csv.berlinmod.instants;";
    let _ = DATAFUSION_CTX.sql(query).await?.collect().await?;
    println!("Instants table populated successfully.");
    let query = "INSERT INTO influx.berlinmod.periods SELECT * FROM csv.berlinmod.periods;";
    DATAFUSION_CTX
        .sql(query)
        .await?
        .collect()
        .await
        .expect("Error");
    println!("Periods table populated successfully.");
    let query = "INSERT INTO influx.berlinmod.licences SELECT * FROM csv.berlinmod.licences;";
    let _ = DATAFUSION_CTX.sql(query).await?.collect().await?;
    println!("Licenece table populated successfully.");
    Ok(())
}

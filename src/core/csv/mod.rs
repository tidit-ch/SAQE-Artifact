pub mod berlin_mod_csv;
pub mod catalog_provider;
pub mod oystercatcher_belgium;
pub mod porto_taxi;
pub mod schema_provider;

use super::DATAFUSION_CTX;
use crate::server::utils::config::get_string_or;
use datafusion::catalog::CatalogProvider;
use std::sync::Arc;

/// Path for a CSV table: the config value, or `default` when the key is absent or empty.
fn csv_path(key: &str, default: &str) -> String {
    get_string_or(&format!("data_source.csv.{key}"), default)
}

pub async fn init_csv() {
    // Register catalog for BERLIN MOD CSV files
    let trips_csv = (
        "trips".to_string(),
        csv_path("berlinmod.trips", "./data/berlin_mod/trips"),
    );
    let query_periods_csv = (
        "periods".to_string(),
        csv_path("berlinmod.periods", "./data/berlin_mod/query_periods"),
    );
    let query_regions_csv = (
        "regions".to_string(),
        csv_path("berlinmod.regions", "./data/berlin_mod/query_regions"),
    );
    let query_points_csv = (
        "points".to_string(),
        csv_path("berlinmod.points", "./data/berlin_mod/query_points"),
    );
    let instants_csv = (
        "instants".to_string(),
        csv_path("berlinmod.instants", "./data/berlin_mod/query_instants"),
    );
    let datamcar_csv = (
        "datamcar".to_string(),
        csv_path("berlinmod.datamcar", "./data/berlin_mod/datamcar"),
    );
    let licences_csv = (
        "licences".to_string(),
        csv_path("berlinmod.licences", "./data/berlin_mod/query_licences"),
    );

    let porto_taxi_csv = (
        "porto_taxi".to_string(),
        csv_path("porto_taxi", "./data/porto_taxi"),
    );

    let oystercatcher_belgium_csv = (
        "oystercatcher_belgium".to_string(),
        csv_path(
            "oystercatcher_belgium",
            "./data/oystercatcher_belgium/sample_oystercatcher.csv",
        ),
    );

    let custom_catalog: Arc<dyn CatalogProvider> =
        Arc::new(catalog_provider::CsvCatalogProvider::new(
            vec![
                trips_csv,
                query_periods_csv,
                query_regions_csv,
                query_points_csv,
                datamcar_csv,
                instants_csv,
                licences_csv,
            ],
            vec![porto_taxi_csv, oystercatcher_belgium_csv],
        ));
    DATAFUSION_CTX.register_catalog("csv", custom_catalog);
}

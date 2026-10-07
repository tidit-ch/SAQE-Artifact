use datafusion::{
    catalog::{SchemaProvider, TableProvider},
    error::Result,
};
use std::{
    any::Any,
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, RwLock},
};

#[derive(Debug, Clone)]
pub struct BerlinModInfluxSchemaProvider {
    table_info_list: Vec<String>, // Stores the [(table_name), ...]
    influx_options: HashMap<String, String>,
    registered_tables: Arc<RwLock<HashMap<String, Arc<dyn TableProvider + Send + Sync>>>>,
}

impl BerlinModInfluxSchemaProvider {
    pub fn new(table_info_list: Vec<String>, influx_options: HashMap<String, String>) -> Self {
        return Self {
            table_info_list,
            influx_options,
            registered_tables: Arc::new(RwLock::new(HashMap::new())),
        };
    }
}

impl SchemaProvider for BerlinModInfluxSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let mut names = self.table_info_list.clone();
        let reg = self.registered_tables.read().unwrap();
        for k in reg.keys() {
            if !names.contains(k) {
                names.push(k.clone());
            }
        }
        names
    }

    fn table_exist(&self, name: &str) -> bool {
        self.table_info_list.iter().any(|t| t == name)
            || self.registered_tables.read().unwrap().contains_key(name)
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        if self.table_exist(name.as_str()) {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "Table '{}' already exists",
                name
            )));
        }

        Ok(self
            .registered_tables
            .write()
            .unwrap()
            .insert(name, table)
            .map(|tbl| tbl as Arc<dyn TableProvider>))
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        Ok(self
            .registered_tables
            .write()
            .unwrap()
            .remove(name)
            .map(|tbl| tbl as Arc<dyn TableProvider>))
    }

    fn table<'life0, 'life1, 'async_trait>(
        &'life0 self,
        name: &'life1 str,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<Arc<dyn TableProvider>>>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        // Check registered tables first
        if let Some(tbl) = self.registered_tables.read().unwrap().get(name) {
            let tbl: Arc<dyn TableProvider> = tbl.clone();
            return Box::pin(async move { Ok(Some(tbl)) });
        }

        if self.table_exist(name) {
            match name {
                "trips" => {
                    let provider = crate::core::influx::berlin_mod_influx::trips::table_provider::BerlinModTripsTableProvider::new(
                        self.influx_options.clone(),
                        crate::core::influx::berlin_mod_influx::trips::schema::new_get_schema(),
                        name.to_owned(),
                    );
                    let generic_table_provider: Arc<dyn TableProvider> = Arc::new(provider);
                    return Box::pin(async { Ok(Some(generic_table_provider)) });
                }
                "datamcar" => {
                    let provider = crate::core::influx::berlin_mod_influx::datamcar::table_provider::BerlinModDatamcarTableProvider::new(
                        self.influx_options.clone(),
                        crate::core::influx::berlin_mod_influx::datamcar::schema::new_get_schema(),
                        name.to_owned(),
                    );
                    let generic_table_provider: Arc<dyn TableProvider> = Arc::new(provider);
                    return Box::pin(async { Ok(Some(generic_table_provider)) });
                }
                "instants" => {
                    let provider = crate::core::influx::berlin_mod_influx::query_instants::table_provider::BerlinModQueryInstantsTableProvider::new(
                        self.influx_options.clone(),
                        crate::core::influx::berlin_mod_influx::query_instants::schema::new_get_schema(),
                        name.to_owned(),
                    );
                    let generic_table_provider: Arc<dyn TableProvider> = Arc::new(provider);
                    return Box::pin(async { Ok(Some(generic_table_provider)) });
                }
                "periods" => {
                    let provider = crate::core::influx::berlin_mod_influx::query_periods::table_provider::BerlinModQueryPeriodsTableProvider::new(
                        self.influx_options.clone(),
                        crate::core::influx::berlin_mod_influx::query_periods::schema::new_get_schema(),
                        name.to_owned(),
                    );
                    let generic_table_provider: Arc<dyn TableProvider> = Arc::new(provider);
                    return Box::pin(async { Ok(Some(generic_table_provider)) });
                }
                "licences" => {
                    let provider = crate::core::influx::berlin_mod_influx::query_licences::table_provider::BerlinModQueryLicencesTableProvider::new(
                        self.influx_options.clone(),
                        crate::core::influx::berlin_mod_influx::query_licences::schema::new_get_schema(),
                        name.to_owned(),
                    );
                    let generic_table_provider: Arc<dyn TableProvider> = Arc::new(provider);
                    return Box::pin(async { Ok(Some(generic_table_provider)) });
                }
                "points" => {
                    let provider = crate::core::influx::berlin_mod_influx::query_points::table_provider::BerlinModQueryPointsTableProvider::new(
                        self.influx_options.clone(),
                        crate::core::influx::berlin_mod_influx::query_points::schema::new_get_schema(),
                        name.to_owned(),
                    );
                    let generic_table_provider: Arc<dyn TableProvider> = Arc::new(provider);
                    return Box::pin(async { Ok(Some(generic_table_provider)) });
                }
                "regions" => {
                    let provider = crate::core::influx::berlin_mod_influx::query_regions::table_provider::BerlinModQueryRegionsTableProvider::new(
                        self.influx_options.clone(),
                        crate::core::influx::berlin_mod_influx::query_regions::schema::new_get_schema(),
                        name.to_owned(),
                    );
                    let generic_table_provider: Arc<dyn TableProvider> = Arc::new(provider);
                    return Box::pin(async { Ok(Some(generic_table_provider)) });
                }
                _ => {
                    return Box::pin(async {
                        Err(datafusion::error::DataFusionError::Plan(format!(
                            "Table '{}' does not exist",
                            name.to_string()
                        )))
                    });
                }
            }
        } else {
            let name_cloned = name.to_string();
            return Box::pin(async move {
                Err(datafusion::error::DataFusionError::Plan(format!(
                    "Table '{}' does not exist",
                    name_cloned
                )))
            });
        };
    }
}

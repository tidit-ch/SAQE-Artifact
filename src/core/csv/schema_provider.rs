use datafusion::{
    catalog::{SchemaProvider, TableProvider},
    error::Result,
};
use std::collections::HashMap;
use std::{
    any::Any,
    future::Future,
    pin::Pin,
    sync::{Arc, RwLock},
};

use crate::core::csv::{oystercatcher_belgium, porto_taxi};

/// Schema provider for the BerlinMod CSV tables
#[derive(Debug, Clone)]
pub struct BerlinModCsvSchemaProvider {
    table_info_list: Vec<(String, String)>, // Stores the [(file_name, path_to_file), ...]
    registered_tables: Arc<RwLock<HashMap<String, Arc<dyn TableProvider + Send + Sync>>>>,
}

impl BerlinModCsvSchemaProvider {
    pub fn new(table_info_list: Vec<(String, String)>) -> Self {
        return Self {
            table_info_list,
            registered_tables: Arc::new(RwLock::new(HashMap::new())),
        };
    }

    fn find_info_for_table(&self, table_name: String) -> Option<(String, String)> {
        for table_info in &self.table_info_list {
            if table_info.0 == table_name {
                return Some((table_info.0.clone(), table_info.1.clone()));
            }
        }
        return None;
    }
}

impl SchemaProvider for BerlinModCsvSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let mut names = self.table_info_list.clone();
        let reg = self.registered_tables.read().unwrap();
        for k in reg.keys() {
            if !names.iter().any(|(table_name, _)| table_name == k) {
                names.push((k.clone(), String::new()));
            }
        }
        names
            .into_iter()
            .map(|(table_name, _)| table_name)
            .collect()
    }

    fn table_exist(&self, name: &str) -> bool {
        self.table_info_list
            .iter()
            .any(|(table_name, _)| table_name == name)
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
            let table_info = self.find_info_for_table(name.to_string()).unwrap();
            match name {
                "trips" => {
                    let provider = crate::core::csv::berlin_mod_csv::trips::table_provider::BerlinModTripsTableProvider::new(
                        table_info.1.as_str(),
                        crate::core::csv::berlin_mod_csv::trips::schema::new_get_schema(),
                    );
                    let generic_table_provider: Arc<dyn TableProvider> = Arc::new(provider);
                    return Box::pin(async { Ok(Some(generic_table_provider)) });
                }
                "points" => {
                    let provider = crate::core::csv::berlin_mod_csv::query_points::table_provider::BerlinModQueryPointsTableProvider::new(
                        table_info.1.as_str(),
                        crate::core::csv::berlin_mod_csv::query_points::schema::new_get_schema(),
                    );
                    let generic_table_provider: Arc<dyn TableProvider> = Arc::new(provider);
                    return Box::pin(async { Ok(Some(generic_table_provider)) });
                }
                "datamcar" => {
                    let provider = crate::core::csv::berlin_mod_csv::datamcar::table_provider::BerlinModDatamcarTableProvider::new(
                        table_info.1.as_str(),
                        crate::core::csv::berlin_mod_csv::datamcar::schema::new_get_schema(),
                    );
                    let generic_table_provider: Arc<dyn TableProvider> = Arc::new(provider);
                    return Box::pin(async { Ok(Some(generic_table_provider)) });
                }
                "instants" => {
                    let provider = crate::core::csv::berlin_mod_csv::query_instants::table_provider::BerlinModQueryInstantTableProvider::new(
                        table_info.1.as_str(),
                        crate::core::csv::berlin_mod_csv::query_instants::schema::new_get_schema(),
                    );
                    let generic_table_provider: Arc<dyn TableProvider> = Arc::new(provider);
                    return Box::pin(async { Ok(Some(generic_table_provider)) });
                }
                "periods" => {
                    let provider = crate::core::csv::berlin_mod_csv::query_periods::table_provider::BerlinModQueryPeriodsTableProvider::new(
                        table_info.1.as_str(),
                        crate::core::csv::berlin_mod_csv::query_periods::schema::new_get_schema(),
                    );
                    let generic_table_provider: Arc<dyn TableProvider> = Arc::new(provider);
                    return Box::pin(async { Ok(Some(generic_table_provider)) });
                }
                "licences" => {
                    let provider = crate::core::csv::berlin_mod_csv::query_licences::table_provider::BerlinModQueryLicencesTableProvider::new(
                        table_info.1.as_str(),
                        crate::core::csv::berlin_mod_csv::query_licences::schema::new_get_schema(),
                    );
                    let generic_table_provider: Arc<dyn TableProvider> = Arc::new(provider);
                    return Box::pin(async { Ok(Some(generic_table_provider)) });
                }
                "regions" => {
                    let provider = crate::core::csv::berlin_mod_csv::query_regions::table_provider::BerlinModQueryRegionsTableProvider::new(
                        table_info.1.as_str(),
                        crate::core::csv::berlin_mod_csv::query_regions::schema::new_get_schema(),
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

/// Schema provider for all other tables
#[derive(Debug, Clone)]
pub struct CsvSchemaProvider {
    table_info_list: Vec<(String, String)>, // Stores the [(file_name, path_to_file), ...]
}

impl CsvSchemaProvider {
    pub fn new(table_info_list: Vec<(String, String)>) -> Self {
        return Self { table_info_list };
    }

    fn find_info_for_table(&self, table_name: String) -> Option<(String, String)> {
        for table_info in &self.table_info_list {
            if table_info.0 == table_name {
                return Some((table_info.0.clone(), table_info.1.clone()));
            }
        }
        return None;
    }
}

impl SchemaProvider for CsvSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let mut table_names_list: Vec<String> = Vec::new();
        self.table_info_list
            .iter()
            .for_each(|v: &(String, String)| {
                table_names_list.push(v.0.clone());
            });
        table_names_list
    }

    fn table_exist(&self, name: &str) -> bool {
        for (table_name, _) in &self.table_info_list {
            if table_name == name {
                return true;
            }
        }
        return false;
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
        if self.table_exist(name) {
            let (table_name, table_file_path) = self.find_info_for_table(name.to_string()).unwrap();
            match name {
                "porto_taxi" => {
                    let table_provider =
                        porto_taxi::get_table_provider(table_name.clone(), table_file_path.clone());
                    return Box::pin(async move { Ok(Some(table_provider)) });
                }
                "oystercatcher_belgium" => {
                    let table_provider = oystercatcher_belgium::get_table_provider(
                        table_name.clone(),
                        table_file_path.clone(),
                    );
                    return Box::pin(async move { Ok(Some(table_provider)) });
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

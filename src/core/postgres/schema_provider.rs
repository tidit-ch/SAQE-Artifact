use datafusion::{
    catalog::{SchemaProvider, TableProvider},
    error::Result,
};
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use std::{
    any::Any,
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, RwLock},
};

#[derive(Debug, Clone)]
pub struct BerlinModPostgresSchemaProvider {
    table_info_list: Vec<String>, // Stores the [(table_name), ...]
    postgres_pool: Arc<PostgresConnectionPool>,
    registered_tables: Arc<RwLock<HashMap<String, Arc<dyn TableProvider + Send + Sync>>>>,
}

impl BerlinModPostgresSchemaProvider {
    pub fn new(table_info_list: Vec<String>, postgres_pool: Arc<PostgresConnectionPool>) -> Self {
        return Self {
            table_info_list,
            postgres_pool,
            registered_tables: Arc::new(RwLock::new(HashMap::new())),
        };
    }
}

impl SchemaProvider for BerlinModPostgresSchemaProvider {
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
                    let provider = crate::core::postgres::berlin_mod_postgres::trips::table_provider::BerlinModTripsTableProvider::new(
                        self.postgres_pool.clone(),
                        crate::core::postgres::berlin_mod_postgres::trips::schema::new_get_schema(),
                        name.to_owned(),
                    );
                    let generic_table_provider: Arc<dyn TableProvider> = Arc::new(provider);
                    return Box::pin(async { Ok(Some(generic_table_provider)) });
                }
                // *1/*2 sample views (see config/init.sql: `CREATE VIEW
                // points1 AS SELECT * FROM points LIMIT 10`, etc. - already
                // exist natively in Postgres, always in sync with the base
                // table, no separate load step). Same schema/provider as
                // the base table, just a different underlying relation
                // name - the constructor already takes that name as its own
                // parameter (used directly in its scan SQL), so grouping
                // them into the same arm is exact, not approximate.
                "points" | "points1" => {
                    let provider = crate::core::postgres::berlin_mod_postgres::query_points::table_provider::BerlinModQueryPointsTableProvider::new(
                        self.postgres_pool.clone(),
                        crate::core::postgres::berlin_mod_postgres::query_points::schema::new_get_schema(),
                        name.to_owned(),
                    );
                    let generic_table_provider: Arc<dyn TableProvider> = Arc::new(provider);
                    return Box::pin(async { Ok(Some(generic_table_provider)) });
                }
                "datamcar" => {
                    let provider = crate::core::postgres::berlin_mod_postgres::datamcar::table_provider::BerlinModDatamcarTableProvider::new(
                        self.postgres_pool.clone(),
                        crate::core::postgres::berlin_mod_postgres::datamcar::schema::new_get_schema(),
                        name.to_owned(),
                    );
                    let generic_table_provider: Arc<dyn TableProvider> = Arc::new(provider);
                    return Box::pin(async { Ok(Some(generic_table_provider)) });
                }
                "regions" | "regions1" => {
                    let provider = crate::core::postgres::berlin_mod_postgres::query_regions::table_provider::BerlinModQueryRegionsTableProvider::new(
                        self.postgres_pool.clone(),
                        crate::core::postgres::berlin_mod_postgres::query_regions::schema::new_get_schema(),
                        name.to_owned(),
                    );
                    let generic_table_provider: Arc<dyn TableProvider> = Arc::new(provider);
                    return Box::pin(async { Ok(Some(generic_table_provider)) });
                }
                "instants" | "instants1" => {
                    let provider = crate::core::postgres::berlin_mod_postgres::query_instants::table_provider::BerlinModQueryInstantsTableProvider::new(
                        self.postgres_pool.clone(),
                        crate::core::postgres::berlin_mod_postgres::query_instants::schema::new_get_schema(),
                        name.to_owned(),
                    );
                    let generic_table_provider: Arc<dyn TableProvider> = Arc::new(provider);
                    return Box::pin(async { Ok(Some(generic_table_provider)) });
                }
                "periods" | "periods1" => {
                    let provider = crate::core::postgres::berlin_mod_postgres::query_periods::table_provider::BerlinModQueryPeriodsTableProvider::new(
                        self.postgres_pool.clone(),
                        crate::core::postgres::berlin_mod_postgres::query_periods::schema::new_get_schema(),
                        name.to_owned(),
                    );
                    let generic_table_provider: Arc<dyn TableProvider> = Arc::new(provider);
                    return Box::pin(async { Ok(Some(generic_table_provider)) });
                }
                "licences" | "licences1" | "licences2" => {
                    let provider = crate::core::postgres::berlin_mod_postgres::query_licences::table_provider::BerlinModQueryLicencesTableProvider::new(
                        self.postgres_pool.clone(),
                        crate::core::postgres::berlin_mod_postgres::query_licences::schema::new_get_schema(),
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

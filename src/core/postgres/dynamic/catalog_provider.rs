//! A `CatalogProvider` that exposes every Postgres schema as a DataFusion schema.

use super::schema_provider::DynamicPostgresSchemaProvider;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use std::{
    any::Any,
    collections::HashMap,
    sync::{Arc, RwLock},
};

#[derive(Debug)]
pub struct DynamicPostgresCatalogProvider {
    pool: Arc<PostgresConnectionPool>,
    schemas: RwLock<HashMap<String, Arc<DynamicPostgresSchemaProvider>>>,
    /// Schema names discovered at start-up, for `schema_names()` / `SHOW SCHEMAS`.
    known_schemas: RwLock<Vec<String>>,
}

impl DynamicPostgresCatalogProvider {
    pub fn new(pool: Arc<PostgresConnectionPool>) -> Self {
        Self {
            pool,
            schemas: RwLock::new(HashMap::new()),
            known_schemas: RwLock::new(Vec::new()),
        }
    }

    /// Records the schema list and pre-warms each schema's table list.
    pub async fn refresh(&self, schema_names: Vec<String>) {
        *self.known_schemas.write().unwrap() = schema_names.clone();
        for name in schema_names {
            let schema = self.get_or_create(&name);
            // A schema we cannot list is not fatal: tables in it still resolve by name.
            let _ = schema.refresh().await;
        }
    }

    fn get_or_create(&self, name: &str) -> Arc<DynamicPostgresSchemaProvider> {
        if let Some(existing) = self.schemas.read().unwrap().get(name) {
            return existing.clone();
        }
        let mut schemas = self.schemas.write().unwrap();
        schemas
            .entry(name.to_string())
            .or_insert_with(|| {
                Arc::new(DynamicPostgresSchemaProvider::new(
                    name.to_string(),
                    self.pool.clone(),
                ))
            })
            .clone()
    }
}

impl CatalogProvider for DynamicPostgresCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.known_schemas.read().unwrap().clone()
    }

    /// Builds a provider for any requested name. `schema()` is synchronous so it cannot ask
    /// the database whether the schema exists; an unknown name simply yields a schema whose
    /// table lookups all miss, which surfaces as a normal "table not found".
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        Some(self.get_or_create(name) as Arc<dyn SchemaProvider>)
    }
}

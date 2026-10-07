use crate::core::postgres::schema_provider::BerlinModPostgresSchemaProvider;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;

use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct PostgresCatalogProvider {
    berlinmod_schema: Arc<BerlinModPostgresSchemaProvider>,
}

impl PostgresCatalogProvider {
    pub fn new(table_info_list: Vec<String>, postgres_pool: Arc<PostgresConnectionPool>) -> Self {
        let berlinmod_schema = Arc::new(BerlinModPostgresSchemaProvider::new(
            table_info_list,
            postgres_pool,
        ));
        Self { berlinmod_schema }
    }
}

impl CatalogProvider for PostgresCatalogProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec!["berlinmod".to_string()]
    }

    fn schema(&self, name: &str) -> Option<std::sync::Arc<dyn SchemaProvider>> {
        match name {
            "berlinmod" => Some(self.berlinmod_schema.clone()),
            _ => None,
        }
    }
}

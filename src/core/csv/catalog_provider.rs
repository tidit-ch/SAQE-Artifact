use crate::core::csv::schema_provider::{BerlinModCsvSchemaProvider, CsvSchemaProvider};
use datafusion::catalog::{CatalogProvider, SchemaProvider};

use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct CsvCatalogProvider {
    berlinmod_schema: Arc<BerlinModCsvSchemaProvider>,
    public_schema: Arc<CsvSchemaProvider>,
}

impl CsvCatalogProvider {
    pub fn new(
        berlinmod_table_list: Vec<(String, String)>,
        public_table_list: Vec<(String, String)>,
    ) -> Self {
        let berlinmod_schema = Arc::new(BerlinModCsvSchemaProvider::new(
            berlinmod_table_list.clone(),
        ));
        let public_schema = Arc::new(CsvSchemaProvider::new(public_table_list));
        Self {
            berlinmod_schema,
            public_schema,
        }
    }
}

impl CatalogProvider for CsvCatalogProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec!["berlinmod".to_string(), "public".to_string()]
    }

    fn schema(&self, name: &str) -> Option<std::sync::Arc<dyn SchemaProvider>> {
        match name {
            "berlinmod" => Some(self.berlinmod_schema.clone()),
            "public" => Some(self.public_schema.clone()),
            _ => None,
        }
    }
}

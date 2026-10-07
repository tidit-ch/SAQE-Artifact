use crate::core::influx::schema_provider::BerlinModInfluxSchemaProvider;
use datafusion::catalog::{CatalogProvider, SchemaProvider};

use std::{collections::HashMap, sync::Arc};

#[derive(Debug, Clone)]
pub struct InfluxCatalogProvider {
    berlinmod_schema: Arc<BerlinModInfluxSchemaProvider>,
}

impl InfluxCatalogProvider {
    pub fn new(table_info_list: Vec<String>, influx_options: HashMap<String, String>) -> Self {
        let berlinmod_schema = Arc::new(BerlinModInfluxSchemaProvider::new(
            table_info_list,
            influx_options,
        ));
        Self { berlinmod_schema }
    }
}

impl CatalogProvider for InfluxCatalogProvider {
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

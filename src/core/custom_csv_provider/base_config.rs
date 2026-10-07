use crate::core::utils::common::list_data_files;
use datafusion::{
    arrow::datatypes::SchemaRef,
    datasource::TableType,
    physical_expr::EquivalenceProperties,
    physical_plan::{PlanProperties, Statistics},
};
use std::sync::Arc;

/// CsvBaseConfig struct helps in easily implementing CsvBaseBehavior trait for different CSV table providers.
/// It holds common configuration parameters required for CSV table providers.
#[derive(Debug, Clone)]
pub struct CsvBaseConfig {
    pub table_name: String,
    pub has_headers: bool,
    pub file_path: String,
    pub file_list: Vec<String>,
    pub schema: SchemaRef,
    pub table_type: TableType,
    pub execution_plan_name: String,
    pub execution_plan_properties: PlanProperties,
    pub execution_plan_statistics: Statistics,
    pub execution_plan_batch_size: usize,
}

impl CsvBaseConfig {
    pub fn new(
        table_name: String,
        has_headers: bool,
        file_path: String,
        file_list: Vec<String>,
        schema: SchemaRef,
        table_type: TableType,
        execution_plan_name: String,
        execution_plan_properties: PlanProperties,
        execution_plan_statistics: Statistics,
        execution_plan_batch_size: usize,
    ) -> Self {
        Self {
            table_name,
            has_headers,
            file_path,
            file_list,
            schema,
            table_type,
            execution_plan_name,
            execution_plan_properties,
            execution_plan_statistics,
            execution_plan_batch_size,
        }
    }

    #[allow(dead_code)]
    fn get_schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

pub struct CsvBaseConfigBuilder {
    table_name: String,
    has_headers: bool,
    file_path: String,
    schema: SchemaRef,
    table_type: TableType,
    execution_plan_name: String,
    execution_plan_properties: PlanProperties,
    execution_plan_statistics: Statistics,
    execution_plan_batch_size: usize,
}

impl CsvBaseConfigBuilder {
    pub fn new() -> Self {
        let empty_schema = Arc::new(datafusion::arrow::datatypes::Schema::empty());
        Self {
            table_name: String::new(),
            has_headers: false,
            file_path: String::new(),
            schema: empty_schema.clone(),
            table_type: TableType::Base,
            execution_plan_name: String::new(),
            execution_plan_properties: PlanProperties::new(
                EquivalenceProperties::new(empty_schema.clone()),
                datafusion::physical_plan::Partitioning::UnknownPartitioning(0),
                datafusion::physical_plan::execution_plan::EmissionType::Both,
                datafusion::physical_plan::execution_plan::Boundedness::Bounded,
            ),
            execution_plan_statistics: Statistics {
                num_rows: datafusion::common::stats::Precision::Exact(0),
                total_byte_size: datafusion::common::stats::Precision::Exact(0),
                column_statistics: datafusion::physical_plan::Statistics::unknown_column(
                    &empty_schema.clone(),
                ),
            },
            execution_plan_batch_size: 1024,
        }
    }

    pub fn build(self) -> CsvBaseConfig {
        // A file path yields that file, a directory the files inside it.
        let file_list = list_data_files(&self.file_path).unwrap_or_else(|e| {
            panic!("[CsvBaseConfigBuilder](build) {e}");
        });
        CsvBaseConfig {
            table_name: self.table_name,
            has_headers: self.has_headers,
            file_path: self.file_path,
            file_list,
            schema: self.schema,
            table_type: self.table_type,
            execution_plan_name: self.execution_plan_name,
            execution_plan_properties: self.execution_plan_properties,
            execution_plan_statistics: self.execution_plan_statistics,
            execution_plan_batch_size: self.execution_plan_batch_size,
        }
    }

    pub fn with_table_name(mut self, table_name: String) -> Self {
        self.table_name = table_name;
        self
    }

    pub fn with_has_headers(mut self, has_headers: bool) -> Self {
        self.has_headers = has_headers;
        self
    }

    pub fn with_file_path(mut self, file_path: String) -> Self {
        self.file_path = file_path;
        self
    }

    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = schema;
        self
    }

    pub fn with_table_type(mut self, table_type: TableType) -> Self {
        self.table_type = table_type;
        self
    }

    pub fn with_execution_plan_name(mut self, execution_plan_name: String) -> Self {
        self.execution_plan_name = execution_plan_name;
        self
    }

    pub fn with_execution_plan_properties(
        mut self,
        execution_plan_properties: PlanProperties,
    ) -> Self {
        self.execution_plan_properties = execution_plan_properties;
        self
    }

    pub fn with_execution_plan_statistics(mut self, execution_plan_statistics: Statistics) -> Self {
        self.execution_plan_statistics = execution_plan_statistics;
        self
    }

    pub fn with_execution_plan_batch_size(mut self, execution_plan_batch_size: usize) -> Self {
        self.execution_plan_batch_size = execution_plan_batch_size;
        self
    }
}

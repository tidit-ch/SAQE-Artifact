use arrow_schema::extension::{ExtensionType, EXTENSION_TYPE_NAME_KEY};
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema, SchemaRef},
    catalog::TableProvider,
    common::{stats::Precision, Result as DataFusionResult},
    datasource::TableType,
    error::DataFusionError,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        execution_plan::{Boundedness, EmissionType},
        Partitioning, PlanProperties, Statistics,
    },
};
use geoarrow_schema::LineStringType;
use std::{fs::File, io::BufReader, sync::Arc};

use crate::core::custom_csv_provider::table_provider::CustomCsvTableProvider;
use crate::core::utils::common::list_data_files;
use crate::core::utils::common::{build_batch, get_builders_for_schema};
use crate::core::{
    custom_csv_provider::{
        base_behavior::CsvBaseBehavior,
        base_config::{CsvBaseConfig, CsvBaseConfigBuilder},
    },
    DATAFUSION_BATCH_SIZE,
};

use crate::core::csv::porto_taxi::parser::TaxiTrajectoryData;
use crate::core::utils::schema::TRAJECTORY_DATATYPE;

pub mod parser;

#[derive(Debug, Clone)]
pub struct TaxiTrajectoryTable {
    pub config: CsvBaseConfig,
}

impl TaxiTrajectoryTable {
    pub fn new(config: CsvBaseConfig) -> Self {
        Self { config }
    }
}

impl CsvBaseBehavior for TaxiTrajectoryTable {
    fn get_table_name(&self) -> String {
        self.config.table_name.clone()
    }

    fn get_schema(&self) -> SchemaRef {
        self.config.schema.clone()
    }

    fn get_table_type(&self) -> TableType {
        self.config.table_type
    }

    fn get_execution_plan_name(&self) -> &str {
        &self.config.execution_plan_name
    }

    fn get_execution_plan_properties(&self) -> &PlanProperties {
        &self.config.execution_plan_properties
    }

    fn get_execution_plan_statistics(&self) -> Statistics {
        let statistics = self.config.execution_plan_statistics.clone();
        statistics
    }

    fn get_execution_plan_batch_size(&self) -> usize {
        self.config.execution_plan_batch_size
    }

    fn get_csv_reader(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> DataFusionResult<csv::Reader<BufReader<File>>> {
        let file = File::open(self.config.file_list[partition].clone())?;
        let reader = BufReader::new(file);

        let csv_reader: csv::Reader<BufReader<File>> = csv::ReaderBuilder::new()
            .has_headers(self.config.has_headers) // Adjust as needed
            .delimiter(b',') // Adjust as needed
            .from_reader(reader);
        Ok(csv_reader)
    }

    fn get_record_batch_from_csv_reader(
        &self,
        csv_reader: &mut csv::Reader<BufReader<File>>,
        projection: Arc<Vec<usize>>,
        projected_schema: SchemaRef,
    ) -> datafusion::error::Result<arrow::array::RecordBatch> {
        let mut rows_processed_in_batch = 0;
        let batch_size = self.get_execution_plan_batch_size();
        let builders = get_builders_for_schema(projected_schema.clone(), batch_size);

        let mut taxi_data_record = TaxiTrajectoryData::new(projection, builders);

        for result in csv_reader.records() {
            let mut record: csv::StringRecord = match result {
                Ok(record) => record,
                Err(e) => {
                    return Err(DataFusionError::Execution(format!(
                        "CSV parsing error: {}",
                        e
                    )));
                }
            };
            record.trim();
            taxi_data_record
                .add_to_builder_from_record(&record)
                .map_err(|e| DataFusionError::Execution(format!("CSV record error: {}", e)))?;
            rows_processed_in_batch += 1;
            if rows_processed_in_batch >= batch_size {
                let batch = build_batch(&mut taxi_data_record.builders, projected_schema.clone());
                return batch;
            }
        }
        return build_batch(&mut taxi_data_record.builders, projected_schema.clone());
    }
}

pub fn get_table_provider(table_name: String, file_path: String) -> Arc<dyn TableProvider> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("trip_id", DataType::Int64, false),
        Field::new("call_type", DataType::Utf8, false),
        Field::new("origin_call", DataType::Int64, true),
        Field::new("origin_stand", DataType::Int64, true),
        Field::new("taxi_id", DataType::Int64, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("day_type", DataType::Utf8, false),
        Field::new("missing_data", DataType::Boolean, false),
        Field::new("polyline", TRAJECTORY_DATATYPE.clone(), false).with_metadata(
            [(
                EXTENSION_TYPE_NAME_KEY.to_owned(),
                LineStringType::NAME.to_owned(),
            )]
            .into_iter()
            .collect(),
        ),
    ]));
    // A file path yields that file, a directory the files inside it.
    let file_list = list_data_files(&file_path)
        .unwrap_or_else(|e| panic!("[PortoTaxi](create_table_provider) {e}"));
    let plan_properties = PlanProperties::new(
        EquivalenceProperties::new(schema.clone()),
        Partitioning::RoundRobinBatch(file_list.len()),
        EmissionType::Incremental,
        Boundedness::Bounded,
    );
    let statistics = Statistics {
        num_rows: datafusion::common::stats::Precision::Exact(1710670),
        total_byte_size: Precision::Inexact(1932735283),
        column_statistics: Statistics::unknown_column(&schema.clone()),
    };

    let config = CsvBaseConfigBuilder::new()
        .with_table_name(table_name.clone())
        .with_has_headers(true)
        .with_file_path(file_path)
        .with_schema(schema)
        .with_table_type(TableType::Base)
        .with_execution_plan_name(format!("{}_execution_plan", table_name.clone()))
        .with_execution_plan_properties(plan_properties)
        .with_execution_plan_statistics(statistics)
        .with_execution_plan_batch_size(*DATAFUSION_BATCH_SIZE)
        .build();

    Arc::new(CustomCsvTableProvider::new(TaxiTrajectoryTable::new(
        config,
    )))
}

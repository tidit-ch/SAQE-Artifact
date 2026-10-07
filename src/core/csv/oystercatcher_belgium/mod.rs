use arrow_schema::extension::{ExtensionType, EXTENSION_TYPE_NAME_KEY};
use datafusion::{
    arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef},
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

use crate::core::{
    custom_csv_provider::{
        base_behavior::CsvBaseBehavior,
        base_config::{CsvBaseConfig, CsvBaseConfigBuilder},
        table_provider::CustomCsvTableProvider,
    },
    utils::{
        common::{build_batch, get_builders_for_schema},
        schema::TRAJECTORY_DATATYPE,
    },
    DATAFUSION_BATCH_SIZE,
};

pub mod event;
pub mod record;
pub mod utils;

use crate::core::csv::oystercatcher_belgium::record::OystercatcherRecord;

#[derive(Debug, Clone)]
pub struct OystercatcherBelgiumTable {
    pub config: CsvBaseConfig,
}

impl OystercatcherBelgiumTable {
    pub fn new(config: CsvBaseConfig) -> Self {
        Self { config }
    }
}

impl CsvBaseBehavior for OystercatcherBelgiumTable {
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
        self.config.execution_plan_statistics.clone()
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
        let mut builders = get_builders_for_schema(projected_schema.clone(), batch_size);

        let mut parsed_record: Option<OystercatcherRecord> = None;
        for result in csv_reader.records() {
            let record = result.map_err(|e| {
                DataFusionError::Execution(format!("Failed to read record from CSV: {}", e))
            })?;

            if parsed_record.is_none() {
                parsed_record =
                    Some(OystercatcherRecord::from_csv(record.clone()).map_err(|e| {
                        DataFusionError::Execution(format!("Failed to parse record: {}", e))
                    })?);
            } else if parsed_record.is_some() {
                let unwrapped_record = parsed_record.as_mut().unwrap();
                let record_belongs_to_same_individual = unwrapped_record
                    .belongs_to_same_individual(record.clone())
                    .map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to check if record belongs to same individual: {}",
                            e
                        ))
                    })?;
                if record_belongs_to_same_individual {
                    unwrapped_record.append_event(record.clone()).map_err(|e| {
                        DataFusionError::Execution(format!("Failed to append event: {}", e))
                    })?;
                } else {
                    // If the record belongs to a different individual, add this record to builders
                    unwrapped_record
                        .add_data_to_builders(&mut builders, projection.as_ref())
                        .map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Failed to add data to builders: {}",
                                e
                            ))
                        })?;
                    rows_processed_in_batch += 1;
                    if rows_processed_in_batch >= batch_size {
                        let batch = build_batch(&mut builders, projected_schema.clone());
                        return batch;
                    }
                    parsed_record = Some(OystercatcherRecord::from_csv(record).map_err(|e| {
                        DataFusionError::Execution(format!("Failed to parse record: {}", e))
                    })?);
                }
            }
        }
        return build_batch(&mut builders, projected_schema.clone());
    }
}

pub fn get_table_provider(table_name: String, file_path: String) -> Arc<dyn TableProvider> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("polyline", TRAJECTORY_DATATYPE.clone(), false).with_metadata(
            [(
                EXTENSION_TYPE_NAME_KEY.to_owned(),
                LineStringType::NAME.to_owned(),
            )]
            .into_iter()
            .collect(),
        ),
        Field::new("individual_taxon_canonical_name", DataType::Utf8, true),
        Field::new("tag_local_identifier", DataType::Utf8, true),
        Field::new("individual_local_identifier", DataType::Utf8, true),
        Field::new("study_name", DataType::Utf8, true),
        Field::new(
            "event_list",
            DataType::List(Arc::new(Field::new(
                "event",
                DataType::Struct(Fields::from(vec![
                    Field::new("event_id", DataType::Int64, false),
                    Field::new("location_long", DataType::Float64, true),
                    Field::new("location_lat", DataType::Float64, true),
                    Field::new("timestamp", DataType::Int64, true),
                    Field::new("visible", DataType::Boolean, false),
                    Field::new("acceleration_raw_x", DataType::Float64, true),
                    Field::new("acceleration_raw_y", DataType::Float64, true),
                    Field::new("acceleration_raw_z", DataType::Float64, true),
                    Field::new("bar_barometric_pressure", DataType::Float64, true),
                    Field::new("external_temperature", DataType::Float64, true),
                    Field::new("gps_dop", DataType::Float64, true),
                    Field::new("gps_satellite_count", DataType::Int64, true),
                    Field::new("gps_time_to_fix", DataType::Float64, true),
                    Field::new("ground_speed", DataType::Float64, true),
                    Field::new("heading", DataType::Float64, true),
                    Field::new("height_above_msl", DataType::Float64, true),
                    Field::new("location_error_numerical", DataType::Float64, true),
                    Field::new("manually_marked_outlier", DataType::Boolean, true),
                    Field::new("tilt_x", DataType::Float64, true),
                    Field::new("tilt_y", DataType::Float64, true),
                    Field::new("tilt_z", DataType::Float64, true),
                    Field::new("vertical_error_numerical", DataType::Float64, true),
                    Field::new("sensor_type", DataType::Utf8, true),
                ])),
                true,
            ))),
            true,
        ),
    ]));

    let mut file_list = Vec::new();
    file_list.push(file_path.clone());

    let plan_properties = PlanProperties::new(
        EquivalenceProperties::new(schema.clone()),
        Partitioning::RoundRobinBatch(file_list.len()),
        EmissionType::Incremental,
        Boundedness::Bounded,
    );

    let statistics = Statistics {
        num_rows: datafusion::common::stats::Precision::Inexact(200),
        total_byte_size: Precision::Inexact(1932735283),
        column_statistics: Statistics::unknown_column(&schema.clone()),
    };

    let config = CsvBaseConfigBuilder::new()
        .with_has_headers(true)
        .with_table_name(table_name.clone())
        .with_schema(schema.clone())
        .with_file_path(file_path.clone())
        .with_table_type(TableType::Base)
        .with_execution_plan_name(format!("{}_execution_plan", table_name.clone()))
        .with_execution_plan_properties(plan_properties)
        .with_execution_plan_statistics(statistics)
        .with_execution_plan_batch_size(*DATAFUSION_BATCH_SIZE)
        .build();

    Arc::new(CustomCsvTableProvider::new(OystercatcherBelgiumTable::new(
        config,
    )))
}

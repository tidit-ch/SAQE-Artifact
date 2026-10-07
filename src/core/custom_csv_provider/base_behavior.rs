use datafusion::{
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    common::Result as DataFusionResult,
    datasource::TableType,
    execution::context::TaskContext,
    physical_plan::{PlanProperties, Statistics},
};
use std::{fs::File, io::BufReader, sync::Arc};

/// CsvBaseBehavior trait defines the essential behavior required for CSV table providers.
/// Using this trait, we can easily implement a table provider and execution plan for different CSV datasets
pub trait CsvBaseBehavior {
    fn get_table_name(&self) -> String;
    fn get_schema(&self) -> SchemaRef;
    fn get_table_type(&self) -> TableType;
    fn get_execution_plan_name(&self) -> &str;
    fn get_execution_plan_properties(&self) -> &PlanProperties;
    fn get_execution_plan_statistics(&self) -> Statistics;
    fn get_execution_plan_batch_size(&self) -> usize;

    fn get_csv_reader(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<csv::Reader<BufReader<File>>>;

    fn get_record_batch_from_csv_reader(
        &self,
        csv_reader: &mut csv::Reader<BufReader<File>>,
        projection: Arc<Vec<usize>>,
        projected_schema: SchemaRef,
    ) -> DataFusionResult<RecordBatch>;
}

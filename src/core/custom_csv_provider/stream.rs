use datafusion::{
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    common::Result as DataFusionResult,
    execution::context::TaskContext,
};
use futures::stream::Stream;
use std::{fs::File, io::BufReader, pin::Pin, sync::Arc, task::Context, task::Poll};

use crate::core::custom_csv_provider::base_behavior::CsvBaseBehavior;

/// Adds the Stream implementation for reading CSV files based on the CsvBaseBehavior trait.
pub struct CustomCsvStream<T: CsvBaseBehavior> {
    pub source: Arc<T>,
    pub csv_reader: csv::Reader<BufReader<File>>,
    pub projection: Arc<Vec<usize>>,
    pub projected_schema: SchemaRef,
}

impl<T: CsvBaseBehavior> CustomCsvStream<T> {
    pub fn new(
        source: Arc<T>,
        partition: usize,
        context: Arc<TaskContext>,
        projection: Arc<Vec<usize>>,
        projected_schema: SchemaRef,
    ) -> Self {
        let csv_reader = source.get_csv_reader(partition, context);
        let csv_reader = match csv_reader {
            Ok(reader) => reader,
            Err(e) => panic!("[CustomCsvStream](new) Failed to create CSV reader: {}", e),
        };
        Self {
            source,
            csv_reader,
            projection,
            projected_schema,
        }
    }

    pub fn build_batch(&mut self) -> DataFusionResult<RecordBatch> {
        self.source.get_record_batch_from_csv_reader(
            &mut self.csv_reader,
            self.projection.clone(),
            self.projected_schema.clone(),
        )
    }
}

impl<T: CsvBaseBehavior> Stream for CustomCsvStream<T> {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        let batch_result = this.build_batch();
        match batch_result {
            Ok(batch) => {
                if batch.num_rows() == 0 {
                    return Poll::Ready(None);
                }
                Poll::Ready(Some(Ok(batch)))
            }
            Err(e) => Poll::Ready(Some(Err(e))),
        }
    }
}

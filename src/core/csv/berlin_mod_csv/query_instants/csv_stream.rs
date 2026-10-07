use datafusion::{
    arrow::{array::*, datatypes::SchemaRef},
    error::{DataFusionError, Result},
};
use futures::stream::Stream;
use std::{
    fs::File,
    io::BufReader,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::core::csv::berlin_mod_csv::query_instants::schema as BerlinModQueryInstantSchema;

pub struct BerlinModQueryInstantDataStream {
    schema: SchemaRef,
    csv_reader: csv::Reader<BufReader<File>>,
    batch_size: usize,
    builders: Vec<Box<dyn ArrayBuilder>>,
    finished: bool,
    unwrapped_projection: Vec<usize>,
}

impl BerlinModQueryInstantDataStream {
    pub fn try_new(
        path: String,
        schema: SchemaRef,
        batch_size: usize,
        projection: Option<Vec<usize>>,
        has_headers: bool,
    ) -> Result<Self> {
        let file = File::open(&path).map_err(|e| {
            DataFusionError::Execution(format!("Failed to open file {}: {}", path, e))
        })?;
        let reader = BufReader::new(file);

        let csv_reader: csv::Reader<BufReader<File>> = csv::ReaderBuilder::new()
            .has_headers(has_headers) // Adjust as needed
            .delimiter(b',') // Adjust as needed
            .from_reader(reader);
        let builders = schema
            .fields()
            .iter()
            .map(|field| make_builder(field.data_type(), batch_size))
            .collect::<Vec<_>>();
        let unwrapped_projection: Vec<usize> = projection.clone().unwrap_or_default();

        Ok(Self {
            schema,
            csv_reader,
            batch_size,
            builders,
            finished: false,
            unwrapped_projection,
        })
    }

    fn build_batch(&mut self) -> Result<RecordBatch> {
        let columns = self
            .builders
            .iter_mut()
            .map(|builder| builder.finish())
            .collect::<Vec<Arc<dyn Array>>>();
        RecordBatch::try_new(self.schema.clone(), columns).map_err(|e| {
            DataFusionError::ArrowError(
                Box::new(e),
                Some(String::from("Failed to build record batch")),
            )
        })
    }
}

impl Stream for BerlinModQueryInstantDataStream {
    type Item = Result<RecordBatch>; // Each item is a Result containing a RecordBatch

    fn poll_next(
        self: Pin<&mut BerlinModQueryInstantDataStream>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let csv_stream = self.get_mut();
        if csv_stream.finished {
            return Poll::Ready(None); // Stream is finished
        }

        let mut rows_processed_in_batch = 0;

        for result in csv_stream.csv_reader.records() {
            let record: csv::StringRecord = match result {
                Ok(record) => record,
                Err(e) => {
                    csv_stream.finished = true;
                    return Poll::Ready(Some(Err(DataFusionError::Execution(format!(
                        "CSV parsing error: {}",
                        e
                    )))));
                }
            };

            let point_data_record =
                BerlinModQueryInstantSchema::BerlinModQueryInstantData::new_from_csv_row(
                    record.clone(),
                )
                .unwrap();
            point_data_record
                .add_data_to_builders(&mut csv_stream.builders, &csv_stream.unwrapped_projection);
            rows_processed_in_batch += 1;

            if rows_processed_in_batch >= csv_stream.batch_size {
                let batch = csv_stream.build_batch();
                csv_stream.builders = csv_stream
                    .schema
                    .fields()
                    .iter()
                    .map(|field| make_builder(field.data_type(), csv_stream.batch_size))
                    .collect::<Vec<_>>();
                return Poll::Ready(Some(batch));
            }
        }

        csv_stream.finished = true;

        if rows_processed_in_batch > 0 {
            let batch = csv_stream.build_batch();
            Poll::Ready(Some(batch))
        } else {
            Poll::Ready(None)
        }
    }
}

use csv::{self, StringRecord};
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

use crate::core::csv::berlin_mod_csv::trips::schema as BerlinModTripsSchema;

pub struct BerlinModTripsDataStream {
    schema: SchemaRef,
    csv_reader: csv::Reader<BufReader<File>>,
    batch_size: usize,
    builders: Vec<Box<dyn ArrayBuilder>>,
    finished: bool,
    unwrapped_projection: Vec<usize>,
    next_record: Option<StringRecord>,
    prev_tripid: Option<i64>,
}

impl BerlinModTripsDataStream {
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
            next_record: None,
            prev_tripid: None, // At the beginning there is no previous tripid
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

impl Stream for BerlinModTripsDataStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        self: Pin<&mut BerlinModTripsDataStream>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        let csv_stream = self.get_mut();

        if csv_stream.finished {
            return Poll::Ready(None);
        }

        let mut buffer: Vec<StringRecord> = Vec::new();
        let mut rows_processed_in_batch = 0;

        // Pre-process any saved line from the last run
        if let Some(record) = csv_stream.next_record.take() {
            let tripid: i64 = record.get(1).unwrap().parse().unwrap();
            csv_stream.prev_tripid = Some(tripid);
            buffer.push(record);
        }

        for result in csv_stream.csv_reader.records() {
            let record = match result {
                Ok(r) => r,
                Err(e) => {
                    csv_stream.finished = true;
                    return Poll::Ready(Some(Err(DataFusionError::Execution(format!(
                        "CSV parsing error: {}",
                        e
                    )))));
                }
            };

            let tripid: i64 = match record.get(1).unwrap().parse() {
                Ok(id) => id,
                Err(e) => {
                    csv_stream.finished = true;
                    return Poll::Ready(Some(Err(DataFusionError::Execution(format!(
                        "Invalid tripid: {}",
                        e
                    )))));
                }
            };

            match csv_stream.prev_tripid {
                Some(prev_id) if tripid != prev_id => {
                    // Trip change: remember current record, process previous trip
                    csv_stream.next_record = Some(record.clone());
                    rows_processed_in_batch += 1;

                    let mut trips_data_record =
                        BerlinModTripsSchema::BerlinModTripsData::new_from_csv_rows(buffer.clone())
                            .unwrap();
                    trips_data_record.add_data_to_builders(
                        &mut csv_stream.builders,
                        &csv_stream.unwrapped_projection,
                    );

                    if rows_processed_in_batch >= csv_stream.batch_size {
                        let batch = csv_stream.build_batch();
                        return Poll::Ready(Some(batch));
                    }

                    buffer.clear();
                    buffer.push(record);
                    csv_stream.prev_tripid = Some(tripid);
                }
                _ => {
                    // Same trip or first record
                    buffer.push(record);
                    csv_stream.prev_tripid = Some(tripid);
                }
            }
        }

        // EOF
        csv_stream.finished = true;

        // Process last trip (if available)
        if !buffer.is_empty() {
            let mut trips_data_record =
                BerlinModTripsSchema::BerlinModTripsData::new_from_csv_rows(buffer.clone())
                    .unwrap();
            trips_data_record
                .add_data_to_builders(&mut csv_stream.builders, &csv_stream.unwrapped_projection);
            let batch = csv_stream.build_batch();

            return Poll::Ready(Some(batch));
        }

        Poll::Ready(None)
    }
}

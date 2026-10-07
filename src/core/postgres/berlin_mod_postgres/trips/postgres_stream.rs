use datafusion::{
    arrow::{array::*, datatypes::SchemaRef},
    error::Result as DatafusionResult,
};
use datafusion_table_providers::sql::db_connection_pool::dbconnection::query_arrow;
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;
use futures::stream::{Stream, StreamExt};
use std::{pin::Pin, sync::Arc};

use crate::{
    core::postgres::berlin_mod_postgres::trips::schema as BerlinModTripsSchema,
    utils::error::ToDataFusionError,
};

pub struct BerlinModTripsDataStream {
    inner: Pin<Box<dyn Stream<Item = DatafusionResult<RecordBatch>> + Send>>,
}

impl BerlinModTripsDataStream {
    pub fn try_new(
        postgres_pool: Arc<PostgresConnectionPool>,
        query: String,
        batch_size: usize,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> DatafusionResult<Self> {
        let unwrapped_projection: Vec<usize> = projection.clone().unwrap_or_default();

        let stream = async_stream::try_stream! {
            let conn = postgres_pool.connect()
                .await
                .to_df_error_ctx("[BerlinModTrips](Stream) Failed to connect to PostgreSQL")?;

            println!("Executing query: {}", query);
            let record_batch_stream = query_arrow(
                conn,
                query,
                None,
            )
            .await
            .to_df_error_ctx("[BerlinModTrips](Stream) Failed to execute query on PostgreSQL")?;

            futures::pin_mut!(record_batch_stream);

            while let Some(batch) = record_batch_stream.next().await {
                let batch = batch.to_df_error_ctx("[BerlinModTrips](Stream) Failed to get next batch from PostgreSQL stream")?;

                let columns = BerlinModTripsSchema::BerlinModTripsData::transform_batch_from_postgres(&batch, &unwrapped_projection)
                    .to_df_error_ctx("[BerlinModTrips](Stream) Failed to transform batch from PostgreSQL")?;

                let batch = RecordBatch::try_new(schema.clone(), columns)?;
                let num_rows = batch.num_rows();
                if num_rows <= batch_size { // Return batches of desired batch size
                    yield batch;
                } else {
                    let mut start = 0;
                    while start < num_rows {
                        let len = std::cmp::min(batch_size, num_rows - start);
                        let sliced_columns = (0..batch.num_columns())
                            .map(|i| batch.column(i).slice(start, len))
                            .collect::<Vec<_>>();
                        let sliced_batch = RecordBatch::try_new(batch.schema().clone(), sliced_columns)?;
                        yield sliced_batch;
                        start += len;
                    }
                }
            }
        };

        Ok(Self {
            inner: Box::pin(stream),
        })
    }
}

impl Stream for BerlinModTripsDataStream {
    type Item = DatafusionResult<RecordBatch>; // Each item is a DatafusionResult containing a RecordBatch

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

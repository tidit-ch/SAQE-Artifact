use crate::utils::error::ToDataFusionError;
use datafusion::{
    arrow::{array::*, datatypes::SchemaRef},
    error::{DataFusionError, Result as DatafusionResult},
};
use datafusion_table_providers::sql::db_connection_pool::dbconnection::query_arrow;
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;
use futures::stream::{Stream, StreamExt};
use std::{pin::Pin, sync::Arc};

use crate::core::postgres::berlin_mod_postgres::query_instants::schema as BerlinModQueryInstantsSchema;

pub struct BerlinModQueryInstantsDataStream {
    inner: Pin<Box<dyn Stream<Item = DatafusionResult<RecordBatch>> + Send>>,
}

impl BerlinModQueryInstantsDataStream {
    pub fn try_new(
        postgres_pool: Arc<PostgresConnectionPool>,
        query: String,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> DatafusionResult<Self> {
        let unwrapped_projection: Vec<usize> = projection.clone().unwrap_or_default();

        let stream = async_stream::try_stream! {
            let conn = postgres_pool.connect()
                .await
                .to_df_error_ctx("[BerlinModQueryInstants](Stream) Failed to connect to PostgreSQL")?;

            let record_batch_stream = query_arrow(
                conn,
                query,
                None
            )
            .await
            .to_df_error_ctx("[BerlinModQueryInstants](Stream) Failed to execute query on PostgreSQL")?;

            futures::pin_mut!(record_batch_stream);

            while let Some(batch) = record_batch_stream.next().await {
                let batch = batch.map_err(|e| DataFusionError::External(Box::new(e)))?;

                let columns = BerlinModQueryInstantsSchema::BerlinModQueryInstantsData::transform_batch_from_postgres(&batch, &unwrapped_projection)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let batch = RecordBatch::try_new(schema.clone(), columns)?;
                yield batch;
            }

        };

        Ok(Self {
            inner: Box::pin(stream),
        })
    }
}

impl Stream for BerlinModQueryInstantsDataStream {
    type Item = DatafusionResult<RecordBatch>; // Each item is a DatafusionResult containing a RecordBatch

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

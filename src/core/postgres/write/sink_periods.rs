// A custom DataSink that writes (period_id, start_period, end_period) pairs into the PostGIS instants table.

use arrow::array::TimestampMillisecondArray;
use arrow::array::{Int64Array, RecordBatch};
use arrow_schema::Schema;
use chrono::NaiveDateTime;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::sink::DataSink;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::{RecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::DisplayFormatType;
use datafusion_table_providers::postgres::Postgres;
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;
use futures::stream::StreamExt;
use std::any::Any;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio_postgres::types::ToSql;

#[derive(Debug)]
pub struct PostgresSinkPeriods {
    pub schema: SchemaRef,
    pub table_name: String,
    pub postgres_pool: Arc<PostgresConnectionPool>,
    pub insert_op: datafusion::logical_expr::dml::InsertOp,
}

impl DisplayAs for PostgresSinkPeriods {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "PostgresSink: table_name={}", self.table_name)
            }
            DisplayFormatType::TreeRender => {
                write!(f, "PostgresSink\n  table_name: {}", self.table_name)
            }
        }
    }
}

impl DataSink for PostgresSinkPeriods {
    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    fn as_any(&self) -> &(dyn Any + 'static) {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn write_all<'life0, 'life1, 'async_trait>(
        &'life0 self,
        mut data: Pin<
            Box<
                dyn RecordBatchStream<Item = DatafusionResult<RecordBatch, DataFusionError>> + Send,
            >,
        >,
        _context: &'life1 Arc<TaskContext>,
    ) -> Pin<Box<dyn Future<Output = DatafusionResult<u64, DataFusionError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        let table_name = self.table_name.clone();

        Box::pin(async move {
            let mut db_conn = self
                .postgres_pool
                .connect()
                .await
                .map_err(|e| DataFusionError::External(e))?;

            let postgres_conn = Postgres::postgres_conn(&mut db_conn)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            // Start transaction
            let tx = postgres_conn
                .conn
                .transaction()
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            if matches!(
                self.insert_op,
                datafusion::logical_expr::dml::InsertOp::Overwrite
            ) {
                let delete_sql = format!("DELETE FROM {}", table_name);
                tx.execute(&delete_sql, &[])
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
            }

            let mut rows_written = 0;

            while let Some(batch) = data.next().await {
                let batch = batch?;

                let period_id_array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("Expected Int64 for period_id".to_string())
                    })?;

                let start_array = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "Expected TimestampMillisecond for start of period".to_string(),
                        )
                    })?;

                let end_array = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "Expected TimestampMillisecond for end of period".to_string(),
                        )
                    })?;

                for row in 0..batch.num_rows() {
                    let period_id: i32 = period_id_array.value(row) as i32; // Postgres sql periods table expects an int4 (32bit)!!
                    let start_ts: i64 = start_array.value(row);
                    let end_ts: i64 = end_array.value(row);

                    #[allow(deprecated)]
                    let start =
                        NaiveDateTime::from_timestamp_millis(start_ts).ok_or_else(|| {
                            DataFusionError::Execution(format!(
                                "Invalid timestamp (ms): {}",
                                start_ts
                            ))
                        })?;

                    #[allow(deprecated)]
                    let end = NaiveDateTime::from_timestamp_millis(end_ts).ok_or_else(|| {
                        DataFusionError::Execution(format!("Invalid timestamp (ms): {}", end_ts))
                    })?;

                    let sql = match self.insert_op {
                        datafusion::logical_expr::dml::InsertOp::Replace => format!(
                            "INSERT INTO {} (period_id, start_period, end_period) VALUES ($1, $2, $3) \
                            ON CONFLICT (period_id) DO UPDATE \
                            SET start_period = EXCLUDED.start_period, \
                            end_period = EXCLUDED.end_period",
                            table_name
                        ),
                        _ => format!("INSERT INTO {} (period_id, start_period, end_period) VALUES ($1, $2, $3)", table_name), // InsertOp::Append or InsertOp::Overwrite
                    };

                    let values: &[&(dyn ToSql + Sync)] = &[&period_id, &start, &end];

                    tx.execute(&sql, values)
                        .await
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;

                    rows_written += 1;
                }
            }

            // Commit transaction
            tx.commit()
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            Ok(rows_written)
        })
    }
}

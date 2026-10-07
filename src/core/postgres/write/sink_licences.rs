// A custom DataSink that writes (licence_id, licence) pairs into the PostGIS licence table.

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::Schema;
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
pub struct PostgresSinkLicences {
    pub schema: SchemaRef,
    pub table_name: String,
    pub postgres_pool: Arc<PostgresConnectionPool>,
    pub insert_op: datafusion::logical_expr::dml::InsertOp,
}

impl DisplayAs for PostgresSinkLicences {
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

impl DataSink for PostgresSinkLicences {
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

                let licence_id_array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("Expected Int64 for licence_id".to_string())
                    })?;

                let licence_array = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("Expected StringArray for licence".to_string())
                    })?;

                for row in 0..batch.num_rows() {
                    let licence_id: i32 = licence_id_array.value(row) as i32; // Postgres sql licences table expects an int4 (32bit)!!
                    let licence: String = licence_array.value(row).to_string();

                    let sql = match self.insert_op {
                        datafusion::logical_expr::dml::InsertOp::Replace => format!(
                            "INSERT INTO {} (licence_id, licence) VALUES ($1, $2) \
                            ON CONFLICT (licence_id) DO UPDATE \
                            SET licence = EXCLUDED.licence",
                            table_name
                        ),
                        _ => format!(
                            "INSERT INTO {} (licence_id, licence) VALUES ($1, $2)",
                            table_name
                        ), // InsertOp::Append or InsertOp::Overwrite
                    };

                    let values: &[&(dyn ToSql + Sync)] = &[&licence_id, &licence];

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

// A custom DataSink that writes (trip_id, moid, polyline) pairs into the PostGIS trips table using EWKB format.

use arrow::array::{Float64Array, Int64Array, ListArray, RecordBatch, StructArray};
use arrow_schema::Schema;
use bytes::Bytes;
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
use futures::{pin_mut, SinkExt};
use postgis::ewkb::{AsEwkbLineString, EwkbWrite, LineStringM, PointM};
use std::any::Any;
use std::fmt;
use std::fmt::Write as _;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio_postgres::types::ToSql;

#[derive(Debug)]
pub struct PostgresSinkTrips {
    pub schema: SchemaRef,
    pub table_name: String,
    pub postgres_pool: Arc<PostgresConnectionPool>,
    pub insert_op: datafusion::logical_expr::dml::InsertOp,
}

impl DisplayAs for PostgresSinkTrips {
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

impl DataSink for PostgresSinkTrips {
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

            let rows_written = if matches!(
                self.insert_op,
                datafusion::logical_expr::dml::InsertOp::Replace
            ) {
                // COPY has no ON CONFLICT equivalent, so upserts still go
                // through per-row INSERT ... ON CONFLICT.
                let sql = format!(
                    "INSERT INTO {} (trip_id, moid, polyline) VALUES ($1, $2, $3) \
                    ON CONFLICT (trip_id) DO UPDATE \
                    SET moid = EXCLUDED.moid, \
                        polyline = EXCLUDED.polyline",
                    table_name
                );

                let mut rows_written: u64 = 0;
                while let Some(batch) = data.next().await {
                    let batch = batch?;
                    let columns = TripColumns::from_batch(&batch)?;
                    for row in 0..batch.num_rows() {
                        let (trip_id, moid, linestring) = columns.row(row);
                        let values: &[&(dyn ToSql + Sync)] =
                            &[&trip_id, &moid, &linestring.as_ewkb()];
                        tx.execute(&sql, values)
                            .await
                            .map_err(|e| DataFusionError::External(Box::new(e)))?;
                        rows_written += 1;
                    }
                }
                rows_written
            } else {
                // InsertOp::Append or InsertOp::Overwrite: bulk-load via
                // COPY FROM STDIN, matching MobilityDB's own COPY-based
                // loaders instead of paying one round trip per row - see
                // build/mobilitydb/NOTES.md, "Loading scripts". The
                // geometry column accepts hex-encoded EWKB as plain CSV
                // text, so no binary COPY format/OID lookup is needed.
                let copy_sql = format!(
                    "COPY {} (trip_id, moid, polyline) FROM STDIN WITH (FORMAT csv)",
                    table_name
                );
                let sink = tx
                    .copy_in(&copy_sql)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                pin_mut!(sink);

                while let Some(batch) = data.next().await {
                    let batch = batch?;
                    let columns = TripColumns::from_batch(&batch)?;
                    let mut buf = String::new();
                    for row in 0..batch.num_rows() {
                        let (trip_id, moid, linestring) = columns.row(row);
                        writeln!(
                            buf,
                            "{},{},{}",
                            trip_id,
                            moid,
                            linestring.as_ewkb().to_hex_ewkb()
                        )
                        .expect("writing to a String cannot fail");
                    }
                    sink.send(Bytes::from(buf.into_bytes()))
                        .await
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                }

                sink.finish()
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
            };

            // Commit transaction
            tx.commit()
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            Ok(rows_written)
        })
    }
}

/// The typed Arrow array views needed to read one (trip_id, moid, polyline)
/// row out of a batch - factored out since both the COPY and upsert code
/// paths above need identical row extraction.
struct TripColumns<'a> {
    trip_id: &'a Int64Array,
    moid: &'a Int64Array,
    polyline: &'a ListArray,
    x: &'a Float64Array,
    y: &'a Float64Array,
    m: &'a Float64Array,
}

impl<'a> TripColumns<'a> {
    fn from_batch(batch: &'a RecordBatch) -> DatafusionResult<Self, DataFusionError> {
        let trip_id = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DataFusionError::Execution("Expected Int64 for trip_id".to_string()))?;

        let moid = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DataFusionError::Execution("Expected Int64 for moid".to_string()))?;

        let polyline = batch
            .column(2)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("Expected ListArray for polyline".to_string())
            })?;

        let struct_array = polyline
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("Expected StructArray inside polyline".to_string())
            })?;

        let x = struct_array
            .column_by_name("x")
            .and_then(|a| a.as_any().downcast_ref::<Float64Array>())
            .ok_or_else(|| DataFusionError::Execution("Expected Float64 x".to_string()))?;

        let y = struct_array
            .column_by_name("y")
            .and_then(|a| a.as_any().downcast_ref::<Float64Array>())
            .ok_or_else(|| DataFusionError::Execution("Expected Float64 y".to_string()))?;

        let m = struct_array
            .column_by_name("m")
            .and_then(|a| a.as_any().downcast_ref::<Float64Array>())
            .ok_or_else(|| DataFusionError::Execution("Expected Float64 for m".to_string()))?;

        Ok(Self {
            trip_id,
            moid,
            polyline,
            x,
            y,
            m,
        })
    }

    /// Postgres's trips table expects an int4 (32-bit) trip_id/moid.
    fn row(&self, row: usize) -> (i32, i32, LineStringM) {
        let trip_id = self.trip_id.value(row) as i32;
        let moid = self.moid.value(row) as i32;

        let offsets = self.polyline.value_offsets();
        let offset = offsets[row] as usize;
        let length = (offsets[row + 1] - offsets[row]) as usize;

        let mut points: Vec<PointM> = Vec::with_capacity(length);
        for i in offset..offset + length {
            points.push(PointM {
                x: self.x.value(i),
                y: self.y.value(i),
                m: self.m.value(i), // Timestamp as f64 for M-value
                srid: Some(4326),
            });
        }

        (
            trip_id,
            moid,
            LineStringM {
                points,
                srid: Some(4326),
            },
        )
    }
}

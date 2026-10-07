// A custom DataSink that writes (polygon_id, polygon) pairs into the PostGIS regions table using EWKB format.

use arrow::array::{Array, Int64Array, ListArray, RecordBatch};
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
use geo_traits::{CoordTrait, LineStringTrait, PolygonTrait};
use geoarrow_array::array::PolygonArray;
use geoarrow_array::GeoArrowArrayAccessor;
use geoarrow_schema::{Metadata, PolygonType};
use postgis::ewkb::{AsEwkbPolygon, LineStringT, Point, Polygon};
use std::any::Any;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio_postgres::types::ToSql;

use crate::utils::error::geo_arrow_error_to_datafusion_error;

#[derive(Debug)]
pub struct PostgresSinkRegions {
    pub schema: SchemaRef,
    pub table_name: String,
    pub postgres_pool: Arc<PostgresConnectionPool>,
    pub insert_op: datafusion::logical_expr::dml::InsertOp,
}

impl DisplayAs for PostgresSinkRegions {
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

impl DataSink for PostgresSinkRegions {
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

                let polygon_id_array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("Expected Int64 for trip_id".to_string())
                    })?;

                let polygon_array = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("Expected ListArray for polyline".to_string())
                    })?;

                let polygon_type = PolygonType::new(
                    geoarrow_schema::Dimension::XY,
                    Arc::new(Metadata::default()),
                );
                let polygon_array =
                    PolygonArray::try_from((polygon_array, polygon_type)).map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to cast polygon array to PolygonArray: {}",
                            e
                        ))
                    })?;

                for (index, polygon) in polygon_array.iter().enumerate() {
                    if polygon.is_none() {
                        continue;
                    }
                    let polygon = polygon
                        .unwrap()
                        .map_err(geo_arrow_error_to_datafusion_error)?;

                    let exterior = polygon.exterior().unwrap();
                    let length = exterior.num_coords();

                    let mut points: Vec<Point> = Vec::with_capacity(length);
                    for i in 0..length {
                        let coord = exterior.coord(i).unwrap();
                        points.push(Point {
                            x: coord.x(),
                            y: coord.y(),
                            srid: None,
                        });
                    }
                    let linestring = LineStringT {
                        points,
                        srid: Some(4326),
                    };

                    let polygon = Polygon {
                        rings: vec![linestring], // BerlinMod regions has only one ring!
                        srid: Some(4326),
                    };

                    let polygon_id = polygon_id_array.value(index) as i32;

                    let sql = match self.insert_op {
                        datafusion::logical_expr::dml::InsertOp::Replace => format!(
                            "INSERT INTO {} (polygon_id, polygon) VALUES ($1, $2) \
                            ON CONFLICT (polygon_id) DO UPDATE \
                            SET polygon = EXCLUDED.polygon",
                            table_name
                        ),
                        _ => format!(
                            "INSERT INTO {} (polygon_id, polygon) VALUES ($1, $2)",
                            table_name
                        ), // InsertOp::Append or InsertOp::Overwrite
                    };

                    let values: &[&(dyn ToSql + Sync)] = &[&polygon_id, &polygon.as_ewkb()];

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

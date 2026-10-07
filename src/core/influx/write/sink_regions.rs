//! A custom DataSink that writes (polygon_id, index, x, y, timestamp) pairs into the InfluxDB regions measurement (table).
//! 'index' is the insertion order number

use arrow::array::{Float64Array, Int64Array, ListArray, RecordBatch, StructArray};
use arrow_schema::Schema;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::sink::DataSink;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{RecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::DisplayFormatType;
use futures::stream::StreamExt;
use reqwest::Client;
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub struct InfluxSinkRegions {
    pub schema: SchemaRef,
    pub table_name: String,
    pub influx_options: HashMap<String, String>,
    pub insert_op: datafusion::logical_expr::dml::InsertOp,
}

impl DisplayAs for InfluxSinkRegions {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "InfluxSink: table_name={}", self.table_name)
            }
            DisplayFormatType::TreeRender => {
                write!(f, "InfluxSink\n  table_name: {}", self.table_name)
            }
        }
    }
}

impl DataSink for InfluxSinkRegions {
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
            Box<dyn RecordBatchStream<Item = Result<RecordBatch, DataFusionError>> + Send>,
        >,
        _context: &'life1 Arc<TaskContext>,
    ) -> Pin<Box<dyn Future<Output = Result<u64, DataFusionError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        let table_name = self.table_name.clone();
        const MAX_LINES_PER_REQUEST: usize = 5000;

        // Only support insert_op 'Insert Into'
        if self.insert_op != datafusion::logical_expr::dml::InsertOp::Append {
            return Box::pin(async {
                Err(DataFusionError::NotImplemented("Insert operator 'Insert Overwrite' and 'Insert Or Replace' are not implemented for InlfuxDB".to_string()))
            });
        }

        Box::pin(async move {
            let client = Client::new();
            let mut rows_written = 0;
            let mut index: i32 = 0;

            // Get the parameters from the input hashmap
            let influx_url = self.influx_options.get("influx_url").unwrap();
            let database_name = self.influx_options.get("database_name").unwrap();
            let precision = self.influx_options.get("precision").unwrap();
            let influx_token = self.influx_options.get("password").unwrap();

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
                        DataFusionError::Execution("Expected ListArray for polygon".to_string())
                    })?;
                let inner_polygon_array = polygon_array
                    .values()
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("Expected ListArray for polygon".to_string())
                    })?;

                let struct_array = inner_polygon_array
                    .values()
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "Expected StructArray inside polygon list array".to_string(),
                        )
                    })?;

                let x_array = struct_array
                    .column_by_name("x")
                    .and_then(|a| a.as_any().downcast_ref::<Float64Array>())
                    .ok_or_else(|| DataFusionError::Execution("Expected Float64 x".to_string()))?;

                let y_array = struct_array
                    .column_by_name("y")
                    .and_then(|a| a.as_any().downcast_ref::<Float64Array>())
                    .ok_or_else(|| DataFusionError::Execution("Expected Float64 y".to_string()))?;

                let offsets = polygon_array.value_offsets();
                let mut lines = Vec::new();

                for row in 0..batch.num_rows() {
                    let polygon_id = polygon_id_array.value(row);

                    let start_offset = offsets[row] as usize;
                    let end_offset = offsets[row + 1] as usize;

                    for i in start_offset..end_offset {
                        let x = x_array.value(i);
                        let y = y_array.value(i);

                        // Get the current system time
                        let now = SystemTime::now();
                        let duration_since_epoch = now.duration_since(UNIX_EPOCH).unwrap();
                        let timestamp_nanos = duration_since_epoch.as_nanos();

                        // Compose line protocol string:
                        // <measurement>,<tags> <fields> <timestamp>
                        // Tag: polygon_id
                        // Fields: x, y, index
                        // Timestamp: insertion timestamp in nanos
                        let line = format!(
                            "{},polygon_id={} x={},y={},index={}i {}",
                            table_name, polygon_id, x, y, index, timestamp_nanos
                        );
                        lines.push(line);
                        index += 1;
                        rows_written += 1;
                    }
                }

                for chunk in lines.chunks(MAX_LINES_PER_REQUEST) {
                    let body = chunk.join("\n");

                    let res = client
                        .post(&format!(
                            "{}/api/v3/write_lp?db={}&precision={}",
                            influx_url, database_name, precision
                        ))
                        .bearer_auth(&influx_token)
                        .body(body)
                        .send()
                        .await
                        .map_err(|e| {
                            DataFusionError::Execution(format!("HTTP send error: {}", e))
                        })?;

                    if !res.status().is_success() {
                        let text = res.text().await.unwrap_or_default();
                        return Err(DataFusionError::Execution(format!(
                            "Influx write failed: {}",
                            text
                        )));
                    }
                }
            }

            Ok(rows_written)
        })
    }
}

//! A custom DataSink that writes (period_id, start_period, end_period) pairs into the InfluxDB periods measurement (table).

use arrow::array::{Int64Array, RecordBatch, TimestampMillisecondArray};
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

#[derive(Debug)]
pub struct InfluxSinkQueryPeriods {
    pub schema: SchemaRef,
    pub table_name: String,
    pub influx_options: HashMap<String, String>,
    pub insert_op: datafusion::logical_expr::dml::InsertOp,
}

impl DisplayAs for InfluxSinkQueryPeriods {
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

impl DataSink for InfluxSinkQueryPeriods {
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

        // Get the parameters from the input hashmap
        let influx_url = self.influx_options.get("influx_url").unwrap();
        let database_name = self.influx_options.get("database_name").unwrap();
        let precision = self.influx_options.get("precision").unwrap();
        let influx_token = self.influx_options.get("password").unwrap();

        Box::pin(async move {
            let client = Client::new();
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

                let start_period_array = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "Expected TimestampMillisecond for start period".to_string(),
                        )
                    })?;

                let end_period_array = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "Expected TimestampMillisecond for end period".to_string(),
                        )
                    })?;

                let mut lines = Vec::new();

                for row in 0..batch.num_rows() {
                    let period_id = period_id_array.value(row);
                    let start_ms = start_period_array.value(row);
                    let end_ms = end_period_array.value(row);

                    // Compose line protocol string:
                    // <measurement>,<tags> <fields> <timestamp>
                    // Tag: period_id
                    // Fields: start_period, end_period
                    // Timestamp: start_ms
                    let line = format!(
                        "{},period_id={} start_period={}i,end_period={}i {}",
                        table_name, period_id, start_ms, end_ms, start_ms
                    );
                    lines.push(line);
                    rows_written += 1;
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

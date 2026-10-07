use crate::core::influx::berlin_mod_influx::trips::schema as BerlinModTripsSchema;
use crate::utils::error::ToDataFusionError;
use datafusion::{
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    error::{DataFusionError, Result},
};
use futures::stream::{Stream, StreamExt};
use reqwest::Client;
use serde_json::Value;
use std::{collections::HashMap, pin::Pin};

pub struct BerlinModTripsDataStream {
    inner: Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>,
}

impl BerlinModTripsDataStream {
    pub fn try_new(
        influx_options: HashMap<String, String>,
        query: String,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        batch_size: usize,
    ) -> Result<Self> {
        let unwrapped_projection: Vec<usize> = projection.clone().unwrap_or_default();

        let client = Client::new();

        // Get the Influx parameters from the input hashmap
        let influx_url = influx_options.get("influx_url").unwrap().clone();
        let database_name = influx_options.get("database_name").unwrap().clone();
        let influx_token = influx_options.get("password").unwrap().clone();
        let url = format!("{}/api/v3/query_sql", influx_url);

        let mut params = HashMap::new();
        params.insert("db", database_name.clone());
        params.insert("q", query.clone());
        params.insert("format", "jsonl".to_string());

        let stream = async_stream::try_stream! {
            let res = client
                .get(url)
                .bearer_auth(influx_token)
                .query(&params)
                .send()
                .await
                .to_df_error_ctx("[BerlinModTrips](Stream) Failed to send request to InfluxDB")?;

            if !res.status().is_success() {
                Err(DataFusionError::External(Box::new(
                    std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("[BerlinModTrips](Stream) Failed to fetch data from InfluxDB (status: {})", res.status()),
                    ),
                )))?
            }

            let mut stream = res.bytes_stream();

            let mut line_buffer: Vec<Value> = Vec::new();
            let mut batch_rows: Vec<Vec<Value>> = Vec::new();
            let mut prev_trip_id: Option<i64> = None;

            // Buffer for potentially incomplete lines
            let mut leftover = String::new();

            while let Some(item) = stream.next().await {
                let chunk = item.to_df_error_ctx("[BerlinModTrips](Stream) Failed to read chunk from InfluxDB response stream")?;
                let chunk_text = String::from_utf8_lossy(&chunk);

                // Attach leftover at the front
                let mut text = leftover.clone();
                text.push_str(&chunk_text);

                let mut lines = text.lines();

                leftover.clear();

                while let Some(line) = lines.next() {
                    let trimmed_line = line.trim();
                    if trimmed_line.is_empty() {
                        continue;
                    }

                    // Try to parse the JSON line
                    match serde_json::from_str::<Value>(trimmed_line) {
                        Ok(json_value) => {
                            let trip_id: i64 = json_value["trip_id"].as_str().unwrap().parse().unwrap();

                            if let Some(prev_id) = prev_trip_id {
                                if trip_id != prev_id {
                                    batch_rows.push(std::mem::take(&mut line_buffer));

                                    if batch_rows.len() >= batch_size {
                                        let batch = BerlinModTripsSchema::build_arrow_batch(&batch_rows, schema.clone(), unwrapped_projection.clone())?;
                                        yield batch;
                                        batch_rows.clear();
                                    }
                                }
                            }
                            line_buffer.push(json_value);
                            prev_trip_id = Some(trip_id);
                        }
                        Err(_) => {
                            // Likely an incomplete line, keep it for next chunk
                            leftover.push_str(trimmed_line);
                            for rest_line in lines {
                                leftover.push_str(rest_line.trim());
                            }
                            break;
                        }
                    }
                }
            }

            // Handle leftover data after stream finishes
            if !leftover.is_empty() {
                let json_value: Value = serde_json::from_str(&leftover).to_df_error_ctx("[BerlinModTrips](Stream) Failed to parse leftover JSON from InfluxDB response stream")?;
                let trip_id: i64 = json_value["trip_id"].as_str().unwrap().parse().unwrap();

                if let Some(prev_id) = prev_trip_id {
                    if trip_id != prev_id {
                        batch_rows.push(std::mem::take(&mut line_buffer));
                    }
                }
                line_buffer.push(json_value);
            }

            if !line_buffer.is_empty() {
                batch_rows.push(std::mem::take(&mut line_buffer));
            }

            if !batch_rows.is_empty() {
                let batch = BerlinModTripsSchema::build_arrow_batch(&batch_rows, schema.clone(), unwrapped_projection)?;
                yield batch;
            }
        };

        Ok(Self {
            inner: Box::pin(stream),
        })
    }
}

impl Stream for BerlinModTripsDataStream {
    type Item = Result<RecordBatch>; // Each item is a Result containing a RecordBatch

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

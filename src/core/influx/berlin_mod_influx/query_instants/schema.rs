use arrow::array::{ArrayRef, Int64Builder, TimestampMillisecondBuilder};
use arrow::record_batch::RecordBatch;
use chrono::{NaiveDateTime, TimeZone, Utc};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::Result;
use serde_json::Value;
use std::collections::HashSet;
use std::{sync::Arc, vec};

pub fn new_get_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("instant_id", DataType::Int64, true),
        Field::new(
            "instant",
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        ),
    ]))
}

pub fn build_arrow_batch(
    batch_rows: &Vec<Value>,
    schema: SchemaRef,
    unwrapped_projection: Vec<usize>,
) -> Result<RecordBatch> {
    let projection_set: HashSet<_> = unwrapped_projection.iter().cloned().collect();

    let mut instant_id_builder = projection_set.contains(&0).then(|| Int64Builder::new());
    let mut instant_builder = projection_set
        .contains(&1)
        .then(|| TimestampMillisecondBuilder::new());

    for row in batch_rows {
        if let Some(b) = instant_id_builder.as_mut() {
            let instant_id_str = row["instant_id"].as_str().unwrap();
            let instant_id = instant_id_str.parse::<i64>().unwrap();
            b.append_value(instant_id);
        }

        if let Some(b) = instant_builder.as_mut() {
            let timestamp_str = row["time"].as_str().unwrap();

            let dt = NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%dT%H:%M:%S%.f")
                .map(|ndt| Utc.from_utc_datetime(&ndt).timestamp_millis())
                .unwrap();

            b.append_value(dt);
        }
    }

    let mut arrays: Vec<ArrayRef> = Vec::new();

    for &idx in &unwrapped_projection {
        let arr: ArrayRef = match idx {
            0 => Arc::new(instant_id_builder.take().unwrap().finish()),
            1 => Arc::new(instant_builder.take().unwrap().finish()),
            _ => continue,
        };
        arrays.push(arr);
    }

    Ok(RecordBatch::try_new(schema, arrays)?)
}

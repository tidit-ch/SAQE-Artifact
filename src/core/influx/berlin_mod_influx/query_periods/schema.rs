use arrow::array::{ArrayRef, Int64Builder, TimestampMillisecondBuilder};
use arrow::record_batch::RecordBatch;
use arrow_schema::TimeUnit;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::Result;
use serde_json::Value;
use std::collections::HashSet;
use std::{sync::Arc, vec};

pub fn new_get_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("period_id", DataType::Int64, true),
        Field::new(
            "start_period",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new(
            "end_period",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]))
}

pub fn build_arrow_batch(
    batch_rows: &Vec<Value>,
    schema: SchemaRef,
    unwrapped_projection: Vec<usize>,
) -> Result<RecordBatch> {
    let projection_set: HashSet<_> = unwrapped_projection.iter().cloned().collect();

    let mut period_id_builder = projection_set.contains(&0).then(|| Int64Builder::new());
    let mut start_period_builder = projection_set
        .contains(&1)
        .then(|| TimestampMillisecondBuilder::new());
    let mut end_period_builder = projection_set
        .contains(&2)
        .then(|| TimestampMillisecondBuilder::new());

    for row in batch_rows {
        if let Some(b) = period_id_builder.as_mut() {
            let instant_id_str = row["period_id"].as_str().unwrap();
            let instant_id = instant_id_str.parse::<i64>().unwrap();
            b.append_value(instant_id);
        }

        if let Some(b) = start_period_builder.as_mut() {
            let ts = row["start_period"].as_i64().unwrap();
            b.append_value(ts);
        }

        if let Some(b) = end_period_builder.as_mut() {
            let ts = row["end_period"].as_i64().unwrap();
            b.append_value(ts);
        }
    }

    let mut arrays: Vec<ArrayRef> = Vec::new();

    for &idx in &unwrapped_projection {
        let arr: ArrayRef = match idx {
            0 => Arc::new(period_id_builder.take().unwrap().finish()),
            1 => Arc::new(start_period_builder.take().unwrap().finish()),
            2 => Arc::new(end_period_builder.take().unwrap().finish()),
            _ => continue,
        };
        arrays.push(arr);
    }

    Ok(RecordBatch::try_new(schema, arrays)?)
}

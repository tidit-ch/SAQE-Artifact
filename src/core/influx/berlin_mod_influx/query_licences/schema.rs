use arrow::array::{ArrayRef, Int64Builder};
use arrow::{array::StringBuilder, record_batch::RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::Result;
use serde_json::Value;
use std::collections::HashSet;
use std::{sync::Arc, vec};

pub fn new_get_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("licence_id", DataType::Int64, true),
        Field::new("licence", DataType::Utf8, true),
    ]))
}

pub fn build_arrow_batch(
    batch_rows: &Vec<Value>,
    schema: SchemaRef,
    unwrapped_projection: Vec<usize>,
) -> Result<RecordBatch> {
    let projection_set: HashSet<_> = unwrapped_projection.iter().cloned().collect();

    let mut licence_id_builder = projection_set.contains(&0).then(|| Int64Builder::new());
    let mut licence_builder = projection_set.contains(&1).then(|| StringBuilder::new());

    for row in batch_rows {
        if let Some(b) = licence_id_builder.as_mut() {
            let moid_str = row["licence_id"].as_str().unwrap();
            let moid_id = moid_str.parse::<i64>().unwrap();
            b.append_value(moid_id);
        }
        if let Some(b) = licence_builder.as_mut() {
            let licence = row["licence"].as_str().unwrap();
            b.append_value(licence);
        }
    }

    let mut arrays: Vec<ArrayRef> = Vec::new();

    for &idx in &unwrapped_projection {
        let arr: ArrayRef = match idx {
            0 => Arc::new(licence_id_builder.take().unwrap().finish()),
            1 => Arc::new(licence_builder.take().unwrap().finish()),
            _ => continue,
        };
        arrays.push(arr);
    }

    Ok(RecordBatch::try_new(schema, arrays)?)
}

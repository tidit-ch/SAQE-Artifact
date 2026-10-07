use arrow::array::{ArrayRef, Int64Builder};
use arrow::{
    array::{Float64Builder, StructBuilder},
    record_batch::RecordBatch,
};
use arrow_schema::Fields;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::Result;
use serde_json::Value;
use std::collections::HashSet;
use std::{sync::Arc, vec};

pub fn new_get_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("point_id", DataType::Int64, false),
        Field::new(
            "point",
            DataType::Struct(Fields::from(vec![
                Field::new("x", DataType::Float64, true),
                Field::new("y", DataType::Float64, true),
            ])),
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
    //println!("row: {:?}", batch_rows);

    let mut point_id_builder = projection_set.contains(&0).then(|| Int64Builder::new());
    let mut point_builder = projection_set.contains(&1).then(|| {
        let x_builder = Float64Builder::new();
        let y_builder = Float64Builder::new();
        StructBuilder::new(
            vec![
                arrow::datatypes::Field::new("x", arrow::datatypes::DataType::Float64, true),
                arrow::datatypes::Field::new("y", arrow::datatypes::DataType::Float64, true),
            ],
            vec![Box::new(x_builder), Box::new(y_builder)],
        )
    });

    for row in batch_rows {
        if let Some(b) = point_id_builder.as_mut() {
            let point_id_str = row["point_id"].as_str().unwrap();
            let point_id = point_id_str.parse::<i64>().unwrap();
            b.append_value(point_id);
        }
        if let Some(sb) = point_builder.as_mut() {
            let x = row["x"].as_f64().unwrap();
            let y = row["y"].as_f64().unwrap();

            sb.field_builder::<Float64Builder>(0)
                .unwrap()
                .append_value(x);
            sb.field_builder::<Float64Builder>(1)
                .unwrap()
                .append_value(y);

            sb.append(true);
        }
    }

    let mut arrays: Vec<ArrayRef> = Vec::new();

    for &idx in &unwrapped_projection {
        let arr: ArrayRef = match idx {
            0 => Arc::new(point_id_builder.take().unwrap().finish()),
            1 => Arc::new(point_builder.take().unwrap().finish()),
            _ => continue,
        };
        arrays.push(arr);
    }

    Ok(RecordBatch::try_new(schema, arrays)?)
}

use arrow::array::{ArrayRef, Float64Builder, Int64Builder, ListBuilder, StructBuilder};
use arrow::record_batch::RecordBatch;
use arrow_schema::extension::{ExtensionType, EXTENSION_TYPE_NAME_KEY};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::error::Result;
use geoarrow_schema::PolygonType;
use serde_json::Value;
use std::collections::HashSet;
use std::{sync::Arc, vec};

use crate::core::utils::schema::POLYGON_DATATYPE;

pub fn new_get_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("polygon_id", DataType::Int64, false),
        Field::new("polygon", POLYGON_DATATYPE.clone(), false).with_metadata(
            [(
                EXTENSION_TYPE_NAME_KEY.to_owned(),
                PolygonType::NAME.to_owned(),
            )]
            .into_iter()
            .collect(),
        ),
    ]))
}

pub fn build_arrow_batch(
    batch_rows: &Vec<Vec<Value>>,
    schema: SchemaRef,
    unwrapped_projection: Vec<usize>,
) -> Result<RecordBatch> {
    let projection_set: HashSet<_> = unwrapped_projection.iter().cloned().collect();

    let mut polygon_id_builder = projection_set.contains(&0).then(|| Int64Builder::new());

    let mut list_builder = if projection_set.contains(&1) {
        let struct_fields = Fields::from(vec![
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false),
        ]);

        let list_builder = ListBuilder::new(StructBuilder::new(
            struct_fields.clone(),
            vec![
                Box::new(Float64Builder::new()),
                Box::new(Float64Builder::new()),
            ],
        ));

        Some(ListBuilder::new(list_builder))
        // let field = Field::new("item", DataType::Struct(struct_fields), false);
        // Some(ListBuilder::with_field(struct_builder, field))
    } else {
        None
    };

    for traj in batch_rows {
        let first = &traj[0];

        if let Some(b) = polygon_id_builder.as_mut() {
            let polygon_id = first["polygon_id"]
                .as_str()
                .unwrap()
                .parse::<i64>()
                .unwrap();
            b.append_value(polygon_id);
        }

        if let Some(outer_list_builder) = list_builder.as_mut() {
            let list_builder = outer_list_builder.values();
            let struct_builder = list_builder.values();

            for point in traj {
                let x = point["x"].as_f64().unwrap();
                let y = point["y"].as_f64().unwrap();

                struct_builder
                    .field_builder::<Float64Builder>(0)
                    .unwrap()
                    .append_value(x);
                struct_builder
                    .field_builder::<Float64Builder>(1)
                    .unwrap()
                    .append_value(y);

                struct_builder.append(true);
            }

            list_builder.append(true);
            outer_list_builder.append(true);
        }
    }

    let mut arrays: Vec<ArrayRef> = Vec::new();

    for &idx in &unwrapped_projection {
        let arr: ArrayRef = match idx {
            0 => Arc::new(polygon_id_builder.take().unwrap().finish()),
            1 => Arc::new(list_builder.take().unwrap().finish()),
            _ => continue,
        };
        arrays.push(arr);
    }

    Ok(RecordBatch::try_new(schema, arrays)?)
}

use crate::core::utils::schema::TRAJECTORY_DATATYPE;
use arrow::array::{ArrayRef, Float64Builder, Int64Builder, ListBuilder, StructBuilder};
use arrow::record_batch::RecordBatch;
use arrow_schema::extension::{ExtensionType, EXTENSION_TYPE_NAME_KEY};
use chrono::{NaiveDateTime, TimeZone, Utc};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::error::Result;
use geoarrow_schema::LineStringType;
use serde_json::Value;
use std::collections::HashSet;
use std::{sync::Arc, vec};

pub fn new_get_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("trip_id", DataType::Int64, false),
        Field::new("moid", DataType::Int64, false),
        Field::new("polyline", TRAJECTORY_DATATYPE.clone(), false).with_metadata(
            [(
                EXTENSION_TYPE_NAME_KEY.to_owned(),
                LineStringType::NAME.to_owned(),
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

    let mut trip_id_builder = projection_set.contains(&0).then(|| Int64Builder::new());
    let mut moid_builder = projection_set.contains(&1).then(|| Int64Builder::new());

    let mut list_builder = if projection_set.contains(&2) {
        let struct_fields = Fields::from(vec![
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false),
            Field::new("m", DataType::Float64, false),
        ]);

        let struct_builder = ListBuilder::new(StructBuilder::new(
            struct_fields.clone(),
            vec![
                Box::new(Float64Builder::new()),
                Box::new(Float64Builder::new()),
                Box::new(Float64Builder::new()),
            ],
        ));

        let field = Field::new("vertices", DataType::Struct(struct_fields), false);
        Some(ListBuilder::with_field(struct_builder, field))
    } else {
        None
    };

    for traj in batch_rows {
        let first = &traj[0];

        if let Some(b) = trip_id_builder.as_mut() {
            let trip_id = first["trip_id"].as_str().unwrap().parse::<i64>().unwrap();
            b.append_value(trip_id);
        }

        if let Some(b) = moid_builder.as_mut() {
            let moid = first["moid"].as_str().unwrap().parse::<i64>().unwrap();
            b.append_value(moid);
        }

        if let Some(lb) = list_builder.as_mut() {
            let sb = lb.values();

            for point in traj {
                let x = point["x"].as_f64().unwrap();
                let y = point["y"].as_f64().unwrap();
                let timestamp_str = point["time"].as_str().unwrap();

                let dt = NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%dT%H:%M:%S%.f")
                    .map(|ndt| Utc.from_utc_datetime(&ndt).timestamp_millis())
                    .unwrap();

                sb.field_builder::<Float64Builder>(0)
                    .unwrap()
                    .append_value(x);
                sb.field_builder::<Float64Builder>(1)
                    .unwrap()
                    .append_value(y);
                sb.field_builder::<Float64Builder>(2)
                    .unwrap()
                    .append_value(dt as f64);

                sb.append(true);
            }

            lb.append(true);
        }
    }

    let mut arrays: Vec<ArrayRef> = Vec::new();

    for &idx in &unwrapped_projection {
        let arr: ArrayRef = match idx {
            0 => Arc::new(trip_id_builder.take().unwrap().finish()),
            1 => Arc::new(moid_builder.take().unwrap().finish()),
            2 => Arc::new(list_builder.take().unwrap().finish()),
            _ => continue,
        };
        arrays.push(arr);
    }

    Ok(RecordBatch::try_new(schema, arrays)?)
}

use arrow_schema::extension::{ExtensionType, EXTENSION_TYPE_NAME_KEY};
use datafusion::{
    arrow::{
        array::*,
        datatypes::{DataType, Field, Fields, Schema, SchemaRef},
    },
    error::DataFusionError,
};
use geoarrow_schema::LineStringType;
use postgis::ewkb::LineStringM;
use std::{sync::Arc, vec};

use crate::core::utils::schema::TRAJECTORY_DATATYPE;

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

#[derive(Debug, Clone)]
pub struct BerlinModTripsData {}

impl BerlinModTripsData {
    /// Converts Datatypes in a RecordBatch:
    /// moid (Int64)
    /// trip_id (Int64)
    /// polyline (EWKB LineStringM -> List of Structs with x, y, timestamp)
    /// Only includes columns specified by 'unwrapped_projection'
    pub fn transform_batch_from_postgres(
        batch: &RecordBatch,
        unwrapped_projection: &Vec<usize>,
    ) -> Result<Vec<ArrayRef>, DataFusionError> {
        let mut columns: Vec<ArrayRef> = Vec::new();
        let mut field_index = 0;

        // If the projection contains the trip_id
        if unwrapped_projection.contains(&0) {
            let array = batch.column(field_index);
            let casted_array = arrow::compute::cast(array, &arrow::datatypes::DataType::Int64)?;
            columns.push(casted_array);
            field_index += 1;
        }

        // If the projection contains the moid
        if unwrapped_projection.contains(&1) {
            let array = batch.column(field_index);
            let casted_array = arrow::compute::cast(array, &arrow::datatypes::DataType::Int64)?;
            columns.push(casted_array);
            field_index += 1;
        }

        // If the projection contains the polyline
        if unwrapped_projection.contains(&2) {
            let ewkb_array = batch
                .column(field_index)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("Expected BinaryArray for polyline".to_string())
                })?;

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
            let mut list_builder = ListBuilder::with_field(struct_builder, field);

            for i in 0..ewkb_array.len() {
                let bytes = ewkb_array.value(i);
                let mut cursor = std::io::Cursor::new(bytes);
                let linestring: LineStringM = postgis::ewkb::EwkbRead::read_ewkb(&mut cursor)
                    .map_err(|e| {
                        DataFusionError::Execution(format!("Failed to parse LineStringM: {e}"))
                    })?;

                let struct_builder = list_builder.values();
                for point in linestring.points {
                    struct_builder
                        .field_builder::<Float64Builder>(0)
                        .unwrap()
                        .append_value(point.x);
                    struct_builder
                        .field_builder::<Float64Builder>(1)
                        .unwrap()
                        .append_value(point.y);
                    struct_builder
                        .field_builder::<Float64Builder>(2)
                        .unwrap()
                        .append_value(point.m as f64);
                    struct_builder.append(true);
                }

                list_builder.append(true);
            }
            columns.push(Arc::new(list_builder.finish()));
        }
        Ok(columns)
    }
}

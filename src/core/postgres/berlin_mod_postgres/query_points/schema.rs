use datafusion::arrow::{
    array::*,
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use datafusion::error::DataFusionError;
use std::{sync::Arc, vec};

use crate::core::utils::schema::POINT_XY_DATATYPE;

pub fn new_get_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("point_id", DataType::Int64, false),
        Field::new("point", POINT_XY_DATATYPE.clone(), false),
    ]))
}

#[derive(Debug, Clone)]
pub struct BerlinModQueryPointsData {}

impl BerlinModQueryPointsData {
    /// Converts Datatypes in a RecordBatch:
    /// point_id (Int64)
    /// point (EWKB -> Struct{x: f64, y: f64})
    /// Only includes columns specified by 'unwrapped_projection'
    pub fn transform_batch_from_postgres(
        batch: &RecordBatch,
        unwrapped_projection: &Vec<usize>,
    ) -> Result<Vec<ArrayRef>, DataFusionError> {
        let mut columns: Vec<ArrayRef> = Vec::new();
        let mut field_index = 0;

        // If the projection contains the point_id
        if unwrapped_projection.contains(&0) {
            let array = batch.column(field_index);
            let casted_array = arrow::compute::cast(array, &arrow::datatypes::DataType::Int64)?;
            columns.push(casted_array);
            field_index += 1;
        }

        // If the projection contains the point
        if unwrapped_projection.contains(&1) {
            let ewkb_array = batch
                .column(field_index)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();

            let mut struct_builder = StructBuilder::new(
                vec![
                    Field::new("x", DataType::Float64, false),
                    Field::new("y", DataType::Float64, false),
                ],
                vec![
                    Box::new(Float64Builder::new()),
                    Box::new(Float64Builder::new()),
                ],
            );

            for i in 0..ewkb_array.len() {
                let bytes = ewkb_array.value(i);
                let mut cursor = std::io::Cursor::new(bytes);
                let point: postgis::ewkb::Point =
                    postgis::ewkb::EwkbRead::read_ewkb(&mut cursor).unwrap();

                struct_builder
                    .field_builder::<Float64Builder>(0)
                    .unwrap()
                    .append_value(point.x);
                struct_builder
                    .field_builder::<Float64Builder>(1)
                    .unwrap()
                    .append_value(point.y);

                struct_builder.append(true);
            }
            columns.push(Arc::new(struct_builder.finish()));
        }
        Ok(columns)
    }
}

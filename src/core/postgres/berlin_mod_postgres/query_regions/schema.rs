use datafusion::arrow::{
    array::*,
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use datafusion::error::DataFusionError;
use postgis::ewkb::Polygon;
use std::{sync::Arc, vec};

use crate::core::utils::schema::POLYGON_DATATYPE;

pub fn new_get_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("polygon_id", DataType::Int64, false),
        Field::new("polygon", POLYGON_DATATYPE.clone(), false),
    ]))
}

#[derive(Debug, Clone)]
pub struct BerlinModQueryRegionsData {}

impl BerlinModQueryRegionsData {
    /// Converts Datatypes in a RecordBatch:
    /// polygon_id (Int32) -> (Int64)
    /// polygon (EWKB Polygon -> List of Structs with x, y)
    /// Only includes columns specified by 'projection'
    pub fn transform_batch_from_postgres(
        batch: &RecordBatch,
        projection: &Vec<usize>,
        mut builders: Vec<Box<dyn ArrayBuilder>>,
    ) -> Result<Vec<ArrayRef>, DataFusionError> {
        let mut field_index = 0;
        // If the projection contains the point_id
        if projection.contains(&0) {
            let field_builder = builders[0]
                .as_any_mut()
                .downcast_mut::<Int64Builder>()
                .unwrap();
            let array = batch.column(field_index);
            let casted_array = arrow::compute::cast(array, &arrow::datatypes::DataType::Int64)?;
            field_builder.append_array(casted_array.as_any().downcast_ref::<Int64Array>().unwrap());
            field_index += 1;
        }

        // If the projection contains the polygon
        if projection.contains(&1) {
            let ewkb_array = batch
                .column(field_index)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("Expected BinaryArray for polygon".to_string())
                })?;

            let polygon_column_builder = builders[1]
                .as_any_mut()
                .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
                .unwrap();

            for i in 0..ewkb_array.len() {
                let bytes = ewkb_array.value(i);
                let mut cursor = std::io::Cursor::new(bytes);
                let polygon: Polygon =
                    postgis::ewkb::EwkbRead::read_ewkb(&mut cursor).map_err(|e| {
                        DataFusionError::Execution(format!("Failed to parse Polygon: {e}"))
                    })?;
                let rings_builder = polygon_column_builder
                    .values()
                    .as_any_mut()
                    .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
                    .unwrap();
                for ring in polygon.rings {
                    let struct_builder = rings_builder
                        .values()
                        .as_any_mut()
                        .downcast_mut::<StructBuilder>()
                        .unwrap();
                    for point in ring.points {
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
                    rings_builder.append(true);
                }
                polygon_column_builder.append(true);
            }
        }
        let columns: Vec<ArrayRef> = builders
            .iter_mut()
            .map(|builder| builder.finish())
            .collect();
        Ok(columns)
    }
}

use datafusion::arrow::{
    array::*,
    datatypes::{DataType, Field, Schema, SchemaRef},
};

use crate::core::utils::schema::POINT_XY_DATATYPE;
use crate::utils::error::{CsvError, SaqeError};
use arrow_schema::extension::{ExtensionType, EXTENSION_TYPE_NAME_KEY};
use geoarrow_schema::PointType;
use std::{sync::Arc, vec};

use csv::StringRecord;

pub fn new_get_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("point_id", DataType::Int64, false),
        Field::new("point", POINT_XY_DATATYPE.clone(), false).with_metadata(
            [(
                EXTENSION_TYPE_NAME_KEY.to_owned(),
                PointType::NAME.to_owned(),
            )]
            .into_iter()
            .collect(),
        ),
    ]))
}

#[derive(Debug, Clone)]
pub struct BerlinModQueryPointsData {
    point_id: i64,
    point: (f64, f64),
}

impl BerlinModQueryPointsData {
    pub fn new(point_id: i64, point: (f64, f64)) -> Self {
        BerlinModQueryPointsData { point_id, point }
    }

    pub fn field_count() -> usize {
        3
    }

    // Extract id and points from the StringRecord
    pub fn new_from_csv_row(record: StringRecord) -> Result<BerlinModQueryPointsData, SaqeError> {
        let position = record.position();
        let position = if position.is_some() {
            Some(position.unwrap().clone())
        } else {
            None
        };
        if record.len() != Self::field_count() {
            return Err(CsvError::Validation {
                message: "Mismatched number of fields".to_string(),
                position: None,
            })?;
        }
        let point_id: i64 = record[0].parse::<i64>().map_err(|e| CsvError::CellParse {
            expected: "i64".to_string(),
            found: record[0].to_string(),
            source: e.into(),
            position: position.clone(),
        })?;
        let pos_x: f64 = record[1].parse::<f64>().map_err(|e| CsvError::CellParse {
            expected: "f64".to_string(),
            found: record[1].to_string(),
            source: e.into(),
            position: position.clone(),
        })?;
        let pos_y: f64 = record[2].parse::<f64>().map_err(|e| CsvError::CellParse {
            expected: "f64".to_string(),
            found: record[2].to_string(),
            source: e.into(),
            position: position.clone(),
        })?;
        let point: (f64, f64) = (pos_x, pos_y);

        Ok(BerlinModQueryPointsData { point_id, point })
    }

    pub fn add_data_to_builders(
        &self,
        builders: &mut Vec<Box<dyn ArrayBuilder>>,
        unwrapped_projection: &Vec<usize>,
    ) {
        let mut field_index: usize = 0;
        let mut builder_index: usize = 0;

        if BerlinModQueryPointsData::should_add_this_index(
            field_index,
            builder_index,
            unwrapped_projection,
        ) {
            builders[builder_index]
                .as_any_mut()
                .downcast_mut::<Int64Builder>()
                .unwrap()
                .append_value(self.point_id);
            builder_index += 1;
        }
        field_index += 1;

        if BerlinModQueryPointsData::should_add_this_index(
            field_index,
            builder_index,
            unwrapped_projection,
        ) {
            let struct_builder = builders[builder_index]
                .as_any_mut()
                .downcast_mut::<StructBuilder>()
                .unwrap();

            let (x, y) = self.point;

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
    }

    pub fn should_add_this_index(
        field_index: usize,
        builder_index: usize,
        projection: &Vec<usize>,
    ) -> bool {
        if builder_index >= projection.len() {
            return false;
        }
        if projection[builder_index] != field_index {
            return false;
        }
        if projection[builder_index] == field_index {
            return true;
        }
        return false;
    }
}

impl Default for BerlinModQueryPointsData {
    fn default() -> Self {
        BerlinModQueryPointsData {
            point_id: 0,
            point: (0.0, 0.0),
        }
    }
}

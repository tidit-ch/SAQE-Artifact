use arrow_schema::extension::{ExtensionType, EXTENSION_TYPE_NAME_KEY};
use core::panic;
use datafusion::arrow::{
    array::*,
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use geoarrow_schema::PolygonType;
use std::{sync::Arc, vec};

use csv::StringRecord;

use crate::core::utils::schema::POLYGON_DATATYPE;
use crate::utils::error::{CsvError, SaqeError};

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

#[derive(Debug, Clone)]
pub struct BerlinModQueryRegionsData {
    polygon_id: i64,
    polygon: Vec<(f64, f64)>,
}

impl BerlinModQueryRegionsData {
    pub fn new(polygon_id: i64, polygon: Vec<(f64, f64)>) -> Self {
        BerlinModQueryRegionsData {
            polygon_id,
            polygon,
        }
    }

    pub fn field_count() -> usize {
        6
    }

    // Builds a polygon record from a vector of string records that consists of CSV rows
    pub fn new_from_csv_rows(
        buffer: Vec<StringRecord>,
    ) -> Result<BerlinModQueryRegionsData, SaqeError> {
        let mut polygon: Vec<(f64, f64)> = Vec::new();

        if buffer[0].len() != Self::field_count() {
            return Err(CsvError::Validation {
                message: "mismatched number of fields".to_string(),
                position: buffer[0].position().cloned(),
            }
            .into());
        }
        let polygon_id: i64 = buffer[0][0].parse().map_err(|e| CsvError::CellParse {
            expected: "i64".to_string(),
            found: format!("invalid polygon_id {}", buffer[0][0].to_string()),
            source: Box::new(e),
            position: buffer[0].position().cloned(),
        })?;
        // Process first polygon segment:
        let x_start = buffer[0][2]
            .parse::<f64>()
            .map_err(|e| CsvError::CellParse {
                expected: "f64".to_string(),
                found: format!("invalid x_start {}", buffer[0][2].to_string()),
                source: Box::new(e),
                position: buffer[0].position().cloned(),
            })?;
        let y_start = buffer[0][3]
            .parse::<f64>()
            .map_err(|e| CsvError::CellParse {
                expected: "f64".to_string(),
                found: format!("invalid y_start {}", buffer[0][3].to_string()),
                source: Box::new(e),
                position: buffer[0].position().cloned(),
            })?;
        let x_end = buffer[0][4]
            .parse::<f64>()
            .map_err(|e| CsvError::CellParse {
                expected: "f64".to_string(),
                found: format!("invalid x_end {}", buffer[0][4].to_string()),
                source: Box::new(e),
                position: buffer[0].position().cloned(),
            })?;
        let y_end = buffer[0][5]
            .parse::<f64>()
            .map_err(|e| CsvError::CellParse {
                expected: "f64".to_string(),
                found: format!("invalid y_end {}", buffer[0][5].to_string()),
                source: Box::new(e),
                position: buffer[0].position().cloned(),
            })?;
        polygon.push((x_start, y_start)); // First Polygon point
        polygon.push((x_end, y_end)); // Second Second point

        for record in buffer.iter().skip(1) {
            // First csv row is already processed -> skip it
            if record.len() != Self::field_count() {
                return Err(CsvError::Validation {
                    message: "mismatched number of fields".to_string(),
                    position: record.position().cloned(),
                }
                .into());
            }
            let record_polygon_id: i64 = record[0].parse().map_err(|e| CsvError::CellParse {
                expected: "i64".to_string(),
                found: format!("invalid polygon_id {}", record[0].to_string()),
                source: Box::new(e),
                position: record.position().cloned(),
            })?;
            if record_polygon_id != polygon_id {
                // Buffer should only contain csv rows of the same trip
                // This should be never reached!
                panic!("Polygon ID mismatch: expected {polygon_id}, found {record_polygon_id}");
            }
            // Push only end points of the polygon segment
            let x_end = record[4].parse::<f64>().map_err(|e| CsvError::CellParse {
                expected: "f64".to_string(),
                found: format!("invalid x_end {}", record[4].to_string()),
                source: Box::new(e),
                position: record.position().cloned(),
            })?;
            let y_end = record[5].parse::<f64>().map_err(|e| CsvError::CellParse {
                expected: "f64".to_string(),
                found: format!("invalid y_end {}", record[5].to_string()),
                source: Box::new(e),
                position: record.position().cloned(),
            })?;
            polygon.push((x_end, y_end));
        }

        Ok(BerlinModQueryRegionsData {
            polygon_id,
            polygon,
        })
    }

    pub fn add_data_to_builders(
        &mut self,
        builders: &mut Vec<Box<dyn ArrayBuilder>>,
        unwrapped_projection: &Vec<usize>,
    ) {
        let mut field_index: usize = 0;
        let mut builder_index: usize = 0;

        if BerlinModQueryRegionsData::should_add_this_index(
            field_index,
            builder_index,
            unwrapped_projection,
        ) {
            builders[builder_index]
                .as_any_mut()
                .downcast_mut::<Int64Builder>()
                .unwrap()
                .append_value(self.polygon_id);
            builder_index += 1;
        }
        field_index += 1;

        if BerlinModQueryRegionsData::should_add_this_index(
            field_index,
            builder_index,
            unwrapped_projection,
        ) {
            let list_builder = builders[builder_index]
                .as_any_mut()
                .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
                .unwrap();
            let internal_list_builder = list_builder
                .values()
                .as_any_mut()
                .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
                .unwrap();
            let struct_builder = internal_list_builder
                .values()
                .as_any_mut()
                .downcast_mut::<StructBuilder>()
                .unwrap();
            for point in &self.polygon {
                let (x, y) = point;
                // struct_builder.append(true);
                let x_builder = struct_builder.field_builder::<Float64Builder>(0).unwrap();
                x_builder.append_value(*x);
                let y_builder = struct_builder.field_builder::<Float64Builder>(1).unwrap();
                y_builder.append_value(*y);
                struct_builder.append(true);
            }
            internal_list_builder.append(true);
            list_builder.append(true);
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

impl Default for BerlinModQueryRegionsData {
    fn default() -> Self {
        BerlinModQueryRegionsData {
            polygon_id: 0,
            polygon: Vec::new(),
        }
    }
}

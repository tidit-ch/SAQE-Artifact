use datafusion::arrow::{
    array::*,
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use std::{sync::Arc, vec};

use csv::StringRecord;

use crate::utils::error::{CsvError, SaqeError};

pub fn new_get_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("moid", DataType::Int64, true),
        Field::new("licence", DataType::Utf8, true),
        Field::new("type", DataType::Utf8, true),
        Field::new("model", DataType::Utf8, true),
    ]))
}

#[derive(Debug, Clone)]
pub struct BerlinModDatamcarData {
    moid: i64,
    licence: String,
    car_type: String,
    model: String,
}

impl BerlinModDatamcarData {
    pub fn new(moid: i64, licence: String, car_type: String, model: String) -> Self {
        BerlinModDatamcarData {
            moid,
            licence,
            car_type,
            model,
        }
    }

    pub fn field_count() -> usize {
        4
    }

    // Extract all columns from the StringRecord
    pub fn new_from_csv_row(record: StringRecord) -> Result<BerlinModDatamcarData, SaqeError> {
        if record.len() != Self::field_count() {
            return Err(CsvError::Validation {
                message: "mismatched number of fields".to_string(),
                position: record.position().cloned(),
            }
            .into());
        }
        let moid: i64 = record[0].parse().map_err(|e| CsvError::CellParse {
            expected: "i64".to_string(),
            found: format!("invalid moid {}", record[0].to_string()),
            source: Box::new(e),
            position: record.position().cloned(),
        })?;
        let licence: String = record[1].parse().map_err(|e| CsvError::CellParse {
            expected: "string".to_string(),
            found: format!("invalid licence :: {}", record[1].to_string()),
            source: Box::new(e),
            position: record.position().cloned(),
        })?;
        let car_type: String = record[2].parse().map_err(|e| CsvError::CellParse {
            expected: "String".to_string(),
            found: format!("invalid car type: {}", record[2].to_string()),
            source: Box::new(e),
            position: record.position().cloned(),
        })?;
        let model: String = record[3].parse().map_err(|e| CsvError::CellParse {
            expected: "String".to_string(),
            found: format!("invalid model: {}", record[3].to_string()),
            source: Box::new(e),
            position: record.position().cloned(),
        })?;

        Ok(BerlinModDatamcarData {
            moid,
            licence,
            car_type,
            model,
        })
    }

    pub fn add_data_to_builders(
        &self,
        builders: &mut Vec<Box<dyn ArrayBuilder>>,
        unwrapped_projection: &Vec<usize>,
    ) {
        let mut field_index: usize = 0;
        let mut builder_index: usize = 0;

        if Self::should_add_this_index(field_index, builder_index, unwrapped_projection) {
            builders[builder_index]
                .as_any_mut()
                .downcast_mut::<Int64Builder>()
                .unwrap()
                .append_value(self.moid);
            builder_index += 1;
        }
        field_index += 1;

        if Self::should_add_this_index(field_index, builder_index, unwrapped_projection) {
            builders[builder_index]
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap()
                .append_value(&self.licence);
            builder_index += 1;
        }
        field_index += 1;

        if Self::should_add_this_index(field_index, builder_index, unwrapped_projection) {
            builders[builder_index]
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap()
                .append_value(&self.car_type);
            builder_index += 1;
        }
        field_index += 1;

        if Self::should_add_this_index(field_index, builder_index, unwrapped_projection) {
            builders[builder_index]
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap()
                .append_value(&self.model);
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

impl Default for BerlinModDatamcarData {
    fn default() -> Self {
        Self {
            moid: 0,
            licence: "UNKNOWN".to_string(),
            car_type: "unknown".to_string(),
            model: "unspecified".to_string(),
        }
    }
}

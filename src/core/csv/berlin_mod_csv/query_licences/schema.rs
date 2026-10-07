use datafusion::arrow::{
    array::*,
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use std::{sync::Arc, vec};

use csv::StringRecord;

use crate::utils::error::{CsvError, SaqeError};

pub fn new_get_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("licence_id", DataType::Int64, true),
        Field::new("licence", DataType::Utf8, true),
    ]))
}

#[derive(Debug, Clone)]
pub struct BerlinModQueryLicencesData {
    licence_id: i64,
    licence: String,
}

impl BerlinModQueryLicencesData {
    pub fn new(licence_id: i64, licence: String) -> Self {
        BerlinModQueryLicencesData {
            licence_id,
            licence,
        }
    }

    pub fn field_count() -> usize {
        2
    }

    // Extract all columns from the StringRecord
    pub fn new_from_csv_row(record: StringRecord) -> Result<BerlinModQueryLicencesData, SaqeError> {
        if record.len() != Self::field_count() {
            return Err(CsvError::Validation {
                message: "mismatched number of fields".to_string(),
                position: record.position().cloned(),
            }
            .into());
        }

        // In querylicence.csv "licence" is the first column and "licence_id" the second column!
        let licence: String = record[0].parse().map_err(|e| CsvError::CellParse {
            expected: "string".to_string(),
            found: format!("invalid licence :: {}", record[0].to_string()),
            source: Box::new(e),
            position: record.position().cloned(),
        })?;
        let licence_id: i64 = record[1].parse().map_err(|e| CsvError::CellParse {
            expected: "i64".to_string(),
            found: format!("invalid licence_id {}", record[1].to_string()),
            source: Box::new(e),
            position: record.position().cloned(),
        })?;

        Ok(BerlinModQueryLicencesData {
            licence_id,
            licence,
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
                .append_value(self.licence_id);
            builder_index += 1;
        }
        field_index += 1;

        if Self::should_add_this_index(field_index, builder_index, unwrapped_projection) {
            builders[builder_index]
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap()
                .append_value(&self.licence);
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

impl Default for BerlinModQueryLicencesData {
    fn default() -> Self {
        Self {
            licence_id: 0,
            licence: "UNKNOWN".to_string(),
        }
    }
}

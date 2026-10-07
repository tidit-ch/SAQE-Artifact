use chrono::NaiveDateTime;
use datafusion::arrow::{
    array::*,
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use std::{sync::Arc, vec};

use csv::StringRecord;

use crate::utils::error::{CsvError, SaqeError};

pub fn new_get_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("instant_id", DataType::Int64, true),
        Field::new(
            "instant",
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        ),
    ]))
}

#[derive(Debug, Clone)]
pub struct BerlinModQueryInstantData {
    instant_id: i64,
    instant: i64,
}

impl BerlinModQueryInstantData {
    pub fn new(instant_id: i64, instant: i64) -> Self {
        BerlinModQueryInstantData {
            instant_id,
            instant,
        }
    }

    pub fn field_count() -> usize {
        2
    }

    // Extract all columns from the StringRecord
    pub fn new_from_csv_row(record: StringRecord) -> Result<BerlinModQueryInstantData, SaqeError> {
        if record.len() != Self::field_count() {
            return Err(CsvError::Validation {
                message: "mismatched number of fields".to_string(),
                position: record.position().cloned(),
            }
            .into());
        }
        let instant_id: i64 = record[0].parse().map_err(|e| CsvError::CellParse {
            expected: "i64".to_string(),
            found: format!("invalid instant_id {}", record[0].to_string()),
            source: Box::new(e),
            position: record.position().cloned(),
        })?;
        // Parse timestamp
        let instant_str = &record[1];
        let naive_dt = NaiveDateTime::parse_from_str(instant_str, "%Y-%m-%d %H:%M:%S%.3f")
            .map_err(|e| CsvError::CellParse {
                expected: "YYYY-MM-DD HH:MM:SS.sss".to_string(),
                found: format!("invalid instant {}", instant_str.to_string()),
                source: Box::new(e),
                position: record.position().cloned(),
            })?;
        let instant: i64 = naive_dt.and_utc().timestamp_millis();

        Ok(BerlinModQueryInstantData {
            instant_id,
            instant,
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
                .append_value(self.instant_id);
            builder_index += 1;
        }
        field_index += 1;

        if Self::should_add_this_index(field_index, builder_index, unwrapped_projection) {
            builders[builder_index]
                .as_any_mut()
                .downcast_mut::<TimestampMillisecondBuilder>()
                .unwrap()
                .append_value(self.instant);
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

impl Default for BerlinModQueryInstantData {
    fn default() -> Self {
        Self {
            instant_id: 0,
            instant: 0,
        }
    }
}

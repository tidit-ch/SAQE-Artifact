use arrow_schema::TimeUnit;
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
        Field::new("period_id", DataType::Int64, true),
        Field::new(
            "start_period",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new(
            "end_period",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]))
}

#[derive(Debug, Clone)]
pub struct BerlinModQueryPeriodsData {
    period_id: i64,
    start: i64,
    end: i64,
}

impl BerlinModQueryPeriodsData {
    pub fn new(period_id: i64, start: i64, end: i64) -> Self {
        BerlinModQueryPeriodsData {
            period_id,
            start,
            end,
        }
    }

    pub fn field_count() -> usize {
        3
    }

    // Extract all columns from the StringRecord
    pub fn new_from_csv_row(record: StringRecord) -> Result<BerlinModQueryPeriodsData, SaqeError> {
        if record.len() != Self::field_count() {
            return Err(CsvError::Validation {
                message: "mismatched number of fields".to_string(),
                position: record.position().cloned(),
            }
            .into());
        }
        let period_id: i64 = record[0].parse().map_err(|e| CsvError::CellParse {
            expected: "i64".to_string(),
            found: format!("invalid period_id {}", record[0].to_string()),
            source: Box::new(e),
            position: record.position().cloned(),
        })?;
        // Parse start timestamp
        let start_str = &record[1];
        let start_naive_dt = NaiveDateTime::parse_from_str(start_str, "%Y-%m-%d %H:%M:%S%.3f")
            .map_err(|e| CsvError::CellParse {
                expected: "timestamp".to_string(),
                found: format!("invalid start_period {}", start_str.to_string()),
                source: Box::new(e),
                position: record.position().cloned(),
            })?;
        let start: i64 = start_naive_dt.and_utc().timestamp_millis();
        // Parse end timestamp
        let end_str = &record[2];
        let end_naive_dt = NaiveDateTime::parse_from_str(end_str, "%Y-%m-%d %H:%M:%S%.3f")
            .map_err(|e| CsvError::CellParse {
                expected: "timestamp".to_string(),
                found: format!("invalid end_period {}", end_str.to_string()),
                source: Box::new(e),
                position: record.position().cloned(),
            })?;
        let end: i64 = end_naive_dt.and_utc().timestamp_millis();

        Ok(BerlinModQueryPeriodsData {
            period_id,
            start,
            end,
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
                .append_value(self.period_id);
            builder_index += 1;
        }
        field_index += 1;

        if Self::should_add_this_index(field_index, builder_index, unwrapped_projection) {
            builders[builder_index]
                .as_any_mut()
                .downcast_mut::<TimestampMillisecondBuilder>()
                .unwrap()
                .append_value(self.start);
            builder_index += 1;
        }
        field_index += 1;

        if Self::should_add_this_index(field_index, builder_index, unwrapped_projection) {
            builders[builder_index]
                .as_any_mut()
                .downcast_mut::<TimestampMillisecondBuilder>()
                .unwrap()
                .append_value(self.end);
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

impl Default for BerlinModQueryPeriodsData {
    fn default() -> Self {
        Self {
            period_id: 0,
            start: 0,
            end: 0,
        }
    }
}

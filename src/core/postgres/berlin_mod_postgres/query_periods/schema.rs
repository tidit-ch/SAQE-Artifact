use arrow_schema::TimeUnit;
use datafusion::arrow::{
    array::*,
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use datafusion::error::DataFusionError;
use std::{sync::Arc, vec};

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
pub struct BerlinModQueryPeriodsData {}

impl BerlinModQueryPeriodsData {
    /// Converts Datatypes in a RecordBatch:
    /// - period_id (Int32) -> Int64
    /// - start_period Timestamp(Nanosecond, None) -> Timestamp(Millisecond, None)
    /// - end_period Timestamp(Nanosecond, None) -> Timestamp(Millisecond, None)
    /// Only includes fields from 'unwrapped_projection'.
    pub fn transform_batch_from_postgres(
        batch: &RecordBatch,
        unwrapped_projection: &Vec<usize>,
    ) -> Result<Vec<ArrayRef>, DataFusionError> {
        let mut columns: Vec<ArrayRef> = Vec::new();
        let mut field_index = 0;

        // If the projection contains the moid
        if unwrapped_projection.contains(&0) {
            let array = batch.column(field_index);
            let casted_array = arrow::compute::cast(array, &arrow::datatypes::DataType::Int64)?;
            columns.push(casted_array);
            field_index += 1;
        }

        // If the projection contains the start_period
        if unwrapped_projection.contains(&1) {
            let array = batch.column(field_index);
            let casted_array = arrow::compute::cast(
                array,
                &arrow::datatypes::DataType::Timestamp(
                    arrow::datatypes::TimeUnit::Millisecond,
                    None,
                ),
            )?;
            columns.push(casted_array);
            field_index += 1;
        }

        // If the projection contains the end_period
        if unwrapped_projection.contains(&2) {
            let array = batch.column(field_index);
            let casted_array = arrow::compute::cast(
                array,
                &arrow::datatypes::DataType::Timestamp(
                    arrow::datatypes::TimeUnit::Millisecond,
                    None,
                ),
            )?;
            columns.push(casted_array);
        }

        Ok(columns)
    }
}

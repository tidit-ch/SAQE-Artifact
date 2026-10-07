use datafusion::arrow::{
    array::*,
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use datafusion::error::DataFusionError;
use std::{sync::Arc, vec};

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
pub struct BerlinModQueryInstantsData {}

impl BerlinModQueryInstantsData {
    /// Converts Datatypes in a RecordBatch:
    /// - instant_id (Int32) -> Int64
    /// - instant Timestamp(Nanosecond, None) -> Timestamp(Millisecond, None)
    /// Only includes fields from 'unwrapped_projection'.
    pub fn transform_batch_from_postgres(
        batch: &RecordBatch,
        unwrapped_projection: &Vec<usize>,
    ) -> Result<Vec<ArrayRef>, DataFusionError> {
        let mut columns: Vec<ArrayRef> = Vec::new();
        let mut field_index = 0;

        // If the projection contains the instant_id
        if unwrapped_projection.contains(&0) {
            let array = batch.column(field_index);
            let casted_array = arrow::compute::cast(array, &arrow::datatypes::DataType::Int64)?;
            columns.push(casted_array);
            field_index += 1;
        }

        // If the projection contains the instant
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
        }

        Ok(columns)
    }
}

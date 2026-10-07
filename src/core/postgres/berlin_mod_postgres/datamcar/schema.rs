use datafusion::arrow::{
    array::*,
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use datafusion::error::DataFusionError;
use std::{sync::Arc, vec};

pub fn new_get_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("moid", DataType::Int64, true),
        Field::new("licence", DataType::Utf8, true),
        Field::new("type", DataType::Utf8, true),
        Field::new("model", DataType::Utf8, true),
    ]))
}

#[derive(Debug, Clone)]
pub struct BerlinModDatamcarData {}

impl BerlinModDatamcarData {
    /// Converts Datatypes in a RecordBatch:
    /// - moid (Int32) -> Int64
    /// - other fields (licence, type, model) cloned
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

        // If the projection contains the licence
        if unwrapped_projection.contains(&1) {
            let array = batch.column(field_index);
            columns.push(array.clone());
            field_index += 1;
        }

        // If the projection contains the type
        if unwrapped_projection.contains(&2) {
            let array = batch.column(field_index);
            columns.push(array.clone());
            field_index += 1;
        }

        // If the projection contains the model
        if unwrapped_projection.contains(&3) {
            let array = batch.column(field_index);
            columns.push(array.clone());
            //field_index += 1;
        }

        Ok(columns)
    }
}

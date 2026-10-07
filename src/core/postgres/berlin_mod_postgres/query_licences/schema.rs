use datafusion::arrow::{
    array::*,
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use datafusion::error::DataFusionError;
use std::{sync::Arc, vec};

pub fn new_get_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("licence_id", DataType::Int64, true),
        Field::new("licence", DataType::Utf8, true),
        // Populated once at load time from datamcar (see
        // benches/saqe_vs_mobilitydb/mod.rs's populate_licences_moid()) -
        // mirrors MobilityDB's own Licences.VehId, letting queries join
        // trips.moid = licences1.moid directly instead of bridging through
        // datamcar.
        Field::new("moid", DataType::Int64, true),
    ]))
}

#[derive(Debug, Clone)]
pub struct BerlinModQueryLicencesData {}

impl BerlinModQueryLicencesData {
    /// Converts Datatypes in a RecordBatch:
    /// - licence_id(Int32) -> Int64
    /// - fields licence cloned
    /// - moid(Int32) -> Int64
    /// Only includes fields from 'unwrapped_projection'.
    pub fn transform_batch_from_postgres(
        batch: &RecordBatch,
        unwrapped_projection: &Vec<usize>,
    ) -> Result<Vec<ArrayRef>, DataFusionError> {
        let mut columns: Vec<ArrayRef> = Vec::new();
        let mut field_index = 0;

        // If the projection contains the licence_id
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

        // If the projection contains moid
        if unwrapped_projection.contains(&2) {
            let array = batch.column(field_index);
            let casted_array = arrow::compute::cast(array, &arrow::datatypes::DataType::Int64)?;
            columns.push(casted_array);
        }

        Ok(columns)
    }
}

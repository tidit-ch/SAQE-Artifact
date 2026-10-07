//! This module implements the trajectory during function.
//! The 'during' UDF checks if a given polyline is entirely contained within a specified time period.
//! //! It takes three arguments:
//! 1. The polyline, as a trajectory (geoarrow layout, PostGIS EWKB, or WKT text)
//! 2. A 'Timestamp' representing the start of the period.
//! 3. A 'Timestamp' representing the end of the period.
//! The function returns a Boolean indicating for each trajectory whether
//! its first timestamp is after the start of the period and its last timestamp
//! is before the end of the period.

use crate::core::utils::trajectory_arg::{as_trajectory_array, coerce_udf_args, UdfArg};
use arrow::array::{as_list_array, as_primitive_array, ArrayRef, BooleanArray};
use arrow::datatypes::TimestampMillisecondType;
use arrow_schema::{DataType, TimeUnit};
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    scalar_doc_sections::DOC_SECTION_OTHER, ColumnarValue, Documentation, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, Volatility,
};
use geo_traits::{CoordTrait, LineStringTrait};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use std::any::Any;
use std::sync::{Arc, LazyLock};

use crate::core::utils::geo_utils::generic_list_array_to_geo_linestring_array;
use geoarrow_schema::Dimension;

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct During {
    signature: Signature,
}

impl During {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

static DURING_UDF_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Checks if a trajectory is entirely contained within a time period, \
         i.e. the trajectory's first timestamp is after the period start and \
         its last timestamp is before the period end.",
        "during(trajectory: List<Struct{x: Float64, y: Float64, m: Float64}> | Binary | Utf8, start_timestamp: Timestamp(Millisecond), end_timestamp: Timestamp(Millisecond)) -> Boolean",
    )
    .with_argument("trajectory", "The trajectory, as the geoarrow layout, as PostGIS EWKB (Binary), or as WKT text (Utf8).")
    .with_argument("start_timestamp", "Start of the temporal range as a Timestamp(Millisecond).")
    .with_argument("end_timestamp", "End of the temporal range as a Timestamp(Millisecond).")
    .with_sql_example("SELECT during(trajectory_from_text('LINESTRING M(0 0 500, 1 1 1000)'), TIMESTAMP '1970-01-01 00:00:00', TIMESTAMP '1970-01-01 00:00:02')")
    .build()
});

/// Implement the ScalarUDFImpl trait for During
impl ScalarUDFImpl for During {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "during"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&DURING_UDF_DOC)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_udf_args(
            self.name(),
            arg_types,
            &[
                UdfArg::Trajectory,
                UdfArg::Exact(DataType::Timestamp(TimeUnit::Millisecond, None)),
                UdfArg::Exact(DataType::Timestamp(TimeUnit::Millisecond, None)),
            ],
        )
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // `args` is shadowed below, so keep the field description first.
        let trajectory_input_field = args.arg_fields[0].clone();
        let args = ColumnarValue::values_to_arrays(&args.args)?;
        let (trajectory_0, _) = as_trajectory_array(&args[0], &trajectory_input_field)?;
        let trajectory_array = as_list_array(&trajectory_0);
        let start_array = as_primitive_array::<TimestampMillisecondType>(&args[1]);
        let end_array = as_primitive_array::<TimestampMillisecondType>(&args[2]);

        let trajectory_array =
            generic_list_array_to_geo_linestring_array(trajectory_array, Dimension::XYM)?;

        if start_array.len() != trajectory_array.len() || end_array.len() != trajectory_array.len()
        {
            return Err(DataFusionError::Execution(format!(
                "Length mismatch: polylines = {}, start = {}, end = {}",
                trajectory_array.len(),
                start_array.len(),
                end_array.len()
            )));
        }

        let mut column_result = vec![false; trajectory_array.len()];

        for (i, trajectory) in trajectory_array.iter().enumerate() {
            if let Some(Ok(trajectory)) = trajectory {
                let num_coords = trajectory.num_coords();
                if num_coords > 0 {
                    let first_timestamp = trajectory.coord(0).unwrap().nth(2);
                    let last_timestamp = trajectory.coord(num_coords - 1).unwrap().nth(2);
                    if first_timestamp.is_some()
                        && last_timestamp.is_some()
                        && first_timestamp.unwrap() > start_array.value(i) as f64
                        && last_timestamp.unwrap() < end_array.value(i) as f64
                    {
                        column_result[i] = true;
                    }
                }
            }
        }

        let column_result = BooleanArray::from(column_result);
        Ok(ColumnarValue::from(Arc::new(column_result) as ArrayRef))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::udf::utils::line::TrajectoryFromText;
    use arrow::array::as_boolean_array;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn test_during_udf() {
        let ctx = SessionContext::new();
        ctx.register_udf(TrajectoryFromText::default().into());
        ctx.register_udf(During::new().into());

        // Test case where the trajectory is during the specified period
        let linestring_wkt = "LINESTRING M (0 0 1000, 1 1 2000, 2 2 3000)";
        let sql = format!(
            "SELECT during(
                trajectory_from_text('{linestring_wkt}'),
                arrow_cast(500, 'Timestamp(Millisecond, None)'),
                arrow_cast(3500, 'Timestamp(Millisecond, None)')
            ) AS is_during"
        );
        let df = ctx.sql(&sql).await.unwrap();
        let result = df.collect().await.unwrap();
        let result = as_boolean_array(result[0].column(0));
        assert_eq!(result.len(), 1);
        assert_eq!(result.value(0), true);

        // Test case where the trajectory is not during the specified period
        let sql = format!(
            "SELECT during(
                trajectory_from_text('{linestring_wkt}'),
                arrow_cast(1500, 'Timestamp(Millisecond, None)'),
                arrow_cast(2500, 'Timestamp(Millisecond, None)')
            ) AS is_during"
        );
        let df = ctx.sql(&sql).await.unwrap();
        let result = df.collect().await.unwrap();
        let result = as_boolean_array(result[0].column(0));
        assert_eq!(result.len(), 1);
        assert_eq!(result.value(0), false);
    }

    /// The trajectory argument is accepted in every encoding, and a non-trajectory is rejected.
    #[tokio::test]
    async fn accepts_trajectory_encodings() {
        crate::core::utils::trajectory_arg::test_support::accepts_trajectory_encodings(
            "during({t}, arrow_cast(TIMESTAMP '1970-01-01 00:00:00', 'Timestamp(Millisecond, None)'), arrow_cast(TIMESTAMP '1970-01-01 00:00:10', 'Timestamp(Millisecond, None)'))",
        )
        .await;
    }
}

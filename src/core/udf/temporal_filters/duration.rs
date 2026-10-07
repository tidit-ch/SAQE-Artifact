//! This module implements the duration function.
//! This function calculates the duration of a polyline based on the timestamps of its points.
//!
//! Input:
//! A trajectory (geoarrow layout, PostGIS EWKB, or WKT text)
//!
//! Output:
//! A Duration in milliseconds representing the time difference between the first and last timestamp of the polyline.

use crate::core::utils::trajectory_arg::{as_trajectory_array, coerce_udf_args, UdfArg};
use arrow::array::{as_list_array, ArrayRef, DurationMillisecondArray};
use arrow_schema::{DataType, TimeUnit};
use datafusion::common::Result;
use datafusion::logical_expr::{
    scalar_doc_sections::DOC_SECTION_OTHER, ColumnarValue, Documentation, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, Volatility,
};
use geo_traits::{CoordTrait, LineStringTrait};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::Dimension;
use std::any::Any;
use std::sync::{Arc, LazyLock};

use crate::core::utils::geo_utils::generic_list_array_to_geo_linestring_array;

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct Duration {
    signature: Signature,
}

impl Duration {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

static DURATION_UDF_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Calculates the duration of a trajectory in milliseconds, as the time difference between its first and last timestamp.",
        "duration(trajectory: List<Struct{x: Float64, y: Float64, m: Float64}> | Binary | Utf8) -> Duration(Millisecond)",
    )
    .with_argument("trajectory", "The trajectory, as the geoarrow layout, as PostGIS EWKB (Binary), or as WKT text (Utf8).")
    .with_sql_example("SELECT duration(trajectory_from_text('LINESTRING M(0 0 0, 1 1 1000, 2 2 5000)'))")
    .build()
});

/// Implement the ScalarUDFImpl trait for Duration
impl ScalarUDFImpl for Duration {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "duration"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&DURATION_UDF_DOC)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_udf_args(self.name(), arg_types, &[UdfArg::Trajectory])
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Duration(TimeUnit::Millisecond))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // `args` is shadowed below, so keep the field description first.
        let trajectory_input_field = args.arg_fields[0].clone();
        let args = ColumnarValue::values_to_arrays(&args.args)?;
        let (trajectory_0, _) = as_trajectory_array(&args[0], &trajectory_input_field)?;
        let trajectory_array: &arrow::array::GenericListArray<i32> = as_list_array(&trajectory_0);
        let trajectory_array =
            generic_list_array_to_geo_linestring_array(trajectory_array, Dimension::XYM)?;

        let mut durations: Vec<Option<i64>> = vec![None; trajectory_array.len()];

        for (i, trajectory) in trajectory_array.iter().enumerate() {
            if let Some(Ok(trajectory)) = trajectory {
                let coords_len = trajectory.num_coords();
                if coords_len > 0 {
                    let first_coord = trajectory.coord(0).unwrap().nth(2);
                    let last_coord = trajectory.coord(coords_len - 1).unwrap().nth(2);
                    if first_coord.is_some() && last_coord.is_some() {
                        let start = first_coord.unwrap();
                        let end = last_coord.unwrap();
                        let duration = end - start;
                        durations[i] = Some(duration as i64);
                    }
                }
            }
        }

        let duration_array = DurationMillisecondArray::from(durations);
        Ok(ColumnarValue::from(Arc::new(duration_array) as ArrayRef))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::udf::utils::line::TrajectoryFromText;
    use arrow::array::{downcast_array, DurationMillisecondArray};
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn test_duration_udf() {
        let ctx = SessionContext::new();
        ctx.register_udf(TrajectoryFromText::default().into());
        ctx.register_udf(Duration::new().into());

        let sql = "SELECT duration(trajectory_from_text('LINESTRING M (0 0 1000, 1 1 5000)'))";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        let result = downcast_array::<DurationMillisecondArray>(result[0].column(0));
        assert_eq!(result.value(0), 4000);
    }

    /// The trajectory argument is accepted in every encoding, and a non-trajectory is rejected.
    #[tokio::test]
    async fn accepts_trajectory_encodings() {
        crate::core::utils::trajectory_arg::test_support::accepts_trajectory_encodings(
            "duration({t})",
        )
        .await;
    }
}

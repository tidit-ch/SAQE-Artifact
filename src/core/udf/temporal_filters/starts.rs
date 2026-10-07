//! This module implements the starts predicate.

use crate::core::utils::trajectory_arg::{as_trajectory_array, coerce_udf_args, UdfArg};
use crate::utils::error::geo_arrow_error_to_datafusion_error;
use arrow::array::builder::BooleanBuilder;
use arrow_schema::DataType;

use datafusion::{
    common::{cast::as_int64_array, Result},
    error::DataFusionError,
    logical_expr::{
        scalar_doc_sections::DOC_SECTION_OTHER, ColumnarValue, Documentation, ScalarFunctionArgs,
        ScalarUDFImpl, Signature, Volatility,
    },
};
use geo_traits::{CoordTrait, LineStringTrait};
use geoarrow_array::{
    array::from_arrow_array, cast::AsGeoArrowArray, GeoArrowArray, GeoArrowArrayAccessor,
};
use std::any::Any;
use std::sync::{Arc, LazyLock};

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct Starts {
    signature: Signature,
}

impl Starts {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

static STARTS_UDF_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Checks if a trajectory starts a time period, i.e. the trajectory's start equals the period start but the trajectory's end is before the period end.",
        "starts(trajectory: List<Struct{x: Float64, y: Float64, m: Float64}> | Binary | Utf8, start_timestamp: Int64, end_timestamp: Int64) -> Boolean",
    )
    .with_argument("trajectory", "The trajectory, as the geoarrow layout, as PostGIS EWKB (Binary), or as WKT text (Utf8).")
    .with_argument("start_timestamp", "Start of the temporal range as an Int64 Unix timestamp.")
    .with_argument("end_timestamp", "End of the temporal range as an Int64 Unix timestamp.")
    .with_sql_example("SELECT starts(trajectory_from_text('LINESTRING M(0 0 0, 1 1 500)'), 0, 1000)")
    .build()
});

impl ScalarUDFImpl for Starts {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "starts"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&STARTS_UDF_DOC)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_udf_args(
            self.name(),
            arg_types,
            &[
                UdfArg::Trajectory,
                UdfArg::Exact(DataType::Int64),
                UdfArg::Exact(DataType::Int64),
            ],
        )
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let (trajectory_0, trajectory_0_field) =
            as_trajectory_array(&arrays[0], &args.arg_fields[0])?;
        let trajectory_array = from_arrow_array(&trajectory_0, &trajectory_0_field)
            .map_err(geo_arrow_error_to_datafusion_error)?;
        let trajectory_array = trajectory_array.as_line_string();

        let arg_start_timestamp = as_int64_array(&arrays[1])?.value(0);
        let arg_end_timestamp = as_int64_array(&arrays[2])?.value(0);

        if arg_end_timestamp < arg_start_timestamp {
            return Err(DataFusionError::Execution(format!("Invalid time interval: start_timestamp ({}) must be less than or equal to end_timestamp ({})", arg_start_timestamp, arg_end_timestamp)));
        }

        let mut result_builder = BooleanBuilder::with_capacity(trajectory_array.len());

        for trajectory in trajectory_array.iter() {
            if let Some(Ok(trajectory)) = trajectory {
                let num_coords = trajectory.num_coords();
                if num_coords == 0 {
                    result_builder.append_value(false);
                    continue;
                }
                let start_timestamp = trajectory.coord(0).unwrap().nth(2).unwrap() as i64;
                let end_timestamp =
                    trajectory.coord(num_coords - 1).unwrap().nth(2).unwrap() as i64;
                let result =
                    start_timestamp == arg_start_timestamp && arg_end_timestamp > end_timestamp;
                result_builder.append_value(result);
            } else {
                result_builder.append_null();
            }
        }

        Ok(ColumnarValue::Array(Arc::new(result_builder.finish())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::udf::utils::line::TrajectoryFromText;
    use arrow::array::as_boolean_array;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn test_starts_udf() {
        let ctx = SessionContext::new();
        ctx.register_udf(TrajectoryFromText::default().into());
        ctx.register_udf(Starts::new().into());

        let sql = format!(
            "
            SELECT starts(
                trajectory_from_text(wkt),
                1000,
                4000 
            )
            FROM (
                VALUES
                    ('LINESTRING M (0 0 1000, 1 1 2000, 2 2 3000)'),
                    ('LINESTRING M (0 0 1000, 1 1 2000, 2 2 4000)'),
                    ('LINESTRING M (0 0 500, 1 1 200, 2 2 4500)'),
                    ('LINESTRING M (0 0 1500, 1 1 2000, 2 2 2500)'),
                    ('LINESTRING M (0 0 200, 1 1 400, 2 2 800)'),
                    ('LINESTRING M (0 0 5000, 1 1 6000, 2 2 7000)'),
                    ('LINESTRING M EMPTY')
            ) AS t(wkt)
        "
        );
        let df = ctx.sql(&sql).await.unwrap();
        let result = df.collect().await.unwrap();
        let result = as_boolean_array(result[0].column(0));
        assert_eq!(result.len(), 7);

        print!("result :: {:?}", result);

        let test_cases = vec![
            (
                true,
                "Trajectory starts with the interval and ends before it",
            ),
            (
                false,
                "Trajectory overlaps the interval, starting and ending within it",
            ),
            (
                false,
                "Trajectory starts before the interval and ends after it",
            ),
            (
                false,
                "Trajectory starts after the interval and ends before it",
            ),
            (false, "Trajectory starts and ends before the interval"),
            (
                false,
                "Trajectory starts after the interval and ends after it",
            ),
            (false, "Empty trajectory"),
        ];

        for (i, (expected, msg)) in test_cases.iter().enumerate() {
            assert_eq!(result.value(i), *expected, "{}", msg);
        }
    }

    /// The trajectory argument is accepted in every encoding, and a non-trajectory is rejected.
    #[tokio::test]
    async fn accepts_trajectory_encodings() {
        crate::core::utils::trajectory_arg::test_support::accepts_trajectory_encodings(
            "starts({t}, 1000, 3000)",
        )
        .await;
    }
}

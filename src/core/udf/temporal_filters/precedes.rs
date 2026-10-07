//! This module implements the precedes predicate.

use crate::core::utils::trajectory_arg::{as_trajectory_array, coerce_udf_args, UdfArg};
use crate::utils::error::geo_arrow_error_to_datafusion_error;
use arrow::array::builder::BooleanBuilder;
use arrow_schema::DataType;
use datafusion::{
    common::{cast::as_int64_array, Result},
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
pub struct Precedes {
    signature: Signature,
}

impl Precedes {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

static PRECEDES_UDF_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Checks if a trajectory precedes a timestamp, i.e. the trajectory's last timestamp is before the given timestamp.",
        "precedes(trajectory: List<Struct{x: Float64, y: Float64, m: Float64}> | Binary | Utf8, timestamp: Int64) -> Boolean",
    )
    .with_argument("trajectory", "The trajectory, as the geoarrow layout, as PostGIS EWKB (Binary), or as WKT text (Utf8).")
    .with_argument("timestamp", "A Unix timestamp (Int64) to check against the trajectory's end time.")
    .with_sql_example("SELECT precedes(trajectory_from_text('LINESTRING M(0 0 0, 1 1 500)'), 1000)")
    .build()
});

impl ScalarUDFImpl for Precedes {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "precedes"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&PRECEDES_UDF_DOC)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_udf_args(
            self.name(),
            arg_types,
            &[UdfArg::Trajectory, UdfArg::Exact(DataType::Int64)],
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

        let mut result_builder = BooleanBuilder::with_capacity(trajectory_array.len());

        for trajectory in trajectory_array.iter() {
            if let Some(Ok(trajectory)) = trajectory {
                let num_coords = trajectory.num_coords();
                if num_coords == 0 {
                    result_builder.append_value(false);
                    continue;
                }
                let end_timestamp =
                    trajectory.coord(num_coords - 1).unwrap().nth(2).unwrap() as i64;
                result_builder.append_value(end_timestamp < arg_start_timestamp);
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
    async fn test_precedes_udf() {
        let ctx = SessionContext::new();
        ctx.register_udf(TrajectoryFromText::default().into());
        ctx.register_udf(Precedes::new().into());

        let sql = format!(
            "
            SELECT precedes(
                trajectory_from_text(wkt),
                3500
            )
            FROM (
                VALUES
                    ('LINESTRING M (0 0 1000, 1 1 2000, 2 2 3000)'),
                    ('LINESTRING M (0 0 1000, 1 1 2000, 2 2 4000)'),
                    ('LINESTRING M (0 0 4000, 1 1 4500, 2 2 5000)'),
                    ('LINESTRING M (0 0 1000, 1 1 2000, 2 2 3500)'),
                    ('LINESTRING M EMPTY')
            ) AS t(wkt)
        "
        );
        let df = ctx.sql(&sql).await.unwrap();
        let result = df.collect().await.unwrap();
        let result = as_boolean_array(result[0].column(0));
        assert_eq!(result.len(), 5);

        let test_cases = vec![
            (true, "Trajectory ends before the timestamp"),
            (false, "Trajectory ends after the timestamp"),
            (false, "Trajectory starts after the timestamp"),
            (false, "Trajectory ends at the same time as the timestamp"),
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
            "precedes({t}, 4000)",
        )
        .await;
    }
}

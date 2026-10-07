//! This module implements the trajectory is present function.
//! Checks if a trajectory is present during a timestamp
//! The function takes two arguments: a trajectory (geoarrow layout, PostGIS EWKB, or WKT text) and a timestamp value.

use crate::core::utils::trajectory_arg::{as_trajectory_array, coerce_udf_args, UdfArg};
use arrow::array::{as_list_array, as_primitive_array, builder::BooleanBuilder, ArrayRef};
use arrow::datatypes::TimestampMillisecondType;
use arrow_schema::{DataType, TimeUnit};
use datafusion::common::Result;
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
pub struct Present {
    signature: Signature,
}

impl Present {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

static PRESENT_UDF_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Checks if a trajectory is present at a given timestamp, i.e. the timestamp falls \
         within the trajectory's time range (between first and last timestamp, inclusive).",
        "present(trajectory: List<Struct{x: Float64, y: Float64, m: Float64}> | Binary | Utf8, instant: Timestamp(Millisecond)) -> Boolean",
    )
    .with_argument("trajectory", "The trajectory, as the geoarrow layout, as PostGIS EWKB (Binary), or as WKT text (Utf8).")
    .with_argument("instant", "A Timestamp(Millisecond) to check against the trajectory's time range.")
    .with_sql_example("SELECT present(trajectory_from_text('LINESTRING M(0 0 0, 1 1 1000)'), TIMESTAMP '1970-01-01 00:00:00.500')")
    .build()
});

/// Implementation of the present user-defined-function
impl ScalarUDFImpl for Present {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "present"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&PRESENT_UDF_DOC)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_udf_args(
            self.name(),
            arg_types,
            &[
                UdfArg::Trajectory,
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

        // Argument 0: polyline (List<Struct<x, y, timestamp>>)
        let trajectory_array = generic_list_array_to_geo_linestring_array(
            as_list_array(&trajectory_0),
            Dimension::XYM,
        )?;
        // Argument 1: instant (Timestamp)
        let instant_array = as_primitive_array::<TimestampMillisecondType>(&args[1]);

        let mut result = BooleanBuilder::with_capacity(trajectory_array.len());

        for (i, trajectory) in trajectory_array.iter().enumerate() {
            if let Some(Ok(trajectory)) = trajectory {
                let coords_len = trajectory.num_coords();
                if coords_len > 0 {
                    let start_timestamp = trajectory.coord(0).unwrap().nth(2);
                    let end_timestamp = trajectory.coord(coords_len - 1).unwrap().nth(2);
                    if start_timestamp.is_some()
                        && end_timestamp.is_some()
                        && start_timestamp.unwrap() <= instant_array.value(i) as f64
                        && end_timestamp.unwrap() >= instant_array.value(i) as f64
                    {
                        result.append_value(true);
                        continue;
                    }
                }
            }
            result.append_value(false);
        }

        Ok(ColumnarValue::Array(Arc::new(result.finish()) as ArrayRef))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::udf::utils::line::TrajectoryFromText;
    use arrow::array::as_boolean_array;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn test_present_udf() {
        let ctx = SessionContext::new();
        ctx.register_udf(TrajectoryFromText::default().into());
        ctx.register_udf(Present::new().into());

        let sql = format!(
            "
            SELECT present(
                trajectory_from_text(wkt),
                arrow_cast(timestamp, 'Timestamp(Millisecond, None)')
            )
            FROM (
                VALUES
                    ('LINESTRING M (0 0 1000, 1 1 1500, 2 2 2000)', 1200),
                    ('LINESTRING M (0 0 1000, 1 1 1500, 2 2 2000)', 1000),
                    ('LINESTRING M (0 0 1000, 1 1 1500, 2 2 2000)', 2000),
                    ('LINESTRING M (0 0 1000, 1 1 1500, 2 2 2000)', 500),
                    ('LINESTRING M (0 0 1000, 1 1 1500, 2 2 2000)', 2500),
                    ('LINESTRING M EMPTY', 1200)
            ) AS t(wkt, timestamp)
        "
        );
        let df = ctx.sql(&sql).await.unwrap();
        let result = df.collect().await.unwrap();
        let result = as_boolean_array(result[0].column(0));
        assert_eq!(result.len(), 6);

        let test_cases = vec![
            (true, "During the trajectory"),
            (true, "At the start instant of the trajectory"),
            (true, "At the end instant of the trajectory"),
            (false, "Before the trajectory"),
            (false, "After the trajectory"),
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
            "present({t}, arrow_cast(TIMESTAMP '1970-01-01 00:00:02', 'Timestamp(Millisecond, None)'))",
        )
        .await;
    }
}

//! tdwithin` function
//!
//! Checks if two trajectories are "within" a given spatial tolerance
//! during a shared minute of time.
//!
//! # Arguments
//! - polyline: A list of points (Structs with x, y, and timestamp in milliseconds) representing a trajectory.
//! - polyline: A list of points (Structs with x, y, and timestamp in milliseconds) representing a trajectory.
//! - tolerance: A Float64 value representing the maximum allowed spatial distance (in meter).
//! - precision: A Utf8 string specifying the time granularity for comparison.
//!   Valid values include "year", "day", "hour", "minute", "second".
//!
//! # Returns
//! - Boolean: True if any point pair from the trajectories is within the tolerance
//!   and shares the same year, day, hour, and minute;
//!   -> False otherwise.

use crate::core::utils::geo_utils;
use crate::core::utils::trajectory_arg::{as_trajectory_array, coerce_udf_args, UdfArg};
use crate::utils::error::geo_arrow_error_to_datafusion_error;
use arrow::array::{as_primitive_array, Array, BooleanBuilder, StringArray};
use arrow::datatypes::{DataType, Float64Type};
use chrono::{DateTime, Datelike, Timelike, Utc};
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    scalar_doc_sections::DOC_SECTION_OTHER, ColumnarValue, Documentation, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, Volatility,
};
use geo::{Distance, Euclidean};
use geo_traits::{CoordTrait, LineStringTrait};
use geoarrow_array::{
    array::from_arrow_array, cast::AsGeoArrowArray, GeoArrowArray, GeoArrowArrayAccessor,
};
use std::any::Any;
use std::sync::{Arc, LazyLock};

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct Tdwithin {
    signature: Signature,
}

impl Tdwithin {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

static TDWITHIN_UDF_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Checks if two trajectories are within a given spatial tolerance during a shared unit of time. \
         Returns true if any pair of points (one from each trajectory) are within the tolerance distance \
         and share the same time granularity (e.g. same minute, same hour).",
        "tdwithin(trajectory1: List<Struct{x: Float64, y: Float64, m: Float64}> | Binary | Utf8, trajectory2: List<Struct{x: Float64, y: Float64, m: Float64}> | Binary | Utf8, tolerance: Float64, precision: Utf8) -> Boolean",
    )
    .with_argument("trajectory1", "The first trajectory, as the geoarrow layout, as PostGIS EWKB (Binary), or as WKT text (Utf8).")
    .with_argument("trajectory2", "The second trajectory, as the geoarrow layout, as PostGIS EWKB (Binary), or as WKT text (Utf8).")
    .with_argument("tolerance", "Maximum allowed spatial distance (Float64) in meters.")
    .with_argument("precision", "Time granularity for comparison (Utf8). Valid values: 'year', 'day', 'hour', 'minute', 'second'.")
    .with_sql_example("SELECT tdwithin(trajectory_from_text('LINESTRING M(0 0 1000, 1 1 2000)'), trajectory_from_text('LINESTRING M(0.5 0.5 1000, 1.5 1.5 2000)'), 100.0, 'minute')")
    .build()
});

impl ScalarUDFImpl for Tdwithin {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "tdwithin"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&TDWITHIN_UDF_DOC)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_udf_args(
            self.name(),
            arg_types,
            &[
                UdfArg::Trajectory,
                UdfArg::Trajectory,
                UdfArg::Exact(DataType::Float64),
                UdfArg::Exact(DataType::Utf8),
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
        let (trajectory_1, trajectory_1_field) =
            as_trajectory_array(&arrays[1], &args.arg_fields[1])?;
        let trajectory_array_1 = from_arrow_array(&trajectory_0, &trajectory_0_field)
            .map_err(geo_arrow_error_to_datafusion_error)?;
        let trajectory_array_1 = trajectory_array_1.as_line_string();
        let trajectory_array_2 = from_arrow_array(&trajectory_1, &trajectory_1_field)
            .map_err(geo_arrow_error_to_datafusion_error)?;
        let trajectory_array_2 = trajectory_array_2.as_line_string();

        let tolerance = as_primitive_array::<Float64Type>(&arrays[2]);
        let precision_arr = arrays[3]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("Expected StringArray for precision".to_string())
            })?;

        let mut builder = BooleanBuilder::new();

        'outer: for i in 0..trajectory_array_1.len() {
            let trajectory_1 = trajectory_array_1
                .value(i)
                .map_err(geo_arrow_error_to_datafusion_error)?;
            let trajectory_2 = trajectory_array_2
                .value(i)
                .map_err(geo_arrow_error_to_datafusion_error)?;

            let tolerance_value = geo_utils::meters_to_degrees(tolerance.value(i));
            let precision = precision_arr.value(i);

            for idx1 in 0..trajectory_1.num_coords() {
                let coord1 = trajectory_1.coord(idx1).unwrap();

                for idx2 in 0..trajectory_2.num_coords() {
                    let coord2 = trajectory_2.coord(idx2).unwrap();

                    let dt1 = DateTime::<Utc>::from_timestamp_millis(coord1.nth(2).unwrap() as i64)
                        .ok_or_else(|| DataFusionError::Execution("Invalid timestamp".into()))?;
                    let dt2 = DateTime::<Utc>::from_timestamp_millis(coord2.nth(2).unwrap() as i64)
                        .ok_or_else(|| DataFusionError::Execution("Invalid timestamp".into()))?;
                    let same = match precision {
                        "year" => dt1.year() == dt2.year(),
                        "day" => dt1.year() == dt2.year() && dt1.ordinal() == dt2.ordinal(),
                        "hour" => {
                            dt1.year() == dt2.year()
                                && dt1.ordinal() == dt2.ordinal()
                                && dt1.hour() == dt2.hour()
                        }
                        "minute" => {
                            dt1.year() == dt2.year()
                                && dt1.ordinal() == dt2.ordinal()
                                && dt1.hour() == dt2.hour()
                                && dt1.minute() == dt2.minute()
                        }
                        "second" => dt1.timestamp() == dt2.timestamp(),
                        _ => {
                            return Err(DataFusionError::Execution(format!(
                                "Unsupported precision: {}",
                                precision
                            )))
                        }
                    };

                    if same {
                        let p1 = geo::Point::new(coord1.x(), coord1.y());
                        let p2 = geo::Point::new(coord2.x(), coord2.y());
                        let dist = Euclidean.distance(p1, p2);
                        if dist <= tolerance_value {
                            builder.append_value(true);
                            continue 'outer;
                        }
                    }
                }
            }
            builder.append_value(false);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::udf::utils::line::TrajectoryFromText;
    use arrow::array::as_boolean_array;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn test_tdwithin() {
        let ctx = SessionContext::new();
        ctx.register_udf(TrajectoryFromText::default().into());
        ctx.register_udf(Tdwithin::new().into());

        let sql = format!(
            "
            SELECT tdwithin(
                trajectory_from_text(t1),
                trajectory_from_text(t2),
                tolerance_value,
                precision
            )
            FROM (
                VALUES
                    ('LINESTRING M(0 0 0, 0 2 1000)', 'LINESTRING M(1 0 0, 2 0 1000)', 100000, 'second'),
                    ('LINESTRING M(0 0 0, 0 2 1000)', 'LINESTRING M(1 0 0, 2 0 1000)', 50000, 'second'),
                    ('LINESTRING M(0 0 0, 0 2 1000)', 'LINESTRING M(1 0 2000, 2 0 3000)', 100000, 'minute'),
                    ('LINESTRING M(0 0 0, 0 2 1000)', 'LINESTRING M(1 0 2000, 2 0 3000)', 100000, 'second')
            ) AS t(t1, t2, tolerance_value, precision)
        "
        );

        let df = ctx.sql(&sql).await.unwrap();
        let result = df.collect().await.unwrap();
        let result = as_boolean_array(result[0].column(0));
        let test_cases = vec![
            (true, "Test Case 1: Within tolerance and same second"),
            (false, "Test Case 2: Exceeds tolerance"),
            (true, "Test Case 3: Within tolerance and same minute"),
            (false, "Test Case 4: Within tolerance but different second"),
        ];
        for (i, (expected, description)) in test_cases.iter().enumerate() {
            let actual = result.value(i);
            assert_eq!(
                actual, *expected,
                "{} failed: expected {}, got {}",
                description, expected, actual
            );
        }
    }

    /// The trajectory argument is accepted in every encoding, and a non-trajectory is rejected.
    #[tokio::test]
    async fn accepts_trajectory_encodings() {
        crate::core::utils::trajectory_arg::test_support::accepts_trajectory_encodings(
            "tdwithin({t}, {t}, 100.0, 'second')",
        )
        .await;
    }
}

//! This module implements the 'timestamp_at_position' function for trajectories.
//!
//! The function returns the timestamp of the polyline at a given point.
//!
//! It accepts two arguments:
//! 1. polyline: A list of points (Structs with x, y, and timestamp in milliseconds) representing a trajectory.
//! 2. point: A struct with x and y coordinates representing the target point.
//!
//! The function returns a TimestampMillisecond.
//!
//! If the target point lies exactly on one of the polyline points, the corresponding timestamp is returned.
//! If the point lies between two polyline points, linear interpolation is used:
//! ```text
//!     t = (1 - alpha) * t_k + alpha * t_{k+1}
//!     where alpha = ||q - p_k|| / ||p_{k+1} - p_k||
//! ```
//!
//! q is the target point, and p_k, p_{k+1} are the closest segment endpoints in the polyline.

use crate::core::utils::trajectory_arg::{as_trajectory_array, coerce_udf_args, UdfArg};
use arrow::array::builder::TimestampMillisecondBuilder;
use arrow_schema::{DataType, TimeUnit};
use datafusion::common::Result;
use datafusion::logical_expr::{
    scalar_doc_sections::DOC_SECTION_OTHER, ColumnarValue, Documentation, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, Volatility,
};
use geo::{Contains, Distance, Euclidean, Point};
use geo_traits::{CoordTrait, LineStringTrait, PointTrait};
use geoarrow_array::{
    array::from_arrow_array, cast::AsGeoArrowArray, GeoArrowArray, GeoArrowArrayAccessor,
};
use std::any::Any;
use std::sync::{Arc, LazyLock};

use crate::core::utils::schema::POINT_XY_DATATYPE;
use crate::utils::error::geo_arrow_error_to_datafusion_error;

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct TimestampAtPosition {
    signature: Signature,
}

impl TimestampAtPosition {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

static TIMESTAMP_AT_POSITION_UDF_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Returns the timestamp at a given spatial position on a trajectory. \
         If the point lies exactly on a polyline vertex, that vertex's timestamp is returned. \
         If the point lies between two vertices, linear interpolation is used.",
        "timestamp_at_position(trajectory: List<Struct{x: Float64, y: Float64, m: Float64}> | Binary | Utf8, point: Struct{x: Float64, y: Float64}) -> Timestamp(Millisecond)",
    )
    .with_argument("trajectory", "The trajectory, as the geoarrow layout, as PostGIS EWKB (Binary), or as WKT text (Utf8).")
    .with_argument("point", "A Struct with x and y coordinates representing the target position.")
    .with_sql_example("SELECT timestamp_at_position(trajectory_from_text('LINESTRING M(0 0 0, 10 10 1000)'), ROW(5.0, 5.0))")
    .build()
});

/// Implement the ScalarUDFImpl trait for the timestamp_at_position predicate
impl ScalarUDFImpl for TimestampAtPosition {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "timestamp_at_position"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&TIMESTAMP_AT_POSITION_UDF_DOC)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_udf_args(
            self.name(),
            arg_types,
            &[UdfArg::Trajectory, UdfArg::Exact(POINT_XY_DATATYPE.clone())],
        )
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let (trajectory_0, trajectory_0_field) =
            as_trajectory_array(&arrays[0], &args.arg_fields[0])?;

        let trajectory_array = from_arrow_array(&trajectory_0, &trajectory_0_field)
            .map_err(geo_arrow_error_to_datafusion_error)?;
        let trajectory_array = trajectory_array.as_line_string();

        let point_array = from_arrow_array(&arrays[1], &args.arg_fields[1])
            .map_err(geo_arrow_error_to_datafusion_error)?;
        let point_array = point_array.as_point();

        let mut result_builder = TimestampMillisecondBuilder::with_capacity(trajectory_array.len());

        'outer: for (traj_idx, trajectory) in trajectory_array.iter().enumerate() {
            let target_point = point_array.value(traj_idx);
            if target_point.is_err() {
                result_builder.append_null();
                continue;
            }
            if let Some(Ok(trajectory)) = trajectory {
                let target_coord = target_point.unwrap().coord().unwrap();
                let target_point = Point::new(target_coord.x(), target_coord.y());
                if trajectory.num_coords() == 1 {
                    let coord = trajectory.coord(0).unwrap();
                    if coord.x() == target_point.x() && coord.y() == target_point.y() {
                        let time = trajectory.coord(0).unwrap().nth(2).unwrap();
                        result_builder.append_value(time as i64);
                        continue 'outer;
                    }
                }
                for j in 0..trajectory.num_coords() - 1 {
                    let start_coord = trajectory.coord(j).unwrap();
                    let end_coord = trajectory.coord(j + 1).unwrap();
                    let start_time = trajectory.coord(j).unwrap().nth(2).unwrap();
                    let end_time = trajectory.coord(j + 1).unwrap().nth(2).unwrap();
                    if start_coord.x() == target_point.x() && start_coord.y() == target_point.y() {
                        result_builder.append_value(start_time as i64);
                        continue 'outer;
                    } else if end_coord.x() == target_point.x() && end_coord.y() == target_point.y()
                    {
                        result_builder.append_value(end_time as i64);
                        continue 'outer;
                    } else {
                        let line = geo::Line::new(
                            Point::new(start_coord.x(), start_coord.y()),
                            Point::new(end_coord.x(), end_coord.y()),
                        );
                        if line.contains(&target_point) {
                            let total_len = Euclidean.distance(
                                &Point::new(start_coord.x(), start_coord.y()),
                                &Point::new(end_coord.x(), end_coord.y()),
                            );
                            let partial_len = Euclidean.distance(
                                &Point::new(start_coord.x(), start_coord.y()),
                                &target_point,
                            );
                            if total_len > 0.0 {
                                let alpha = partial_len / total_len;
                                let t0 = trajectory.coord(j).unwrap().nth(2).unwrap() as f64;
                                let t1 = trajectory.coord(j + 1).unwrap().nth(2).unwrap() as f64;
                                let interp_ts = (1.0 - alpha) * t0 + alpha * t1;
                                result_builder.append_value(interp_ts.round() as i64);
                                continue 'outer;
                            }
                        }
                    }
                }
            }
            result_builder.append_null();
        }

        let result = result_builder.finish();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::udf::utils::line::TrajectoryFromText;
    use arrow::array::Array;
    use datafusion::{common::cast::as_timestamp_millisecond_array, prelude::SessionContext};
    use geodatafusion::udf::native::constructors::MakePoint;

    #[tokio::test]
    async fn test_timestamp_at_position_udf() {
        let ctx = SessionContext::new();
        ctx.register_udf(TrajectoryFromText::default().into());
        ctx.register_udf(MakePoint::default().into());
        ctx.register_udf(TimestampAtPosition::new().into());

        let test_cases = vec![
            (
                "LINESTRING M(1 1 1000, 3 3 3000, 5 5 5000)",
                "3, 3",
                Some(3000),
                "Exact match on a trajectory point",
            ),
            (
                "LINESTRING M(1 1 1000, 3 3 3000, 5 5 5000)",
                "2, 2",
                Some(2000),
                "Interpolation between two trajectory points",
            ),
            (
                "LINESTRING M(1 1 1000, 3 3 3000, 5 5 5000)",
                "8, 8",
                None,
                "Point outside the trajectory range",
            ),
        ];

        let sql = format!(
            "
                SELECT timestamp_at_position(
                    trajectory_from_text(trajectory_wkt),
                    st_makepoint(point_x, point_y)
                )
                FROM (VALUES {}) AS T(trajectory_wkt, point_x, point_y)
            ",
            test_cases
                .iter()
                .map(|(traj_wkt, coord, _, _)| format!("('{}', {})", traj_wkt, coord))
                .collect::<Vec<String>>()
                .join(", ")
        );

        let df = ctx.sql(&sql).await.unwrap();
        let result = df.collect().await.unwrap();
        let result = result[0].column(0);
        let result = as_timestamp_millisecond_array(result).unwrap();

        for (i, (_, _, expected_ts, description)) in test_cases.iter().enumerate() {
            if let Some(expected_ts) = expected_ts {
                assert!(
                    result.is_valid(i),
                    "Expected valid timestamp for test case {}: {}",
                    i,
                    description
                );
                let actual_ts = result.value(i);
                assert_eq!(
                    actual_ts, *expected_ts,
                    "Mismatch in timestamp for test case {}: {}",
                    i, description
                );
            } else {
                assert!(
                    result.is_null(i),
                    "Expected null timestamp for test case {}: {}",
                    i,
                    description
                );
            }
        }
    }
}

//! This module implements the trajectory passes point predicate.
//! Checks if a point is contained in a trajectory (polyline).
//! The function takes three arguments: a trajectory (geoarrow layout, PostGIS EWKB, or WKT text), a struct representing a point, and a tolerance value (in meter).
//! The function works by computing the shortest distance between the point and each segment
//! of the polyline. If any segment is within 'epsilon', the function returns true; otherwise, false.

use crate::core::utils::geo_utils;
use crate::core::utils::trajectory_arg::{as_trajectory_array, coerce_udf_args, UdfArg};

use arrow::array::{as_list_array, as_primitive_array, builder::BooleanBuilder, Array, ArrayRef};
use arrow::datatypes::Float64Type;
use arrow_schema::{DataType, Field, Fields};
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    scalar_doc_sections::DOC_SECTION_OTHER, ColumnarValue, Documentation, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, Volatility,
};
use geoarrow_array::{array::from_arrow_array, GeoArrowArray};
use geoarrow_expr_geo::euclidean_distance;

use std::{any::Any, sync::Arc, sync::LazyLock};

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct PassesPoint {
    signature: Signature,
}

impl PassesPoint {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

static PASSES_POINT_UDF_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Checks if a trajectory passes within a tolerance distance of a given point. \
         Returns true if the shortest distance between the point and any segment of the polyline is within the tolerance.",
        "passes_point(trajectory: List<Struct{x: Float64, y: Float64, m: Float64}> | Binary | Utf8, point: Struct{x: Float64, y: Float64}, tolerance: Float64) -> Boolean",
    )
    .with_argument("trajectory", "The trajectory, as the geoarrow layout, as PostGIS EWKB (Binary), or as WKT text (Utf8).")
    .with_argument("point", "A Struct with x and y coordinates representing the target point.")
    .with_argument("tolerance", "Maximum allowed distance (Float64) in meters.")
    .with_sql_example("SELECT passes_point(trajectory_from_text('LINESTRING M(0 0 0, 10 10 1000)'), ROW(5.0, 5.0), 1.0)")
    .build()
});

/// Implement the ScalarUDFImpl trait for the passes_point predicate
impl ScalarUDFImpl for PassesPoint {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "passes_point"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&PASSES_POINT_UDF_DOC)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_udf_args(
            self.name(),
            arg_types,
            &[
                UdfArg::Trajectory,
                UdfArg::Exact(DataType::Struct(Fields::from(vec![
                    Field::new("x", DataType::Float64, false),
                    Field::new("y", DataType::Float64, false),
                ]))),
                UdfArg::Exact(DataType::Float64),
            ],
        )
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let casted_args = ColumnarValue::values_to_arrays(&args.args)?;
        let (trajectory_0, trajectory_0_field) =
            as_trajectory_array(&casted_args[0], &args.arg_fields[0])?;

        let tolerance_array = as_primitive_array::<Float64Type>(&casted_args[2]);
        let tolerance_array = tolerance_array
            .iter()
            .map(|v| geo_utils::meters_to_degrees(v.unwrap()))
            .collect::<Vec<f64>>();

        let trajectory_geo_array =
            from_arrow_array(&trajectory_0, &trajectory_0_field).map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to convert polyline argument to geo array: {}",
                    e
                ))
            })?;

        let point_geo_array =
            from_arrow_array(&casted_args[1], &args.arg_fields[1]).map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to convert point argument to geo array: {}",
                    e
                ))
            })?;

        let dist = euclidean_distance(&trajectory_geo_array, &point_geo_array).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to compute euclidean distance between trajectory and point: {}",
                e
            ))
        })?;

        // An empty trajectory (e.g. subpolyline_between()/subpolyline_at()
        // clipped a trip to a period/instant it doesn't actually overlap)
        // has no segments to measure distance against, so it should never
        // "pass" any point - but euclidean_distance() on an empty geometry
        // degenerates to a spurious ~0 distance instead of None, which
        // would otherwise make every non-overlapping row match every point
        // (observed directly: 10 points x 10 periods x 894 licences, the
        // full cross-product ceiling, in the q15 BerlinMOD-MobilityDB
        // comparison). Checked explicitly here from the raw list lengths,
        // ahead of the GeoArrow distance computation, rather than trusted
        // to it.
        // Read from the converted array, not the raw argument: the trajectory may have
        // arrived as EWKB or WKT, which is not a list.
        let trajectory_list_array = as_list_array(&trajectory_0);
        let is_empty: Vec<bool> = (0..trajectory_list_array.len())
            .map(|i| trajectory_list_array.value_length(i) == 0)
            .collect();

        let mut result = BooleanBuilder::with_capacity(trajectory_geo_array.len());

        dist.iter()
            .zip(tolerance_array.iter())
            .zip(is_empty.iter())
            .for_each(|((d, &tolerance), &empty)| {
                if empty {
                    result.append_value(false);
                } else if let Some(distance) = d {
                    if distance <= tolerance {
                        result.append_value(true);
                    } else {
                        result.append_value(false);
                    }
                } else {
                    result.append_value(false);
                }
            });

        Ok(ColumnarValue::Array(Arc::new(result.finish()) as ArrayRef))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::udf::utils::line::TrajectoryFromText;
    use arrow::array::as_boolean_array;
    use datafusion::prelude::SessionContext;
    use geodatafusion::udf::native::constructors::Point;

    #[tokio::test]
    async fn test_passes_point_udf() {
        let ctx = SessionContext::new();
        ctx.register_udf(TrajectoryFromText::default().into());
        ctx.register_udf(Point::default().into());
        ctx.register_udf(PassesPoint::new().into());

        let test_cases = vec![
            (
                "LINESTRING M(0 0 0, 5 5 0, 10 10 0)",
                "5, 5",
                10.0,
                true,
                "Point lies exactly on the trajectory",
            ),
            (
                "LINESTRING M(0 0 0, 5 5 0, 10 10 0)",
                "6, 5",
                50000.0,
                true,
                "Point is within tolerance distance from the trajectory",
            ),
            (
                "LINESTRING M(0 0 0, 5 5 0, 10 10 0)",
                "8, 5",
                2.0,
                false,
                "Point is outside tolerance distance from the trajectory",
            ),
        ];

        let sql = format!(
            "
            SELECT
                passes_point(
                    trajectory_from_text(trajectory),
                    st_point(point_x, point_y),
                    tolerance
                ) AS passes
            FROM (
                VALUES
                {}
            ) AS t(trajectory, point_x, point_Y, tolerance)
            ",
            test_cases
                .iter()
                .map(|(traj, point_wkt, tolerance, _, _)| {
                    format!("('{}', {}, {})", traj, point_wkt, tolerance)
                })
                .collect::<Vec<String>>()
                .join(", ")
        );

        let df = ctx.sql(&sql).await.unwrap();
        let results = df.collect().await.unwrap();
        let results = as_boolean_array(results[0].column(0));

        for (i, (_, _, _, expected, description)) in test_cases.iter().enumerate() {
            assert_eq!(
                results.value(i),
                *expected,
                "Test case {} failed: {}",
                i,
                description
            );
        }
    }

    /// The trajectory argument is accepted in every encoding, and a non-trajectory is rejected.
    #[tokio::test]
    async fn accepts_trajectory_encodings() {
        crate::core::utils::trajectory_arg::test_support::accepts_trajectory_encodings(
            "passes_point({t}, st_point(2.0, 2.0), 100.0)",
        )
        .await;
    }
}

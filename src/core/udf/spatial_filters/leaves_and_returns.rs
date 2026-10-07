//! This module implements the leaves_and_returns predicate.

use crate::core::utils::geoarrow::get_polygon_array_from_arrow_utf8;
use crate::core::utils::trajectory_arg::{as_trajectory_array, coerce_udf_args, UdfArg};
use crate::utils::error::geo_arrow_error_to_datafusion_error;
use arrow::array::{ArrayRef, BooleanArray};
use arrow_schema::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{
    scalar_doc_sections::DOC_SECTION_OTHER, ColumnarValue, Documentation, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, Volatility,
};
use geo::{Contains, CoordsIter};
use geo_traits::{to_geo::ToGeoLineString, LineStringTrait};
use geoarrow_array::{
    array::from_arrow_array, cast::AsGeoArrowArray, GeoArrowArray, GeoArrowArrayAccessor,
};
use geoarrow_expr_geo::util::to_geo::geometry_to_geo;
use std::any::Any;
use std::sync::{Arc, LazyLock};

static LEAVES_AND_RETURNS_UDF_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Determines if a trajectory leaves a polygon and returns to it.", 
        "leaves_and_returns(trajectory: List<Struct<{x: Float64, y: Float64, m: Float64}>> | Binary | Utf8, polygon: Utf8) -> Boolean")
    .with_argument("trajectory", "The trajectory, as the geoarrow layout, as PostGIS EWKB (Binary), or as WKT text (Utf8).")
    .with_argument("polygon_wkt", "A polygon represented in Well-Known Text (WKT) format. Ex: `POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))`")
    .with_sql_example("SELECT leaves_and_returns(trajectory_from_text('LINESTRING M(2 2 10, 5 5 20, 3 3 30)'), 'POLYGON ((1 1, 4 1, 4 4, 1 4, 1 1))');")
    .build()
});

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct LeavesAndReturns {
    signature: Signature,
}

impl LeavesAndReturns {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for LeavesAndReturns {
    fn default() -> Self {
        Self::new()
    }
}

/// Implement the ScalarUDFImpl trait for the leaves_and_returns predicate
impl ScalarUDFImpl for LeavesAndReturns {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "leaves_and_returns"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&LEAVES_AND_RETURNS_UDF_DOC)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_udf_args(
            self.name(),
            arg_types,
            &[UdfArg::Trajectory, UdfArg::Exact(DataType::Utf8)],
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

        let polygon_array = get_polygon_array_from_arrow_utf8(&arrays[1])
            .map_err(geo_arrow_error_to_datafusion_error)?;
        let polygon_array = polygon_array.as_polygon();

        let mut result_builder = BooleanArray::builder(trajectory_array.len());

        for (i, trajectory) in trajectory_array.iter().enumerate() {
            if trajectory.is_none() {
                result_builder.append_value(false);
                continue;
            }
            let trajectory = trajectory
                .unwrap()
                .map_err(geo_arrow_error_to_datafusion_error)?
                .to_line_string();
            let polygon = polygon_array
                .value(i)
                .map_err(geo_arrow_error_to_datafusion_error)?;
            let polygon = geometry_to_geo(&polygon).map_err(geo_arrow_error_to_datafusion_error)?;

            let traj_length = trajectory.coords_count();
            if traj_length == 0 {
                result_builder.append_value(false);
                continue;
            }

            let start_point = trajectory.coord(0).unwrap();
            let end_point = trajectory.coord(traj_length - 1).unwrap();
            if !(polygon.contains(&start_point) && polygon.contains(&end_point)) {
                result_builder.append_value(false);
                continue;
            }

            let mut result = false;
            for coord in trajectory.coords_iter() {
                if !polygon.contains(&coord) {
                    result = true;
                    break;
                }
            }
            result_builder.append_value(result);
        }

        Ok(ColumnarValue::from(
            Arc::new(result_builder.finish()) as ArrayRef
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::udf::utils::line::TrajectoryFromText;
    use arrow::array::as_boolean_array;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn test_leaves_and_returns_udf() {
        let ctx = SessionContext::new();
        ctx.register_udf(TrajectoryFromText::default().into());
        ctx.register_udf(LeavesAndReturns::default().into());

        let test_cases = vec![
            (
                "LINESTRING M(2 2 10, 5 5 20, 3 3 30)",
                "POLYGON((1 1, 4 1, 4 4, 1 4, 1 1))",
                true,
                "Start and end points are inside polygon, but mid point is outside",
            ),
            (
                "LINESTRING M(2 2 10, 3 3 20, 2.5 2.5 30)",
                "POLYGON((1 1, 4 1, 4 4, 1 4, 1 1))",
                false,
                "All points are inside polygon",
            ),
            (
                "LINESTRING M(0 0 10, 5 5 20, 3 3 30)",
                "POLYGON((1 1, 4 1, 4 4, 1 4, 1 1))",
                false,
                "Start point is outside polygon",
            ),
            (
                "LINESTRING M(2 2 10, 5 5 20, 6 6 30)",
                "POLYGON((1 1, 4 1, 4 4, 1 4, 1 1))",
                false,
                "End point is outside polygon",
            ),
        ];

        let sql = format!(
            "
            SELECT leaves_and_returns(
                trajectory_from_text(trajectory_wkt),
                polygon_wkt
            )
            FROM (VALUES
                {}
            ) AS T(trajectory_wkt, polygon_wkt)
        ",
            test_cases
                .iter()
                .map(|(traj, poly, _, _)| format!("('{}', '{}')", traj, poly))
                .collect::<Vec<String>>()
                .join(", ")
        );

        let df = ctx.sql(&sql).await.unwrap();
        let results = df.collect().await.unwrap();
        let results = as_boolean_array(results[0].column(0));

        for (i, (_, _, expected, description)) in test_cases.iter().enumerate() {
            assert_eq!(
                results.value(i),
                *expected,
                "Test case {} failed: {}",
                i + 1,
                description
            );
        }
    }

    /// The trajectory argument is accepted in every encoding, and a non-trajectory is rejected.
    #[tokio::test]
    async fn accepts_trajectory_encodings() {
        crate::core::utils::trajectory_arg::test_support::accepts_trajectory_encodings(
            "leaves_and_returns({t}, 'POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))')",
        )
        .await;
    }
}

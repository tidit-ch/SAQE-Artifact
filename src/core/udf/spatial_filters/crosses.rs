//! This module implements the crosses predicate from the DE-9IM Relationships model.

use crate::core::utils::geoarrow::get_polygon_array_from_arrow_utf8;
use crate::core::utils::trajectory_arg::{as_trajectory_array, coerce_udf_args, UdfArg};
use crate::utils::error::geo_arrow_error_to_datafusion_error;
use arrow::array::{as_string_array, ArrayRef, BooleanArray};
use arrow_schema::DataType;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    scalar_doc_sections::DOC_SECTION_OTHER, ColumnarValue, Documentation, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, Volatility,
};
use geo::{Contains, CoordsIter, Intersects};
use geo_traits::{to_geo::ToGeoLineString, LineStringTrait};
use geoarrow_array::{
    array::from_arrow_array, cast::AsGeoArrowArray, GeoArrowArray, GeoArrowArrayAccessor,
};
use geoarrow_expr_geo::util::to_geo::geometry_to_geo;
use std::{any::Any, sync::Arc, sync::LazyLock};

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct Crosses {
    signature: Signature,
}

impl Crosses {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

static CROSSES_UDF_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Determines if a trajectory crosses a polygon according to the DE-9IM model.",
        "crosses(trajectory: List<Struct{x: Float64, y: Float64, m: Float64}> | Binary | Utf8, strictness: Utf8, polygon_wkt: Utf8) -> Boolean",
    )
    .with_argument("trajectory", "The trajectory, as the geoarrow layout, as PostGIS EWKB (Binary), or as WKT text (Utf8).")
    .with_argument("strictness", "A string parameter that specifies the strictness level ('strict' or 'relaxed').")
    .with_argument("polygon_wkt", "A polygon represented in Well-Known Text (WKT) format. Ex: `POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))`")
    .with_sql_example("SELECT crosses(trajectory_from_text('LINESTRING M(5 5 10, 2 2 20, 6 6 30)'), 'strict', 'POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))')")
    .build()
});

/// Implement the ScalarUDFImpl trait for the crosses predicate
impl ScalarUDFImpl for Crosses {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "crosses"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&CROSSES_UDF_DOC)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_udf_args(
            self.name(),
            arg_types,
            &[
                UdfArg::Trajectory,
                UdfArg::Exact(DataType::Utf8),
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

        let trajectory_array = from_arrow_array(&trajectory_0, &trajectory_0_field)
            .map_err(geo_arrow_error_to_datafusion_error)?;
        let trajectory_array = trajectory_array.as_line_string();

        let strictness_array = as_string_array(&arrays[1]);
        let polygon_array = get_polygon_array_from_arrow_utf8(&arrays[2])
            .map_err(geo_arrow_error_to_datafusion_error)?;
        let polygon_array = polygon_array.as_polygon();

        let mut result_builder = BooleanArray::builder(trajectory_array.len());
        for (i, trajectory) in trajectory_array.iter().enumerate() {
            let strictness_parameter = strictness_array.value(i);
            let polygon = polygon_array
                .value(i)
                .map_err(geo_arrow_error_to_datafusion_error)?;

            if trajectory.is_none() {
                result_builder.append_null();
                continue;
            }

            let trajectory = trajectory
                .unwrap()
                .map_err(geo_arrow_error_to_datafusion_error)?
                .to_line_string();
            let polygon = geometry_to_geo(&polygon).map_err(geo_arrow_error_to_datafusion_error)?;

            let traj_length = trajectory.coords_count();

            if traj_length == 0 {
                result_builder.append_value(false);
                continue;
            }
            let start_coord = trajectory.coord(0).unwrap();
            let end_coord = trajectory.coord(traj_length - 1).unwrap();

            if polygon.contains(&start_coord)
                || polygon.contains(&end_coord)
                || polygon.intersects(&start_coord)
                || polygon.intersects(&end_coord)
            {
                // if start or end point in polygon
                result_builder.append_value(false);
                continue;
            }

            match strictness_parameter {
                "strict" => {
                    // the polygon should contain or intersect a coordinate from the trajectory
                    let mut result = false;
                    for coord in trajectory.coords_iter() {
                        if polygon.contains(&coord) || polygon.intersects(&coord) {
                            result = true;
                            break;
                        }
                    }
                    result_builder.append_value(result);
                }
                "relaxed" => {
                    result_builder.append_value(polygon.intersects(&trajectory));
                }
                _ => {
                    return Err(DataFusionError::Execution(format!(
                        "Invalid strictness parameter. Choose between 'strict' or 'relaxed'"
                    )))
                }
            }
        }

        let result = result_builder.finish();
        Ok(ColumnarValue::from(Arc::new(result) as ArrayRef))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::udf::utils::line::TrajectoryFromText;
    use arrow::array::as_boolean_array;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn test_crosses_udf() {
        let ctx = SessionContext::new();
        ctx.register_udf(TrajectoryFromText::default().into());
        ctx.register_udf(Crosses::new().into());

        let test_cases = vec![
            (
                "LINESTRING M(5 5 10, 2 2 20, 6 6 30)",
                "strict",
                "POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))",
                true,
                "Trajectory has the mid point inside polygon (strict)"
            ),
            (
                "LINESTRING M(5 5 10, 2 2 20, 6 6 30)",
                "relaxed",
                "POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))",
                true,
                "Trajectory has the mid point inside polygon (relaxed)"
            ),
            (
                "LINESTRING M(0 0 10, 5 5 20)",
                "strict",
                "POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))",
                false,
                "Trajectory has no points inside polygon but intersects boundary (strict)"
            ),
            (
                "LINESTRING M(0 0 10, 5 5 20)",
                "relaxed",
                "POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))",
                true,
                "Trajectory has no points inside polygon but intersects boundary (relaxed)"
            ),
            (
                "LINESTRING M(1 1 10, 2 2 20, 0 2 30)",
                "strict",
                "POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))",
                false,
                "Trajectory starts on polygon boundary, has a point inside and ends outside (strict)"
            ),
            (
                "LINESTRING M(1 1 10, 2 2 20, 0 2 30)",
                "relaxed",
                "POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))",
                false,
                "Trajectory starts on polygon boundary, has a point inside and ends outside (relaxed)"
            ),
            (
                "LINESTRING M(0 0 10, 2 2 20, 0 2 30)",
                "strict",
                "POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))",
                true,
                "Trajectory starts/ends outside polygon, but has a point inside (strict)"
            ),
            (
                "LINESTRING M(0 0 10, 2 2 20, 0 2 30)",
                "relaxed",
                "POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))",
                true,
                "Trajectory starts/ends outside polygon, but has a point inside (relaxed)"
            ),
            (
                "LINESTRING M(10 10 10, 20 20 20, 30 30 30)",
                "strict",
                "POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))",
                false,
                "Trajectory completely outside polygon (strict)"
            ),
            (
                "LINESTRING M(10 10 10, 20 20 20, 30 30 30)",
                "relaxed",
                "POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))",
                false,
                "Trajectory completely outside polygon (relaxed)"
            )
        ];

        let sql = format!(
            "
            SELECT crosses(
                trajectory_from_text(trajectory),
                strictness,
                polygon_wkt
            )
            FROM (
                VALUES
                    {}
                )
            AS t (trajectory, strictness, polygon_wkt)
        ",
            test_cases
                .iter()
                .map(|(traj, strictness, poly, _, _)| {
                    format!("('{}', '{}', '{}')", traj, strictness, poly)
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
                "Failed test case: {}",
                description
            );
        }
    }

    /// The trajectory argument is accepted in every encoding, and a non-trajectory is rejected.
    #[tokio::test]
    async fn accepts_trajectory_encodings() {
        crate::core::utils::trajectory_arg::test_support::accepts_trajectory_encodings(
            "crosses({t}, 'POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))', 'POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))')",
        )
        .await;
    }
}

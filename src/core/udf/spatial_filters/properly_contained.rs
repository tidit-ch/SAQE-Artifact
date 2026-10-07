//! This module implements the properly contained predicate.

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
use geo::Contains;
use geo_traits::{to_geo::ToGeoCoord, LineStringTrait};
use geoarrow_array::{
    array::from_arrow_array, cast::AsGeoArrowArray, GeoArrowArray, GeoArrowArrayAccessor,
};
use geoarrow_expr_geo::util::to_geo::geometry_to_geo;
use std::any::Any;
use std::sync::{Arc, LazyLock};

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct ProperlyContained {
    signature: Signature,
}

impl ProperlyContained {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

static PROPERLY_CONTAINED_UDF_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Checks if all points of a trajectory are strictly inside a polygon (boundary excluded). \
         Unlike 'contained', points on the polygon boundary are not considered inside.",
        "properly_contained(trajectory: List<Struct{x: Float64, y: Float64, m: Float64}> | Binary | Utf8, polygon_wkt: Utf8) -> Boolean",
    )
    .with_argument("trajectory", "The trajectory, as the geoarrow layout, as PostGIS EWKB (Binary), or as WKT text (Utf8).")
    .with_argument("polygon_wkt", "A polygon represented in Well-Known Text (WKT) format. Ex: `POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))`")
    .with_sql_example("SELECT properly_contained(trajectory_from_text('LINESTRING M(2 2 10, 3 3 20)'), 'POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))')")
    .build()
});

/// Implement the ScalarUDFImpl trait for the properly contained predicate
impl ScalarUDFImpl for ProperlyContained {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "properly_contained"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&PROPERLY_CONTAINED_UDF_DOC)
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
            let polygon = polygon_array
                .value(i)
                .map_err(geo_arrow_error_to_datafusion_error)?;
            let polygon = geometry_to_geo(&polygon).map_err(geo_arrow_error_to_datafusion_error)?;
            match trajectory {
                Some(trajectory) => {
                    let trajectory = trajectory.map_err(geo_arrow_error_to_datafusion_error)?;
                    let mut result = true;
                    for i in 0..trajectory.num_coords() {
                        let coord = trajectory.coord(i).unwrap().to_coord();
                        if !polygon.contains(&coord) {
                            result = false;
                            break;
                        }
                    }
                    result_builder.append_value(result);
                }
                None => {
                    result_builder.append_null();
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
    async fn test_properly_contained_udf() {
        let ctx = SessionContext::new();
        ctx.register_udf(TrajectoryFromText::default().into());
        ctx.register_udf(ProperlyContained::new().into());

        let test_cases = vec![
            (
                "LINESTRING M (1 1 10, 2 2 20, 3 3 30)",
                "POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))",
                true,
                "Polyline completely inside polygon",
            ),
            (
                "LINESTRING M (1 1 10, 3 3 20, 6 6 30)",
                "POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))",
                false,
                "Polyline partially outside polygon",
            ),
            (
                "LINESTRING M (0 0 10, 5 5 20)",
                "POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))",
                false,
                "Polyline touching polygon boundary",
            ),
            (
                "LINESTRING M (6 6 10, 7 7 20)",
                "POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))",
                false,
                "Polyline completely outside polygon",
            ),
        ];
        let sql = format!(
            "
            SELECT properly_contained(
                trajectory_from_text(trajectory),
                polygon_wkt
            )
            FROM (
                VALUES
                    {}
                )
            AS t (trajectory, polygon_wkt)
        ",
            test_cases
                .iter()
                .map(|(traj, poly, _, _)| { format!("('{}', '{}')", traj, poly) })
                .collect::<Vec<String>>()
                .join(", ")
        );
        let df = ctx.sql(&sql).await.unwrap();
        let results = df.collect().await.unwrap();
        let results = as_boolean_array(results[0].column(0));
        println!("results :: {:?}", results);
        for (i, (_, _, expected, description)) in test_cases.iter().enumerate() {
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
            "properly_contained({t}, 'POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))')",
        )
        .await;
    }
}

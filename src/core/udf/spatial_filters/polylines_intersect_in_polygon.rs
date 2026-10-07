//! polylines_intersect_in_polygon function
//! Checks whether two polylines intersect inside a given polygon.
//!
//! # Arguments
//!
//! polyline: A list of points (Structs with x, y, and m in milliseconds) representing a trajectory.
//! polyline: A list of points (Structs with x, y, and m in milliseconds) representing a trajectory.
//! polygon: A list of points (Structs with x, y) representing a polygon.
//!
//! ## Returns
//!
//! Returns true if the two polylines intersect and their intersection point (or overlap segment) lies inside the polygon.
//! Otherwise false

use crate::core::utils::geoarrow::get_polygon_array_from_arrow_utf8;
use crate::core::utils::schema::POLYGON_DATATYPE;
use crate::core::utils::trajectory_arg::{as_trajectory_array, coerce_udf_args_any, UdfArg};
use crate::utils::error::geo_arrow_error_to_datafusion_error;
use arrow::array::{ArrayRef, BooleanBuilder};
use arrow_schema::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{
    scalar_doc_sections::DOC_SECTION_OTHER, ColumnarValue, Documentation, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, Volatility,
};
use geo::line_intersection::{line_intersection, LineIntersection};
use geo::Contains;
use geo_traits::to_geo::{ToGeoLineString, ToGeoPolygon};
use geoarrow_array::{
    array::from_arrow_array, cast::AsGeoArrowArray, GeoArrowArray, GeoArrowArrayAccessor,
};
use std::any::Any;
use std::sync::{Arc, LazyLock};

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct PolylinesIntersectInPolygon {
    signature: Signature,
}

impl PolylinesIntersectInPolygon {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

static POLYLINES_INTERSECT_IN_POLYGON_UDF_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Checks whether two polylines intersect inside a given polygon. \
         Returns true if the two polylines intersect and their intersection point \
         (or overlap segment) lies inside the polygon.",
        "polylines_intersect_in_polygon(trajectory1: List<Struct{x: Float64, y: Float64, m: Float64}> | Binary | Utf8, trajectory2: List<Struct{x: Float64, y: Float64, m: Float64}> | Binary | Utf8, polygon: Utf8) -> Boolean",
    )
    .with_argument("trajectory1", "The first trajectory, as the geoarrow layout, as PostGIS EWKB (Binary), or as WKT text (Utf8).")
    .with_argument("trajectory2", "The second trajectory, as the geoarrow layout, as PostGIS EWKB (Binary), or as WKT text (Utf8).")
    .with_argument("polygon", "A polygon in WKT format (Utf8) or as a List<List<Struct{x, y}>>.")
    .with_sql_example("SELECT polylines_intersect_in_polygon(trajectory_from_text('LINESTRING M(0 0 0, 5 5 1000)'), trajectory_from_text('LINESTRING M(5 0 0, 0 5 1000)'), 'POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))')")
    .build()
});

impl ScalarUDFImpl for PolylinesIntersectInPolygon {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "polylines_intersect_in_polygon"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&POLYLINES_INTERSECT_IN_POLYGON_UDF_DOC)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_udf_args_any(
            self.name(),
            arg_types,
            &[
                &[
                    UdfArg::Trajectory,
                    UdfArg::Trajectory,
                    UdfArg::Exact(DataType::Utf8),
                ],
                &[
                    UdfArg::Trajectory,
                    UdfArg::Trajectory,
                    UdfArg::Exact(POLYGON_DATATYPE.clone()),
                ],
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

        let polylines1 = from_arrow_array(&trajectory_0, &trajectory_0_field)
            .map_err(geo_arrow_error_to_datafusion_error)?;
        let polylines1 = polylines1.as_line_string();
        let polylines2 = from_arrow_array(&trajectory_1, &trajectory_1_field)
            .map_err(geo_arrow_error_to_datafusion_error)?;
        let polylines2 = polylines2.as_line_string();

        let polygon_array: Arc<dyn GeoArrowArray>;
        if args.arg_fields[2].data_type() == &DataType::Utf8 {
            // Parse from WKT
            polygon_array = get_polygon_array_from_arrow_utf8(&arrays[2])
                .map_err(geo_arrow_error_to_datafusion_error)?;
        } else {
            polygon_array = from_arrow_array(&arrays[2], &args.arg_fields[2])
                .map_err(geo_arrow_error_to_datafusion_error)?;
        }

        let polygon_array = polygon_array.as_polygon();

        let mut builder = BooleanBuilder::new();

        'outer: for i in 0..polylines1.len() {
            let (trajectory1, trajectory2, polygon) = (
                polylines1.value(i),
                polylines2.value(i),
                polygon_array.value(i),
            );

            if trajectory1.is_ok() && trajectory2.is_ok() && polygon.is_ok() {
                let (trajectory1, trajectory2, polygon) = (
                    trajectory1.unwrap().to_line_string(),
                    trajectory2.unwrap().to_line_string(),
                    polygon.unwrap().to_polygon(),
                );
                for seg1 in trajectory1.lines() {
                    for seg2 in trajectory2.lines() {
                        if let Some(intersection) = line_intersection(seg1, seg2) {
                            match intersection {
                                LineIntersection::SinglePoint {
                                    intersection: p, ..
                                } => {
                                    if polygon.contains(&p) {
                                        builder.append_value(true);
                                        continue 'outer;
                                    }
                                }
                                LineIntersection::Collinear {
                                    intersection: overlap_line,
                                } => {
                                    if polygon.contains(&overlap_line.start)
                                        || polygon.contains(&overlap_line.end)
                                    {
                                        builder.append_value(true);
                                        continue 'outer;
                                    }
                                }
                            }
                        }
                    }
                }
                builder.append_value(false);
            } else {
                builder.append_null();
            }
        }
        let array = Arc::new(builder.finish()) as ArrayRef;
        Ok(ColumnarValue::Array(array))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::udf::utils::line::TrajectoryFromText;
    use arrow::array::as_boolean_array;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn test_polylines_intersect_in_polygon_udf() {
        let ctx = SessionContext::new();
        ctx.register_udf(TrajectoryFromText::default().into());
        ctx.register_udf(PolylinesIntersectInPolygon::new().into());

        let test_cases = vec![
            (
                "LINESTRING M(0 0 0, 5 5 0, 10 10 0)",
                "LINESTRING M(0 10 0, 5 5 0, 10 0 0)",
                "POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))",
                true,
                "Polylines intersect inside the polygon",
            ),
            (
                "LINESTRING M(0 0 0, 5 5 0, 10 10 0)",
                "LINESTRING M(0 10 0, 5 15 0, 10 20 0)",
                "POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))",
                false,
                "Polylines do not intersect",
            ),
            (
                "LINESTRING M(0 0 0, 5 5 0, 10 10 0)",
                "LINESTRING M(0 10 0, 5 5 0, 10 0 0)",
                "POLYGON((6 0, 20 0, 20 20, 6 20, 6 0))",
                false,
                "Polylines intersect outside the polygon",
            ),
        ];

        let sql = format!(
            "
            SELECT
                polylines_intersect_in_polygon(
                    trajectory_from_text(polyline1),
                    trajectory_from_text(polyline2),
                    polygon
                ) AS intersects
            FROM (
                VALUES
                {}
            ) AS t(polyline1, polyline2, polygon)
            ",
            test_cases
                .iter()
                .map(|(polyline1, polyline2, polygon, _, _)| {
                    format!("('{}', '{}', '{}')", polyline1, polyline2, polygon)
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
            "polylines_intersect_in_polygon({t}, {t}, 'POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))')",
        )
        .await;
    }
}

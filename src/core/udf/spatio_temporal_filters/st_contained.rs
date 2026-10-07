//! This module implements the contained predicate.

use crate::core::utils::trajectory_arg::{as_trajectory_array, coerce_udf_args, UdfArg};
use crate::utils::error::geo_arrow_error_to_datafusion_error;
use arrow::array::{as_string_array, builder::BooleanBuilder};
use arrow_schema::DataType;
use datafusion::common::cast::as_int64_array;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    scalar_doc_sections::DOC_SECTION_OTHER, ColumnarValue, Documentation, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, Volatility,
};
use geo::{contains::Contains, geometry::Polygon, intersects::Intersects, LineString};
use geo_traits::CoordTrait;
use geo_traits::{to_geo::ToGeoGeometry, LineStringTrait};
use geoarrow_array::{
    array::from_arrow_array, cast::AsGeoArrowArray, GeoArrowArray, GeoArrowArrayAccessor,
};
use std::any::Any;
use std::sync::{Arc, LazyLock};
use wkt::Wkt;

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct SpatioTemporalContained {
    signature: Signature,
}

impl SpatioTemporalContained {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

static ST_CONTAINED_UDF_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Determines if a trajectory is spatially contained within a polygon during a given time range. \
         Returns true when every point of the trajectory that falls within [start_timestamp, end_timestamp] \
         is inside the specified polygon.",
        "st_contained(trajectory: List<Struct{x: Float64, y: Float64, m: Float64}> | Binary | Utf8, polygon_wkt: Utf8, start_timestamp: Int64, end_timestamp: Int64) -> Boolean",
    )
    .with_argument("trajectory", "The trajectory, as the geoarrow layout, as PostGIS EWKB (Binary), or as WKT text (Utf8).")
    .with_argument("polygon_wkt", "A polygon represented in Well-Known Text (WKT) format. Ex: `POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))`")
    .with_argument("start_timestamp", "Start of the temporal range (inclusive) as an Int64 Unix timestamp.")
    .with_argument("end_timestamp", "End of the temporal range (inclusive) as an Int64 Unix timestamp.")
    .with_sql_example("SELECT st_contained(trajectory_from_text('LINESTRING M(2 2 10, 3 3 20, 2 3 30)'), 'POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))', 10, 30)")
    .build()
});

/// Implement the ScalarUDFImpl trait for SpatioTemporalContained
impl ScalarUDFImpl for SpatioTemporalContained {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "st_contained"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&ST_CONTAINED_UDF_DOC)
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_udf_args(
            self.name(),
            arg_types,
            &[
                UdfArg::Trajectory,
                UdfArg::Exact(DataType::Utf8),
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
        let arg_start_timestamp_array = as_int64_array(&arrays[2])?;
        let arg_end_timestamp_array = as_int64_array(&arrays[3])?;
        let polygon_array = as_string_array(&arrays[1]);
        let mut result_builder = BooleanBuilder::with_capacity(trajectory_array.len());

        for (i, trajectory) in trajectory_array.iter().enumerate() {
            if let Some(Ok(trajectory)) = trajectory {
                let arg_start_timestamp = arg_start_timestamp_array.value(i);
                let arg_end_timestamp = arg_end_timestamp_array.value(i);
                if arg_end_timestamp < arg_start_timestamp {
                    return Err(DataFusionError::Execution(format!("Invalid time interval: start_timestamp ({}) must be less than or equal to end_timestamp ({})", arg_start_timestamp, arg_end_timestamp)));
                }
                let polygon_str = polygon_array.value(i);
                let wkt: Wkt<f64> = polygon_str.parse().map_err(|_| {
                    DataFusionError::Execution(
                        "Invalid WKT format: Failed to parse WKT polygon string".to_string(),
                    )
                })?;
                let polygon = match wkt {
                    wkt::Wkt::Polygon(wkt_polygon) => {
                        let coords = wkt_polygon.rings()[0].coords();
                        let exterior_coords: Vec<(f64, f64)> =
                            coords.iter().map(|p| (p.x, p.y)).collect();

                        let exterior = LineString::from(exterior_coords);
                        Ok(Polygon::new(exterior, vec![]))
                    }
                    _ => Err(DataFusionError::Execution(
                        "Invalid WKT format: No polygon found".to_string(),
                    )),
                }?;

                let num_coords = trajectory.num_coords();
                if num_coords == 0 {
                    result_builder.append_null();
                    continue;
                }
                let start_timestamp = trajectory.coord(0).unwrap().nth(2).unwrap() as i64;
                let end_timestamp =
                    trajectory.coord(num_coords - 1).unwrap().nth(2).unwrap() as i64;

                let trajectory = trajectory.to_geometry();

                if polygon.contains(&trajectory) || polygon.intersects(&trajectory) {
                    if start_timestamp <= arg_end_timestamp && end_timestamp >= arg_start_timestamp
                    {
                        result_builder.append_value(true);
                    } else {
                        result_builder.append_value(false);
                    }
                } else {
                    result_builder.append_value(false);
                }
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
    async fn test_st_contained() {
        let ctx = SessionContext::new();
        ctx.register_udf(TrajectoryFromText::default().into());
        ctx.register_udf(SpatioTemporalContained::new().into());

        let test_cases = vec![
            (
                "LINESTRING M(0 0 0, 1 1 1000, 2 2 2000)",
                "POLYGON((0 0, 0 3, 3 3, 3 0, 0 0))",
                0,
                3000,
                true,
                "Trajectory fully inside polygon and time interval",
            ),
            (
                "LINESTRING M(0 0 0, 1 1 1000, 2 2 2000)",
                "POLYGON((10 10, 10 15, 15 15, 15 10, 10 10))",
                0,
                3000,
                false,
                "Trajectory inside time interval but outside polygon",
            ),
            (
                "LINESTRING M(0 0 0, 1 1 1000, 2 2 2000)",
                "POLYGON((0 0, 0 3, 3 3, 3 0, 0 0))",
                5000,
                8000,
                false,
                "Trajectory inside polygon but outside time interval",
            ),
            (
                "LINESTRING M(0 0 1000, 1 1 1500, 2 2 3000)",
                "POLYGON((0 0, 0 3, 3 3, 3 0, 0 0))",
                500,
                1800,
                true,
                "Trajectory partially overlapping time interval and inside polygon",
            ),
        ];
        let sql = format!(
            "
            SELECT st_contained(
                trajectory_from_text(trajectory),
                polygon,
                start_timestamp,
                end_timestamp
            )
            FROM (
                VALUES
                {}
            ) AS t(trajectory, polygon, start_timestamp, end_timestamp)
        ",
            test_cases
                .iter()
                .map(|(traj, poly, start, end, _, _)| {
                    format!("('{}', '{}', {}, {})", traj, poly, start, end)
                })
                .collect::<Vec<String>>()
                .join(", ")
        );
        let df = ctx.sql(&sql).await.unwrap();
        let results = df.collect().await.unwrap();
        let results = as_boolean_array(results[0].column(0));
        for (i, (_, _, _, _, expected, description)) in test_cases.iter().enumerate() {
            let result = results.value(i);
            assert_eq!(
                result,
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
            "st_contained({t}, 'POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))', 1000, 3000)",
        )
        .await;
    }
}

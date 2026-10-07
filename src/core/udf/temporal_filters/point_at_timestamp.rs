//! This module implements the 'point_at_timestamp' function for trajectories.
//! For a given timestamp, it returns the corresponding interpolated point on a polyline.
//! The polyline is provided as a trajectory (geoarrow layout, PostGIS EWKB, or WKT text).
//! If the requested timestamp is before or after the timestamps in the polyline, 'NULL' is returned.
//! ## Example:
//!
//! Input polyline:
//! [
//!   {x: 13.43593, y: 52.41721, timestamp: 2007-05-27T00:00:00},
//!   {x: 13.43593, y: 52.41721, timestamp: 2007-05-28T08:36:49.846},
//!   {x: 13.43593, y: 52.41721, timestamp: 2007-05-28T19:08:50.114}
//! ]
//!
//! Input instant:
//! 2007-05-28T19:08:40.114Z
//!
//! The instant lies between the second and third point:
//! - t0 = 2007-05-28T08:36:49.846Z = 1180341409846 ms
//! - t1 = 2007-05-28T19:08:50.114Z = 1180379330114 ms
//! - instant = 2007-05-28T19:08:40.114Z = 1180379320114 ms
//!
//! We compute:
//!   dt = t1 - t0 = 37920268 ms
//!   alpha = (instant - t0) / dt = (1180379320114 - 1180341409846) / 37920268 = 0.99997
//!
//! Interpolated point:
//!   x = x0 + alpha * (x1 - x0) = 13.43593 + 0.99997 * (13.43593 - 13.43593) = 13.43593
//!   y = y0 + alpha * (y1 - y0) = 52.41721 + 0.99997 * (52.41721 - 52.41721) = 52.41721
//!
//! Result:
//!  {x: 13.43593, y: 52.41721}

use crate::core::utils::trajectory_arg::{as_trajectory_array, coerce_udf_args, UdfArg};
use arrow::array::{as_list_array, as_primitive_array, ArrayRef};
use arrow::datatypes::{DataType, TimeUnit, TimestampMillisecondType};
use arrow_schema::extension::{ExtensionType, EXTENSION_TYPE_NAME_KEY};
use arrow_schema::{Field, FieldRef};
use datafusion::common::Result;
use datafusion::logical_expr::{
    scalar_doc_sections::DOC_SECTION_OTHER, ColumnarValue, Documentation, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use geo::Coord;
use geo_traits::{CoordTrait, LineStringTrait};
use geoarrow_array::IntoArrow;
use geoarrow_array::{builder::PointBuilder, GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::{Metadata, PointType};
use std::any::Any;
use std::sync::{Arc, LazyLock};

use crate::core::utils::geo_utils::generic_list_array_to_geo_linestring_array;
use crate::core::utils::schema::POINT_XY_DATATYPE;
use crate::core::utils::udf::get_time_range_for_instant;

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct PointAtTimestamp {
    signature: Signature,
}

impl PointAtTimestamp {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

static POINT_AT_TIMESTAMP_UDF_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Returns the interpolated point on a trajectory at a given timestamp. \
         If the timestamp is before or after the trajectory's time range, NULL is returned. \
         Uses linear interpolation between the two surrounding points.",
        "point_at_timestamp(trajectory: List<Struct{x: Float64, y: Float64, m: Float64}> | Binary | Utf8, instant: Timestamp(Millisecond)) -> Struct{x: Float64, y: Float64}",
    )
    .with_argument("trajectory", "The trajectory, as the geoarrow layout, as PostGIS EWKB (Binary), or as WKT text (Utf8).")
    .with_argument("instant", "A Timestamp(Millisecond) at which to interpolate the point.")
    .with_sql_example("SELECT point_at_timestamp(trajectory_from_text('LINESTRING M(0 0 0, 10 10 1000)'), TIMESTAMP '2007-05-28 00:00:00.500')")
    .build()
});

/// Implementation of t point_at_timestamp user-defined-function
impl ScalarUDFImpl for PointAtTimestamp {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "point_at_timestamp"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&POINT_AT_TIMESTAMP_UDF_DOC)
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
        Ok(POINT_XY_DATATYPE.clone())
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        // return_type() above already validates the args and returns
        // POINT_XY_DATATYPE; this additionally tags the output field with
        // GeoArrow Point extension metadata, which invoke_with_args' bare
        // DataType alone does not carry. Without it, downstream GeoArrow
        // predicates (e.g. st_intersects) that consume this UDF's output
        // directly (not via a table column, which has no schema to inherit
        // the metadata from) fail with "Field extension type name missing" -
        // same fix as subpolyline_between.rs already applies for its own
        // LineString output.
        Ok(Arc::new(
            Field::new("point_at_timestamp", POINT_XY_DATATYPE.clone(), true).with_metadata(
                [(
                    EXTENSION_TYPE_NAME_KEY.to_string(),
                    PointType::NAME.to_owned(),
                )]
                .into_iter()
                .collect(),
            ),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let (trajectory_0, _) = as_trajectory_array(&arrays[0], &args.arg_fields[0])?;

        let trajectory_array = generic_list_array_to_geo_linestring_array(
            as_list_array(&trajectory_0),
            geoarrow_schema::Dimension::XYM,
        )?;
        let instant_array = as_primitive_array::<TimestampMillisecondType>(&arrays[1]);

        let mut point_builder = PointBuilder::with_capacity(
            PointType::new(
                geoarrow_schema::Dimension::XY,
                Arc::new(Metadata::default()),
            ),
            trajectory_array.len(),
        );

        for (i, trajectory) in trajectory_array.iter().enumerate() {
            if let Some(Ok(trajectory)) = trajectory {
                let time_instant = instant_array.value(i);

                let range = get_time_range_for_instant(&trajectory, time_instant as f64);
                if range.is_none() {
                    point_builder.push_null();
                    continue;
                }
                let (idx0, idx1) = range.unwrap();
                if idx0 == idx1 {
                    // Exact match
                    let coord = trajectory.coord(idx0).unwrap();
                    point_builder.push_coord(Some(&Coord::from((coord.x(), coord.y()))));
                } else {
                    // Interpolate
                    let coord0 = trajectory.coord(idx0).unwrap();
                    let coord1 = trajectory.coord(idx1).unwrap();

                    let t0 = coord0.nth(2).unwrap();
                    let t1 = coord1.nth(2).unwrap();

                    let dt = (t1 - t0) as f64;
                    let alpha = if dt.abs() < 1e-6 {
                        0.0 // Avoid division by zero, fallback to first point
                    } else {
                        (time_instant as f64 - t0 as f64) / dt
                    };

                    let interp_x = coord0.x() + alpha * (coord1.x() - coord0.x());
                    let interp_y = coord0.y() + alpha * (coord1.y() - coord0.y());

                    point_builder.push_coord(Some(&Coord::from((interp_x, interp_y))));
                }
            } else {
                point_builder.push_null();
            }
        }

        let result = point_builder.finish().into_arrow();
        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::udf::utils::line::TrajectoryFromText;
    use datafusion::prelude::SessionContext;
    use geo_traits::to_geo::ToGeoPoint;
    use geo_types::Point;
    use geoarrow_array::{array::from_arrow_array, cast::AsGeoArrowArray, GeoArrowArrayAccessor};

    #[tokio::test]
    async fn test_point_at_timestamp_udf() {
        let ctx = SessionContext::new();
        ctx.register_udf(TrajectoryFromText::default().into());
        ctx.register_udf(PointAtTimestamp::new().into());

        let sql = format!(
            "
            SELECT point_at_timestamp(
                trajectory_from_text(wkt),
                arrow_cast(timestamp, 'Timestamp(Millisecond, None)')
            )
            FROM (
                VALUES
                    ('LINESTRING M (0 0 1000, 2 2 2000, 4 4 4000)', 2000),
                    ('LINESTRING M (0 0 1000, 2 2 2000, 4 4 4000)', 4000),
                    ('LINESTRING M (0 0 1000, 2 2 2000, 4 4 4000)', 1500),
                    ('LINESTRING M (0 0 1000, 2 2 2000, 4 4 4000)', 5000),
                    ('LINESTRING M EMPTY', 2000)
            ) AS t(wkt, timestamp)
        "
        );
        let df = ctx.sql(&sql).await.unwrap();
        let result = df.collect().await.unwrap();

        let point_type = PointType::new(
            geoarrow_schema::Dimension::XY,
            Arc::new(Metadata::default()),
        );
        let result_point_array =
            from_arrow_array(&result[0].column(0), &point_type.to_field("point", true)).unwrap();
        let result_point_array = result_point_array.as_point();
        let result_point_array = result_point_array
            .iter()
            .map(|p| match p {
                Some(pt) => {
                    let pt = pt.unwrap().try_to_point().unwrap();
                    Some(pt)
                }
                None => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(result_point_array.len(), 5);
        let test_cases = vec![
            (
                Some(Point::from((2.0, 2.0))),
                "Exact match at timestamp 2000",
            ),
            (
                Some(Point::from((4.0, 4.0))),
                "Exact match at timestamp 4000",
            ),
            (
                Some(Point::from((1.0, 1.0))),
                "Interpolated point at timestamp 1500",
            ),
            (None, "Timestamp 5000 out of range"),
            (None, "Empty trajectory"),
        ];
        for (i, (expected, description)) in test_cases.into_iter().enumerate() {
            assert_eq!(
                result_point_array[i], expected,
                "Failed test case {}: {}",
                i, description
            );
        }
    }

    /// The trajectory argument is accepted in every encoding, and a non-trajectory is rejected.
    #[tokio::test]
    async fn accepts_trajectory_encodings() {
        crate::core::utils::trajectory_arg::test_support::accepts_trajectory_encodings(
            "point_at_timestamp({t}, arrow_cast(TIMESTAMP '1970-01-01 00:00:02', 'Timestamp(Millisecond, None)'))",
        )
        .await;
    }
}

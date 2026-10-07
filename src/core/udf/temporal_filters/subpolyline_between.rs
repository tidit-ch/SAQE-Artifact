//! SubPolylineBetween Function
//! Function is inspired by PostGIS function ST_LocateBetween
//!
//! This function extracts a portion of a polyline based on a time interval.
//!
//! # Arguments
//!
//! polyline: A list of points (Structs with x, y, and timestamp in milliseconds) representing a trajectory.
//! period_start: The start timestamp (in milliseconds) of the desired time interval.
//! period_end: The end timestamp (in milliseconds) of the desired time interval.
//!
//! # Returns
//!
//! A new polyline containing all points from the input whose timestamps
//! fall within or on the boundaries of the specified start and end period.

use crate::core::utils::trajectory_arg::{as_trajectory_array, coerce_udf_args, UdfArg};
use arrow::array::{as_primitive_array, ArrayRef, Float64Builder, ListBuilder, StructBuilder};
use arrow::datatypes::TimestampMillisecondType;
use arrow_schema::extension::{ExtensionType, EXTENSION_TYPE_NAME_KEY};
use arrow_schema::{DataType, Field, FieldRef, Fields, TimeUnit};
use datafusion::common::{plan_err, Result};
use datafusion::logical_expr::{
    scalar_doc_sections::DOC_SECTION_OTHER, ColumnarValue, Documentation, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use geo_traits::{CoordTrait, LineStringTrait};
use geoarrow_array::{
    array::from_arrow_array, cast::AsGeoArrowArray, GeoArrowArray, GeoArrowArrayAccessor,
};
use geoarrow_schema::LineStringType;
use std::any::Any;
use std::sync::{Arc, LazyLock};

use crate::core::utils::schema::TRAJECTORY_DATATYPE;
use crate::utils::error::geo_arrow_error_to_datafusion_error;

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct SubPolylineBetween {
    signature: Signature,
}

impl SubPolylineBetween {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

static SUBPOLYLINE_BETWEEN_UDF_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Extracts a portion of a trajectory based on a time interval. \
         Returns a new polyline containing all points whose timestamps fall within \
         the specified start and end period. Inspired by PostGIS ST_LocateBetween.",
        "subpolyline_between(trajectory: List<Struct{x: Float64, y: Float64, m: Float64}> | Binary | Utf8, period_start: Timestamp(Millisecond), period_end: Timestamp(Millisecond)) -> List<Struct{x: Float64, y: Float64, m: Float64}>",
    )
    .with_argument("trajectory", "The trajectory, as the geoarrow layout, as PostGIS EWKB (Binary), or as WKT text (Utf8).")
    .with_argument("period_start", "Start timestamp (Timestamp(Millisecond)) of the desired time interval.")
    .with_argument("period_end", "End timestamp (Timestamp(Millisecond)) of the desired time interval.")
    .with_sql_example("SELECT subpolyline_between(trajectory_from_text('LINESTRING M(0 0 0, 5 5 500, 10 10 1000)'), TIMESTAMP '1970-01-01 00:00:00.200', TIMESTAMP '1970-01-01 00:00:00.800')")
    .build()
});

impl ScalarUDFImpl for SubPolylineBetween {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "subpolyline_between"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&SUBPOLYLINE_BETWEEN_UDF_DOC)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_udf_args(
            self.name(),
            arg_types,
            &[
                UdfArg::Trajectory,
                UdfArg::Exact(DataType::Timestamp(TimeUnit::Millisecond, None)),
                UdfArg::Exact(DataType::Timestamp(TimeUnit::Millisecond, None)),
            ],
        )
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(TRAJECTORY_DATATYPE.clone())
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        // return_type() above already validates the args and returns
        // TRAJECTORY_DATATYPE; this additionally tags the output field with
        // GeoArrow LineString extension metadata, which invoke_with_args'
        // bare DataType alone does not carry. Without it, downstream GeoArrow
        // predicates (e.g. st_intersects) that consume this UDF's output
        // reject it with "InvalidGeoArrow" since a computed column has no
        // table schema to inherit the metadata from.
        Ok(Arc::new(
            Field::new("subpolyline_between", TRAJECTORY_DATATYPE.clone(), true).with_metadata(
                [(
                    EXTENSION_TYPE_NAME_KEY.to_string(),
                    LineStringType::NAME.to_owned(),
                )]
                .into_iter()
                .collect(),
            ),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let (trajectory_0, trajectory_0_field) =
            as_trajectory_array(&arrays[0], &args.arg_fields[0])?;

        let trajectory_array = from_arrow_array(&trajectory_0, &trajectory_0_field)
            .map_err(geo_arrow_error_to_datafusion_error)?;
        let trajectory_array = trajectory_array.as_line_string();
        let start_array = as_primitive_array::<TimestampMillisecondType>(&arrays[1]);
        let end_array = as_primitive_array::<TimestampMillisecondType>(&arrays[2]);

        let struct_fields = Fields::from(vec![
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false),
            Field::new("m", DataType::Float64, false),
        ]);

        let struct_builder = ListBuilder::new(StructBuilder::new(
            struct_fields.clone(),
            vec![
                Box::new(Float64Builder::new()),
                Box::new(Float64Builder::new()),
                Box::new(Float64Builder::new()),
            ],
        ));
        let field = Field::new("vertices", DataType::Struct(struct_fields), false);
        let mut list_builder = ListBuilder::with_field(struct_builder, field);

        for i in 0..trajectory_array.len() {
            let mut coord_idx = 0;
            let trajectory = trajectory_array
                .value(i)
                .map_err(geo_arrow_error_to_datafusion_error)?;
            let ts_start = start_array.value(i) as f64;
            let ts_end = end_array.value(i) as f64;
            let struct_builder = list_builder.values();
            let trajectory_len = trajectory.num_coords();

            if ts_start > ts_end {
                return plan_err!("Start timestamp must be less than or equal to end timestamp");
            }

            if trajectory_len == 0 {
                // Empty trajectory or invalid time range
                list_builder.append(true);
                continue;
            }

            if trajectory_len == 1 {
                // Single point trajectory
                let coord = trajectory.coord(0).unwrap();
                let m = coord.nth(2).unwrap();
                if m >= ts_start && m <= ts_end {
                    add_coord_to_builder(struct_builder, coord.x(), coord.y(), m);
                }
                list_builder.append(true);
                continue;
            }

            // Get the first coordinate with timestamp >= ts_start
            while coord_idx < trajectory_len - 1 {
                let coord = trajectory.coord(coord_idx).unwrap();
                let next_coord = trajectory.coord(coord_idx + 1).unwrap();
                let (x, y, m) = (coord.x(), coord.y(), coord.nth(2).unwrap());
                let (next_x, next_y, next_m) =
                    (next_coord.x(), next_coord.y(), next_coord.nth(2).unwrap());

                if m >= ts_start && m <= ts_end {
                    add_coord_to_builder(struct_builder, x, y, m);
                    coord_idx += 1;
                    break;
                } else if m < ts_start && next_m > ts_start {
                    let ratio = (ts_start - m) / (next_m - m);
                    let interp_x = x + (next_x - x) * ratio;
                    let interp_y = y + (next_y - y) * ratio;
                    add_coord_to_builder(struct_builder, interp_x, interp_y, ts_start);
                    coord_idx += 1;
                    break;
                }
                coord_idx += 1;
            }

            // Collect all coordinates with timestamp between ts_start and ts_end
            while coord_idx < trajectory_len {
                let coord = trajectory.coord(coord_idx).unwrap();

                let (x, y, m) = (coord.x(), coord.y(), coord.nth(2).unwrap());
                if m > ts_start && m < ts_end {
                    add_coord_to_builder(struct_builder, x, y, m);
                    coord_idx += 1;
                } else {
                    break;
                }
            }

            // Get the last coordinate with timestamp <= ts_end
            if coord_idx < trajectory_len {
                let prev_coord = trajectory.coord(coord_idx - 1).unwrap();
                let coord = trajectory.coord(coord_idx).unwrap();
                let (prev_x, prev_y, prev_m) =
                    (prev_coord.x(), prev_coord.y(), prev_coord.nth(2).unwrap());
                let (x, y, m) = (coord.x(), coord.y(), coord.nth(2).unwrap());
                if m == ts_end {
                    add_coord_to_builder(struct_builder, x, y, m);
                } else if prev_m < ts_end && m > ts_end {
                    let ratio = (ts_end - prev_m) / (m - prev_m);
                    let interp_x = prev_x + (x - prev_x) * ratio;
                    let interp_y = prev_y + (y - prev_y) * ratio;
                    add_coord_to_builder(struct_builder, interp_x, interp_y, ts_end);
                }
            }
            list_builder.append(true);
        }

        let result_array = Arc::new(list_builder.finish()) as ArrayRef;
        Ok(ColumnarValue::Array(result_array))
    }
}

fn add_coord_to_builder(struct_builder: &mut StructBuilder, x: f64, y: f64, m: f64) {
    struct_builder
        .field_builder::<Float64Builder>(0)
        .unwrap()
        .append_value(x);
    struct_builder
        .field_builder::<Float64Builder>(1)
        .unwrap()
        .append_value(y);
    struct_builder
        .field_builder::<Float64Builder>(2)
        .unwrap()
        .append_value(m);
    struct_builder.append(true);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::udf::utils::line::TrajectoryFromText;
    use arrow_schema::extension::{ExtensionType, EXTENSION_TYPE_NAME_KEY};
    use datafusion::prelude::SessionContext;
    use geoarrow_array::{array::from_arrow_array, cast::AsGeoArrowArray, GeoArrowArrayAccessor};
    use geoarrow_schema::LineStringType;

    #[tokio::test]
    async fn test_subpolyline_between_udf() {
        let ctx = SessionContext::new();
        ctx.register_udf(TrajectoryFromText::default().into());
        ctx.register_udf(SubPolylineBetween::new().into());

        let sql = format!(
            "
            SELECT subpolyline_between(
                trajectory_from_text(wkt),
                arrow_cast(start_timestamp, 'Timestamp(Millisecond, None)'),
                arrow_cast(end_timestamp, 'Timestamp(Millisecond, None)')
            )
            FROM (
                VALUES
                    ('LINESTRING M (0 0 1000, 2 2 2000, 4 4 3000)', 1000, 3000),
                    ('LINESTRING M (0 0 1000, 2 2 2000, 4 4 3000, 6 6 4000)', 1500, 3500),
                    ('LINESTRING M (0 0 1000, 2 2 2000, 4 4 3000)', 2500, 5000),
                    ('LINESTRING M (0 0 1000, 2 2 2000, 4 4 3000)', 500, 1500),
                    ('LINESTRING M (0 0 1000, 2 2 2000, 4 4 3000)', 500, 800),
                    ('LINESTRING M (0 0 1000, 2 2 2000, 4 4 3000)', 3500, 4000),
                    ('LINESTRING M EMPTY', 1000, 2000)
            ) AS t(wkt, start_timestamp, end_timestamp)
        "
        );
        let df = ctx.sql(&sql).await.unwrap();
        let result = df.collect().await.unwrap();
        let trajectory_field = Field::new("subpolyline_between", TRAJECTORY_DATATYPE.clone(), true)
            .with_metadata(
                [(
                    EXTENSION_TYPE_NAME_KEY.to_owned(),
                    LineStringType::NAME.to_owned(),
                )]
                .into_iter()
                .collect(),
            );
        let trajectory_array = from_arrow_array(result[0].column(0), &trajectory_field).unwrap();
        let trajectory_array = trajectory_array.as_line_string();
        let test_cases = vec![
            (
                vec![(0.0, 0.0, 1000.0), (2.0, 2.0, 2000.0), (4.0, 4.0, 3000.0)],
                "Interval exactly overlaps the entire trajectory",
            ),
            (
                vec![
                    (1.0, 1.0, 1500.0),
                    (2.0, 2.0, 2000.0),
                    (4.0, 4.0, 3000.0),
                    (5.0, 5.0, 3500.0),
                ],
                "Interval starts and ends within the trajectory",
            ),
            (
                vec![(3.0, 3.0, 2500.0), (4.0, 4.0, 3000.0)],
                "Interval starts within the trajectory and ends after it",
            ),
            (
                vec![(0.0, 0.0, 1000.0), (1.0, 1.0, 1500.0)],
                "Interval starts before the trajectory and ends within it",
            ),
            (vec![], "Interval ends before the trajectory starts"),
            (vec![], "Interval starts after the trajectory ends"),
            (vec![], "Empty trajectory results in empty polyline"),
        ];

        for (i, trajectory) in trajectory_array.iter().enumerate() {
            let trajectory = trajectory.unwrap().unwrap();
            assert_eq!(
                trajectory.num_coords(),
                test_cases[i].0.len(),
                "Test case {}, Coords length did not match, details :: {}",
                i,
                test_cases[i].1
            );
            let mut result_coords: Vec<(f64, f64, f64)> = Vec::new();
            for i in 0..trajectory.num_coords() {
                let coord = trajectory.coord(i).unwrap();
                result_coords.push((coord.x(), coord.y(), coord.nth(2).unwrap()));
            }
            assert_eq!(
                result_coords, test_cases[i].0,
                "Test case {}, Coords did not match, details :: {}",
                i, test_cases[i].1
            );
        }
    }

    /// The trajectory argument is accepted in every encoding, and a non-trajectory is rejected.
    #[tokio::test]
    async fn accepts_trajectory_encodings() {
        crate::core::utils::trajectory_arg::test_support::accepts_trajectory_encodings(
            "subpolyline_between({t}, arrow_cast(TIMESTAMP '1970-01-01 00:00:01', 'Timestamp(Millisecond, None)'), arrow_cast(TIMESTAMP '1970-01-01 00:00:03', 'Timestamp(Millisecond, None)'))",
        )
        .await;
    }
}

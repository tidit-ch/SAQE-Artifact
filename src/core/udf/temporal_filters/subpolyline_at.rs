//! SubPolylineAt Function
//!
//! This function extracts a portion of a polyline based on an instant value.
//!
//! # Arguments
//!
//! polyline: A list of points (Structs with x, y, and m in milliseconds) representing a trajectory.
//! instant: Instant timestamp (in milliseconds).
//!
//! # Returns
//! A new polyline point at a given instant.
//! Returns an empty polyline if the instant is outside the trajectory.

use crate::core::utils::trajectory_arg::{as_trajectory_array, coerce_udf_args, UdfArg};
use arrow::array::{as_primitive_array, ArrayRef, Float64Builder, ListBuilder, StructBuilder};
use arrow::datatypes::TimestampMillisecondType;
use arrow_schema::{DataType, Field, Fields, TimeUnit};
use datafusion::common::Result;
use datafusion::logical_expr::{
    scalar_doc_sections::DOC_SECTION_OTHER, ColumnarValue, Documentation, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, Volatility,
};
use geo_traits::{CoordTrait, LineStringTrait};
use geoarrow_array::{
    array::from_arrow_array, cast::AsGeoArrowArray, GeoArrowArray, GeoArrowArrayAccessor,
};
use std::any::Any;
use std::sync::{Arc, LazyLock};

use crate::core::utils::schema::TRAJECTORY_DATATYPE;
use crate::core::utils::udf::get_time_range_for_instant;
use crate::utils::error::geo_arrow_error_to_datafusion_error;

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct SubPolylineAt {
    signature: Signature,
}

impl SubPolylineAt {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

static SUBPOLYLINE_AT_UDF_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Extracts a point from a trajectory at a given instant. \
         Returns the interpolated point at the specified timestamp, or an empty polyline \
         if the instant is outside the trajectory's time range.",
        "subpolyline_at(trajectory: List<Struct{x: Float64, y: Float64, m: Float64}> | Binary | Utf8, instant: Timestamp(Millisecond)) -> List<Struct{x: Float64, y: Float64, m: Float64}>",
    )
    .with_argument("trajectory", "The trajectory, as the geoarrow layout, as PostGIS EWKB (Binary), or as WKT text (Utf8).")
    .with_argument("instant", "A Timestamp(Millisecond) at which to extract the point.")
    .with_sql_example("SELECT subpolyline_at(trajectory_from_text('LINESTRING M(0 0 0, 10 10 1000)'), TIMESTAMP '1970-01-01 00:00:00.500')")
    .build()
});

/// Implement the ScalarUDFImpl trait for SubPolylineAt
impl ScalarUDFImpl for SubPolylineAt {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "subpolyline_at"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&SUBPOLYLINE_AT_UDF_DOC)
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
        Ok(TRAJECTORY_DATATYPE.clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let (trajectory_0, trajectory_0_field) =
            as_trajectory_array(&arrays[0], &args.arg_fields[0])?;

        let trajectory_array = from_arrow_array(&trajectory_0, &trajectory_0_field)
            .map_err(geo_arrow_error_to_datafusion_error)?;
        let trajectory_array = trajectory_array.as_line_string();

        let instant_array = as_primitive_array::<TimestampMillisecondType>(&arrays[1]);

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
            let trajectory = trajectory_array
                .value(i)
                .map_err(geo_arrow_error_to_datafusion_error)?;
            let instant = instant_array.value(i) as f64;
            let struct_builder = list_builder.values();

            let time_range = get_time_range_for_instant(&trajectory, instant);

            if let Some((start_idx, end_idx)) = time_range {
                if start_idx == end_idx {
                    // Exact match
                    let coord = trajectory.coord(start_idx).unwrap();
                    struct_builder
                        .field_builder::<Float64Builder>(0)
                        .unwrap()
                        .append_value(coord.x());
                    struct_builder
                        .field_builder::<Float64Builder>(1)
                        .unwrap()
                        .append_value(coord.y());
                    struct_builder
                        .field_builder::<Float64Builder>(2)
                        .unwrap()
                        .append_value(coord.nth(2).unwrap());
                    struct_builder.append(true);
                } else {
                    // Interpolated point
                    let coord1 = trajectory.coord(start_idx).unwrap();
                    let coord2 = trajectory.coord(end_idx).unwrap();
                    let t1 = coord1.nth(2).unwrap();
                    let t2 = coord2.nth(2).unwrap();
                    let t = (instant - t1 as f64) / (t2 as f64 - t1 as f64);
                    let x_interp = coord1.x() + t * (coord2.x() - coord1.x());
                    let y_interp = coord1.y() + t * (coord2.y() - coord1.y());

                    struct_builder
                        .field_builder::<Float64Builder>(0)
                        .unwrap()
                        .append_value(x_interp);
                    struct_builder
                        .field_builder::<Float64Builder>(1)
                        .unwrap()
                        .append_value(y_interp);
                    struct_builder
                        .field_builder::<Float64Builder>(2)
                        .unwrap()
                        .append_value(instant);
                    struct_builder.append(true);
                }
            }
            list_builder.append(true);
        }

        let result_array = Arc::new(list_builder.finish()) as ArrayRef;
        Ok(ColumnarValue::Array(result_array))
    }
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
    async fn test_subpolyline_at_udf() {
        let ctx = SessionContext::new();
        ctx.register_udf(TrajectoryFromText::default().into());
        ctx.register_udf(SubPolylineAt::new().into());

        let sql = format!(
            "
            SELECT subpolyline_at(
                trajectory_from_text(wkt),
                arrow_cast(timestamp, 'Timestamp(Millisecond, None)')
            )
            FROM (
                VALUES
                    ('LINESTRING M (0 0 1000, 2 2 2000, 4 4 3000)', 1500),
                    ('LINESTRING M (0 0 1000, 1 1 1500, 2 2 2000)', 1000),
                    ('LINESTRING M (0 0 1000, 1 1 1500, 2 2 2000)', 2000),
                    ('LINESTRING M (0 0 1000, 1 1 1500, 2 2 2000)', 500),
                    ('LINESTRING M (0 0 1000, 1 1 1500, 2 2 2000)', 2500),
                    ('LINESTRING M EMPTY', 1200)
            ) AS t(wkt, timestamp)
        "
        );
        let df = ctx.sql(&sql).await.unwrap();
        let result = df.collect().await.unwrap();
        let trajectory_field = Field::new("subpolyline_at", TRAJECTORY_DATATYPE.clone(), true)
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
                vec![(1.0, 1.0, 1500.0)],
                "Interpolated point at 1500ms between (0,0,1000) and (2,2,2000)",
            ),
            (
                vec![(0.0, 0.0, 1000.0)],
                "Exact match at start point (0,0,1000)",
            ),
            (
                vec![(2.0, 2.0, 2000.0)],
                "Exact match at end point (2,2,2000)",
            ),
            (
                vec![],
                "Instant before the trajectory results in empty polyline",
            ),
            (
                vec![],
                "Instant after the trajectory results in empty polyline",
            ),
            (vec![], "Empty trajectory results in empty polyline"),
        ];

        for (i, trajectory) in trajectory_array.iter().enumerate() {
            let trajectory = trajectory.unwrap().unwrap();
            let (expected_coords, msg) = &test_cases[i];
            assert_eq!(
                expected_coords.len(),
                trajectory.num_coords(),
                "Test case {}, Coords length did not match, details :: {}",
                i,
                msg
            );
            if expected_coords.is_empty() {
                continue;
            }
            let start_coord = trajectory.coord(0).unwrap();
            let result_coords = vec![(
                start_coord.x(),
                start_coord.y(),
                start_coord.nth(2).unwrap(),
            )];
            assert_eq!(
                &result_coords, expected_coords,
                "Test case {}, Coords did not match, details :: {}",
                i, msg
            );
        }
    }

    /// The trajectory argument is accepted in every encoding, and a non-trajectory is rejected.
    #[tokio::test]
    async fn accepts_trajectory_encodings() {
        crate::core::utils::trajectory_arg::test_support::accepts_trajectory_encodings(
            "subpolyline_at({t}, arrow_cast(TIMESTAMP '1970-01-01 00:00:02', 'Timestamp(Millisecond, None)'))",
        )
        .await;
    }
}

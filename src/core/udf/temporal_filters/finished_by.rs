use crate::core::utils::trajectory_arg::{as_trajectory_array, coerce_udf_args, UdfArg};
use crate::utils::error::geo_arrow_error_to_datafusion_error;
use arrow::array::builder::BooleanBuilder;
use arrow_schema::DataType;
use datafusion::{
    common::{cast::as_int64_array, Result},
    error::DataFusionError,
    logical_expr::{
        scalar_doc_sections::DOC_SECTION_OTHER, ColumnarValue, Documentation, ScalarFunctionArgs,
        ScalarUDFImpl, Signature, Volatility,
    },
};
use geo_traits::{CoordTrait, LineStringTrait};
use geoarrow_array::{
    array::from_arrow_array, cast::AsGeoArrowArray, GeoArrowArray, GeoArrowArrayAccessor,
};
use std::any::Any;
use std::sync::{Arc, LazyLock};

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct FinishedBy {
    signature: Signature,
}

impl FinishedBy {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

static FINISHED_BY_UDF_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Checks if a trajectory is finished by a time period, i.e. the trajectory's start is before the period start but the trajectory's end equals the period end.",
        "finished_by(trajectory: List<Struct{x: Float64, y: Float64, m: Float64}> | Binary | Utf8, start_timestamp: Int64, end_timestamp: Int64) -> Boolean",
    )
    .with_argument("trajectory", "The trajectory, as the geoarrow layout, as PostGIS EWKB (Binary), or as WKT text (Utf8).")
    .with_argument("start_timestamp", "Start of the temporal range as an Int64 Unix timestamp.")
    .with_argument("end_timestamp", "End of the temporal range as an Int64 Unix timestamp.")
    .with_sql_example("SELECT finished_by(trajectory_from_text('LINESTRING M(0 0 0, 1 1 1000)'), 500, 1000)")
    .build()
});

impl ScalarUDFImpl for FinishedBy {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "finished_by"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&FINISHED_BY_UDF_DOC)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_udf_args(
            self.name(),
            arg_types,
            &[
                UdfArg::Trajectory,
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

        let arg_start_timestamp = as_int64_array(&arrays[1])?.value(0);
        let arg_end_timestamp = as_int64_array(&arrays[2])?.value(0);

        if arg_end_timestamp < arg_start_timestamp {
            return Err(DataFusionError::Execution(format!("Invalid time interval: start_timestamp ({}) must be less than or equal to end_timestamp ({})", arg_start_timestamp, arg_end_timestamp)));
        }

        let mut result_builder = BooleanBuilder::with_capacity(trajectory_array.len());

        for trajectory in trajectory_array.iter() {
            if let Some(Ok(trajectory)) = trajectory {
                let num_coords = trajectory.num_coords();
                if num_coords == 0 {
                    result_builder.append_value(false);
                    continue;
                }
                let start_timestamp = trajectory.coord(0).unwrap().nth(2).unwrap() as i64;
                let end_timestamp =
                    trajectory.coord(num_coords - 1).unwrap().nth(2).unwrap() as i64;
                result_builder.append_value(
                    start_timestamp < arg_start_timestamp && end_timestamp == arg_end_timestamp,
                );
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
    async fn test_finished_by_udf() {
        let ctx = SessionContext::new();
        ctx.register_udf(TrajectoryFromText::default().into());
        ctx.register_udf(FinishedBy::new().into());

        let sql = format!(
            "
            SELECT finished_by(
                trajectory_from_text(wkt),
                1000,
                3000
            )
            FROM (
                VALUES
                    ('LINESTRING M (0 0 500, 1 1 2000, 2 2 3000)'),
                    ('LINESTRING M (0 0 500, 1 1 2000, 2 2 4000)'),
                    ('LINESTRING M (0 0 1000, 1 1 1500, 2 2 3000)')
            ) AS t(wkt)
        "
        );
        let df = ctx.sql(&sql).await.unwrap();
        let result = df.collect().await.unwrap();
        let result = as_boolean_array(result[0].column(0));
        assert_eq!(result.len(), 3);
        assert_eq!(result.value(0), true);
        assert_eq!(result.value(1), false);
        assert_eq!(result.value(2), false);
    }

    /// The trajectory argument is accepted in every encoding, and a non-trajectory is rejected.
    #[tokio::test]
    async fn accepts_trajectory_encodings() {
        crate::core::utils::trajectory_arg::test_support::accepts_trajectory_encodings(
            "finished_by({t}, 1000, 3000)",
        )
        .await;
    }
}

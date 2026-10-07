//! This module implements the contained predicate.

use crate::core::utils::trajectory_arg::{
    as_polygon_array, as_trajectory_array, coerce_udf_args, is_polygon_input, is_trajectory_input,
    UdfArg,
};
use crate::utils::error::geo_arrow_error_to_datafusion_error;
use arrow::array::{as_string_array, builder::BooleanBuilder};
use arrow_schema::DataType;
use datafusion::common::{plan_err, Result};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    scalar_doc_sections::DOC_SECTION_OTHER, ColumnarValue, Documentation, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, Volatility,
};
use geo::{contains::Contains, intersects::Intersects};
use geo_traits::to_geo::{ToGeoGeometry, ToGeoPolygon};
use geoarrow_array::{
    array::from_arrow_array, cast::AsGeoArrowArray, GeoArrowArray, GeoArrowArrayAccessor,
};
use std::any::Any;
use std::sync::{Arc, LazyLock};

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct Contained {
    signature: Signature,
}

impl Contained {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

static CONTAINED_UDF_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Checks if a trajectory is spatially contained within or intersects a polygon. \
         In 'strict' mode, the trajectory must be fully contained. \
         In 'relaxed' mode, the trajectory only needs to intersect the polygon.",
        "contained(trajectory: List<Struct{x: Float64, y: Float64, m: Float64}> | Binary | Utf8, strictness: Utf8, polygon: List<List<Struct{x: Float64, y: Float64}>> | Binary | Utf8) -> Boolean",
    )
    .with_argument("trajectory", "The trajectory, as the geoarrow layout, as PostGIS EWKB (Binary), or as WKT text (Utf8).")
    .with_argument("strictness", "A string parameter that specifies the strictness level ('strict' or 'relaxed').")
    .with_argument("polygon", "The polygon, as a geoarrow polygon, as PostGIS EWKB (Binary), or as WKT text (Utf8). Ex: `POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))`")
    .with_sql_example("SELECT contained(trajectory_from_text('LINESTRING M(2 2 10, 3 3 20)'), 'strict', 'POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))')")
    .build()
});

/// Implement the ScalarUDFImpl trait for Contained
impl ScalarUDFImpl for Contained {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "contained"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&CONTAINED_UDF_DOC)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_udf_args(
            self.name(),
            arg_types,
            &[
                UdfArg::Trajectory,
                UdfArg::Exact(DataType::Utf8),
                UdfArg::Polygon,
            ],
        )
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        let [trajectory, strictness, polygon] = args else {
            return plan_err!("The 'contained' function expects exactly 3 arguments");
        };
        if !is_trajectory_input(trajectory) {
            return plan_err!(
                "Expected a trajectory, Binary or Utf8 first argument, found {trajectory}"
            );
        }
        if !matches!(strictness, DataType::Utf8) {
            return plan_err!("Expected an UTF8 as the second argument, found {strictness}");
        }
        if !is_polygon_input(polygon) {
            return plan_err!("Expected a polygon, Binary or Utf8 third argument, found {polygon}");
        }
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let (trajectory, trajectory_field) = as_trajectory_array(&arrays[0], &args.arg_fields[0])?;
        let trajectory_array = from_arrow_array(&trajectory, &trajectory_field)
            .map_err(geo_arrow_error_to_datafusion_error)?;
        let trajectory_array = trajectory_array.as_line_string();
        let strictness_parameter_array = as_string_array(&arrays[1]);
        let (polygons, polygon_field) = as_polygon_array(&arrays[2], &args.arg_fields[2])?;
        let polygon_array = from_arrow_array(&polygons, &polygon_field)
            .map_err(geo_arrow_error_to_datafusion_error)?;
        let polygon_array = polygon_array.as_polygon();

        let mut result_builder = BooleanBuilder::with_capacity(trajectory_array.len());

        for (i, trajectory) in trajectory_array.iter().enumerate() {
            if let Some(Ok(trajectory)) = trajectory {
                let strictness_parameter = strictness_parameter_array.value(i);
                let polygon = polygon_array
                    .value(i)
                    .map_err(geo_arrow_error_to_datafusion_error)?
                    .to_polygon();

                let trajectory = trajectory.to_geometry();
                let mut cur_result = false;
                match strictness_parameter {
                    "strict" => {
                        if polygon.contains(&trajectory) {
                            cur_result = true;
                        }
                    }
                    "relaxed" => {
                        if polygon.intersects(&trajectory) || polygon.contains(&trajectory) {
                            cur_result = true;
                        }
                    }
                    _ => {
                        return Err(DataFusionError::Execution(format!(
                            "Invalid strictness parameter. Choose between 'strict' or 'relaxed'"
                        )))
                    }
                }
                result_builder.append_value(cur_result);
            } else {
                result_builder.append_null();
            }
        }

        Ok(ColumnarValue::Array(Arc::new(result_builder.finish())))
    }
}

// TODO: Revisit to refactor
#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::udf::utils::line::TrajectoryFromText;
    use crate::core::udf::utils::trajectory_to_wkb::TrajectoryToWkb;
    use arrow::array::as_boolean_array;
    use datafusion::prelude::SessionContext;

    /// One geometric fact, asserted in every encoding a trajectory can arrive in.
    struct Case {
        name: &'static str,
        trajectory: &'static str,
        strictness: &'static str,
        polygon: &'static str,
        expected: bool,
    }

    const UNIT_SQUARE: &str = "POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))";
    const BIG_SQUARE: &str = "POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))";
    const FAR_SQUARE: &str = "POLYGON((20 20, 20 25, 25 25, 25 20, 20 20))";

    const CASES: &[Case] = &[
        Case {
            name: "touches boundary, strict",
            trajectory: "LINESTRING M(0 0 10, 2 2 20, 3 3 30)",
            strictness: "strict",
            polygon: UNIT_SQUARE,
            expected: false,
        },
        Case {
            name: "touches boundary, relaxed",
            trajectory: "LINESTRING M(0 0 10, 2 2 20, 3 3 30)",
            strictness: "relaxed",
            polygon: UNIT_SQUARE,
            expected: true,
        },
        Case {
            name: "inside, strict",
            trajectory: "LINESTRING M(0 0 10, 2 2 20, 3 3 30)",
            strictness: "strict",
            polygon: BIG_SQUARE,
            expected: true,
        },
        Case {
            name: "inside, relaxed",
            trajectory: "LINESTRING M(0 0 10, 2 2 20, 3 3 30)",
            strictness: "relaxed",
            polygon: BIG_SQUARE,
            expected: true,
        },
        Case {
            name: "wholly outside, strict",
            trajectory: "LINESTRING M(0 0 10, 2 2 20, 3 3 30)",
            strictness: "strict",
            polygon: FAR_SQUARE,
            expected: false,
        },
        Case {
            name: "wholly outside, relaxed",
            trajectory: "LINESTRING M(9 9 10, 8 8 20)",
            strictness: "relaxed",
            polygon: BIG_SQUARE,
            expected: false,
        },
        Case {
            name: "crosses the edge, strict",
            trajectory: "LINESTRING M(1 1 10, 9 9 20)",
            strictness: "strict",
            polygon: BIG_SQUARE,
            expected: false,
        },
        Case {
            name: "crosses the edge, relaxed",
            trajectory: "LINESTRING M(1 1 10, 9 9 20)",
            strictness: "relaxed",
            polygon: BIG_SQUARE,
            expected: true,
        },
    ];

    /// The three shapes a data source can hand the first argument in.
    #[derive(Clone, Copy, Debug)]
    enum Encoding {
        /// `csv.*`, `postgres.*` — the geoarrow layout.
        Geoarrow,
        /// `pg.*` — a geometry column, which arrives as EWKB.
        Ewkb,
        /// WKT text, e.g. `ST_AsText` output.
        Wkt,
    }

    impl Encoding {
        fn argument(self, trajectory: &str) -> String {
            match self {
                Self::Geoarrow => format!("trajectory_from_text('{trajectory}')"),
                Self::Ewkb => format!("trajectory_to_wkb(trajectory_from_text('{trajectory}'))"),
                Self::Wkt => format!("'{trajectory}'"),
            }
        }
    }

    fn context() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udf(TrajectoryFromText::new(geoarrow_schema::CoordType::Separated).into());
        ctx.register_udf(TrajectoryToWkb::new().into());
        ctx.register_udf(Contained::new().into());
        ctx
    }

    async fn evaluate(ctx: &SessionContext, sql: &str) -> bool {
        let batches = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|e| panic!("planning failed for {sql}: {e}"))
            .collect()
            .await
            .unwrap_or_else(|e| panic!("execution failed for {sql}: {e}"));
        as_boolean_array(batches[0].column(0)).value(0)
    }

    /// Every case in every encoding: the encodings agreeing is structural here rather than a
    /// separate assertion. Results are compared as whole named vectors so a failure names the
    /// case instead of an index.
    #[tokio::test]
    async fn holds_in_every_encoding() {
        let ctx: SessionContext = context();

        for encoding in [Encoding::Geoarrow, Encoding::Ewkb, Encoding::Wkt] {
            let mut actual = Vec::with_capacity(CASES.len());
            let mut expected = Vec::with_capacity(CASES.len());

            for case in CASES {
                let sql = format!(
                    "SELECT contained({}, '{}', '{}')",
                    encoding.argument(case.trajectory),
                    case.strictness,
                    case.polygon
                );
                actual.push((case.name, evaluate(&ctx, &sql).await));
                expected.push((case.name, case.expected));
            }

            assert_eq!(actual, expected, "encoding {encoding:?}");
        }
    }

    /// A geoarrow polygon column, the shape `csv.berlinmod.regions.polygon` and
    /// `postgres.berlinmod.regions.polygon` have. The Postgres one carries no extension
    /// metadata, so both variants are covered.
    #[tokio::test]
    async fn accepts_a_geoarrow_polygon_column() {
        use crate::core::utils::geoarrow::get_polygon_array_from_arrow_utf8;
        use crate::core::utils::schema::{GEOARROW_POLYGON_FIELD, POLYGON_DATATYPE};
        use arrow::array::StringArray;
        use arrow::datatypes::{Field, Schema};
        use arrow::record_batch::RecordBatch;
        use datafusion::datasource::MemTable;

        let wkt = StringArray::from(vec!["POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))"]);
        let polygons = get_polygon_array_from_arrow_utf8(&wkt)
            .unwrap()
            .to_array_ref();

        for with_metadata in [true, false] {
            let field = if with_metadata {
                GEOARROW_POLYGON_FIELD.clone().with_name("polygon")
            } else {
                Field::new("polygon", POLYGON_DATATYPE.clone(), true)
            };
            let schema = Arc::new(Schema::new(vec![field]));
            let batch = RecordBatch::try_new(schema.clone(), vec![polygons.clone()]).unwrap();

            let ctx = context();
            ctx.register_table(
                "regions",
                Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
            )
            .unwrap();

            let sql = "SELECT contained(trajectory_from_text('LINESTRING M(1 1 10, 2 2 20)'), \
                       'strict', polygon) FROM regions";
            assert!(
                evaluate(&ctx, sql).await,
                "polygon column with_metadata={with_metadata} was not accepted"
            );
        }
    }

    /// A polygon with a hole: the trajectory runs through the hole, so it is not contained.
    /// The old WKT path dropped interior rings and would have returned true here.
    #[tokio::test]
    async fn honours_interior_rings() {
        let ctx = context();
        let donut = "'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0), (3 3, 3 7, 7 7, 7 3, 3 3))'";
        let sql = format!(
            "SELECT contained(trajectory_from_text('LINESTRING M(4 4 10, 5 5 20)'), 'strict', {donut})"
        );
        assert!(!evaluate(&ctx, &sql).await, "{sql}");
    }

    /// Widening the first argument must not make the function untyped: these are rejected while
    /// planning, not per-row at execution.
    ///
    /// Only the trajectory slot is strict. The other arguments keep DataFusion's ordinary
    /// implicit casts, so `contained(traj, 7, …)` still plans — `7` becomes `'7'` and fails as a
    /// strictness value at execution instead.
    #[tokio::test]
    async fn rejects_wrong_argument_types_while_planning() {
        let ctx = context();

        for sql in [
            "SELECT contained(42, 'strict', 'POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))')",
            "SELECT contained(TIMESTAMP '2007-05-01 00:00:00', 'strict', 'POLYGON((0 0))')",
            "SELECT contained(true, 'strict', 'POLYGON((0 0))')",
            "SELECT contained('LINESTRING M(1 1 10)', 'strict')",
        ] {
            let error = ctx
                .sql(sql)
                .await
                .expect_err(&format!("should not have planned: {sql}"))
                .to_string();

            assert!(
                error.contains("contained"),
                "error should name the function, got: {error}\nfor: {sql}"
            );
        }
    }

    /// An unknown strictness is a value error, so it surfaces at execution.
    #[tokio::test]
    async fn rejects_an_unknown_strictness_at_execution() {
        let ctx = context();
        let error = ctx
            .sql("SELECT contained('LINESTRING M(1 1 10, 2 2 20)', 'sloppy', 'POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))')")
            .await
            .unwrap()
            .collect()
            .await
            .expect_err("'sloppy' is not a strictness")
            .to_string();

        assert!(error.contains("strict"), "unhelpful error: {error}");
    }

    /// The trajectory argument is accepted in every encoding, and a non-trajectory is rejected.
    #[tokio::test]
    async fn accepts_trajectory_encodings() {
        crate::core::utils::trajectory_arg::test_support::accepts_trajectory_encodings(
            "contained({t}, 'strict', 'POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))')",
        )
        .await;
    }
}

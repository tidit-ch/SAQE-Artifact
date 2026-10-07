use crate::core::udf::utils::wkb_to_trajectory::ewkb_to_trajectory_array;
use crate::core::utils::schema::{
    GEOARROW_POLYGON_FIELD, GEOARROW_POLYGON_TYPE, GEOARROW_TRAJECTORY_FIELD, TRAJECTORY_TYPE,
};
use crate::utils::error::geo_arrow_error_to_datafusion_error;
use arrow::array::ArrayRef;
use arrow_schema::{extension::EXTENSION_TYPE_NAME_KEY, DataType, Field, FieldRef};
use datafusion::common::{plan_err, Result};
use datafusion::logical_expr::type_coercion::functions::can_coerce_from;
use geoarrow_array::{
    array::{WkbArray, WktArray},
    cast::{from_wkb, from_wkt},
    GeoArrowArray,
};
use geoarrow_schema::GeoArrowType;
use std::sync::Arc;

/// One expected argument of a UDF.
pub enum UdfArg {
    /// A trajectory in any encoding [`as_trajectory_array`] understands.
    Trajectory,
    /// A polygon in any encoding
    Polygon,
    /// Anything else, matched exactly; string types widen to `Utf8`.
    Exact(DataType),
}

/// Rejects wrong arguments at plan time.
///
/// Trajectory arguments pass through in whichever encoding they arrived as; the rest are
/// coerced to the expected type. `Signature::user_defined` is what routes here — a
/// `Signature::one_of` with a `Utf8` alternative would act as a catch-all, since almost
/// everything casts to `Utf8`.
pub fn coerce_udf_args(
    function: &str,
    arg_types: &[DataType],
    expected: &[UdfArg],
) -> Result<Vec<DataType>> {
    if arg_types.len() != expected.len() {
        return plan_err!(
            "'{function}' expects {} arguments, got {}",
            expected.len(),
            arg_types.len()
        );
    }

    let mut coerced = Vec::with_capacity(arg_types.len());

    for (position, (actual, expect)) in arg_types.iter().zip(expected).enumerate() {
        match expect {
            UdfArg::Trajectory if is_trajectory_input(actual) => coerced.push(actual.clone()),
            UdfArg::Trajectory => {
                return plan_err!(
                    "'{function}' expects a trajectory, PostGIS EWKB (Binary) or WKT (Utf8) as argument {}, found {actual}",
                    position + 1
                )
            }
            UdfArg::Polygon if is_polygon_input(actual) => coerced.push(actual.clone()),
            UdfArg::Polygon => {
                return plan_err!(
                    "'{function}' expects a polygon, PostGIS EWKB (Binary) or WKT (Utf8) as argument {}, found {actual}",
                    position + 1
                )
            }
            // `can_coerce_from` is the rule `TypeSignature::Exact` itself applies, so
            // non-trajectory arguments keep the implicit casts they always had (`5` for a
            // `Float64`, say). Only the trajectory slot is stricter, and deliberately so.
            UdfArg::Exact(wanted) if can_coerce_from(wanted, actual) => coerced.push(wanted.clone()),
            UdfArg::Exact(wanted) => {
                return plan_err!(
                    "'{function}' expects {wanted} as argument {}, found {actual}",
                    position + 1
                )
            }
        }
    }

    Ok(coerced)
}

/// [`coerce_udf_args`] against several alternative signatures; first match wins.
pub fn coerce_udf_args_any(
    function: &str,
    arg_types: &[DataType],
    alternatives: &[&[UdfArg]],
) -> Result<Vec<DataType>> {
    let mut last = None;
    for expected in alternatives {
        match coerce_udf_args(function, arg_types, expected) {
            Ok(coerced) => return Ok(coerced),
            Err(e) => last = Some(e),
        }
    }
    Err(last.expect("at least one alternative"))
}

/// True if [`as_trajectory_array`] can turn `data_type` into a trajectory.
pub fn is_trajectory_input(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
    ) || is_trajectory_list(data_type)
}

pub fn is_polygon_input(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
    ) || is_polygon(data_type)
}

/// Matched structurally: several UDFs declare their own `List<Struct<x, y, m>>` rather than
/// reusing [`TRAJECTORY_DATATYPE`], differing in child field name and nullability.
fn is_trajectory_list(data_type: &DataType) -> bool {
    let DataType::List(vertices) = data_type else {
        return false;
    };
    let DataType::Struct(fields) = vertices.data_type() else {
        return false;
    };
    fields.len() == 3
        && fields
            .iter()
            .zip(["x", "y", "m"])
            .all(|(field, name)| field.name() == name && *field.data_type() == DataType::Float64)
}

fn is_polygon(data_type: &DataType) -> bool {
    let DataType::List(rings) = data_type else {
        return false;
    };
    let DataType::List(vertices) = rings.data_type() else {
        return false;
    };
    let DataType::Struct(fields) = vertices.data_type() else {
        return false;
    };
    fields.len() == 2
        && fields
            .iter()
            .zip(["x", "y"])
            .all(|(field, name)| field.name() == name && *field.data_type() == DataType::Float64)
}

/// Normalises a trajectory argument into a geoarrow-readable array and its field.
///
/// The returned field always carries the extension metadata, so this also repairs an array that
/// lost it — a Parquet round trip drops field metadata unless
/// `datafusion.execution.parquet.skip_metadata` is off.
pub fn as_trajectory_array(array: &ArrayRef, field: &FieldRef) -> Result<(ArrayRef, FieldRef)> {
    let trajectory_field: FieldRef = Arc::new(
        GEOARROW_TRAJECTORY_FIELD
            .clone()
            .with_name(field.name())
            .with_nullable(field.is_nullable()),
    );

    match array.data_type() {
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
            Ok((ewkb_to_trajectory_array(array.as_ref())?, trajectory_field))
        }

        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            let wkt = WktArray::try_from((array.as_ref(), field.as_ref()))
                .map_err(geo_arrow_error_to_datafusion_error)?;
            let geometry = from_wkt(&wkt, GeoArrowType::LineString(TRAJECTORY_TYPE.clone()))
                .map_err(geo_arrow_error_to_datafusion_error)?;
            Ok((geometry.to_array_ref(), trajectory_field))
        }

        data_type if is_trajectory_list(data_type) => {
            if field.metadata().contains_key(EXTENSION_TYPE_NAME_KEY) {
                return Ok((Arc::clone(array), Arc::clone(field)));
            }
            // Keep the array's own layout — only the geoarrow name is missing.
            let described = Field::new(field.name(), data_type.clone(), field.is_nullable())
                .with_metadata(GEOARROW_TRAJECTORY_FIELD.metadata().clone());
            Ok((Arc::clone(array), Arc::new(described)))
        }

        other => plan_err!(
            "expected a trajectory, PostGIS EWKB (Binary) or WKT (Utf8) argument, found {other}"
        ),
    }
}

/// Normalises a polygon argument into a geoarrow-readable array and its field.
///
/// The polygon counterpart of [`as_trajectory_array`]: WKT text, EWKB from a `pg.*` geometry
/// column, or a geoarrow polygon that may have lost its extension metadata.
pub fn as_polygon_array(array: &ArrayRef, field: &FieldRef) -> Result<(ArrayRef, FieldRef)> {
    let polygon_field: FieldRef = Arc::new(
        GEOARROW_POLYGON_FIELD
            .clone()
            .with_name(field.name())
            .with_nullable(field.is_nullable()),
    );
    let to_type = GeoArrowType::Polygon(GEOARROW_POLYGON_TYPE.clone());

    match array.data_type() {
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
            let wkb = WkbArray::try_from((array.as_ref(), field.as_ref()))
                .map_err(geo_arrow_error_to_datafusion_error)?;
            let geometry = from_wkb(&wkb, to_type).map_err(geo_arrow_error_to_datafusion_error)?;
            Ok((geometry.to_array_ref(), polygon_field))
        }

        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            let wkt = WktArray::try_from((array.as_ref(), field.as_ref()))
                .map_err(geo_arrow_error_to_datafusion_error)?;
            let geometry = from_wkt(&wkt, to_type).map_err(geo_arrow_error_to_datafusion_error)?;
            Ok((geometry.to_array_ref(), polygon_field))
        }

        data_type if is_polygon(data_type) => {
            if field.metadata().contains_key(EXTENSION_TYPE_NAME_KEY) {
                return Ok((Arc::clone(array), Arc::clone(field)));
            }
            let described = Field::new(field.name(), data_type.clone(), field.is_nullable())
                .with_metadata(GEOARROW_POLYGON_FIELD.metadata().clone());
            Ok((Arc::clone(array), Arc::new(described)))
        }

        other => plan_err!(
            "expected a polygon, PostGIS EWKB (Binary) or WKT (Utf8) argument, found {other}"
        ),
    }
}

/// Shared harness for the per-UDF argument-handling tests.
///
/// Each trajectory UDF asserts here that it accepts every encoding and rejects a non-trajectory.
/// What a UDF computes is covered by its own tests.
#[cfg(test)]
pub mod test_support {
    use crate::core::udf::utils::line::TrajectoryFromText;
    use datafusion::prelude::SessionContext;

    const TRAJECTORY: &str = "LINESTRING M(1 1 1000, 2 2 2000, 3 3 3000)";

    fn context() -> SessionContext {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| {
            if std::env::var("CONFIG_FILE").is_err() {
                std::env::set_var("CONFIG_FILE", "config/config_dev.json");
            }
        });

        let ctx = SessionContext::new();
        crate::core::udf::init(&ctx);
        ctx.register_udf(TrajectoryFromText::new(geoarrow_schema::CoordType::Separated).into());
        // `st_point` builds the point argument `passes_point` takes; it is not part of `init`.
        ctx.register_udf(geodatafusion::udf::native::constructors::Point::default().into());
        ctx
    }

    /// Asserts that `call` — a SQL call with `{t}` where the trajectory goes — accepts a
    /// trajectory as PostGIS EWKB and as WKT text, and rejects a non-trajectory argument.
    pub async fn accepts_trajectory_encodings(call: &str) {
        let ctx = context();

        for (encoding, argument) in [
            (
                "EWKB",
                format!("trajectory_to_wkb(trajectory_from_text('{TRAJECTORY}'))"),
            ),
            ("WKT", format!("'{TRAJECTORY}'")),
        ] {
            let sql = format!("SELECT {}", call.replace("{t}", &argument));
            let batches = ctx
                .sql(&sql)
                .await
                .unwrap_or_else(|e| panic!("{encoding} rejected while planning: {e}\n{sql}"))
                .collect()
                .await
                .unwrap_or_else(|e| panic!("{encoding} failed at execution: {e}\n{sql}"));

            assert_eq!(
                batches.iter().map(|b| b.num_rows()).sum::<usize>(),
                1,
                "{encoding} produced no row for {sql}"
            );
        }

        let sql = format!("SELECT {}", call.replace("{t}", "42"));
        assert!(
            ctx.sql(&sql).await.is_err(),
            "a non-trajectory argument should be rejected while planning: {sql}"
        );
    }
}

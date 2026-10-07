//! Conversions between PostGIS EWKB and the geoarrow trajectory layout.
//!
//! Tables reached through the [`pg`](crate::core::postgres::dynamic) catalog have their schema
//! derived from the database, so a geometry column arrives as `Binary` holding the raw EWKB
//! from the Postgres wire format rather than as
//! [`TRAJECTORY_DATATYPE`](crate::core::utils::schema::TRAJECTORY_DATATYPE). These two UDFs
//! bridge that gap in both directions:
//!
//! ```sql
//! SELECT during(wkb_to_trajectory(polyline), …) FROM pg.public.trips;
//!
//! INSERT INTO pg.public.trips
//!   SELECT trip_id, moid, trajectory_to_wkb(polyline) FROM csv.berlinmod.trips;
//! ```
//!
//! EWKB carries the SRID and the M ordinate, so the round trip is lossless.

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, Float64Builder, ListBuilder, StructBuilder,
};
use arrow_schema::{
    extension::{ExtensionType, EXTENSION_TYPE_NAME_KEY},
    DataType, Field, FieldRef, Fields,
};
use datafusion::common::{plan_err, Result};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    scalar_doc_sections::DOC_SECTION_OTHER, ColumnarValue, Documentation, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use geoarrow_schema::LineStringType;
use postgis::ewkb::{AsEwkbLineString, EwkbRead, EwkbWrite, LineStringM, PointM};
use std::any::Any;
use std::sync::{Arc, LazyLock};

use crate::core::utils::schema::TRAJECTORY_DATATYPE;

/// SRID every trajectory is stored in.
const SRID: Option<i32> = Some(4326);

fn coord_fields() -> Fields {
    Fields::from(vec![
        Field::new("x", DataType::Float64, false),
        Field::new("y", DataType::Float64, false),
        Field::new("m", DataType::Float64, false),
    ])
}

// ---------------------------------------------------------------------------------------
// wkb_to_trajectory
// ---------------------------------------------------------------------------------------

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct WkbToTrajectory {
    signature: Signature,
}

impl Default for WkbToTrajectory {
    fn default() -> Self {
        Self::new()
    }
}

impl WkbToTrajectory {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Binary], Volatility::Immutable),
        }
    }

    pub fn into_udf() -> ScalarUDF {
        ScalarUDF::from(Self::new())
    }
}

static WKB_TO_TRAJECTORY_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Decodes a PostGIS EWKB LineString M into the geoarrow trajectory layout, so that \
         geometry columns read from the `pg` catalog can be passed to the trajectory UDFs.",
        "wkb_to_trajectory(wkb: Binary) -> List<Struct{x: Float64, y: Float64, m: Float64}>",
    )
    .with_argument("wkb", "A Binary column holding PostGIS EWKB, as produced by a geometry column in the `pg` catalog.")
    .with_sql_example("SELECT during(wkb_to_trajectory(polyline), TIMESTAMP '2007-05-28 08:00:00', TIMESTAMP '2007-05-28 09:00:00') FROM pg.public.trips")
    .build()
});

impl ScalarUDFImpl for WkbToTrajectory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "wkb_to_trajectory"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&WKB_TO_TRAJECTORY_DOC)
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        match args {
            [DataType::Binary] => Ok(TRAJECTORY_DATATYPE.clone()),
            _ => plan_err!("'wkb_to_trajectory' expects exactly one Binary argument"),
        }
    }

    /// Attaches the geoarrow extension metadata, without which the trajectory UDFs will not
    /// accept the result.
    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(
            Field::new("wkb_to_trajectory", TRAJECTORY_DATATYPE.clone(), true).with_metadata(
                [(
                    EXTENSION_TYPE_NAME_KEY.to_string(),
                    LineStringType::NAME.to_owned(),
                )]
                .into(),
            ),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let wkb = arrays[0]
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("wkb_to_trajectory expects a Binary array".into())
            })?;

        let fields = coord_fields();
        let mut list = ListBuilder::new(StructBuilder::new(
            fields.clone(),
            vec![
                Box::new(Float64Builder::new()),
                Box::new(Float64Builder::new()),
                Box::new(Float64Builder::new()),
            ],
        ))
        .with_field(Arc::new(Field::new(
            "vertices",
            DataType::Struct(fields),
            false,
        )));

        for i in 0..wkb.len() {
            if wkb.is_null(i) {
                list.append_null();
                continue;
            }

            let mut cursor = std::io::Cursor::new(wkb.value(i));
            let line: LineStringM = EwkbRead::read_ewkb(&mut cursor).map_err(|e| {
                DataFusionError::Execution(format!("wkb_to_trajectory: bad EWKB LineStringM: {e}"))
            })?;

            let sb = list.values();
            for point in line.points {
                sb.field_builder::<Float64Builder>(0)
                    .unwrap()
                    .append_value(point.x);
                sb.field_builder::<Float64Builder>(1)
                    .unwrap()
                    .append_value(point.y);
                sb.field_builder::<Float64Builder>(2)
                    .unwrap()
                    .append_value(point.m);
                sb.append(true);
            }
            list.append(true);
        }

        Ok(ColumnarValue::Array(Arc::new(list.finish()) as ArrayRef))
    }
}

// ---------------------------------------------------------------------------------------
// trajectory_to_wkb
// ---------------------------------------------------------------------------------------

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct TrajectoryToWkb {
    signature: Signature,
}

impl Default for TrajectoryToWkb {
    fn default() -> Self {
        Self::new()
    }
}

impl TrajectoryToWkb {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![TRAJECTORY_DATATYPE.clone()], Volatility::Immutable),
        }
    }

    pub fn into_udf() -> ScalarUDF {
        ScalarUDF::from(Self::new())
    }
}

static TRAJECTORY_TO_WKB_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Encodes a geoarrow trajectory as PostGIS EWKB LineString M (SRID 4326), so it can be \
         inserted into a geometry column through the `pg` catalog.",
        "trajectory_to_wkb(trajectory: List<Struct{x: Float64, y: Float64, m: Float64}>) -> Binary",
    )
    .with_argument("trajectory", "A trajectory column in the geoarrow LineString M layout.")
    .with_sql_example("INSERT INTO pg.public.trips SELECT trip_id, moid, trajectory_to_wkb(polyline) FROM csv.berlinmod.trips")
    .build()
});

impl ScalarUDFImpl for TrajectoryToWkb {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "trajectory_to_wkb"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&TRAJECTORY_TO_WKB_DOC)
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        use arrow::array::{Float64Array, ListArray, StructArray};

        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let list = arrays[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("trajectory_to_wkb expects a trajectory array".into())
            })?;

        let mut out = BinaryBuilder::new();

        for row in 0..list.len() {
            if list.is_null(row) {
                out.append_null();
                continue;
            }

            let coords = list.value(row);
            let coords = coords
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "trajectory_to_wkb: expected Struct coordinates".into(),
                    )
                })?;

            let child = |name: &str| -> Result<&Float64Array> {
                coords
                    .column_by_name(name)
                    .and_then(|a| a.as_any().downcast_ref::<Float64Array>())
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "trajectory_to_wkb: missing Float64 coordinate field '{name}'"
                        ))
                    })
            };
            let (xs, ys, ms) = (child("x")?, child("y")?, child("m")?);

            let points = (0..coords.len())
                .map(|i| PointM {
                    x: xs.value(i),
                    y: ys.value(i),
                    m: ms.value(i),
                    srid: SRID,
                })
                .collect();

            let mut bytes: Vec<u8> = Vec::new();
            LineStringM { points, srid: SRID }
                .as_ewkb()
                .write_ewkb(&mut bytes)
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "trajectory_to_wkb: EWKB encode failed: {e}"
                    ))
                })?;
            out.append_value(&bytes);
        }

        Ok(ColumnarValue::Array(Arc::new(out.finish()) as ArrayRef))
    }
}

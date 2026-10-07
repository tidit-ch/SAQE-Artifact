//! Encodes a geoarrow trajectory as PostGIS EWKB.

use arrow::array::{Array, ArrayRef, BinaryBuilder};
use arrow_schema::DataType;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    scalar_doc_sections::DOC_SECTION_OTHER, ColumnarValue, Documentation, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use postgis::ewkb::{AsEwkbLineString, EwkbWrite, LineStringM, PointM};
use std::any::Any;
use std::sync::{Arc, LazyLock};

use crate::core::utils::schema::TRAJECTORY_DATATYPE;

/// SRID every trajectory is stored in.
const SRID: Option<i32> = Some(4326);

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
        "Encodes a geoarrow trajectory as PostGIS EWKB LineString M (SRID 4326).",
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

//! Decodes PostGIS EWKB into the geoarrow trajectory layout.

use arrow::array::{Array, ArrayRef, BinaryArray, Float64Builder, ListBuilder, StructBuilder};
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
use postgis::ewkb::{EwkbRead, LineStringM};
use std::any::Any;
use std::sync::{Arc, LazyLock};

use crate::core::utils::schema::TRAJECTORY_DATATYPE;

fn coord_fields() -> Fields {
    Fields::from(vec![
        Field::new("x", DataType::Float64, false),
        Field::new("y", DataType::Float64, false),
        Field::new("m", DataType::Float64, false),
    ])
}

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
        "Decodes a PostGIS EWKB LineString M into the geoarrow trajectory layout.",
        "wkb_to_trajectory(wkb: Binary) -> List<Struct{x: Float64, y: Float64, m: Float64}>",
    )
    .with_argument("wkb", "A Binary column holding PostGIS EWKB.")
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
        Ok(ColumnarValue::Array(ewkb_to_trajectory_array(
            arrays[0].as_ref(),
        )?))
    }
}

/// Decodes EWKB LineString M values into the geoarrow trajectory layout.
pub fn ewkb_to_trajectory_array(array: &dyn Array) -> Result<ArrayRef> {
    {
        let wkb = array
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

        Ok(Arc::new(list.finish()) as ArrayRef)
    }
}

use crate::core::utils::schema::MULTILINESTRING_DATATYPE;
use crate::core::utils::trajectory_arg::{as_trajectory_array, coerce_udf_args, UdfArg};
use crate::utils::error::ToDataFusionError;
use arrow_schema::{
    extension::{ExtensionType, EXTENSION_TYPE_NAME_KEY},
    DataType, Field, FieldRef,
};
use datafusion::common::Result as DFResult;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use geo_traits::{CoordTrait, LineStringTrait};
use geoarrow_array::{
    array::from_arrow_array, builder::MultiLineStringBuilder, cast::AsGeoArrowArray, GeoArrowArray,
};
use geoarrow_array::{array::LineStringArray, GeoArrowArrayAccessor};
use geoarrow_schema::{Metadata, MultiLineStringType};
use std::any::Any;
use std::hash::Hash;
use std::sync::Arc;
use wkt::types::{
    Coord as WktCoord, Dimension as WktDimension, LineString as WktLineString,
    MultiLineString as WktMultiLineString,
};

use crate::core::parser::ast::{CropASTNode, Eval};

/// ScalarUDF that performs the crop operation on a polyline column.
///
/// Signature:  `crop(polyline_col)  ->  <same type as polyline_col>`
///
/// The predicate logic is stored directly on this struct as `Arc<CropASTNode>`.
/// No serialization is needed — the Arc is cloned through the logical plan and
/// arrives intact in `invoke_with_args` where the actual crop logic lives.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct CropProjectionUdf {
    pub crop_udf: Arc<CropASTNode>,
    signature: Signature,
}

impl CropProjectionUdf {
    pub fn new(crop_udf: Arc<CropASTNode>) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            crop_udf,
        }
    }
}

impl ScalarUDFImpl for CropProjectionUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "crop"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> datafusion::common::Result<Vec<DataType>> {
        coerce_udf_args(self.name(), arg_types, &[UdfArg::Trajectory])
    }

    fn return_type(&self, _args: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(MULTILINESTRING_DATATYPE.clone())
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> DFResult<FieldRef> {
        Ok(Arc::new(
            Field::new("crop", MULTILINESTRING_DATATYPE.clone(), true).with_metadata(
                [(
                    EXTENSION_TYPE_NAME_KEY.to_string(),
                    MultiLineStringType::NAME.to_owned(),
                )]
                .into(),
            ),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let (trajectory_0, trajectory_0_field) =
            as_trajectory_array(&arrays[0], &args.arg_fields[0])?;
        let trajectory_array =
            from_arrow_array(&trajectory_0, &trajectory_0_field).to_df_error()?;
        let trajectory_array = trajectory_array.as_line_string();

        let wkt_vector = get_wkt_multilinestrings_from_geoarrow_linestrings(trajectory_array);

        let expr = self.crop_udf.expr.clone().unwrap();
        let result_vector = expr.eval(&wkt_vector)?;

        let mut result_builder = MultiLineStringBuilder::new(MultiLineStringType::new(
            geoarrow_schema::Dimension::XYM,
            Arc::new(Metadata::default()),
        ));

        for wkt_mls in result_vector {
            result_builder
                .push_multi_line_string(Some(&wkt_mls))
                .to_df_error()?;
        }
        Ok(ColumnarValue::Array(
            result_builder.finish().into_array_ref(),
        ))
    }
}

pub fn get_wkt_multilinestrings_from_geoarrow_linestrings(
    ls_array: &LineStringArray,
) -> Vec<WktMultiLineString<f64>> {
    let mut wkt_multilinestrings = Vec::new();
    for maybe_ls in ls_array.iter() {
        if let Some(Ok(ls)) = maybe_ls {
            let mut coords = Vec::new();
            for coord in ls.coords() {
                coords.push(WktCoord {
                    x: coord.x(),
                    y: coord.y(),
                    z: None,
                    m: Some(coord.nth(2).unwrap_or_default()),
                });
            }
            wkt_multilinestrings.push(WktMultiLineString::new(
                vec![WktLineString::new(coords, WktDimension::XYM)],
                WktDimension::XYM,
            ));
        } else {
            wkt_multilinestrings.push(WktMultiLineString::new(Vec::new(), WktDimension::XYM));
        }
    }
    wkt_multilinestrings
}

use arrow_schema::extension::{ExtensionType, EXTENSION_TYPE_NAME_KEY};
use arrow_schema::DataType;
use datafusion::logical_expr::{
    scalar_doc_sections::DOC_SECTION_OTHER, Documentation, ScalarUDFImpl, Signature,
};
use geoarrow_array::{array::WktArray, cast::from_wkt};
use geoarrow_schema::{CoordType, Dimension, LineStringType};
use std::sync::{Arc, LazyLock};

use crate::utils::error::geo_arrow_error_to_datafusion_error;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct TrajectoryFromText {
    signature: Signature,
    coord_type: CoordType,
}

impl TrajectoryFromText {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8],
                datafusion::logical_expr::Volatility::Immutable,
            ),
            coord_type,
        }
    }
}

impl Default for TrajectoryFromText {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static TRAJECTORY_FROM_TEXT_UDF_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Converts a Well-Known Text (WKT) LineString M representation into a trajectory \
         (List of Structs with x, y, m coordinates). The M coordinate represents the timestamp.",
        "trajectory_from_text(wkt: Utf8) -> List<Struct{x: Float64, y: Float64, m: Float64}>",
    )
    .with_argument(
        "wkt",
        "A WKT LineString M text representation. Ex: `LINESTRING M(0 0 0, 1 1 1000, 2 2 2000)`",
    )
    .with_sql_example("SELECT trajectory_from_text('LINESTRING M(0 0 0, 1 1 1000, 2 2 2000)')")
    .build()
});

impl ScalarUDFImpl for TrajectoryFromText {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "trajectory_from_text"
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&TRAJECTORY_FROM_TEXT_UDF_DOC)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Err(datafusion::error::DataFusionError::Internal(
            "return_type".to_string(),
        ))
    }

    fn return_field_from_args(
        &self,
        _args: datafusion::logical_expr::ReturnFieldArgs,
    ) -> datafusion::error::Result<arrow_schema::FieldRef> {
        let linestring_type = LineStringType::new(Dimension::XYM, Default::default())
            .with_coord_type(self.coord_type);
        Ok(Arc::new(
            linestring_type.to_field("", true).with_metadata(
                [(
                    EXTENSION_TYPE_NAME_KEY.to_owned(),
                    LineStringType::NAME.to_owned(),
                )]
                .into_iter()
                .collect(),
            ),
        )
        .into())
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> datafusion::error::Result<datafusion::logical_expr::ColumnarValue> {
        let array = &datafusion::logical_expr::ColumnarValue::values_to_arrays(&args.args)?[0];
        let field = &args.arg_fields[0];
        let to_type = geoarrow_schema::GeoArrowType::from_arrow_field(args.return_field.as_ref())
            .map_err(geo_arrow_error_to_datafusion_error)?;
        let geom_arr = from_wkt(
            &WktArray::try_from((array.as_ref(), field.as_ref()))
                .map_err(geo_arrow_error_to_datafusion_error)?,
            to_type,
        )
        .map_err(geo_arrow_error_to_datafusion_error)?;

        Ok(datafusion::logical_expr::ColumnarValue::Array(
            geom_arr.to_array_ref(),
        ))
    }
}

#[cfg(test)]
mod test {
    use datafusion::prelude::SessionContext;
    use geo_traits::{CoordTrait, LineStringTrait};
    use geoarrow_array::{array::LineStringArray, GeoArrowArray, GeoArrowArrayAccessor};
    use geoarrow_schema::CoordType;

    use super::*;

    #[tokio::test]
    async fn test_trajectory_from_text() {
        let ctx = SessionContext::new();

        ctx.register_udf(TrajectoryFromText::new(CoordType::Separated).into());
        let sql_df = ctx
            .sql("SELECT trajectory_from_text('LINESTRING M (1 1 10, 2 2 20, 3 3 30)')")
            .await
            .unwrap();

        let output_batches = sql_df.collect().await.unwrap();
        assert_eq!(output_batches.len(), 1);
        let output_batch = &output_batches[0];
        let output_schema = output_batch.schema();
        let output_field = output_schema.field(0);
        let output_column = output_batch.column(0);

        let linestring_arr =
            LineStringArray::try_from((output_column.as_ref(), output_field)).unwrap();
        assert_eq!(linestring_arr.len(), 1, "Expected one row in result");

        let linestring = linestring_arr.value(0).unwrap();
        assert_eq!(
            linestring.num_coords(),
            3,
            "Expected three coordinates in linestring"
        );
        let mid_point = linestring.coord(1).unwrap();
        assert_eq!(
            mid_point.x(),
            2.0,
            "Expected x coordinate of second point to be 2.0"
        );
        assert_eq!(
            mid_point.y(),
            2.0,
            "Expected y coordinate of second point to be 2.0"
        );
        assert_eq!(
            mid_point.nth(2).unwrap(),
            20.0,
            "Expected m coordinate of second point to be 20.0"
        );
    }
}

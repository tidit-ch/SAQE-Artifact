use arrow_schema::{
    extension::{ExtensionType, EXTENSION_TYPE_NAME_KEY},
    DataType, Field,
};
use datafusion::common::DFSchema;
use geoarrow_schema::{
    Dimension, LineStringType, Metadata, MultiLineStringType, PointType, PolygonType,
};
use std::sync::{Arc, LazyLock};

pub static TRAJECTORY_TYPE: LazyLock<LineStringType> =
    LazyLock::new(|| LineStringType::new(Dimension::XYM, Arc::new(Metadata::default())));

pub static TRAJECTORY_DATATYPE: LazyLock<DataType> = LazyLock::new(|| TRAJECTORY_TYPE.data_type());

/// [`TRAJECTORY_DATATYPE`] with the geoarrow extension metadata, without which the geoarrow
/// readers reject the array.
pub static GEOARROW_TRAJECTORY_FIELD: LazyLock<Field> = LazyLock::new(|| {
    Field::new("", TRAJECTORY_DATATYPE.clone(), true).with_metadata(
        [(
            EXTENSION_TYPE_NAME_KEY.to_owned(),
            LineStringType::NAME.to_owned(),
        )]
        .into_iter()
        .collect(),
    )
});

pub static LINESTRING_XY_DATATYPE: LazyLock<DataType> =
    LazyLock::new(|| LineStringType::new(Dimension::XY, Arc::new(Metadata::default())).data_type());

pub static MULTILINESTRING_DATATYPE: LazyLock<DataType> = LazyLock::new(|| {
    MultiLineStringType::new(Dimension::XYM, Arc::new(Metadata::default())).data_type()
});

pub static POINT_XY_DATATYPE: LazyLock<DataType> =
    LazyLock::new(|| PointType::new(Dimension::XY, Arc::new(Metadata::default())).data_type());

pub static GEOARROW_POLYGON_TYPE: LazyLock<PolygonType> =
    LazyLock::new(|| PolygonType::new(Dimension::XY, Arc::new(Metadata::default())));

pub static POLYGON_DATATYPE: LazyLock<DataType> =
    LazyLock::new(|| GEOARROW_POLYGON_TYPE.data_type());

pub static GEOARROW_POLYGON_FIELD: LazyLock<Field> = LazyLock::new(|| {
    GEOARROW_POLYGON_TYPE
        .clone()
        .to_field("", true)
        .with_metadata(
            [(
                EXTENSION_TYPE_NAME_KEY.to_owned(),
                PolygonType::NAME.to_owned(),
            )]
            .into_iter()
            .collect(),
        )
});

pub fn dfschema_to_hashmap(dfschema: &DFSchema) -> std::collections::HashMap<String, String> {
    dfschema
        .fields()
        .iter()
        .map(|field| {
            let name = field.name().to_string();
            let data_type = if field.metadata().get(EXTENSION_TYPE_NAME_KEY).is_some() {
                field
                    .metadata()
                    .get(EXTENSION_TYPE_NAME_KEY)
                    .unwrap()
                    .to_string()
            } else {
                field.data_type().to_string()
            };
            (name, data_type)
        })
        .collect()
}

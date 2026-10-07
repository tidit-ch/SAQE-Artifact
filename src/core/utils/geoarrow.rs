use arrow::array::Array;
use geoarrow_array::{array::WktArray, cast::from_wkt, GeoArrowArray};
use geoarrow_schema::error::GeoArrowResult;
use std::sync::Arc;

use crate::core::utils::schema::{GEOARROW_POLYGON_FIELD, GEOARROW_POLYGON_TYPE};

pub fn get_polygon_array_from_wkt(array: &WktArray) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let to_type = geoarrow_schema::GeoArrowType::Polygon(
        GEOARROW_POLYGON_TYPE
            .clone()
            .with_coord_type(geoarrow_schema::CoordType::Separated),
    );
    from_wkt(array, to_type)
}

pub fn get_polygon_array_from_arrow_utf8(
    array: &dyn Array,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let polygon_field = GEOARROW_POLYGON_FIELD.clone();
    let wkt_array = WktArray::try_from((array, &polygon_field))?;
    get_polygon_array_from_wkt(&wkt_array)
}

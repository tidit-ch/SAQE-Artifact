use datafusion::{
    arrow::array::GenericListArray,
    common::{error::DataFusionError, Result as DatafusionResult},
};
use geoarrow_array::array::LineStringArray;
use geoarrow_schema::{Dimension, LineStringType, Metadata};
use std::sync::Arc;

/// Converts meters to an approximate degree value.
pub fn meters_to_degrees(meters: f64) -> f64 {
    let meters_per_deg_lat: f64 = 111_320.0;
    let meters_per_deg_lon: f64 = 111_320.0 * 52.52_f64.to_radians().cos();
    let meters_per_deg = meters_per_deg_lat.min(meters_per_deg_lon);
    meters / meters_per_deg
}

// TODO: Remove this function. The metadata should be coming from the table schema itself and we should be using the interal functions of geoarrow to do the conversion.
/// Converts a GenericListArray to a GeoArrow LineStringArray with the specified dimension.
pub fn generic_list_array_to_geo_linestring_array(
    trajectory_array: &GenericListArray<i32>,
    dimension: Dimension,
) -> DatafusionResult<LineStringArray> {
    let ls_type: LineStringType = LineStringType::new(dimension, Arc::new(Metadata::default()));
    let trajectory_array: LineStringArray = LineStringArray::try_from((trajectory_array, ls_type))
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to cast trajectory array to LineStringArray: {}",
                e
            ))
        })?;
    Ok(trajectory_array)
}

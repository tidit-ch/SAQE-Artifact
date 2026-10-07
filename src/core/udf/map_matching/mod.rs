//! Map-matching UDFs — snap raw GPS trajectories onto the road network via a GraphHopper
//! sidecar service (see `services/graphhopper/`).

pub mod map_match;

use datafusion::logical_expr::{Documentation, ScalarUDF};

pub fn get_udf_list() -> Vec<ScalarUDF> {
    vec![map_match::MapMatch::into_udf()]
}

/// Documentation for the map-matching UDFs, keyed by UDF name.
///
/// These are async UDFs wrapped in `AsyncScalarUDF`, which does not forward
/// `ScalarUDF::documentation()`, so `core::udf::get_udf_details` looks them up here to
/// surface them on `/datafusion/udf_details`.
pub fn get_udf_docs() -> Vec<(&'static str, &'static Documentation)> {
    vec![("map_match", map_match::documentation())]
}

//! UDFs bridging PostGIS EWKB and the geoarrow trajectory layout.

pub mod wkb;

use datafusion::logical_expr::ScalarUDF;

pub fn get_udf_list() -> Vec<ScalarUDF> {
    vec![
        wkb::WkbToTrajectory::into_udf(),
        wkb::TrajectoryToWkb::into_udf(),
    ]
}

//! Helpers the UDFs build on rather than UDFs that answer a query.

pub mod line;
pub mod trajectory_to_wkb;
pub mod wkb_to_trajectory;

use datafusion::logical_expr::ScalarUDF;

pub fn get_udf_list() -> Vec<ScalarUDF> {
    vec![
        wkb_to_trajectory::WkbToTrajectory::into_udf(),
        trajectory_to_wkb::TrajectoryToWkb::into_udf(),
    ]
}

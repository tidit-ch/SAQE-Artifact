pub mod contained;
pub mod crosses;
pub mod leaves;
pub mod leaves_and_returns;
pub mod passes_point;
pub mod polylines_intersect_in_polygon;
pub mod properly_contained;
pub mod timestamp_at_position;
use datafusion::logical_expr::ScalarUDF;

pub fn get_udf_list() -> Vec<ScalarUDF> {
    vec![
        contained::Contained::new().into(),
        passes_point::PassesPoint::new().into(),
        polylines_intersect_in_polygon::PolylinesIntersectInPolygon::new().into(),
        properly_contained::ProperlyContained::new().into(),
        crosses::Crosses::new().into(),
        leaves_and_returns::LeavesAndReturns::new().into(),
        leaves::Leaves::new().into(),
        timestamp_at_position::TimestampAtPosition::new().into(),
    ]
}

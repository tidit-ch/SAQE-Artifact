pub mod contains;
pub mod duration;
pub mod during;
pub mod equal;
pub mod finished_by;
pub mod finishes;
pub mod meets;
pub mod met_by;
pub mod overlapped_by;
pub mod overlaps;
pub mod point_at_timestamp;
pub mod preceded_by;
pub mod precedes;
pub mod present;
pub mod started_by;
pub mod starts;
pub mod subpolyline_at;
pub mod subpolyline_between;
pub mod tdwithin;

pub fn get_udf_list() -> Vec<datafusion::logical_expr::ScalarUDF> {
    vec![
        contains::Contains::new().into(),
        duration::Duration::new().into(),
        during::During::new().into(),
        equal::Equal::new().into(),
        finished_by::FinishedBy::new().into(),
        finishes::Finishes::new().into(),
        meets::Meets::new().into(),
        met_by::MetBy::new().into(),
        overlapped_by::OverlappedBy::new().into(),
        overlaps::Overlaps::new().into(),
        point_at_timestamp::PointAtTimestamp::new().into(),
        preceded_by::PrecededBy::new().into(),
        precedes::Precedes::new().into(),
        present::Present::new().into(),
        started_by::StartedBy::new().into(),
        starts::Starts::new().into(),
        subpolyline_at::SubPolylineAt::new().into(),
        subpolyline_between::SubPolylineBetween::new().into(),
        tdwithin::Tdwithin::new().into(),
    ]
}

pub mod st_contained;

pub fn get_udf_list() -> Vec<datafusion::logical_expr::ScalarUDF> {
    vec![st_contained::SpatioTemporalContained::new().into()]
}

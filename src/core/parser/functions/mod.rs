use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use super::ast::CropFunctionCall;
use crate::utils::error::SaqeResult;
use wkt::types::MultiLineString as WktMultiLineString;

pub mod contained;
pub use crate::core::udf::doc::UdfDoc;
use contained::ContainedFn;
pub mod during;
pub mod follows;
pub mod not_contained;
pub mod not_during;
pub mod precedes;

type FnMap = HashMap<&'static str, Arc<dyn CropFunction>>;

pub fn function_registry() -> &'static FnMap {
    static REGISTRY: OnceLock<FnMap> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        let mut m: FnMap = HashMap::new();
        let contained_fn = ContainedFn;
        m.insert(contained_fn.name(), Arc::new(contained_fn));
        let not_contained_fn = not_contained::NotContainedFn;
        m.insert(not_contained_fn.name(), Arc::new(not_contained_fn));
        let not_during_fn = not_during::NotDuringFn;
        m.insert(not_during_fn.name(), Arc::new(not_during_fn));
        let during_fn = during::DuringFn;
        m.insert(during_fn.name(), Arc::new(during_fn));
        let follows_fn = follows::FollowsFn;
        m.insert(follows_fn.name(), Arc::new(follows_fn));
        let precedes_fn = precedes::PrecedesFn;
        m.insert(precedes_fn.name(), Arc::new(precedes_fn));
        m
    })
}

pub trait CropFunction: Send + Sync + UdfDoc {
    fn name(&self) -> &'static str;
    fn eval(
        &self,
        input: &[WktMultiLineString<f64>],
        call: &CropFunctionCall,
    ) -> SaqeResult<Vec<WktMultiLineString<f64>>>;
}

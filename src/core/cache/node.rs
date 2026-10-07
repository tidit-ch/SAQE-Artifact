use datafusion::{
    common::{DFSchemaRef, Result},
    logical_expr::{logical_plan::UserDefinedLogicalNodeCore, LogicalPlan},
    prelude::Expr,
};
use std::vec;

use super::CACHE_MANAGER;

#[derive(Debug, Clone, Hash, PartialEq, PartialOrd, Eq)]
pub struct CacheNode {
    cache_key: String,
    base_plan: LogicalPlan,
    exprs: Vec<Expr>,
    available: bool,
}

impl CacheNode {
    pub fn new(cache_key: String, base_plan: LogicalPlan, exprs: Vec<Expr>) -> Self {
        let available = CACHE_MANAGER.is_in_cache(&cache_key);
        CacheNode {
            cache_key,
            base_plan,
            exprs,
            available,
        }
    }

    pub fn is_available(&self) -> bool {
        self.available
    }

    pub fn get_cache_key(&self) -> String {
        self.cache_key.clone()
    }

    pub fn with_base_plan(&mut self, base_plan: LogicalPlan) -> &mut Self {
        self.base_plan = base_plan;
        self
    }

    pub fn with_cache_key(&mut self, cache_key: String) -> &mut Self {
        self.cache_key = cache_key;
        self
    }
}

impl Default for CacheNode {
    fn default() -> Self {
        CacheNode {
            cache_key: String::new(),
            base_plan: LogicalPlan::default(),
            exprs: vec![],
            available: false,
        }
    }
}

impl UserDefinedLogicalNodeCore for CacheNode {
    fn name(&self) -> &str {
        "CacheNode"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        // if self.available {
        //     vec![]
        // } else {
        vec![&self.base_plan]
        // }
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.base_plan.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "CacheNode: Query results are cached, cache key: {}",
            self.cache_key
        )
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(CacheNode {
            base_plan: self.base_plan.clone(),
            cache_key: self.cache_key.clone(),
            exprs,
            available: self.available,
        })
    }
}

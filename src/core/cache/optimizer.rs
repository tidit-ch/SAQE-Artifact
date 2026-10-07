use super::node::CacheNode;
use datafusion::{
    common::{tree_node::Transformed, Result},
    logical_expr::{Extension, FetchType, Filter, LogicalPlan, SkipType},
    optimizer::OptimizerRule,
};
use std::{sync::Arc, vec};

#[derive(Debug, Clone)]
pub struct CacheOptimizer {}

impl CacheOptimizer {
    pub fn new() -> Self {
        CacheOptimizer {}
    }
}

// Here we modify the logical plan to insert the custom node operators, CacheNode in this case.
impl OptimizerRule for CacheOptimizer {
    fn name(&self) -> &str {
        "CacheOptimizer"
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn datafusion::optimizer::OptimizerConfig,
    ) -> Result<
        datafusion::common::tree_node::Transformed<LogicalPlan>,
        datafusion::error::DataFusionError,
    > {
        // println!("[CacheOptimizer] plan :: {:#?}", plan);
        let LogicalPlan::Limit(ref limit) = plan else {
            return Ok(Transformed::no(plan));
        };
        // println!("[CacheOptimizer] limit :: {:#?}", limit);
        let LogicalPlan::Filter(ref filter) = limit.input.as_ref() else {
            return Ok(Transformed::no(plan));
        };
        // println!("[CacheOptimizer] filter :: {:#?}", filter);
        let Filter {
            predicate,
            input: filter_input,
            ..
        } = filter;

        let skip_type = limit.get_skip_type()?;
        let fetch_type = limit.get_fetch_type()?;

        let mut cache_key = predicate.to_string();
        match skip_type {
            SkipType::Literal(skip) => cache_key.push_str(&format!("_skip_{}", skip)),
            SkipType::UnsupportedExpr => cache_key.push_str("_skip_none"),
        }
        match fetch_type {
            FetchType::Literal(fetch) => {
                cache_key.push_str(&format!("_fetch_{}", fetch.unwrap_or(0)))
            }
            FetchType::UnsupportedExpr => cache_key.push_str("_fetch_none"),
        }
        println!("[CacheOptimizer] cache_key :: {}", cache_key);
        // Replace the TableScan with CacheNode
        if let LogicalPlan::TableScan(ref table_scan) = filter_input.as_ref() {
            return Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                node: Arc::new(CacheNode::new(cache_key.clone(), plan.clone(), vec![])),
            })));
        }
        Ok(Transformed::no(plan))
    }
}

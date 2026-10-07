pub mod execution_planner;
pub mod manager;
pub mod node;
pub mod optimizer;
pub mod query_planner;

use async_trait::async_trait;
use datafusion::{
    common::Result,
    execution::{context::QueryPlanner, SessionState},
    logical_expr::{LogicalPlan, UserDefinedLogicalNode},
    physical_plan::ExecutionPlan,
    physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner},
};
use std::{
    sync::{Arc, LazyLock},
    vec,
};

use super::cache::{
    execution_planner::{CacheExecPlan, CacheUpdateExecPlan},
    manager::CacheManager,
    node::CacheNode,
};

static CACHE_MANAGER: LazyLock<CacheManager> = LazyLock::new(|| CacheManager::new());

#[derive(Debug, Clone)]
pub struct CachePlanner {} // Responsible for giving out the ExecutionPlan for Custom Operators

impl CachePlanner {
    pub fn new() -> Self {
        CachePlanner {}
    }
}

#[async_trait]
impl ExtensionPlanner for CachePlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        println!("[CachePlanner] Planning CacheNode");
        // println!("[CachePlanner] logical_inputs: {:#?}", logical_inputs);
        // println!("[CachePlanner] physical_inputs: {:#?}", physical_inputs);
        Ok(
            if let Some(cache_node) = node.as_any().downcast_ref::<CacheNode>() {
                // assert_eq!(physical_inputs.len(), 1, "Inconsistent number of inputs");
                if cache_node.is_available() {
                    // If the cache is available, return the CacheExecPlan
                    Some(Arc::new(CacheExecPlan::new(
                        cache_node.get_cache_key(),
                        physical_inputs[0].clone().schema(),
                    )))
                } else {
                    // If the cache is not available, return CacheUpdateExecPlan
                    Some(Arc::new(CacheUpdateExecPlan::new_exec_plan(
                        physical_inputs[0].schema(),
                        cache_node.get_cache_key(),
                        physical_inputs[0].clone(),
                    )))
                }
            } else {
                None
            },
        )
    }
}

#[derive(Debug, Clone)]
pub struct CacheQueryPlanner {}

impl CacheQueryPlanner {
    pub fn new() -> Self {
        CacheQueryPlanner {}
    }
}

#[async_trait]
impl QueryPlanner for CacheQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // println!(
        //     "[CacheQueryPlanner] Creating physical plan for logical plan: {:#?}",
        //     logical_plan
        // );
        let physical_planner =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(CachePlanner {})]);
        // Delegate most work of physical planning to the default physical planner
        physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

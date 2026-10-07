use datafusion::{
    common::DFSchema,
    error::DataFusionError,
    logical_expr::{Expr, InvariantLevel, LogicalPlan, UserDefinedLogicalNode},
};
use std::{any::Any, fmt, sync::Arc};

#[derive(Clone, Debug)]
pub struct PostgresPushdownQueryNode {
    pub input_plan: LogicalPlan,
    pub schema: Arc<datafusion::common::DFSchema>,
}

impl UserDefinedLogicalNode for PostgresPushdownQueryNode {
    fn name(&self) -> &str {
        "PostgresPushdownNode"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &Arc<DFSchema> {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "postgres_pushdown\n {}", self.input_plan)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn check_invariants(&self, _check: InvariantLevel) -> Result<(), DataFusionError> {
        Ok(())
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> Result<Arc<dyn UserDefinedLogicalNode>, DataFusionError> {
        Ok(Arc::new(self.clone()))
    }

    fn dyn_hash(&self, _state: &mut dyn std::hash::Hasher) {}

    fn dyn_eq(&self, other: &dyn UserDefinedLogicalNode) -> bool {
        other
            .as_any()
            .downcast_ref::<Self>()
            .map_or(false, |o| self.input_plan == o.input_plan)
    }

    fn dyn_ord(&self, other: &dyn UserDefinedLogicalNode) -> Option<std::cmp::Ordering> {
        use std::cmp::Ordering;
        other.as_any().downcast_ref::<Self>().map(|o| {
            if self.input_plan == o.input_plan {
                Ordering::Equal
            } else {
                Ordering::Less
            }
        })
    }
}

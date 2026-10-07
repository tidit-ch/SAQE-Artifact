use datafusion::common::DFSchema;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    Expr, Extension, InvariantLevel, LogicalPlan, UserDefinedLogicalNode,
};
use std::any::Any;
use std::cmp::Ordering;
use std::fmt;
use std::hash::Hasher;
use std::sync::Arc;

use crate::core::parser::ast::CropASTNode;

/// Custom logical plan node that wraps an existing plan and carries
/// a [`CropASTNode`] down to the physical execution layer.
#[derive(Clone, Debug)]
pub struct CroProjectionLogicalNode {
    /// The child logical plan (the query without the crop() call).
    pub input: LogicalPlan,
    /// Output schema — identical to the input schema since crop only
    /// filters the polyline values, it does not add or remove columns.
    pub schema: Arc<DFSchema>,
    /// Parsed crop function arguments from the original SQL.
    pub crop_udf: Arc<CropASTNode>,
}

impl CroProjectionLogicalNode {
    pub fn new(input: LogicalPlan, crop_udf: CropASTNode) -> Self {
        let schema = input.schema().clone();
        Self {
            input,
            schema,
            crop_udf: Arc::new(crop_udf),
        }
    }

    /// Convenience: wrap `self` in a `LogicalPlan::Extension`.
    pub fn into_logical_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }
}

impl UserDefinedLogicalNode for CroProjectionLogicalNode {
    fn name(&self) -> &str {
        "CroProjectionLogicalNode"
    }

    /// DataFusion will plan the single child and pass the resulting
    /// physical plan to the extension planner.
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &Arc<DFSchema> {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CroProjectionLogicalNode: column={:?}",
            self.crop_udf.column_name
        )
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn check_invariants(&self, _check: InvariantLevel) -> Result<(), DataFusionError> {
        Ok(())
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        if let Some(col) = &self.crop_udf.column_name {
            state.write(col.as_bytes());
        }
    }

    fn dyn_eq(&self, other: &dyn UserDefinedLogicalNode) -> bool {
        other
            .as_any()
            .downcast_ref::<CroProjectionLogicalNode>()
            .map(|o| o.crop_udf.column_name == self.crop_udf.column_name)
            .unwrap_or(false)
    }

    fn dyn_ord(&self, other: &dyn UserDefinedLogicalNode) -> Option<Ordering> {
        other
            .as_any()
            .downcast_ref::<CroProjectionLogicalNode>()
            .and_then(|o| {
                self.crop_udf
                    .column_name
                    .partial_cmp(&o.crop_udf.column_name)
            })
    }

    /// Called by DataFusion's optimizer to rebuild the node with updated
    /// inputs / expressions. We just swap in the new input and keep everything else.
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Arc<dyn UserDefinedLogicalNode>, DataFusionError> {
        let new_input = inputs.into_iter().next().ok_or_else(|| {
            DataFusionError::Plan("CroProjectionLogicalNode requires exactly one input".to_string())
        })?;
        Ok(Arc::new(CroProjectionLogicalNode {
            input: new_input,
            schema: self.schema.clone(),
            crop_udf: self.crop_udf.clone(),
        }))
    }
}

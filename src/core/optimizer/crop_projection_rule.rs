use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Column;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::expr::{Alias, Expr};
use datafusion::logical_expr::{LogicalPlan, Projection, ScalarUDF};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};

use crate::core::logical_plan::crop_projection::CroProjectionLogicalNode;
use crate::core::udf::misc::crop_projection::CropProjectionUdf;

/// Optimizer rule that:
///
/// 1. Detects a `CroProjectionLogicalNode` extension in the logical plan.
/// 2. Walks into its child plan to find the `Projection` node.
/// 3. Appends `crop(polyline_col) AS crop` to that projection's expression list,
///    with the full `CropASTNode` struct stored directly inside the UDF instance.
/// 4. Returns the modified plan with `CroProjectionLogicalNode` removed.
///
/// The resulting plan looks like:
/// ```text
/// Limit
///   Projection: ..., crop(table.polyline)
///     Filter: ...
///       TableScan: ...
/// ```
#[derive(Debug)]
pub struct CropProjectionRule;

impl OptimizerRule for CropProjectionRule {
    fn name(&self) -> &str {
        "CropProjectionRule"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // Only act on our CroProjectionLogicalNode extension.
        let LogicalPlan::Extension(ref ext) = plan else {
            return Ok(Transformed::no(plan));
        };
        let Some(crop_node) = ext.node.as_any().downcast_ref::<CroProjectionLogicalNode>() else {
            return Ok(Transformed::no(plan));
        };

        let crop_udf_data = crop_node.crop_udf.clone(); // Arc<CropASTNode>
        let inner = crop_node.input.clone();

        // Walk the inner plan tree and inject the crop() call into the first Projection.
        let transformed = inner.transform(|node| {
            let LogicalPlan::Projection(proj) = node else {
                return Ok(Transformed::no(node));
            };

            let crop_col = crop_udf_data.get_column()?;
            // Resolve the column: if the user typed an alias (e.g. "traj"), find the
            // underlying expression in this projection. Otherwise use the column directly.
            let col_expr = resolve_column_in_projection(&crop_col, &proj.expr)
                .unwrap_or_else(|| Expr::Column(crop_col));
            let scalar_udf = ScalarUDF::from(CropProjectionUdf::new(crop_udf_data.clone()));
            let alias = if let Some(alias) = &crop_udf_data.alias {
                alias.as_str()
            } else {
                "crop"
            };
            let crop_call = scalar_udf.call(vec![col_expr]).alias(alias);

            // Append to the existing projection expressions.
            let mut new_exprs = proj.expr.clone();
            new_exprs.push(crop_call);

            let new_proj = Projection::try_new(new_exprs, proj.input.clone())
                .map_err(|e| DataFusionError::Plan(e.to_string()))?;

            Ok(Transformed::yes(LogicalPlan::Projection(new_proj)))
        })?;

        // Unwrap CroProjectionLogicalNode — return the rewritten inner plan directly.
        Ok(Transformed::yes(transformed.data))
    }
}

/// Given a column reference (possibly unqualified, e.g. from alias "traj"), look through
/// the projection expressions for a matching alias and return its inner expression.
/// Returns None if no alias matches, so the caller falls back to a direct column reference.
fn resolve_column_in_projection(col: &Column, exprs: &[Expr]) -> Option<Expr> {
    // Only try alias resolution for unqualified names (no table prefix).
    if col.relation.is_some() {
        return None;
    }
    for expr in exprs {
        if let Expr::Alias(Alias {
            expr: inner, name, ..
        }) = expr
        {
            if name == &col.name {
                return Some(*inner.clone());
            }
        }
    }
    None
}

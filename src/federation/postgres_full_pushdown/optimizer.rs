use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::Result;
use datafusion::logical_expr::{Expr, Extension, LogicalPlan};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::scalar::ScalarValue;
use std::sync::Arc;

use super::node::PostgresPushdownQueryNode;

#[derive(Debug)]
pub struct PushdownOptimizerRule;

impl OptimizerRule for PushdownOptimizerRule {
    fn name(&self) -> &str {
        "PushdownOptimizerRule"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if is_already_pushdown(&plan) {
            return Ok(Transformed::no(plan));
        }

        // Check if all tables in the plan are from postgres catalog
        if !all_tables_from_postgres(&plan) {
            return Ok(Transformed::no(plan));
        }

        // passes_point's Postgres-native implementation (config/init.sql)
        // ignores its tolerance argument entirely (always an exact
        // ST_Intersects check), whereas SAQE's own Rust UDF
        // (src/core/udf/spatial_filters/passes_point.rs) genuinely checks
        // distance <= tolerance. The two only agree at tolerance == 0.0 -
        // pushing a non-zero-tolerance call down to Postgres would silently
        // change the query's semantics. Refuse to push the whole query down
        // in that case so it falls back to DataFusion's own execution,
        // which evaluates passes_point correctly.
        if has_unsafe_passes_point_tolerance(&plan)? {
            return Ok(Transformed::no(plan));
        }

        // Create a new PostgresPushdownQueryNode with the original plan
        let new_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(PostgresPushdownQueryNode {
                input_plan: plan.clone(),
                schema: plan.schema().clone(),
            }),
        });

        Ok(Transformed::yes(new_plan))
    }
}

fn is_already_pushdown(plan: &LogicalPlan) -> bool {
    matches!(
        plan,
        LogicalPlan::Extension(Extension { node })
            if node.as_any().is::<PostgresPushdownQueryNode>()
    )
}

fn all_tables_from_postgres(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::TableScan(scan) => scan.table_name.catalog() == Some("postgres"),
        _ => plan
            .inputs()
            .iter()
            .all(|input| all_tables_from_postgres(input)),
    }
}

/// True if any `passes_point(...)` call anywhere in the plan has a tolerance
/// argument (its 3rd argument) that isn't the literal `0.0` - see the
/// `rewrite()` call site for why this makes whole-query pushdown unsafe.
fn has_unsafe_passes_point_tolerance(plan: &LogicalPlan) -> Result<bool> {
    for expr in plan.expressions() {
        if expr.exists(|e| {
            if let Expr::ScalarFunction(fun) = e {
                if fun.name() == "passes_point" {
                    return Ok(!matches!(
                        fun.args.get(2),
                        Some(Expr::Literal(ScalarValue::Float64(Some(tolerance)), _))
                            if *tolerance == 0.0
                    ));
                }
            }
            Ok(false)
        })? {
            return Ok(true);
        }
    }

    for input in plan.inputs() {
        if has_unsafe_passes_point_tolerance(input)? {
            return Ok(true);
        }
    }

    Ok(false)
}

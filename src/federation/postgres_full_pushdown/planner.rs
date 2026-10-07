use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use std::fmt;
use std::sync::Arc;
use tonic::async_trait;

use super::execution::PostgresExec;
use super::node::PostgresPushdownQueryNode;

#[derive(Clone)]
pub struct PostgresExtensionPlanner {
    pub postgres_pool: Arc<PostgresConnectionPool>,
}

#[async_trait]
impl ExtensionPlanner for PostgresExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let my_node = node
            .as_any()
            .downcast_ref::<PostgresPushdownQueryNode>()
            .ok_or_else(|| {
                DataFusionError::Plan("PostgresExtensionPlanner: Unexpected node type".to_string())
            })?;

        let schema = my_node.schema.clone();
        let arrow_schema = Arc::new(schema.as_ref().as_arrow().clone());
        let sql = datafusion::sql::unparser::plan_to_sql(&my_node.input_plan)?
            .to_string()
            .replace("postgres.berlinmod.", ""); // Schema-Strip

        //println!("Generated pushdown SQL: {}", sql);
        let exec = Arc::new(PostgresExec::new(
            arrow_schema,
            sql,
            self.postgres_pool.clone(),
        ));
        Ok(Some(exec))
    }
}

pub struct PostgresQueryPlanner {
    pub physical_planner: Arc<DefaultPhysicalPlanner>,
}

impl fmt::Debug for PostgresQueryPlanner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PostgresQueryPlanner")
    }
}

#[async_trait]
impl QueryPlanner for PostgresQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

use arrow_schema::SchemaRef;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::{stream::RecordBatchStreamAdapter, ExecutionPlan, PlanProperties};
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use futures::TryStreamExt;
use std::{any::Any, sync::Arc};

use crate::core::executors::postgres_executor;

#[derive(Clone, Debug)]
pub struct PostgresExec {
    schema: SchemaRef,
    sql: String,
    postgres_pool: Arc<PostgresConnectionPool>,
    plan_properties: PlanProperties,
}

impl PostgresExec {
    pub fn new(
        schema: arrow_schema::SchemaRef,
        sql: String,
        postgres_pool: Arc<PostgresConnectionPool>,
    ) -> Self {
        let eq_properties = EquivalenceProperties::new(schema.clone());
        let partitioning = Partitioning::UnknownPartitioning(1);
        let emission_type = EmissionType::Incremental;
        let boundedness = Boundedness::Bounded;
        let plan_properties =
            PlanProperties::new(eq_properties, partitioning, emission_type, boundedness);
        Self {
            schema,
            sql,
            postgres_pool,
            plan_properties,
        }
    }
}

impl datafusion::physical_plan::DisplayAs for PostgresExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "PostgresExec")
    }
}

impl ExecutionPlan for PostgresExec {
    fn name(&self) -> &str {
        "DummyExecutionPlan"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self.clone())
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        //println!("Executing SQL FOR POSTGRES: {}", self.sql);
        let future_stream = postgres_executor::execute_remote_postgres(
            self.sql.clone(),
            self.postgres_pool.clone(),
            self.schema.clone(),
        );
        let stream = futures::stream::once(future_stream).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}

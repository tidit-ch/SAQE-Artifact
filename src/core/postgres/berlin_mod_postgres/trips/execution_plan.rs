use datafusion::{
    arrow::datatypes::{Field, Schema, SchemaRef},
    common::stats::Precision,
    error::Result,
    execution::context::TaskContext,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        SendableRecordBatchStream, Statistics,
    },
};
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use std::{any::Any, collections::HashSet, fmt, sync::Arc};

#[derive(Debug, Clone)]
pub struct BerlinModTripsExecutionPlan {
    target_schema: SchemaRef,
    projected_schema: SchemaRef,
    plan_properties: PlanProperties,
    projection: Option<Vec<usize>>,
    postgres_pool: Arc<PostgresConnectionPool>,
    query: String,
}

impl BerlinModTripsExecutionPlan {
    pub fn new(
        postgres_pool: Arc<PostgresConnectionPool>,
        query: String,
        target_schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Self {
        let projected_schema = BerlinModTripsExecutionPlan::get_projected_schema(
            target_schema.clone(),
            projection.clone(),
        );

        let eq_properties = EquivalenceProperties::new(projected_schema.clone());
        let partitioning = Partitioning::UnknownPartitioning(1);
        let emission_type = EmissionType::Incremental;
        let boundedness = Boundedness::Bounded;
        let plan_properties =
            PlanProperties::new(eq_properties, partitioning, emission_type, boundedness);

        Self {
            target_schema,
            projected_schema,
            plan_properties,
            projection,
            postgres_pool,
            query,
        }
    }

    pub fn get_projected_schema(
        target_schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> SchemaRef {
        if projection.is_some() {
            let projection_set: HashSet<usize> = projection
                .unwrap()
                .iter()
                .copied()
                .collect::<HashSet<usize>>();
            let fields = target_schema.fields();
            let filtered_fields: Vec<Arc<Field>> = fields
                .into_iter()
                .enumerate()
                .filter(|(index, _)| projection_set.contains(index))
                .map(|(_, field)| field.clone())
                .collect();
            return Arc::new(Schema::new(filtered_fields));
        } else {
            return target_schema;
        }
    }
}

impl DisplayAs for BerlinModTripsExecutionPlan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "BerlinModTripsExecutionPlan: projection={:?}",
                    self.target_schema
                        .fields()
                        .iter()
                        .map(|f| f.name())
                        .collect::<Vec<_>>()
                )
            }
            DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "BerlinModTripsExecutionPlan\n projection: {:?}",
                    self.target_schema
                        .fields()
                        .iter()
                        .map(|f| f.name())
                        .collect::<Vec<_>>()
                )
            }
        }
    }
}

impl ExecutionPlan for BerlinModTripsExecutionPlan {
    fn name(&self) -> &str {
        "BerlinModTripsExecutionPlan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn with_new_children(
        self: std::sync::Arc<Self>,
        _children: Vec<std::sync::Arc<dyn ExecutionPlan>>,
    ) -> Result<std::sync::Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        context: std::sync::Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Create and return your custom stream
        let stream =
            crate::core::postgres::berlin_mod_postgres::trips::postgres_stream::BerlinModTripsDataStream::try_new(
                self.postgres_pool.clone(),
                self.query.clone(),
                context.session_config().batch_size(),
                self.projected_schema.clone(),
                self.projection.clone(),
            )?;

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.projected_schema.clone(),
            stream,
        )))
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics {
            num_rows: datafusion::common::stats::Precision::Exact(15045),
            total_byte_size: Precision::Absent, // TODO: Adjust this based on actual data
            column_statistics: Statistics::unknown_column(&*self.schema()),
        })
    }
}

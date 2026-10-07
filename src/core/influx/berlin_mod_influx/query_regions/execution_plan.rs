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
use std::{
    any::Any,
    collections::{HashMap, HashSet},
    fmt,
    sync::Arc,
};

#[derive(Debug, Clone)]
pub struct BerlinModQueryRegionsExecutionPlan {
    target_schema: SchemaRef,
    projected_schema: SchemaRef,
    plan_properties: PlanProperties,
    projection: Option<Vec<usize>>,
    influx_options: HashMap<String, String>,
    query: String,
}

impl BerlinModQueryRegionsExecutionPlan {
    pub fn new(
        influx_options: HashMap<String, String>,
        query: String,
        target_schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Self {
        let projected_schema = BerlinModQueryRegionsExecutionPlan::get_projected_schema(
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
            influx_options,
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

impl DisplayAs for BerlinModQueryRegionsExecutionPlan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "BerlinModQueryRegionsExecutionPlan: projection={:?}",
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
                    "BerlinModQueryRegionsExecutionPlan\n projection: {:?}",
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

impl ExecutionPlan for BerlinModQueryRegionsExecutionPlan {
    fn name(&self) -> &str {
        "BerlinModQueryRegionsExecutionPlan"
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
            crate::core::influx::berlin_mod_influx::query_regions::influx_stream::BerlinModQueryRegionsDataStream::try_new(
                self.influx_options.clone(),
                self.query.clone(),
                self.projected_schema.clone(),
                self.projection.clone(),
                context.session_config().batch_size(),
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
            num_rows: datafusion::common::stats::Precision::Exact(200), // TODO: Use here Inexact??
            total_byte_size: Precision::Absent,
            column_statistics: Statistics::unknown_column(&*self.schema()),
        })
    }
}

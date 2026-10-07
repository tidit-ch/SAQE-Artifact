use datafusion::{
    arrow::datatypes::SchemaRef,
    error::Result,
    execution::context::TaskContext,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan,
        PlanProperties, SendableRecordBatchStream,
    },
};
use std::{any::Any, fmt::Debug, sync::Arc};
use tracing::Span;

use crate::core::custom_csv_provider::{base_behavior::CsvBaseBehavior, stream::CustomCsvStream};

/// An execution plan for reading CSV, the CsvBaseBehavior trait provides the necessary functions for streaming data.
#[derive(Debug, Clone)]
pub struct CustomCsvExecutionPlan<T: CsvBaseBehavior + Debug> {
    config: Arc<T>,
    projection: Arc<Vec<usize>>,
    projected_schema: SchemaRef,
    plan_properties: PlanProperties,
    span: Span,
}

// Implement DisplayAs for CustomCsvExecutionPlan
impl<T: CsvBaseBehavior + Debug + Send + Sync + 'static> DisplayAs for CustomCsvExecutionPlan<T> {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(
                    f,
                    "CustomCsvExecutionPlan: {}",
                    self.config.get_execution_plan_name()
                )
            }
            DisplayFormatType::Verbose => {
                write!(f, "CustomCsvExecutionPlan (verbose): {:?}", self)
            }
            DisplayFormatType::TreeRender => {
                todo!("Tree rendering not implemented for CustomCsvExecutionPlan")
            }
        }
    }
}

impl<T: CsvBaseBehavior + Debug> CustomCsvExecutionPlan<T> {
    pub fn new(
        config: Arc<T>,
        projection: Option<Vec<usize>>,
        projected_schema: SchemaRef,
        span: Span,
    ) -> Self {
        let mut plan_properties = (*config.get_execution_plan_properties()).clone();
        plan_properties.eq_properties = EquivalenceProperties::new(projected_schema.clone());
        Self {
            config,
            projection: Arc::new(projection.unwrap_or_default()),
            projected_schema,
            plan_properties,
            span,
        }
    }
}

impl<T: CsvBaseBehavior + 'static + Send + Sync + Debug> ExecutionPlan
    for CustomCsvExecutionPlan<T>
{
    fn name(&self) -> &str {
        self.config.get_execution_plan_name()
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn schema(&self) -> arrow_schema::SchemaRef {
        self.projected_schema.clone()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let _enter = self.span.enter();
        tracing::debug!(
            "CustomCsvExecutionPlan::execute called for partition: {}, thread: {:?}",
            partition,
            std::thread::current().id(),
        );
        let csv_stream = CustomCsvStream::new(
            self.config.clone(),
            partition,
            context,
            self.projection.clone(),
            self.projected_schema.clone(),
        );
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.projected_schema.clone(),
            csv_stream,
        )))
    }
}

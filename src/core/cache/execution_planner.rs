use arrow::array::RecordBatch;
use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef,
    common::{Result, Statistics},
    execution::context::TaskContext,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        collect,
        execution_plan::{Boundedness, EmissionType},
        metrics::ExecutionPlanMetricsSet,
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        SendableRecordBatchStream,
    },
};
use futures::TryFutureExt;
use std::{sync::Arc, vec};

use super::CACHE_MANAGER;

#[derive(Debug, Clone)]
pub struct CacheExecPlan {
    properties: PlanProperties,
    key: String,
}

impl CacheExecPlan {
    pub fn new(key: String, schema: SchemaRef) -> Self {
        CacheExecPlan {
            key,
            properties: Self::get_properties(schema),
        }
    }

    fn get_properties(schema: SchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

#[async_trait]
impl ExecutionPlan for CacheExecPlan {
    fn name(&self) -> &str {
        "CacheExecPlanPlan"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(datafusion::common::DataFusionError::Internal(
                "CacheExecPlan does not support children".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let record_batches = CACHE_MANAGER.get_record_batches_from_cache(&self.key)?;
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::iter(record_batches.into_iter().map(Ok)),
        )))
    }
}

impl DisplayAs for CacheExecPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CacheExecPlan")
    }
}

#[derive(Debug)]
pub struct CacheUpdateExecPlan {
    properties: PlanProperties,
    key: String,
    input: Arc<dyn ExecutionPlan>,
    metrics: ExecutionPlanMetricsSet,
}

impl CacheUpdateExecPlan {
    pub fn new_exec_plan(schema: SchemaRef, key: String, input: Arc<dyn ExecutionPlan>) -> Self {
        CacheUpdateExecPlan {
            properties: Self::get_properties(schema),
            key,
            input,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    fn get_properties(schema: SchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

impl DisplayAs for CacheUpdateExecPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CacheUpdateExecPlan")
    }
}

#[async_trait]
impl ExecutionPlan for CacheUpdateExecPlan {
    fn name(&self) -> &str {
        "CacheUpdateExecPlan"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CacheUpdateExecPlan {
            properties: self.properties.clone(),
            key: self.key.clone(),
            input: children[0].clone(),
            metrics: self.metrics.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.input.schema(),
            execute_store(self.input.clone(), self.key.clone(), context)
                .map_ok(|batches| {
                    // Convert the Vec<RecordBatch> into a stream
                    futures::stream::iter(batches.into_iter().map(Ok))
                })
                .try_flatten_stream(),
        )))
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema()))
    }
}

async fn execute_store(
    input: Arc<dyn ExecutionPlan>,
    cache_key: String,
    context: Arc<TaskContext>,
) -> Result<Vec<RecordBatch>> {
    let batches = collect(input, context).await?;
    CACHE_MANAGER.add_record_batches_to_cache(cache_key, &batches)?;
    Ok(batches)
}

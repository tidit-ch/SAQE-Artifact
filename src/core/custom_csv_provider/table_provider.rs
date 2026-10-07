use datafusion::{
    arrow::datatypes::SchemaRef,
    datasource::{TableProvider, TableType},
    error::Result as DataFusionResult,
    physical_plan::ExecutionPlan,
};
use std::{
    any::Any,
    fmt::Debug,
    future::Future,
    marker::{Send, Sync},
    pin::Pin,
    sync::Arc,
};
use tracing::Span;

use crate::core::{
    custom_csv_provider::{base_behavior::CsvBaseBehavior, execution_plan::CustomCsvExecutionPlan},
    utils::common::get_projected_schema,
};

/// A generic CSV Table Provider that uses the CsvBaseBehavior trait to provide table functionalities.
#[derive(Debug, Clone)]
pub struct CustomCsvTableProvider<T: CsvBaseBehavior> {
    config: Arc<T>,
}

impl<T: CsvBaseBehavior> CustomCsvTableProvider<T> {
    pub fn new(config: T) -> Self {
        Self {
            config: Arc::new(config),
        }
    }
}

impl<T: CsvBaseBehavior + 'static + Sync + Send + Debug> TableProvider
    for CustomCsvTableProvider<T>
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.config.get_schema()
    }

    fn table_type(&self) -> TableType {
        self.config.get_table_type()
    }

    fn scan<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        _state: &'life1 dyn datafusion::catalog::Session,
        projection: Option<&'life2 Vec<usize>>,
        filters: &'life3 [datafusion::prelude::Expr],
        limit: Option<usize>,
    ) -> Pin<
        Box<
            dyn Future<Output = DataFusionResult<Arc<dyn ExecutionPlan>>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        tracing::debug!(
            "CustomCsvTableProvider::scan called with projection: {:?}, filters: {:?}, limit: {:?}",
            projection,
            filters,
            limit
        );
        let span = Span::current();

        let projected_schema = get_projected_schema(self.config.get_schema(), projection.cloned());
        let execution_plan = CustomCsvExecutionPlan::new(
            self.config.clone(),
            projection.cloned(),
            projected_schema,
            span,
        );
        let boxed: Arc<dyn ExecutionPlan> = Arc::new(execution_plan);
        Box::pin(async move { Ok(boxed) })
    }
}

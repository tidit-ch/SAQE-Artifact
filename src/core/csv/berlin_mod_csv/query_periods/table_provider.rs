use datafusion::{
    arrow::datatypes::SchemaRef,
    datasource::{TableProvider, TableType},
    error::Result,
    physical_plan::ExecutionPlan,
};
use std::{any::Any, future::Future, pin::Pin, sync::Arc};

#[derive(Debug, Clone)]
pub struct BerlinModQueryPeriodsTableProvider {
    file_path: String,
    target_schema: SchemaRef,
}

impl BerlinModQueryPeriodsTableProvider {
    pub fn new(file_path: &str, schema: SchemaRef) -> Self {
        Self {
            file_path: file_path.to_string(),
            target_schema: schema,
        }
    }
}

impl TableProvider for BerlinModQueryPeriodsTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.target_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn scan<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        _state: &'life1 dyn datafusion::catalog::Session,
        projection: Option<&'life2 Vec<usize>>,
        _filters: &'life3 [datafusion::prelude::Expr],
        _limit: Option<usize>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Arc<dyn ExecutionPlan>>>
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
        let plan =
            crate::core::csv::berlin_mod_csv::query_periods::execution_plan::BerlinModQueryPeriodsExecutionPlan::new(
                self.file_path.clone(),
                self.target_schema.clone(),
                projection.cloned(),
            );
        let boxed: Arc<dyn ExecutionPlan> = Arc::new(plan);
        Box::pin(async move { Ok(boxed) })
    }
}

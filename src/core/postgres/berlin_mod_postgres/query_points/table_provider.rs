use datafusion::{
    arrow::datatypes::SchemaRef,
    datasource::{sink::DataSinkExec, TableProvider, TableType},
    error::{DataFusionError, Result},
    logical_expr::{utils::conjunction, TableProviderFilterPushDown},
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use std::{any::Any, future::Future, pin::Pin, sync::Arc};

use crate::core::filter_pushdown::{
    filter_expr_to_sql, quote_identifier_double_quotes, PostgresFilterPushdown,
};

#[derive(Debug, Clone)]
pub struct BerlinModQueryPointsTableProvider {
    postgres_pool: Arc<PostgresConnectionPool>,
    target_schema: SchemaRef,
    table_name: String,
}

impl BerlinModQueryPointsTableProvider {
    pub fn new(
        postgres_pool: Arc<PostgresConnectionPool>,
        schema: SchemaRef,
        name: String,
    ) -> Self {
        Self {
            postgres_pool: postgres_pool,
            target_schema: schema,
            table_name: name,
        }
    }
}

impl TableProvider for BerlinModQueryPointsTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.target_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        let filter_pushdown: Vec<TableProviderFilterPushDown> = filters
            .iter()
            .map(
                |filter| match filter_expr_to_sql(filter, PostgresFilterPushdown {}) {
                    Ok(_) => TableProviderFilterPushDown::Exact,
                    Err(_) => TableProviderFilterPushDown::Unsupported,
                },
            )
            .collect();
        Ok(filter_pushdown)
    }

    fn scan<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        _state: &'life1 dyn datafusion::catalog::Session,
        projection: Option<&'life2 Vec<usize>>,
        filters: &'life3 [datafusion::prelude::Expr],
        limit: Option<usize>,
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
        Box::pin(async move {
            let mut schema = self.target_schema.clone();
            let mut columns = "*".to_string();

            // Extract projected columns
            if let Some(indices) = projection {
                schema = Arc::new(schema.project(indices)?);
                columns = schema
                    .fields()
                    .iter()
                    .map(|f| quote_identifier_double_quotes(f.name()))
                    .collect::<Vec<String>>()
                    .join(", ");
            }

            // Add LIMIT condition if specified
            let limit_clause = limit.map_or("".to_string(), |size| format!(" LIMIT {size}"));

            // Build the WHERE clause: all filters should be safe to push down since they have passed 'supports_filter_pushdown'
            let where_clause = if filters.is_empty() {
                "".to_string()
            } else {
                // Merge all filters with AND into a single expression
                // Should be safe since all passed pushdown checks.
                let merged_filter = conjunction(filters.to_vec()).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "Failed merging received filters into one {filters:?}"
                    ))
                })?;

                let filters_sql = filter_expr_to_sql(&merged_filter, PostgresFilterPushdown {})
                    .map_err(|_| {
                        DataFusionError::Execution(format!(
                            "Failed converting filter to SQL {merged_filter}"
                        ))
                    })?;

                format!(" WHERE {filters_sql}")
            };

            // Construct the full SQL query
            let query = format!(
                "SELECT {} FROM {}{}{}",
                columns, self.table_name, where_clause, limit_clause
            );

            //println!("Executing query: {}", query);

            let plan =
                crate::core::postgres::berlin_mod_postgres::query_points::execution_plan::BerlinModQueryPointsExecutionPlan::new(
                    self.postgres_pool.clone(),
                    query,
                    self.target_schema.clone(),
                    projection.cloned(),
                );
            let boxed: Arc<dyn ExecutionPlan> = Arc::new(plan);
            Ok(boxed)
        })
    }

    fn insert_into<'life0, 'life1, 'async_trait>(
        &'life0 self,
        _state: &'life1 dyn datafusion::catalog::Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: datafusion::logical_expr::dml::InsertOp,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Arc<dyn ExecutionPlan>, datafusion::common::DataFusionError>>
                + Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        let sink = Arc::new(
            crate::core::postgres::write::sink_points::PostgresSinkPoints {
                schema: self.schema().clone(),
                postgres_pool: self.postgres_pool.clone(),
                table_name: self.table_name.clone(),
                insert_op: insert_op,
            },
        );

        Box::pin(async move {
            Ok(Arc::new(DataSinkExec::new(
                input.clone(),
                sink,
                None, // No sort_order specified
            )) as Arc<dyn ExecutionPlan>)
        })
    }
}

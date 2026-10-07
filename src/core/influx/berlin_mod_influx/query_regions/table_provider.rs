use datafusion::{
    arrow::datatypes::SchemaRef,
    datasource::{sink::DataSinkExec, TableProvider, TableType},
    error::{DataFusionError, Result},
    logical_expr::{utils::conjunction, TableProviderFilterPushDown},
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use std::{any::Any, collections::HashMap, future::Future, pin::Pin, sync::Arc};

use crate::core::filter_pushdown::{filter_expr_to_sql, InfluxFilterPushdown};

#[derive(Debug, Clone)]
pub struct BerlinModQueryRegionsTableProvider {
    influx_options: HashMap<String, String>,
    target_schema: SchemaRef,
    table_name: String,
}

impl BerlinModQueryRegionsTableProvider {
    pub fn new(influx_options: HashMap<String, String>, schema: SchemaRef, name: String) -> Self {
        Self {
            influx_options: influx_options,
            target_schema: schema,
            table_name: name,
        }
    }
}

impl TableProvider for BerlinModQueryRegionsTableProvider {
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
                |filter| match filter_expr_to_sql(filter, InfluxFilterPushdown {}) {
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
            // If 'polygon' is in the projection -> "polygon_id", "x", "y", "index" are projected in influx
            // If the polyline is not projected -> We project "DISTINCT polygon_id"
            if let Some(indices) = projection {
                schema = Arc::new(schema.project(indices)?);

                let projected_fields = schema.fields();
                if projected_fields.iter().any(|f| f.name() == "polygon") {
                    let mut cols: Vec<String> = Vec::new();

                    for f in projected_fields {
                        if f.name() == "polygon" {
                            cols.push("polygon_id".to_string());
                            cols.push("x".to_string());
                            cols.push("y".to_string());
                            cols.push("index".to_string());
                        }
                    }
                    columns = cols.join(", ");
                } else {
                    columns = "DISTINCT polygon_id".to_string();
                }
            }

            // Check whether polyline is contained in the projection
            let contains_polyline = schema.fields().iter().any(|f| f.name() == "polygon");

            // Add LIMIT condition only if polyline is not in the projection
            // 'query_suffix' is either 'LIMIT' or 'ORDER BY index'
            // If polyline is in the projection -> LIMIT pushdown is allowed. Otherwise we need "ORDER BY index"
            let query_suffix = if !contains_polyline {
                limit.map_or("".to_string(), |size| format!(" LIMIT {size}"))
            } else {
                " ORDER BY index".to_string() // WE NEED AN SORTED OUTPUT to build the polygon
            };

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

                let filters_sql = filter_expr_to_sql(&merged_filter, InfluxFilterPushdown {})
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
                columns, self.table_name, where_clause, query_suffix
            );

            let plan =
                crate::core::influx::berlin_mod_influx::query_regions::execution_plan::BerlinModQueryRegionsExecutionPlan::new(
                    self.influx_options.clone(),
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
            crate::core::influx::write::sink_regions::InfluxSinkRegions {
                schema: self.schema().clone(),
                influx_options: self.influx_options.clone(),
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

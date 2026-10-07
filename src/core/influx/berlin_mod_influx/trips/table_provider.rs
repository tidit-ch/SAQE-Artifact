use datafusion::{
    arrow::datatypes::SchemaRef,
    datasource::{sink::DataSinkExec, TableProvider, TableType},
    error::{DataFusionError, Result},
    logical_expr::TableProviderFilterPushDown,
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use std::{
    any::Any, collections::HashMap, collections::HashSet, future::Future, pin::Pin, sync::Arc,
};

use crate::core::filter_pushdown::{filter_expr_to_sql, InfluxFilterPushdown};

#[derive(Debug, Clone)]
pub struct BerlinModTripsTableProvider {
    influx_options: HashMap<String, String>,
    target_schema: SchemaRef,
    table_name: String,
}

impl BerlinModTripsTableProvider {
    pub fn new(influx_options: HashMap<String, String>, schema: SchemaRef, name: String) -> Self {
        Self {
            influx_options: influx_options,
            target_schema: schema,
            table_name: name,
        }
    }
}

impl TableProvider for BerlinModTripsTableProvider {
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
            // If polyline is in the projection -> "x", "y", "time", "index", "trip_id" are projected in influx
            // If the polyline is not projected -> We project "DISTINCT trip_id, moid"
            if let Some(indices) = projection {
                schema = Arc::new(schema.project(indices)?);

                let projected_fields = schema.fields();
                if projected_fields.iter().any(|f| f.name() == "polyline") {
                    let mut cols: Vec<String> = Vec::new();
                    let mut seen: HashSet<String> = HashSet::new();

                    for f in projected_fields {
                        if f.name() == "polyline" {
                            for name in ["x", "y", "time", "index", "trip_id"] {
                                if seen.insert(name.to_string()) {
                                    cols.push(name.to_string());
                                }
                            }
                        } else {
                            if seen.insert(f.name().clone()) {
                                cols.push(f.name().clone());
                            }
                        }
                    }
                    columns = cols.join(", ");
                } else {
                    columns = "DISTINCT trip_id, moid".to_string();
                }
            }

            // Check whether polyline is contained in the projection
            let contains_polyline = schema.fields().iter().any(|f| f.name() == "polyline");

            // Add LIMIT condition only if polyline is not in the projection
            // 'query_suffix' is either 'LIMIT' or 'ORDER BY index'
            // If polyline is in the projection -> LIMIT pushdown is allowed. Otherwise we need "ORDER BY index"
            let query_suffix = if !contains_polyline {
                limit.map_or("".to_string(), |size| format!(" LIMIT {size}"))
            } else {
                " ORDER BY index".to_string() // WE NEED AN SORTED OUTPUT to build the trajectory
            };

            // Split filters into WHERE and HAVING clauses
            let mut where_filters = vec![];
            let mut having_filters = vec![];
            for f in filters {
                let filter_sql = filter_expr_to_sql(f, InfluxFilterPushdown {});
                if let Ok(ref sql) = filter_sql {
                    if sql.contains("MAX(") || sql.contains("MIN(") {
                        having_filters.push(sql.clone());
                    } else {
                        where_filters.push(sql.clone());
                    }
                }
            }

            let where_clause = if where_filters.is_empty() {
                "".to_string()
            } else {
                format!(" WHERE {}", where_filters.join(" AND "))
            };

            let (group_by_clause, having_clause) = if !having_filters.is_empty() {
                let group_cols: Vec<String> = schema
                    .fields()
                    .iter()
                    .map(|f| f.name().to_string())
                    .filter(|name| name != "polyline")
                    .collect();
                let group_by = group_cols.join(", ");
                (
                    format!(" GROUP BY trip_id, {}", group_by),
                    format!(" HAVING {}", having_filters.join(" AND ")),
                )
            } else {
                ("".to_string(), "".to_string())
            };

            // Construct the full SQL query
            let query = format!(
                "SELECT {} FROM {}{}{}{}{}",
                columns,
                self.table_name,
                where_clause,
                group_by_clause,
                having_clause,
                query_suffix
            );

            //println!("Executing query: {query}");

            let plan =
                crate::core::influx::berlin_mod_influx::trips::execution_plan::BerlinModTripsExecutionPlan::new(
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
        let sink = Arc::new(crate::core::influx::write::sink_trips::InfluxSinkTrips {
            schema: self.schema().clone(),
            influx_options: self.influx_options.clone(),
            table_name: self.table_name.clone(),
            insert_op: insert_op,
        });

        Box::pin(async move {
            Ok(Arc::new(DataSinkExec::new(
                input.clone(),
                sink,
                None, // No sort_order specified
            )) as Arc<dyn ExecutionPlan>)
        })
    }
}

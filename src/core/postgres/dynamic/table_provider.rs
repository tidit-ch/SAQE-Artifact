//! Pairs the crate-provided Postgres reader with [`GenericPostgresSink`] for writes.
//!
//! Scans delegate to the `SqlTable` that [`PostgresTableFactory`] builds by describing the
//! table against the database, so nothing here declares a schema. Writes go through our own
//! sink because the stock one cannot render PostGIS geometry (see [`super::sink`]).

use super::sink::GenericPostgresSink;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Result;
use datafusion::datasource::sink::DataSinkExec;
use datafusion::datasource::TableType;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct DynamicPostgresTable {
    read: Arc<dyn TableProvider>,
    /// Fully qualified target, e.g. `public.trips`.
    table_name: String,
    pool: Arc<PostgresConnectionPool>,
}

impl DynamicPostgresTable {
    pub fn new(
        read: Arc<dyn TableProvider>,
        table_name: String,
        pool: Arc<PostgresConnectionPool>,
    ) -> Self {
        Self {
            read,
            table_name,
            pool,
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for DynamicPostgresTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.read.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.read.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.read.scan(state, projection, filters, limit).await
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let sink = Arc::new(GenericPostgresSink {
            schema: self.schema(),
            table_name: self.table_name.clone(),
            pool: self.pool.clone(),
            insert_op,
        });

        Ok(Arc::new(DataSinkExec::new(input, sink, None)) as Arc<dyn ExecutionPlan>)
    }
}

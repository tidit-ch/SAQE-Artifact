//! A `SchemaProvider` that resolves Postgres tables on demand.
//!
//! Unlike [`BerlinModPostgresSchemaProvider`](crate::core::postgres::schema_provider), this
//! provider knows nothing about individual tables. On a lookup it asks
//! Postgres to describe the table and pairs the result with a writer, so a table created after
//! start-up is queryable without a restart and without any Rust code.
//!
//! This is the same mechanism DataFusion uses for `SELECT * FROM 'file.parquet'`: a provider
//! is still involved, it is just manufactured from the name in the query rather than written
//! by hand.

use super::dialect::PostGisDialect;
use super::table_provider::DynamicPostgresTable;
use datafusion::{
    catalog::{SchemaProvider, TableProvider},
    error::{DataFusionError, Result},
    sql::TableReference,
};
use datafusion_table_providers::{
    postgres::DynPostgresConnectionPool,
    sql::db_connection_pool::postgrespool::PostgresConnectionPool,
    sql::sql_provider_datafusion::SqlTable,
};
use std::{
    any::Any,
    collections::HashMap,
    sync::{Arc, RwLock},
};

pub struct DynamicPostgresSchemaProvider {
    /// Postgres schema this provider maps onto, e.g. `public`.
    schema_name: String,
    pool: Arc<PostgresConnectionPool>,
    /// Providers already built, keyed by table name.
    cache: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
    /// Table names last seen in `information_schema`; refreshed by [`Self::refresh`].
    known_tables: RwLock<Vec<String>>,
}

// The pool is not `Debug`; the useful state here is the schema name.
impl std::fmt::Debug for DynamicPostgresSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynamicPostgresSchemaProvider")
            .field("schema_name", &self.schema_name)
            .field("cached_tables", &self.cache.read().unwrap().len())
            .finish()
    }
}

impl DynamicPostgresSchemaProvider {
    pub fn new(schema_name: impl Into<String>, pool: Arc<PostgresConnectionPool>) -> Self {
        Self {
            schema_name: schema_name.into(),
            pool,
            cache: RwLock::new(HashMap::new()),
            known_tables: RwLock::new(Vec::new()),
        }
    }

    /// Re-reads the table list from `information_schema`.
    ///
    /// Only needed to keep `table_names()` (and therefore `SHOW TABLES` and the
    /// `/datafusion/tables` endpoint) current — resolving a table by name never consults
    /// this list, so a brand-new table is queryable before any refresh happens.
    pub async fn refresh(&self) -> Result<()> {
        let names = self.query_table_names().await?;
        *self.known_tables.write().unwrap() = names;
        Ok(())
    }

    async fn query_table_names(&self) -> Result<Vec<String>> {
        use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;

        let mut db_conn = self
            .pool
            .connect()
            .await
            .map_err(DataFusionError::External)?;
        let conn = datafusion_table_providers::postgres::Postgres::postgres_conn(&mut db_conn)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let rows = conn
            .conn
            .query(
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_schema = $1 ORDER BY table_name",
                &[&self.schema_name],
            )
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
    }
}

#[async_trait::async_trait]
impl SchemaProvider for DynamicPostgresSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let mut names = self.known_tables.read().unwrap().clone();
        for name in self.cache.read().unwrap().keys() {
            if !names.contains(name) {
                names.push(name.clone());
            }
        }
        names
    }

    /// Optimistic: a name absent from the cached list may still be a table created since the
    /// last refresh. Resolution happens in [`Self::table`], which fails cleanly if it is not.
    fn table_exist(&self, name: &str) -> bool {
        self.cache.read().unwrap().contains_key(name)
            || self.known_tables.read().unwrap().iter().any(|t| t == name)
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        if let Some(cached) = self.cache.read().unwrap().get(name) {
            return Ok(Some(cached.clone()));
        }

        let reference = TableReference::partial(self.schema_name.clone(), name.to_string());

        // `SqlTable::new` describes the table against Postgres, so nothing here declares a
        // schema. This is what `PostgresTableFactory::table_provider` does internally; it is
        // spelled out only so the unparser dialect can be swapped for [`PostGisDialect`], which
        // decides which UDFs may cross into the pushed-down SQL.
        let dyn_pool: Arc<DynPostgresConnectionPool> = self.pool.clone();
        let read = match SqlTable::new("postgres", &dyn_pool, reference).await {
            Ok(table) => {
                Arc::new(table.with_dialect(Arc::new(PostGisDialect))) as Arc<dyn TableProvider>
            }
            // A missing table is not an error here: returning None lets DataFusion emit its
            // own "table not found" message instead of a driver-level one.
            Err(_) => return Ok(None),
        };

        let provider: Arc<dyn TableProvider> = Arc::new(DynamicPostgresTable::new(
            read,
            format!("{}.{}", self.schema_name, name),
            self.pool.clone(),
        ));

        self.cache
            .write()
            .unwrap()
            .insert(name.to_string(), provider.clone());

        Ok(Some(provider))
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        Ok(self.cache.write().unwrap().insert(name, table))
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        Ok(self.cache.write().unwrap().remove(name))
    }
}

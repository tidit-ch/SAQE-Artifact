//! A `DataSink` that writes any `RecordBatch` into any Postgres table.
//!
//! The stock writer in `datafusion-table-providers` cannot be used for PostGIS tables: its
//! `InsertBuilder` renders an Arrow `Binary` value as a bytea-escaped literal (`'\x0102…'`),
//! and assigning that to a `geometry` column makes Postgres apply the *text* → geometry cast,
//! which rejects the `\x` prefix ("parse error at position 2 within geometry"). It also
//! `unimplemented!()`-panics on `List<Struct<x, y, m>>`.
//!
//! This sink instead binds every value as text and casts it in SQL to the column's own type
//! (`$1::text::int4`, `$3::text::geometry`, …), with `Binary` rendered as bare hex EWKB —
//! the one form PostGIS accepts without an explicit cast.

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray,
};
use arrow_schema::{Field, Schema, SchemaRef};
use datafusion::common::DataFusionError;
use datafusion::datasource::sink::DataSink;
use datafusion::error::Result as DfResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use datafusion_table_providers::postgres::Postgres;
use datafusion_table_providers::sql::db_connection_pool::{
    postgrespool::PostgresConnectionPool, DbConnectionPool,
};
use futures::StreamExt;
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use tokio_postgres::types::ToSql;

/// Postgres caps a statement at 65535 bind parameters; stay well under it.
const MAX_BIND_PARAMS: usize = 60_000;

pub struct GenericPostgresSink {
    pub schema: SchemaRef,
    /// Fully qualified target, e.g. `public.trips`.
    pub table_name: String,
    pub pool: Arc<PostgresConnectionPool>,
    pub insert_op: InsertOp,
}

impl fmt::Debug for GenericPostgresSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GenericPostgresSink({})", self.table_name)
    }
}

impl DisplayAs for GenericPostgresSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "GenericPostgresSink: table_name={}", self.table_name)
            }
            DisplayFormatType::TreeRender => {
                write!(f, "GenericPostgresSink\n  table_name: {}", self.table_name)
            }
        }
    }
}

#[async_trait::async_trait]
impl DataSink for GenericPostgresSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> DfResult<u64> {
        let mut db_conn = self
            .pool
            .connect()
            .await
            .map_err(DataFusionError::External)?;
        let pg = Postgres::postgres_conn(&mut db_conn)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let column_types = fetch_column_types(&pg.conn, &self.table_name).await?;

        let tx = pg
            .conn
            .transaction()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        if matches!(self.insert_op, InsertOp::Overwrite) {
            tx.execute(&format!("DELETE FROM {}", self.table_name), &[])
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        }

        let mut rows_written = 0u64;
        while let Some(batch) = data.next().await {
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }
            rows_written += write_batch(&tx, &self.table_name, &batch, &column_types).await?;
        }

        tx.commit()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(rows_written)
    }
}

/// Reads the target table's column types so each bind parameter can be cast to its own type.
async fn fetch_column_types(
    client: &tokio_postgres::Client,
    table: &str,
) -> DfResult<HashMap<String, String>> {
    let (schema, name) = table.split_once('.').unwrap_or(("public", table));

    let rows = client
        .query(
            "SELECT column_name, udt_name FROM information_schema.columns \
             WHERE table_schema = $1 AND table_name = $2",
            &[&schema, &name],
        )
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    if rows.is_empty() {
        return Err(DataFusionError::Execution(format!(
            "table {table} not found in information_schema"
        )));
    }

    Ok(rows
        .iter()
        .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
        .collect())
}

async fn write_batch(
    tx: &tokio_postgres::Transaction<'_>,
    table: &str,
    batch: &RecordBatch,
    column_types: &HashMap<String, String>,
) -> DfResult<u64> {
    let schema = batch.schema();
    let fields = schema.fields();

    let mut casts = Vec::with_capacity(fields.len());
    for field in fields {
        casts.push(column_types.get(field.name()).cloned().ok_or_else(|| {
            DataFusionError::Execution(format!(
                "column '{}' does not exist in {table}",
                field.name()
            ))
        })?);
    }

    let column_list = fields
        .iter()
        .map(|f| format!("\"{}\"", f.name()))
        .collect::<Vec<_>>()
        .join(", ");

    let mut encoded: Vec<Vec<Option<String>>> = Vec::with_capacity(fields.len());
    for (i, field) in fields.iter().enumerate() {
        encoded.push(encode_column(field, batch.column(i))?);
    }

    let rows_per_stmt = (MAX_BIND_PARAMS / fields.len().max(1)).max(1);
    let mut rows_written = 0u64;

    for chunk_start in (0..batch.num_rows()).step_by(rows_per_stmt) {
        let chunk_end = (chunk_start + rows_per_stmt).min(batch.num_rows());

        let mut tuples = Vec::with_capacity(chunk_end - chunk_start);
        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
        let mut n = 1usize;

        for row in chunk_start..chunk_end {
            let mut placeholders = Vec::with_capacity(fields.len());
            for (col, _) in fields.iter().enumerate() {
                // `::text::<type>` and not `::<type>`: with a bare cast Postgres infers the
                // parameter itself as the target type and rejects the text binding.
                placeholders.push(format!("${}::text::{}", n, casts[col]));
                params.push(&encoded[col][row] as &(dyn ToSql + Sync));
                n += 1;
            }
            tuples.push(format!("({})", placeholders.join(", ")));
        }

        let sql = format!(
            "INSERT INTO {table} ({column_list}) VALUES {}",
            tuples.join(", ")
        );

        rows_written += tx
            .execute(&sql, &params)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
    }

    Ok(rows_written)
}

/// Encodes one Arrow column to one text value per row.
fn encode_column(field: &Field, column: &ArrayRef) -> DfResult<Vec<Option<String>>> {
    use arrow_schema::DataType as DT;

    macro_rules! text_from {
        ($ty:ty) => {{
            let a = downcast::<$ty>(column, field.name())?;
            Ok((0..a.len())
                .map(|i| (!a.is_null(i)).then(|| a.value(i).to_string()))
                .collect())
        }};
    }

    match field.data_type() {
        DT::Int64 => text_from!(Int64Array),
        DT::Int32 => text_from!(Int32Array),
        DT::Float64 => text_from!(Float64Array),
        DT::Boolean => text_from!(BooleanArray),
        DT::Utf8 => text_from!(StringArray),
        // A geometry column read from Postgres arrives as raw EWKB. PostGIS parses bare hex
        // EWKB from a text literal, so hex-encode without the bytea `\x` prefix.
        DT::Binary => {
            let a = downcast::<BinaryArray>(column, field.name())?;
            Ok((0..a.len())
                .map(|i| (!a.is_null(i)).then(|| hex_encode(a.value(i))))
                .collect())
        }
        // NB. a geoarrow trajectory never arrives here: DataFusion's INSERT coercion rejects
        // List -> Binary while planning, so callers must wrap it in `trajectory_to_wkb()`.
        other => Err(DataFusionError::Execution(format!(
            "column '{}': no Postgres encoding for Arrow type {other}",
            field.name()
        ))),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn downcast<'a, T: 'static>(array: &'a dyn Array, column: &str) -> DfResult<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        DataFusionError::Execution(format!(
            "column '{column}': unexpected array layout {}",
            array.data_type()
        ))
    })
}

use actix_web::{web, HttpMessage, HttpRequest, HttpResponse};
use datafusion::arrow::array::StringArray;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;
use uuid::Uuid;

use crate::server::utils;
use tracing::{span, Level};

use crate::core::{self, execute_query};
use crate::utils::error::SaqeResult;
use std::sync::LazyLock;

pub static DATAFUSION_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_cpus::get())
        .enable_all()
        .build()
        .unwrap()
});

#[derive(Deserialize)]
pub struct RunQueryBody {
    query: String,
}

#[derive(Serialize)]
struct RunQueryResponse {
    data: serde_json::Value,
    schema: serde_json::Value,
}

pub async fn run_query_datafusion(
    req: HttpRequest,
    req_body: web::Json<RunQueryBody>,
) -> SaqeResult<HttpResponse> {
    run_query(req_body.query.clone(), trace_id(&req), false).await
}

/// Same as [`run_query_datafusion`], but plans the SQL exactly as written.
///
/// [`core::execute_query`] first runs [`CustomSqlParser`](crate::core::parser::parser::CustomSqlParser)
/// to lift a `CROP()` call out of the statement. This route skips that and hands the string
/// straight to `SessionContext::sql`, so `CROP()` is not recognised here.
///
/// TODO: remove this route once the CROP pre-parser is fixed. It exists because
/// `CustomSqlParser` matches the token `crop` anywhere in the SQL text rather than an actual
/// `crop(...)` call, so `WHERE name = 'crop'`, `SELECT 1 AS cropland` and even `-- crop(a, b)`
/// in a comment are rejected on `/datafusion/query`. This route is the workaround, not a
/// feature worth keeping.
pub async fn run_raw_query_datafusion(
    req: HttpRequest,
    req_body: web::Json<RunQueryBody>,
) -> SaqeResult<HttpResponse> {
    run_query(req_body.query.clone(), trace_id(&req), true).await
}

fn trace_id(req: &HttpRequest) -> Uuid {
    req.extensions()
        .get::<Uuid>()
        .cloned()
        .unwrap_or(Uuid::new_v4())
}

async fn run_query(query: String, trace_id: Uuid, raw: bool) -> SaqeResult<HttpResponse> {
    let (record_batches, response_schema) = DATAFUSION_RUNTIME
        .spawn(async move {
            let span = span!(
                Level::INFO,
                "datafusion_query",
                "trace_id" = format!("{}", trace_id),
                "raw" = raw
            );
            let _enter = span.enter();
            let ctx = core::DATAFUSION_CTX.clone();
            let df = if raw {
                ctx.sql(&query).await?
            } else {
                core::execute_query(&ctx, &query).await?
            };
            let schema = crate::core::utils::schema::dfschema_to_hashmap(df.schema());
            let response_schema = serde_json::to_value(&schema).unwrap();
            let record_batches = df.collect().await?;
            Ok::<(Vec<RecordBatch>, serde_json::Value), DataFusionError>((
                record_batches,
                response_schema,
            ))
        })
        .await
        .unwrap()?;

    let response = RunQueryResponse {
        data: serde_json::from_slice(&utils::convert_record_batches_list_to_json(record_batches))
            .unwrap_or(serde_json::Value::Null),
        schema: response_schema,
    };

    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(serde_json::to_string(&response)?)
        .into())
}

pub async fn health_check(req: HttpRequest) -> SaqeResult<String> {
    let extensions = req.extensions();
    let uuid = extensions.get::<Uuid>().unwrap();

    tracing::info!("inside the health_check route");
    Ok(format!("Server is up!, {}", uuid))
}

#[derive(Deserialize)]
pub struct PlanRequest {
    query: String,
}

/// Returns both the logical and the physical plan for `query`.
pub async fn get_plan(req: HttpRequest, req_body: web::Json<PlanRequest>) -> SaqeResult<String> {
    let query = format!("Explain format indent {}", req_body.query);
    let trace_id = trace_id(&req);

    let plan = DATAFUSION_RUNTIME
        .spawn(async move {
            let span = span!(
                Level::INFO,
                "datafusion_plan",
                "trace_id" = format!("{}", trace_id)
            );
            let _enter = span.enter();
            let batches = execute_query(&core::DATAFUSION_CTX.clone(), &query)
                .await?
                .collect()
                .await?;
            Ok::<String, DataFusionError>(explain_to_text(&batches))
        })
        .await??;

    Ok(plan)
}

/// Renders the `plan_type` / `plan` rows `EXPLAIN` produces as plain text.
fn explain_to_text(batches: &[RecordBatch]) -> String {
    let mut out = String::new();
    for batch in batches {
        let (Some(kinds), Some(plans)) = (
            batch
                .column_by_name("plan_type")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>()),
            batch
                .column_by_name("plan")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>()),
        ) else {
            continue;
        };
        for row in 0..batch.num_rows() {
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str(kinds.value(row));
            out.push('\n');
            out.push_str(plans.value(row));
            out.push('\n');
        }
    }
    out
}

pub async fn get_all_tables(req: HttpRequest) -> SaqeResult<HttpResponse> {
    let ctx = core::DATAFUSION_CTX.clone();
    let trace_id = req
        .extensions()
        .get::<Uuid>()
        .cloned()
        .unwrap_or(Uuid::new_v4());

    let result = DATAFUSION_RUNTIME
        .spawn(async move {
            let span = span!(
                Level::INFO,
                "get_all_tables",
                "trace_id" = format!("{}", trace_id)
            );
            let _enter = span.enter();
            let sql = "SELECT * FROM datafusion.information_schema.tables \
                        WHERE table_catalog <> 'datafusion'";
            let batches = ctx.sql(sql).await?.collect().await?;
            Ok::<Vec<RecordBatch>, DataFusionError>(batches)
        })
        .await
        .unwrap()?;

    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(utils::convert_record_batches_list_to_json(result))
        .into())
}

pub async fn get_all_udfs(req: HttpRequest) -> SaqeResult<HttpResponse> {
    let ctx = core::DATAFUSION_CTX.clone();
    let trace_id = req
        .extensions()
        .get::<Uuid>()
        .cloned()
        .unwrap_or(Uuid::new_v4());

    let udf_names: Vec<String> = core::udf::get_udf_name_list().to_vec();

    let result = DATAFUSION_RUNTIME
        .spawn(async move {
            let span = span!(
                Level::INFO,
                "get_all_udfs",
                "trace_id" = format!("{}", trace_id)
            );
            let _enter = span.enter();

            let placeholders: Vec<String> = udf_names
                .iter()
                .map(|name| format!("'{}'", name.replace('\'', "''")))
                .collect();
            let in_list = placeholders.join(", ");

            let sql = format!(
                "SELECT * FROM information_schema.routines \
                 WHERE routine_type = 'FUNCTION' \
                 AND routine_name IN ({}) \
                 ORDER BY routine_name",
                in_list
            );
            let batches = ctx.sql(&sql).await?.collect().await?;
            Ok::<Vec<RecordBatch>, DataFusionError>(batches)
        })
        .await
        .unwrap()?;

    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(utils::convert_record_batches_list_to_json(result))
        .into())
}

#[derive(Deserialize)]
pub struct DescribeTableQuery {
    table_name: String,
}

pub async fn get_udf_details(_req: HttpRequest) -> SaqeResult<HttpResponse> {
    let details = core::udf::get_udf_details();
    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(serde_json::to_string(&details)?)
        .into())
}

pub async fn get_crop_function_details(_req: HttpRequest) -> SaqeResult<HttpResponse> {
    let registry = core::parser::functions::function_registry();
    let mut details: Vec<&core::udf::doc::UdfDetail> =
        registry.values().map(|f| f.details()).collect();
    details.sort_by_key(|d| d.name.as_str());
    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(serde_json::to_string(&details)?)
        .into())
}

pub async fn describe_table(
    req: HttpRequest,
    query: web::Query<DescribeTableQuery>,
) -> SaqeResult<HttpResponse> {
    let ctx = core::DATAFUSION_CTX.clone();
    let table_name = query.table_name.clone();
    let trace_id = req
        .extensions()
        .get::<Uuid>()
        .cloned()
        .unwrap_or(Uuid::new_v4());

    let result = DATAFUSION_RUNTIME
        .spawn(async move {
            let span = span!(
                Level::INFO,
                "describe_table",
                "trace_id" = format!("{}", trace_id)
            );
            let _enter = span.enter();
            let sql = format!("DESCRIBE {}", table_name);
            let batches = ctx.sql(&sql).await?.collect().await?;
            Ok::<Vec<RecordBatch>, DataFusionError>(batches)
        })
        .await
        .unwrap()?;

    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(utils::convert_record_batches_list_to_json(result))
        .into())
}

// TODO: Refactor these generated tests
#[cfg(test)]
mod raw_route_tests {
    use crate::core;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use std::sync::Arc;

    async fn ctx_with_table() -> datafusion::prelude::SessionContext {
        std::env::set_var("CONFIG_FILE", "config/config_dev.json");
        let ctx = datafusion::prelude::SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
                .unwrap();
        ctx.register_table(
            "t",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
        ctx
    }

    /// Plain SQL behaves identically on both paths.
    #[tokio::test]
    async fn plain_sql_matches() {
        let ctx = ctx_with_table().await;
        let parsed = core::execute_query(&ctx, "SELECT id FROM t ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let raw = ctx
            .sql("SELECT id FROM t ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(format!("{parsed:?}"), format!("{raw:?}"));
    }

    /// `EXPLAIN format indent` yields one `logical_plan` row and one `physical_plan` row, and
    /// both must survive into the text the route returns.
    #[tokio::test]
    async fn plan_route_returns_both_plans() {
        let ctx = ctx_with_table().await;
        let batches = ctx
            .sql("Explain format indent SELECT count(id) FROM t WHERE id > 1")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let text = super::explain_to_text(&batches);
        assert!(text.contains("logical_plan"), "{text}");
        assert!(text.contains("physical_plan"), "{text}");
        assert!(text.contains("Aggregate"), "{text}");
        assert!(text.contains("AggregateExec"), "{text}");
    }

    /// The raw path never sees `CROP()`, so DataFusion reports it as an unknown function.
    /// The parsed path intercepts it before planning and fails somewhere else.
    #[tokio::test]
    async fn crop_is_only_understood_on_the_parsed_path() {
        let ctx = ctx_with_table().await;
        let sql = "SELECT crop(id, during(1, 2)) FROM t";

        let raw = ctx.sql(sql).await.unwrap_err().to_string();
        assert!(raw.contains("Invalid function 'crop'"), "{raw}");

        let parsed = core::execute_query(&ctx, sql)
            .await
            .unwrap_err()
            .to_string();
        assert!(!parsed.contains("Invalid function 'crop'"), "{parsed}");
    }
}

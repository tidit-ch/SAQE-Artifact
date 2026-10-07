use actix_web::{
    body::MessageBody,
    dev::{ServiceRequest, ServiceResponse},
    middleware::{from_fn, Next},
    web, App, Error as ActixError, HttpMessage, HttpServer,
};
use std::env;
use tracing::{debug, span, Level};
use uuid::Uuid;

use crate::{server::route_handlers::*, utils::error::SaqeResult};

pub mod logger;
pub mod postgres;
pub mod route_handlers;
pub mod utils;

async fn tracing_middleware(
    req: ServiceRequest,
    next: Next<impl MessageBody>,
) -> Result<ServiceResponse<impl MessageBody>, ActixError> {
    let trace_id = Uuid::new_v4();
    let span = span!(
        Level::INFO,
        "web_server",
        "trace_id" = format!("{}", trace_id)
    );
    let _enter = span.enter();
    tracing::trace!("Request ID :: {}", trace_id);
    req.extensions_mut().insert(trace_id);
    next.call(req).await
}

pub async fn init() -> SaqeResult<()> {
    let app_env = env::var("ENV").unwrap_or("default".to_string());
    debug!("[SERVER] App env :: {}", app_env);
    let port = utils::config::GLOBAL_CONFIG
        .get::<u16>("server.port")
        .unwrap();

    HttpServer::new(move || {
        App::new()
            .wrap(
                actix_cors::Cors::default()
                    .allowed_origin("http://localhost:5173")
                    .allowed_origin("http://localhost:8080")
                    .allowed_origin("http://phobos170.inf.uni-konstanz.de:8080")
                    .allow_any_header()
                    .allow_any_method(),
            )
            .wrap(from_fn(tracing_middleware))
            .route("/datafusion/query", web::post().to(run_query_datafusion))
            .route(
                "/datafusion/query/raw",
                web::post().to(run_raw_query_datafusion),
            )
            .route("/datafusion/plan", web::post().to(get_plan))
            .route("/health-check", web::get().to(health_check))
            .route("/datafusion/tables", web::get().to(get_all_tables))
            .route("/datafusion/udfs", web::get().to(get_all_udfs))
            .route("/datafusion/udf_details", web::get().to(get_udf_details))
            .route(
                "/datafusion/crop_function_details",
                web::get().to(get_crop_function_details),
            )
            .route("/datafusion/table", web::get().to(describe_table))
    })
    .bind(("0.0.0.0", port))?
    .run()
    .await?;
    Ok(())
}

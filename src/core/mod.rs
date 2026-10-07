//! Core module
//! Datafusion sits at this module. Here we do all the initialization required for
//! data processing and query execution.
//!
//! Data Sources:
//! Modules involved: csv, csv_tables, custom_csv_provider, influx, postgres
//! We have three different kinds of data sources integrated at the moment.
//! CSV files, InfluxDB and Postgres.
//! Each data source has its own module under core. To add a new data source,
//! one needs to create the corresponding table providers and execution plans.
//! For CSVs, we have created a generic custom_csv_provider module to ease the process
//! of writing new csv table providers for different csv datasets.
//! Ideas from there can be extended to other data sources as well.
//!
//! UDFs:
//! Module involved: udf
//! We needed to implement some custom operators for the trajectory data processing.
//! These operators are implemented as UDFs and registered at the initialization time.
//!
//! cache module:
//! This was added as a prototype to support caching of intermediate query results to speed up.
//! It is not fully integrated yet. But, it can be used to see how we can change the execution plans
//! to include caching operator during the query optimization phase.
//! You can find more details about this in cache/about.md file.
//!
//! custom_csv_provider module:
//! More information in the mod.rs file under custom_csv_provider module.
//!

use crate::utils::error::SaqeResult;
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::OptimizerContext;
use datafusion::optimizer::OptimizerRule;
use datafusion::prelude::*;
use datafusion::prelude::{DataFrame, SessionContext};
use std::sync::LazyLock;

use crate::core::{
    logical_plan::crop_projection::CroProjectionLogicalNode,
    optimizer::crop_projection_rule::CropProjectionRule, parser::parser::CustomSqlParser,
};

pub mod csv;
pub mod custom_csv_provider;
pub mod executors;
pub mod filter_pushdown;
pub mod influx;
pub mod logical_plan;
pub mod optimizer;
pub mod parser;
pub mod postgres;
pub mod udf;
pub mod utils;

pub static DATAFUSION_CTX: LazyLock<SessionContext> = LazyLock::new(|| {
    let config = SessionConfig::new()
        .with_target_partitions(8)
        .with_information_schema(true);
    SessionContext::new_with_config(config).enable_url_table()
});

pub static DATAFUSION_BATCH_SIZE: LazyLock<usize> =
    LazyLock::new(|| DATAFUSION_CTX.copied_config().batch_size());

pub async fn init() -> SaqeResult<()> {
    udf::init(&DATAFUSION_CTX);
    // Register catalogs for different data sources
    csv::init_csv().await;
    postgres::init_postgres().await;
    influx::init_influxdb3().await;
    Ok(())
}

pub async fn execute_query(ctx: &SessionContext, sql: &str) -> DataFusionResult<DataFrame> {
    let (sql, maybe_crop_udf) = {
        let mut parser = CustomSqlParser::new(sql)
            .map_err(|e| DataFusionError::Plan(format!("Parse error: {e}")))?;
        parser
            .parse()
            .map_err(|e| DataFusionError::Plan(format!("Crop parse error: {e}")))?
    }; // parser (with its Rc<Cell<usize>>) is dropped here

    // Now the .await points below are safe — no !Send types in scope
    if maybe_crop_udf.is_none() {
        return Ok(ctx.sql(&sql).await?);
    }
    let crop_udf = maybe_crop_udf.unwrap();
    // 1. Plan the stripped SQL.
    let base_plan: LogicalPlan = ctx.sql(&sql).await?.logical_plan().clone();

    // 2. Wrap in CropNode, apply InjectCropRule.
    let wrapped_plan = CroProjectionLogicalNode::new(base_plan, crop_udf).into_logical_plan();
    let opt_config = OptimizerContext::new();
    let rewritten = CropProjectionRule.rewrite(wrapped_plan, &opt_config)?.data;

    // 3. Execute.
    Ok(DataFrame::new(ctx.state(), rewritten))
}

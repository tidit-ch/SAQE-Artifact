//! Federation module
//! This module handles the query federation across postgres at the moment.
//!
//! Execution
//! It is responsible for executing the queries in postgres.
//!
//! Node
//! Defines the node used in logical plan to represent a full pushdown query to postgres.
//!
//! Optimizer
//! Checks whether all the tables in the query are from postgres source and
//! rewrites the logical plan to use the PostgresPushdownQueryNode defined in Node.
//!
//! Planner
//! Responsible for returning the ExecutionPlan for the PostgresPushdownQueryNode.

pub mod execution;
pub mod node;
pub mod optimizer;
pub mod planner;

use crate::core::postgres;
use crate::core::udf;
use optimizer::PushdownOptimizerRule;
use planner::{PostgresExtensionPlanner, PostgresQueryPlanner};

use datafusion::catalog::CatalogProvider;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::ScalarUDF;
use datafusion::optimizer::optimize_projections::OptimizeProjections;
use datafusion::optimizer::push_down_filter::PushDownFilter;
use datafusion::optimizer::replace_distinct_aggregate::ReplaceDistinctWithAggregate;
use datafusion::physical_planner::DefaultPhysicalPlanner;
use datafusion::{optimizer::OptimizerRule, prelude::*};
use datafusion_table_providers::{
    sql::db_connection_pool::postgrespool::PostgresConnectionPool, util::secrets::to_secret_map,
};
use std::collections::HashMap;
use std::sync::Arc;

pub async fn init_federated_pushdown() -> datafusion::error::Result<()> {
    let postgres_params = to_secret_map(HashMap::from([
        ("host".to_string(), "localhost".to_string()),
        ("user".to_string(), "user".to_string()),
        ("db".to_string(), "gis".to_string()),
        ("pass".to_string(), "password".to_string()),
        ("port".to_string(), "5433".to_string()),
        ("sslmode".to_string(), "disable".to_string()),
    ]));

    let postgres_pool = Arc::new(
        PostgresConnectionPool::new(postgres_params)
            .await
            .expect("unable to create PostgreSQL connection pool"),
    );

    let my_extension_planner = Arc::new(PostgresExtensionPlanner {
        postgres_pool: postgres_pool.clone(),
    });

    let my_query_planner = Arc::new(PostgresQueryPlanner {
        physical_planner: Arc::new(DefaultPhysicalPlanner::with_extension_planners(vec![
            my_extension_planner.clone(),
        ])),
    });

    let custom_rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> = vec![
        Arc::new(PushdownOptimizerRule {}), // Own postgres pushdown rule
        Arc::new(ReplaceDistinctWithAggregate::new()),
        Arc::new(PushDownFilter::new()),
        Arc::new(OptimizeProjections::new()),
    ];

    let state = SessionStateBuilder::new()
        .with_optimizer_rules(custom_rules)
        .with_query_planner(my_query_planner)
        .with_default_features() // We need that to access the default functions like: SUM, AVG...
        .build();

    let ctx = SessionContext::new_with_state(state);

    // Register catalog for Postgres tables
    let tables = vec![
        "trips".to_string(),
        "points".to_string(),
        "datamcar".to_string(),
        "regions".to_string(),
        "instants".to_string(),
        "periods".to_string(),
        "licences".to_string(),
    ];
    let custom_catalog: Arc<dyn CatalogProvider> = Arc::new(
        postgres::catalog_provider::PostgresCatalogProvider::new(tables, postgres_pool.clone()),
    );
    ctx.register_catalog("postgres", custom_catalog);

    init_udf(&ctx).await;

    //let query = "SELECT COUNT(licence) FROM postgres.berlinmod.datamcar WHERE type = 'passenger';";

    let query = r#" EXPLAIN
                    SELECT COUNT(c.moid)
                    FROM postgres.berlinmod.trips t
                    JOIN postgres.berlinmod.datamcar c ON t.moid = c.moid
                    WHERE c.licence = 'B-RL 1';
                "#;
    println!("Executing query: {}", query);
    let df = ctx.sql(query).await.expect("Failed df with optimizer: ");
    df.show().await.unwrap();
    Ok(())
}

async fn init_udf(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(
        udf::spatial_filters::passes_point::PassesPoint::new(),
    ));
    ctx.register_udf(ScalarUDF::from(
        udf::temporal_filters::present::Present::new(),
    ));
    ctx.register_udf(ScalarUDF::from(
        udf::temporal_filters::point_at_timestamp::PointAtTimestamp::new(),
    ));

    ctx.register_udf(ScalarUDF::from(
        udf::spatial_filters::timestamp_at_position::TimestampAtPosition::new(),
    ));
    ctx.register_udf(ScalarUDF::from(udf::temporal_filters::during::During::new()));
}

//! Chameleon DataFusion
//!
//! Core module
//! It is responsible for everything related to data processing and query execution.
//!
//! Server module
//! Here we have the API endpoints, server initialization, and request handling.
//! The query-related requests are forwarded to the core module for processing.
//!
//! Federation module
//! This module handles the query federation across postgres at the moment.
//! The only case supported is the full pushdown if all the tables in the query
//! are from postgres source.

pub mod core;
pub mod federation;
pub mod server;
pub mod utils;

use utils::error::SaqeResult;

#[tokio::main]
async fn main() -> SaqeResult<()> {
    core::init().await?;
    server::logger::init();
    server::init().await?;
    Ok(())
}

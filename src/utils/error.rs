use actix_web::{
    error::ResponseError,
    http::{header::ContentType, StatusCode},
    HttpResponse,
};
use arrow::error::ArrowError;
use csv::Position;
use datafusion::{
    common::DataFusionError,
    sql::sqlparser::tokenizer::{Token, TokenWithSpan},
};
use datafusion_table_providers::sql::db_connection_pool::Error as DbConnectionPoolError;
use datafusion_table_providers::Error as DBTableProviderError;
use deadpool_postgres::PoolError;
use geoarrow_schema::error::GeoArrowError;
use std::{error::Error as StdError, io::Error as IoError};
use thiserror::Error;
use tokio::task::JoinError;
use tokio_postgres::Error as TokioPostgresError;
use tracing::debug;

#[derive(Debug, Error)]
pub enum CsvError {
    #[error("CSV read error: {source}, file: {file_name}")]
    FileRead {
        source: std::io::Error,
        file_name: String,
    },
    #[error("CSV line read error: {source}, file: {file_name}")]
    LineRead {
        source: csv::Error,
        file_name: String,
        position: Option<Position>,
    },
    #[error("CSV validation error, message: {message}")]
    Validation {
        message: String,
        position: Option<Position>,
    },
    #[error("CSV parse error, expected: {expected}, found: {found}, error: {source}")]
    CellParse {
        expected: String,
        found: String,
        source: Box<dyn StdError + Send + Sync>,
        position: Option<Position>,
    },
    #[error("CSV Other error: {source:?}, position: {position:?}")]
    Other {
        source: Box<dyn StdError + Send + Sync>,
        position: Option<Position>,
    },
}

pub fn csv_error_to_datafusion_error(err: CsvError) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}

pub fn io_error_to_datafusion_error(err: std::io::Error) -> DataFusionError {
    DataFusionError::Execution(format!("IO error: {}", err))
}

pub fn parquet_error_to_datafusion_error(err: parquet::errors::ParquetError) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}

pub fn arrow_error_to_datafusion_error(err: arrow::error::ArrowError) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}

pub fn geo_arrow_error_to_datafusion_error(
    e: geoarrow_schema::error::GeoArrowError,
) -> DataFusionError {
    DataFusionError::Execution(format!("GeoArrow error: {}", e))
}

#[derive(Debug, Error)]
pub enum SaqeError {
    #[error("CSV error: {0}")]
    CsvError(#[from] CsvError),

    #[error("std error: {0}")]
    StdError(Box<dyn StdError + Send + Sync>),

    #[error("anyhow error: {0}")]
    AnyhowError(#[from] anyhow::Error),

    #[error("Postgres pool error: {0}")]
    PostgresPoolError(#[from] PoolError),

    // #[error("Database connection pool error: {0}")]
    // DbConnectionPoolError(#[from] DbConnectionPoolError),
    #[error("Postgres tokio error: {0}")]
    PostgresTransactionError(#[from] TokioPostgresError),

    #[error("Failed to execute SQL: {0}")]
    PostgresExecutionError(String),

    #[error("Datafusion error : {0:?}")]
    DataFusionError(#[from] DataFusionError),

    #[error("IO error : {0}")]
    IOError(#[from] IoError),

    #[error("Datafusion table provider error :: {0}")]
    DBTableProviderError(#[from] DBTableProviderError),

    #[error("Arrow error : {0}")]
    ArrowError(#[from] ArrowError),

    #[error("Tokio Join error : {0}")]
    JoinError(#[from] JoinError),

    #[error("String Error : {0}")]
    StringError(String),

    #[error("GeoArrow error : {0}")]
    GeoArrowError(#[from] GeoArrowError),

    #[error("SQL Parse Error: expected {expected}, found {found}")]
    ParseError {
        expected: Token,
        found: TokenWithSpan,
    },

    #[error("Serde JSON error: {0}")]
    SerdeError(#[from] serde_json::Error),
}

impl From<&str> for SaqeError {
    fn from(s: &str) -> Self {
        SaqeError::StringError(s.to_string())
    }
}

impl From<SaqeError> for DataFusionError {
    fn from(err: SaqeError) -> Self {
        DataFusionError::Execution(err.to_string())
    }
}

impl From<DbConnectionPoolError> for SaqeError {
    fn from(err: DbConnectionPoolError) -> Self {
        SaqeError::StdError(err)
    }
}

pub trait ToDataFusionError<T> {
    fn to_df_error(self) -> Result<T, DataFusionError>;
    fn to_df_error_ctx(self, message: &str) -> Result<T, DataFusionError>;
}

impl<T, E> ToDataFusionError<T> for Result<T, E>
where
    E: Into<Box<dyn StdError + Send + Sync>> + 'static,
{
    fn to_df_error(self) -> Result<T, DataFusionError> {
        self.map_err(|e| DataFusionError::External(e.into()))
    }

    fn to_df_error_ctx(self, message: &str) -> Result<T, DataFusionError> {
        self.map_err(|e| {
            DataFusionError::Context(
                message.to_string(),
                Box::new(DataFusionError::External(e.into())),
            )
        })
    }
}

impl ResponseError for SaqeError {
    fn error_response(&self) -> HttpResponse {
        let error_message: String;
        debug!("[ResponseError] error :: {}", self);
        match self {
            _ => error_message = String::from("Internal server error"),
        }
        HttpResponse::build(self.status_code())
            .insert_header(ContentType::html())
            .body(error_message)
    }

    fn status_code(&self) -> StatusCode {
        match self {
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

pub type SaqeResult<T> = anyhow::Result<T, SaqeError>;

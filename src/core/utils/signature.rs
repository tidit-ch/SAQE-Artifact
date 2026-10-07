//! This module contains the schema for data passing.

use arrow::datatypes::DataType;
use datafusion::logical_expr::{Signature, Volatility};

use crate::core::utils::schema::TRAJECTORY_DATATYPE;

pub fn get_signature_temproal() -> Signature {
    Signature::exact(
        vec![
            TRAJECTORY_DATATYPE.clone(),
            DataType::Int64,
            DataType::Int64,
        ],
        Volatility::Immutable,
    )
}

pub fn get_signature_spatial() -> Signature {
    Signature::exact(
        vec![TRAJECTORY_DATATYPE.clone(), DataType::Utf8, DataType::Utf8],
        Volatility::Immutable,
    )
}

pub fn get_signature_spatial_properly_contained() -> Signature {
    Signature::exact(
        vec![TRAJECTORY_DATATYPE.clone(), DataType::Utf8],
        Volatility::Immutable,
    )
}

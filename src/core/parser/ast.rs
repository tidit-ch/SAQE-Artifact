use super::functions::function_registry;
use crate::utils::error::SaqeError;
use crate::utils::error::SaqeResult;
use datafusion::common::{Column, Result as DatafusionResult};
use datafusion::error::DataFusionError;
use wkt::types::{
    Dimension as WktDimension, LineString as WktLineString, MultiLineString as WktMultiLineString,
};

pub trait Eval {
    fn eval(&self, input: &[WktMultiLineString<f64>]) -> SaqeResult<Vec<WktMultiLineString<f64>>>;
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct CropFunctionCall {
    pub function_name: String,
    pub arguments: Vec<String>,
}

impl CropFunctionCall {
    pub fn new(function_name: String, arguments: Vec<String>) -> Self {
        Self {
            function_name,
            arguments,
        }
    }
}

impl Eval for CropFunctionCall {
    fn eval(&self, input: &[WktMultiLineString<f64>]) -> SaqeResult<Vec<WktMultiLineString<f64>>> {
        let registry = function_registry();
        let crop_fn = registry.get(self.function_name.as_str()).ok_or_else(|| {
            SaqeError::StringError(format!("Function not found: {}", self.function_name))
        })?;
        crop_fn.eval(input, self)
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum CropExpr {
    FunctionCall(CropFunctionCall),
    BinaryExpr(CropBinaryExpr),
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct CropBinaryExpr {
    pub left: Box<CropExpr>,
    pub operator: CropOperator,
    pub right: Box<CropExpr>,
}

impl Eval for CropExpr {
    fn eval(&self, input: &[WktMultiLineString<f64>]) -> SaqeResult<Vec<WktMultiLineString<f64>>> {
        match self {
            CropExpr::FunctionCall(call) => call.eval(input),
            CropExpr::BinaryExpr(expr) => expr.eval(input),
        }
    }
}

impl CropBinaryExpr {
    pub fn new(left: CropExpr, operator: CropOperator, right: CropExpr) -> Self {
        Self {
            left: Box::new(left),
            operator,
            right: Box::new(right),
        }
    }

    pub fn eval_and(
        &self,
        input: &[WktMultiLineString<f64>],
    ) -> SaqeResult<Vec<WktMultiLineString<f64>>> {
        let left_result = self.left.eval(input)?;
        let right_result = self.right.eval(&left_result)?;
        Ok(right_result)
    }

    pub fn eval_or(
        &self,
        input: &[WktMultiLineString<f64>],
    ) -> SaqeResult<Vec<WktMultiLineString<f64>>> {
        let left_result = self.left.eval(input)?;
        let right_result = self.right.eval(input)?;
        let mut result = Vec::new();
        for (left_mls, right_mls) in left_result.into_iter().zip(right_result.into_iter()) {
            let mut merged_ls: Vec<WktLineString<f64>> = left_mls.line_strings().to_vec();
            merged_ls.extend_from_slice(right_mls.line_strings());
            result.push(WktMultiLineString::new(merged_ls, WktDimension::XYM));
        }
        Ok(result)
    }
}

impl Eval for CropBinaryExpr {
    fn eval(&self, input: &[WktMultiLineString<f64>]) -> SaqeResult<Vec<WktMultiLineString<f64>>> {
        match self.operator {
            CropOperator::And => self.eval_and(input),
            CropOperator::Or => self.eval_or(input),
        }
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum CropOperator {
    And,
    Or,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct CropASTNode {
    pub column_name: Option<String>,
    pub expr: Option<CropExpr>,
    pub alias: Option<String>,
}

impl CropASTNode {
    pub fn new() -> Self {
        Self {
            column_name: None,
            expr: None,
            alias: None,
        }
    }

    pub fn set_column_name(&mut self, column_name: String) {
        self.column_name = Some(column_name);
    }

    pub fn set_expr(&mut self, expr: CropExpr) {
        self.expr = Some(expr);
    }

    pub fn set_alias(&mut self, alias: String) {
        self.alias = Some(alias);
    }

    pub fn get_column(&self) -> DatafusionResult<Column> {
        if self.column_name.is_none() {
            return Err(DataFusionError::Plan(
                "Column name is not set in CropASTNode".to_string(),
            ));
        }
        let col = self.column_name.as_deref().unwrap();
        if col.contains('.') {
            // This is a qualified column name,
            Ok(Column::from_qualified_name(col))
        } else {
            Ok(Column::from_name(col))
        }
    }
}

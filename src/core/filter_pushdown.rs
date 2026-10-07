/// This code is based on the repository: https://github.com/splitgraph/seafowl/tree/main/datafusion_remote_tables/src
use arrow::temporal_conversions::{
    date32_to_datetime, timestamp_ms_to_datetime, timestamp_ns_to_datetime,
    timestamp_s_to_datetime, timestamp_us_to_datetime,
};
use datafusion::common::tree_node::TreeNode;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::tree_node::TreeNodeVisitor;
use datafusion::common::{Column, DataFusionError};
use datafusion::error::Result;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::scalar::ScalarValue;
use itertools::Itertools;

pub struct FilterPushdownVisitor<T: FilterPushdownConverter> {
    pub source: T,
    pub sql_exprs: Vec<String>,
}

impl<T: FilterPushdownConverter> FilterPushdownVisitor<T> {
    fn pop_sql_expr(&mut self) -> String {
        self.sql_exprs
            .pop()
            .expect("No SQL expression in the stack")
    }
}

pub struct PostgresFilterPushdown {} // Filter pushdown dialect for Postgres
impl FilterPushdownConverter for PostgresFilterPushdown {
    fn supports_duration_udf(&self) -> bool {
        true
    }
}

pub struct InfluxFilterPushdown {} // Filter pushdown dialect for InfluxDb
impl FilterPushdownConverter for InfluxFilterPushdown {
    fn col_to_sql(&self, col: &Column) -> String {
        if col.name == "trip_id" {
            return "trip_id::BIGINT".to_string();
        }
        if col.name == "moid" {
            return "moid::BIGINT".to_string();
        }
        col.name.clone() // Return just the column name, without any double quotation marks
    }
    fn supports_duration_udf(&self) -> bool {
        // Support pushdown of duration UDF. Default is false
        true
    }
}

pub trait FilterPushdownConverter {
    fn col_to_sql(&self, col: &Column) -> String {
        quote_identifier_double_quotes(&col.name)
    }
    fn supports_duration_udf(&self) -> bool {
        false
    }

    fn scalar_value_to_sql(&self, value: &ScalarValue) -> Option<String> {
        match value {
            ScalarValue::Utf8(Some(val)) | ScalarValue::LargeUtf8(Some(val)) => {
                Some(format!("'{}'", val.replace('\'', "''")))
            }
            ScalarValue::Date32(Some(days)) => {
                let date = date32_to_datetime(*days)?.date();
                Some(format!("'{date}'"))
            }
            ScalarValue::Date64(Some(t_ms))
            | ScalarValue::TimestampMillisecond(Some(t_ms), None) => {
                let timestamp = timestamp_ms_to_datetime(*t_ms)?;
                Some(format!("'{timestamp}'"))
            }
            ScalarValue::TimestampSecond(Some(t_s), None) => {
                let timestamp = timestamp_s_to_datetime(*t_s)?;
                Some(format!("'{timestamp}'"))
            }
            ScalarValue::TimestampMicrosecond(Some(t_us), None) => {
                let timestamp = timestamp_us_to_datetime(*t_us)?;
                Some(format!("'{timestamp}'"))
            }
            ScalarValue::TimestampNanosecond(Some(t_ns), None) => {
                let timestamp = timestamp_ns_to_datetime(*t_ns)?;
                Some(format!("'{timestamp}'"))
            }
            ScalarValue::IntervalMonthDayNano(Some(interval)) => {
                if interval.months == 0 && interval.days == 0 {
                    match interval.nanoseconds {
                        3_600_000_000_000 => Some("INTERVAL '1 hour'".to_string()),
                        60_000_000_000 => Some("INTERVAL '1 minute'".to_string()),
                        1_000_000_000 => Some("INTERVAL '1 second'".to_string()),
                        ns if ns % 1_000_000_000 == 0 => {
                            Some(format!("INTERVAL '{} seconds'", ns / 1_000_000_000))
                        }
                        _ => Some(format!("INTERVAL '{} nanoseconds'", interval.nanoseconds)),
                    }
                } else {
                    Some(format!(
                        "INTERVAL '{{ months: {}, days: {}, nanoseconds: {} }}'",
                        interval.months, interval.days, interval.nanoseconds
                    ))
                }
            }
            ScalarValue::TimestampSecond(_, Some(_))
            | ScalarValue::TimestampMillisecond(_, Some(_))
            | ScalarValue::TimestampMicrosecond(_, Some(_))
            | ScalarValue::TimestampNanosecond(_, Some(_)) => None,
            _ => Some(format!("{value}")),
        }
    }

    fn op_to_sql(&self, op: &Operator) -> Option<String> {
        Some(op.to_string())
    }
}

impl<T: FilterPushdownConverter> TreeNodeVisitor<'_> for FilterPushdownVisitor<T> {
    type Node = Expr;

    fn f_down(&mut self, expr: &Expr) -> Result<TreeNodeRecursion> {
        match expr {
            Expr::Column(_)
            | Expr::Literal(_, _)
            | Expr::Not(_)
            | Expr::Negative(_)
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotFalse(_)
            | Expr::InList { .. }
            | Expr::Cast { .. } => {}
            Expr::BinaryExpr(BinaryExpr { op, .. }) => {
                if self.source.op_to_sql(op).is_none() {
                    return Err(DataFusionError::Execution(format!(
                        "Operator {op} not shippable",
                    )));
                }
            }
            Expr::ScalarFunction(fun) => {
                if fun.name() == "duration" && !self.source.supports_duration_udf() {
                    return Err(DataFusionError::Execution(
                        "duration UDF is not pushdown supported for this source".to_string(),
                    ));
                }
                if fun.name() != "duration" && self.source.supports_duration_udf() {
                    return Err(DataFusionError::Execution(format!(
                        "Only duration UDF is supported, got: {}",
                        fun.name()
                    )));
                }
            }
            _ => {
                return Err(DataFusionError::Execution(format!(
                    "Expression {expr:?} not shippable",
                )));
            }
        };
        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, expr: &Expr) -> Result<TreeNodeRecursion> {
        match expr {
            Expr::Column(col) => self.sql_exprs.push(self.source.col_to_sql(col)),
            Expr::Literal(val, _) => {
                let sql_val = self.source.scalar_value_to_sql(val).ok_or_else(|| {
                    DataFusionError::Execution(format!("ScalarValue {val:?} not shippable",))
                })?;
                self.sql_exprs.push(sql_val)
            }
            Expr::BinaryExpr(be @ BinaryExpr { .. }) => {
                let mut right_sql = self.pop_sql_expr();
                let mut left_sql = self.pop_sql_expr();

                if let Expr::BinaryExpr(right_be @ BinaryExpr { .. }) = &*be.right {
                    let p = right_be.op.precedence();
                    if p == 0 || p < be.op.precedence() {
                        right_sql = format!("({right_sql})")
                    }
                }
                if let Expr::BinaryExpr(left_be @ BinaryExpr { .. }) = &*be.left {
                    let p = left_be.op.precedence();
                    if p == 0 || p < be.op.precedence() {
                        left_sql = format!("({left_sql})")
                    }
                }

                let op_sql = self.source.op_to_sql(&be.op).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "Couldn't convert operator {:?} to a compatible one for the remote system",
                        be.op,
                    ))
                })?;

                self.sql_exprs
                    .push(format!("{left_sql} {op_sql} {right_sql}"))
            }
            Expr::Not(_) => {
                let inner_sql = self.pop_sql_expr();
                self.sql_exprs.push(format!("NOT {inner_sql}"));
            }
            Expr::Negative(_) => {
                let inner_sql = self.pop_sql_expr();
                self.sql_exprs.push(format!("(- {inner_sql})"));
            }
            Expr::IsNull(_) => {
                let inner_sql = self.pop_sql_expr();
                self.sql_exprs.push(format!("{inner_sql} IS NULL"));
            }
            Expr::IsNotNull(_) => {
                let inner_sql = self.pop_sql_expr();
                self.sql_exprs.push(format!("{inner_sql} IS NOT NULL"));
            }
            Expr::IsTrue(_) => {
                let inner_sql = self.pop_sql_expr();
                self.sql_exprs.push(format!("{inner_sql} IS TRUE"));
            }
            Expr::IsFalse(_) => {
                let inner_sql = self.pop_sql_expr();
                self.sql_exprs.push(format!("{inner_sql} IS FALSE"));
            }
            Expr::IsNotTrue(_) => {
                let inner_sql = self.pop_sql_expr();
                self.sql_exprs.push(format!("{inner_sql} IS NOT TRUE"));
            }
            Expr::IsNotFalse(_) => {
                let inner_sql = self.pop_sql_expr();
                self.sql_exprs.push(format!("{inner_sql} IS NOT FALSE"));
            }
            Expr::InList(InList { list, negated, .. }) => {
                let index = self.sql_exprs.len() - list.len();
                let list_sql = self.sql_exprs.split_off(index).iter().join(", ");

                let expr_sql = self.pop_sql_expr();
                if *negated {
                    self.sql_exprs
                        .push(format!("{expr_sql} NOT IN ({list_sql})"));
                } else {
                    self.sql_exprs.push(format!("{expr_sql} IN ({list_sql})"));
                }
            }
            Expr::ScalarFunction(func) => {
                if func.name() == "duration" && self.source.supports_duration_udf() {
                    if func.args.len() != 1 {
                        return Err(DataFusionError::Internal("Duration expects 1 arg".into()));
                    }
                    let arg_sql = self.pop_sql_expr();
                    // For Influx: push down as MAX(time) - MIN(time)
                    // For Postgres: just push down duration(arg)
                    let sql = if self.source.col_to_sql(&Column {
                        relation: None,
                        name: "__DIALECT_INFLUX__".to_string(),
                        spans: datafusion::common::Spans::default(),
                    }) == "__DIALECT_INFLUX__"
                    {
                        // Influx dialect
                        "MAX(time) - MIN(time)".to_string()
                    } else {
                        // Postgres or other: push down as duration(arg)
                        format!("duration({})", arg_sql)
                    };
                    self.sql_exprs.push(sql);
                }
            }
            _ => {}
        };
        Ok(TreeNodeRecursion::Continue)
    }
}

pub fn quote_identifier_double_quotes(name: &str) -> String {
    format!("\"{}\"", name.replace('\"', "\"\""))
}

pub fn filter_expr_to_sql<T: FilterPushdownConverter>(filter: &Expr, source: T) -> Result<String> {
    let mut visitor = FilterPushdownVisitor {
        source,
        sql_exprs: vec![],
    };

    filter.visit(&mut visitor)?;
    let sql_exprs = visitor.sql_exprs;

    if sql_exprs.len() != 1 {
        return Err(DataFusionError::Execution(format!(
            "Expected exactly one SQL expression for filter {filter}, found: {sql_exprs:?}",
        )));
    }

    Ok(sql_exprs
        .first()
        .expect("Exactly 1 SQL expression expected")
        .clone())
}

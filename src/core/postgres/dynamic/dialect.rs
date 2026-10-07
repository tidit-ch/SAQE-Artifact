//! Unparser dialect that rewrites this project's UDFs into PostGIS calls.
//!
//! When DataFusion pushes a filter down to Postgres it renders the expression back to SQL with
//! [`Unparser`]. The renderer has no allowlist: any `ScalarFunction` becomes `name(args…)`, so a
//! UDF that exists only in this process is shipped verbatim and Postgres answers
//! `function contained(…) does not exist`. There is no fallback — the filter was already marked
//! `Exact`, so the query simply fails.
//!
//! [`PostGisDialect`] takes over that decision. It maps the UDFs that do have a PostGIS
//! counterpart onto it, and makes every other project UDF fail to unparse, which is how a
//! `TableProvider` says "not pushable": [`default_filter_pushdown`] downgrades it to
//! `Unsupported` and DataFusion evaluates it locally instead.
//!
//! [`default_filter_pushdown`]: datafusion_table_providers::sql::sql_provider_datafusion::default_filter_pushdown

use datafusion::common::{plan_datafusion_err, plan_err, Result, ScalarValue};
use datafusion::logical_expr::Expr;
use datafusion::sql::sqlparser::ast;
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect as PostgresParserDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::unparser::dialect::{Dialect, IntervalStyle, PostgreSqlDialect};
use datafusion::sql::unparser::Unparser;

/// SRID the geometry columns are stored in.
///
/// A WKT literal in a query carries no SRID and PostGIS refuses to compare mixed SRIDs, so one
/// has to be supplied here. It has to be a constant: deriving it as `ST_SRID(column)` would make
/// the argument non-constant, and the planner only rewrites `ST_Contains(a, b)` into an
/// index-backed `a && b AND _ST_Contains(a, b)` when one side is constant.
const GEOMETRY_SRID: i32 = 4326;

/// UDFs that only translate between the geoarrow in-memory form and the on-the-wire encoding.
/// Postgres hands the column over as EWKB and takes it back the same way, so the wrapper has no
/// counterpart there and the argument is unparsed on its own.
const CODEC_UDFS: [&str; 2] = ["wkb_to_trajectory", "trajectory_to_wkb"];

/// UDFs whose name, argument order and semantics already line up with a PostGIS function. These
/// come from `geodatafusion`, which models itself on PostGIS.
const PASS_THROUGH_UDFS: [&str; 3] = ["st_length", "st_distance", "st_intersects"];

/// A [`PostgreSqlDialect`] that also understands this project's trajectory UDFs.
#[derive(Debug, Default)]
pub struct PostGisDialect;

impl Dialect for PostGisDialect {
    fn identifier_quote_style(&self, identifier: &str) -> Option<char> {
        PostgreSqlDialect {}.identifier_quote_style(identifier)
    }

    fn interval_style(&self) -> IntervalStyle {
        PostgreSqlDialect {}.interval_style()
    }

    fn float64_ast_dtype(&self) -> ast::DataType {
        PostgreSqlDialect {}.float64_ast_dtype()
    }

    fn scalar_function_to_sql_overrides(
        &self,
        unparser: &Unparser,
        func_name: &str,
        args: &[Expr],
    ) -> Result<Option<ast::Expr>> {
        match func_name {
            "contained" => contained_to_postgis(unparser, args).map(Some),

            name if CODEC_UDFS.contains(&name) => match args {
                [inner] => unparser.expr_to_sql(inner).map(Some),
                _ => plan_err!("`{name}` takes exactly one argument"),
            },

            name if PASS_THROUGH_UDFS.contains(&name) => {
                PostgreSqlDialect {}.scalar_function_to_sql_overrides(unparser, name, args)
            }

            // Every other UDF this process registers is DataFusion-only. Failing here is what
            // keeps it out of the pushed-down SQL.
            name if is_project_udf(name) => plan_err!(
                "`{name}` has no PostGIS equivalent and cannot be pushed down to Postgres"
            ),

            // Built-in SQL functions are not in the UDF registry and fall through to Postgres.
            name => PostgreSqlDialect {}.scalar_function_to_sql_overrides(unparser, name, args),
        }
    }
}

/// `contained(trajectory, strictness, polygon_wkt)` → `ST_Contains(polygon, trajectory)`.
///
/// The argument order is inverted: PostGIS names the container first. `'relaxed'` asks whether
/// the trajectory intersects *or* is contained, and containment implies intersection, so it
/// collapses to a single `ST_Intersects`.
fn contained_to_postgis(unparser: &Unparser, args: &[Expr]) -> Result<ast::Expr> {
    let [trajectory, strictness, polygon] = args else {
        return plan_err!("`contained` takes exactly three arguments");
    };

    let function = match utf8_literal(strictness) {
        Some("strict") => "ST_Contains",
        Some("relaxed") => "ST_Intersects",
        // A non-literal strictness cannot be resolved at plan time, and an unknown one is an
        // error the local implementation should be the one to report.
        _ => {
            return plan_err!(
                "`contained` can only be pushed down with a literal 'strict' or 'relaxed'"
            )
        }
    };

    // The polygon has to be a literal so it can become a constant geometry; a column would
    // defeat the index rewrite and, if it were already a geometry, be double-converted.
    let Some(polygon) = utf8_literal(polygon) else {
        return plan_err!("`contained` can only be pushed down with a literal WKT polygon");
    };

    let trajectory = unparser.expr_to_sql(trajectory)?;

    raw_sql(&format!(
        "{function}(ST_GeomFromText({}, {GEOMETRY_SRID}), {trajectory})",
        single_quoted(polygon)
    ))
}

fn utf8_literal(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(s)), _) => Some(s),
        _ => None,
    }
}

fn single_quoted(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn is_project_udf(name: &str) -> bool {
    crate::core::udf::get_udf_name_list()
        .iter()
        .any(|registered| registered == name)
}

/// Parses a SQL fragment back into the AST the unparser works in.
///
/// Building `ast::Function` by hand is possible but tracks sqlparser's internals closely; going
/// through the parser keeps this to one line and cannot produce an expression that does not
/// re-parse.
fn raw_sql(sql: &str) -> Result<ast::Expr> {
    Parser::new(&PostgresParserDialect {})
        .try_with_sql(sql)
        .and_then(|mut parser| parser.parse_expr())
        .map_err(|e| plan_datafusion_err!("could not build PostGIS expression `{sql}`: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::udf::spatial_filters::contained::Contained;
    use crate::core::udf::temporal_filters::during::During;
    use crate::core::udf::utils::wkb_to_trajectory::WkbToTrajectory;
    use datafusion::logical_expr::expr::ScalarFunction;
    use datafusion::logical_expr::ScalarUDF;
    use datafusion::prelude::{col, lit, SessionContext};
    use std::sync::Arc;

    /// `is_project_udf` reads the registry `udf::init` fills in, which `core::init` does before
    /// any catalog is registered. Registration also builds the map-matching UDFs, which read
    /// `GLOBAL_CONFIG`, so the config path has to be set first.
    fn init_udf_registry() {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| {
            if std::env::var("CONFIG_FILE").is_err() {
                std::env::set_var("CONFIG_FILE", "config/config_dev.json");
            }
            crate::core::udf::init(&SessionContext::new());
        });
    }

    fn call(udf: impl Into<ScalarUDF>, args: Vec<Expr>) -> Expr {
        Expr::ScalarFunction(ScalarFunction::new_udf(Arc::new(udf.into()), args))
    }

    fn to_sql(expr: &Expr) -> Result<String> {
        Unparser::new(&PostGisDialect)
            .expr_to_sql(expr)
            .map(|sql| sql.to_string())
    }

    /// The wrapper only exists because DataFusion sees the geometry column as `Binary`; on the
    /// Postgres side the column already is the geometry.
    #[test]
    fn drops_the_wkb_wrapper() {
        init_udf_registry();
        let expr = call(WkbToTrajectory::new(), vec![col("polyline")]);
        assert_eq!(to_sql(&expr).unwrap(), r#""polyline""#);
    }

    #[test]
    fn rewrites_strict_contained_to_st_contains() {
        init_udf_registry();
        let expr = call(
            Contained::new(),
            vec![
                call(WkbToTrajectory::new(), vec![col("polyline")]),
                lit("strict"),
                lit("POLYGON((1 1, 1 4, 4 4, 4 1, 1 1))"),
            ],
        );

        assert_eq!(
            to_sql(&expr).unwrap(),
            "ST_Contains(ST_GeomFromText('POLYGON((1 1, 1 4, 4 4, 4 1, 1 1))', 4326), \"polyline\")"
        );
    }

    /// `contained` takes an EWKB column directly, so the `pg` form needs no wrapper.
    #[test]
    fn rewrites_contained_on_a_bare_geometry_column() {
        init_udf_registry();
        let expr = call(
            Contained::new(),
            vec![
                col("polyline"),
                lit("strict"),
                lit("POLYGON((1 1, 1 4, 4 4, 4 1, 1 1))"),
            ],
        );

        assert_eq!(
            to_sql(&expr).unwrap(),
            "ST_Contains(ST_GeomFromText('POLYGON((1 1, 1 4, 4 4, 4 1, 1 1))', 4326), \"polyline\")"
        );
    }

    /// `'relaxed'` is `intersects || contains`, and containment implies intersection.
    #[test]
    fn rewrites_relaxed_contained_to_st_intersects() {
        init_udf_registry();
        let expr = call(
            Contained::new(),
            vec![
                call(WkbToTrajectory::new(), vec![col("polyline")]),
                lit("relaxed"),
                lit("POLYGON((1 1, 1 4, 4 4, 4 1, 1 1))"),
            ],
        );

        assert!(to_sql(&expr).unwrap().starts_with("ST_Intersects("));
    }

    #[test]
    fn escapes_quotes_in_the_polygon_literal() {
        init_udf_registry();
        let expr = call(
            Contained::new(),
            vec![
                col("polyline"),
                lit("strict"),
                lit("POLYGON'); DROP TABLE trips; --"),
            ],
        );

        let sql = to_sql(&expr).unwrap();
        assert!(sql.contains("''"), "quote not doubled: {sql}");
        assert!(
            !sql.contains("DROP TABLE trips; --)"),
            "statement broke out: {sql}"
        );
    }

    /// Failing to unparse is how a filter is kept local: `default_filter_pushdown` turns the
    /// error into `TableProviderFilterPushDown::Unsupported`.
    #[test]
    fn refuses_udfs_without_a_postgis_equivalent() {
        init_udf_registry();
        let expr = call(
            During::new(),
            vec![col("polyline"), lit("2007-05-01"), lit("2008-01-01")],
        );

        let err = to_sql(&expr).unwrap_err().to_string();
        assert!(err.contains("during"), "unexpected error: {err}");
    }

    /// A strictness that is not a literal cannot be resolved at plan time.
    #[test]
    fn refuses_contained_with_a_non_literal_strictness() {
        init_udf_registry();
        let expr = call(
            Contained::new(),
            vec![col("polyline"), col("strictness"), lit("POLYGON((0 0))")],
        );

        assert!(to_sql(&expr).is_err());
    }

    /// The whole statement, not just the expression: this is the shape `SqlTable::scan_to_sql`
    /// sends to Postgres, and the one whose constant polygon lets the planner rewrite
    /// `ST_Contains(a, b)` into an index-backed `a && b AND _ST_Contains(a, b)`.
    #[test]
    fn renders_the_pushed_down_statement() {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::logical_expr::{LogicalPlanBuilder, LogicalTableSource};

        init_udf_registry();

        let schema = Schema::new(vec![
            Field::new("trip_id", DataType::Int64, true),
            Field::new("polyline", DataType::Binary, true),
        ]);
        let filter = call(
            Contained::new(),
            vec![
                call(WkbToTrajectory::new(), vec![col("polyline")]),
                lit("strict"),
                lit("POLYGON((1 1, 1 4, 4 4, 4 1, 1 1))"),
            ],
        );

        // Mirrors `SqlTable::create_logical_plan`: the pushed-down filter rides on the scan,
        // not in a `Filter` node, so it may reference a column the projection drops.
        let plan = LogicalPlanBuilder::scan_with_filters(
            "trips",
            Arc::new(LogicalTableSource::new(Arc::new(schema))),
            Some(vec![0]),
            vec![filter],
        )
        .unwrap()
        .build()
        .unwrap();

        assert_eq!(
            Unparser::new(&PostGisDialect)
                .plan_to_sql(&plan)
                .unwrap()
                .to_string(),
            r#"SELECT "trips"."trip_id" FROM "trips" WHERE ST_Contains(ST_GeomFromText('POLYGON((1 1, 1 4, 4 4, 4 1, 1 1))', 4326), "trips"."polyline")"#
        );
    }

    /// Built-in SQL functions are not in the UDF registry and must still reach Postgres.
    #[test]
    fn leaves_builtin_functions_alone() {
        init_udf_registry();
        let expr = datafusion::prelude::abs(col("speed"));
        assert_eq!(to_sql(&expr).unwrap(), r#"abs("speed")"#);
    }
}

pub mod ast;
pub mod functions;
pub mod parser;

/*
Architecture:
    1. parser.rs   — CustomSqlParser strips crop() from the SQL, returns (new_sql, CropASTNode).
    2. node.rs     — CropNode: a UserDefinedLogicalNode that temporarily wraps the base plan
                     and carries the Arc<CropASTNode> into the optimizer.
    3. optimizer.rs — InjectCropRule: finds CropNode, injects crop(polyline_col) directly
                     into the Projection node, removes CropNode.
    4. udf.rs      — CropProjectionUdf: ScalarUDFImpl that holds Arc<CropASTNode> directly on the
                     struct — no serialization. invoke_with_args has full access to the
                     predicate tree for implementing the crop logic.
    5. execution.rs / planner.rs — kept for reference; no longer needed by this pipeline.
*/

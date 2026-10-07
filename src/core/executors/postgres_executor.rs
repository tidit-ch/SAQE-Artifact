use arrow::array::{
    Array, ArrayRef, BinaryArray, Float64Builder, ListBuilder, RecordBatch, StructBuilder,
    TimestampMillisecondBuilder,
};
use arrow_schema::{DataType, Field, Fields};
use async_stream::stream;
use datafusion::{
    arrow::datatypes::SchemaRef,
    error::{DataFusionError, Result as DatafusionResult},
    physical_plan::{stream::RecordBatchStreamAdapter, SendableRecordBatchStream},
};
use datafusion_table_providers::sql::db_connection_pool::dbconnection::query_arrow;
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;
use futures::StreamExt;
use postgis::{
    ewkb::{EwkbRead, LineStringM, MultiLineStringM, MultiPointM, Point, Polygon},
    MultiLineString,
};
use std::sync::Arc;

// Executes a SQL query on the remote PostgreSQL database and returns a stream of record batches
pub async fn execute_remote_postgres(
    sql: String,
    postgres_pool: Arc<PostgresConnectionPool>,
    projected_schema: SchemaRef,
) -> DatafusionResult<SendableRecordBatchStream> {
    Ok(Box::pin(RecordBatchStreamAdapter::new(
        Arc::clone(&projected_schema),
        {
            stream! {
                let conn = postgres_pool.connect()
                    .await
                    .map_err(|e| DataFusionError::External(e))?;

                let mut record_batch_stream = query_arrow(
                    conn,
                    sql,
                    None
                )
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

                while let Some(batch) = record_batch_stream.next().await {
                    let batch = batch.map_err(|e| DataFusionError::External(Box::new(e)))?;
                    let converted_batch = try_cast_to(batch.clone(), Arc::clone(&projected_schema));
                    yield Ok(converted_batch?);
                }
            }
        },
    )))
}

/**
 * Attempts to cast the columns of a RecordBatch to
 * the types defined in the projected schema.
 */
fn try_cast_to(batch: RecordBatch, projected_schema: SchemaRef) -> DatafusionResult<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::new(); // Casted columns to be returned

    for (i, column) in batch.columns().iter().enumerate() {
        match column.data_type() {
            DataType::Int32 => {
                let casted_array = arrow::compute::cast(column, &DataType::Int64)?;
                columns.push(casted_array);
            }
            DataType::Utf8 => {
                columns.push(column.clone());
            }
            DataType::Int64 => {
                columns.push(column.clone());
            }
            DataType::Float64 => {
                columns.push(column.clone());
            }
            DataType::Binary => {
                let ewkb_array = column
                    .as_any()
                    .downcast_ref::<arrow::array::BinaryArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "Expected BinaryArray for POSTGIS geometry".to_string(),
                        )
                    })?;

                let field = projected_schema.field(i);
                let struct_fields = match field.data_type() {
                    DataType::List(inner_field) => match inner_field.data_type() {
                        DataType::Struct(fields) => fields,
                        _ => {
                            return Err(DataFusionError::Execution(
                                "Expected Struct inside List for geometry".to_string(),
                            ));
                        }
                    },
                    DataType::Struct(fields) => fields,
                    _ => {
                        return Err(DataFusionError::Execution(
                            "Expected List type for geometry".to_string(),
                        ));
                    }
                };

                let x_nullable = struct_fields[0].is_nullable();
                let y_nullable = struct_fields[1].is_nullable();

                let bytes = ewkb_array.value(0); // Check the first element to determine the geometry type
                let geom: DetectedGeom = detect_geom_type(bytes).map_err(|e| {
                    DataFusionError::Execution(format!("Failed to detect geometry type: {e}"))
                })?;

                let array = match (geom.base_type, geom.has_z, geom.has_m, geom.has_srid) {
                    (1, false, false, true) => {
                        parse_point_array(ewkb_array, x_nullable, y_nullable)?
                    } // POSTGIS POINT type
                    (2, false, true, true) => parse_linestringm_array(ewkb_array)?, // POSTGIS LINESTRINGM type
                    (3, false, false, true) => {
                        parse_polygon_array(ewkb_array, x_nullable, y_nullable)?
                    } // POSTGIS POLYGON type
                    (4, false, true, true) | (5, false, true, true) => {
                        parse_multi_array(ewkb_array)?
                    } // Either multipoint or multilinestring
                    _ => {
                        return Err(DataFusionError::NotImplemented(format!(
                            "Unsupported geometry: base_type={}, has_z={}, has_m={}, has_srid={}",
                            geom.base_type, geom.has_z, geom.has_m, geom.has_srid
                        )))
                    }
                };
                columns.push(array);
            }
            DataType::Timestamp(_, _) => {
                let casted_array = arrow::compute::cast(
                    column,
                    &arrow::datatypes::DataType::Timestamp(
                        arrow::datatypes::TimeUnit::Millisecond,
                        None,
                    ),
                )?;
                columns.push(casted_array);
            }
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Casting for data type {:?} is not implemented",
                    column.data_type()
                )));
            }
        }
    }
    let batch = RecordBatch::try_new(projected_schema.clone(), columns)?;
    Ok(batch)
}

#[derive(Debug)]
struct DetectedGeom {
    base_type: u32,
    has_z: bool,
    has_m: bool,
    has_srid: bool,
}

/**
 *  Detects the geometry type from the EWKB byte array.
 *  Source how the WKB geometry types are encoded:
 *  https://loaders.gl/docs/modules/wkt/formats/wkb
 */
fn detect_geom_type(bytes: &[u8]) -> DatafusionResult<DetectedGeom, DataFusionError> {
    if bytes.len() < 5 {
        return Err(DataFusionError::Execution("EWKB too short".into()));
    }
    let geom_type = u32::from_le_bytes(bytes[1..5].try_into().unwrap());
    let base_type = geom_type & 0xFF;
    let has_z = geom_type & 0x80000000 != 0;
    let has_m = geom_type & 0x40000000 != 0;
    let has_srid = geom_type & 0x20000000 != 0;

    Ok(DetectedGeom {
        base_type,
        has_z,
        has_m,
        has_srid,
    })
}

// Parse BinaryArray into our custom datafusion point format
fn parse_point_array(
    ewkb_array: &BinaryArray,
    x_nullable: bool,
    y_nullable: bool,
) -> DatafusionResult<ArrayRef> {
    let mut struct_builder = StructBuilder::new(
        vec![
            Field::new("x", DataType::Float64, x_nullable),
            Field::new("y", DataType::Float64, y_nullable),
        ],
        vec![
            Box::new(Float64Builder::new()),
            Box::new(Float64Builder::new()),
        ],
    );

    for i in 0..ewkb_array.len() {
        let bytes = ewkb_array.value(i);
        let mut cursor = std::io::Cursor::new(bytes);
        let point: Point = EwkbRead::read_ewkb(&mut cursor).unwrap();

        struct_builder
            .field_builder::<Float64Builder>(0)
            .unwrap()
            .append_value(point.x);
        struct_builder
            .field_builder::<Float64Builder>(1)
            .unwrap()
            .append_value(point.y);

        struct_builder.append(true);
    }
    Ok(Arc::new(struct_builder.finish()))
}

// Parse BinaryArray into our custom datafusion polyline format - field
// names/types/nullability match TRAJECTORY_DATATYPE
// (src/core/utils/schema.rs) exactly, since that's what every declared
// polyline schema in this codebase (CSV, Parquet, the other Postgres read
// path in berlin_mod_postgres/trips/schema.rs) actually uses; this function
// previously built a structurally different type ("item"/"timestamp"/
// Timestamp(Millisecond)/nullable) that RecordBatch::try_new would reject
// against any schema declared the normal way. The "m" value is already a
// plain f64 on both the write side (sink_trips.rs) and every other read
// path - there's no actual Timestamp type involved anywhere in this format.
fn parse_linestringm_array(ewkb_array: &BinaryArray) -> DatafusionResult<ArrayRef> {
    let struct_fields = Fields::from(vec![
        Field::new("x", DataType::Float64, false),
        Field::new("y", DataType::Float64, false),
        Field::new("m", DataType::Float64, false),
    ]);

    let struct_builder = ListBuilder::new(StructBuilder::new(
        struct_fields.clone(),
        vec![
            Box::new(Float64Builder::new()),
            Box::new(Float64Builder::new()),
            Box::new(Float64Builder::new()),
        ],
    ));

    let field = Field::new("vertices", DataType::Struct(struct_fields), false);
    let mut list_builder = ListBuilder::with_field(struct_builder, field);

    for i in 0..ewkb_array.len() {
        let bytes = ewkb_array.value(i);
        let mut cursor = std::io::Cursor::new(bytes);
        let linestring: LineStringM = EwkbRead::read_ewkb(&mut cursor)
            .map_err(|e| DataFusionError::Execution(format!("Failed to parse LineStringM: {e}")))?;

        let struct_builder = list_builder.values();
        for point in linestring.points {
            struct_builder
                .field_builder::<Float64Builder>(0)
                .unwrap()
                .append_value(point.x);
            struct_builder
                .field_builder::<Float64Builder>(1)
                .unwrap()
                .append_value(point.y);
            struct_builder
                .field_builder::<Float64Builder>(2)
                .unwrap()
                .append_value(point.m);
            struct_builder.append(true);
        }

        list_builder.append(true);
    }

    Ok(Arc::new(list_builder.finish()))
}

// Parse BinaryArray into our custom datafusion polygon format
fn parse_polygon_array(
    ewkb_array: &BinaryArray,
    x_nullable: bool,
    y_nullable: bool,
) -> DatafusionResult<ArrayRef> {
    let struct_fields = Fields::from(vec![
        Field::new("x", DataType::Float64, x_nullable),
        Field::new("y", DataType::Float64, y_nullable),
    ]);

    let struct_builder = ListBuilder::new(StructBuilder::new(
        struct_fields.clone(),
        vec![
            Box::new(Float64Builder::new()),
            Box::new(Float64Builder::new()),
        ],
    ));

    let field = Field::new("item", DataType::Struct(struct_fields), false);
    let mut list_builder = ListBuilder::with_field(struct_builder, field);

    for i in 0..ewkb_array.len() {
        let bytes = ewkb_array.value(i);
        let mut cursor = std::io::Cursor::new(bytes);
        let polygon: Polygon = EwkbRead::read_ewkb(&mut cursor)
            .map_err(|e| DataFusionError::Execution(format!("Failed to parse Polygon: {e}")))?;

        let struct_builder = list_builder.values();
        for point in polygon.rings[0].points.clone() {
            // BerlinMod regions has only one ring -> always rings[0]!
            struct_builder
                .field_builder::<Float64Builder>(0)
                .unwrap()
                .append_value(point.x);
            struct_builder
                .field_builder::<Float64Builder>(1)
                .unwrap()
                .append_value(point.y);
            struct_builder.append(true);
        }

        list_builder.append(true);
    }
    Ok(Arc::new(list_builder.finish()))
}

// Parse BinaryArray into our custom datafusion polyline format
fn parse_multi_array(ewkb_array: &BinaryArray) -> DatafusionResult<ArrayRef> {
    let struct_fields = Fields::from(vec![
        Field::new("x", DataType::Float64, true),
        Field::new("y", DataType::Float64, true),
        Field::new(
            "timestamp",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        ),
    ]);

    let struct_builder = ListBuilder::new(StructBuilder::new(
        struct_fields.clone(),
        vec![
            Box::new(Float64Builder::new()),
            Box::new(Float64Builder::new()),
            Box::new(TimestampMillisecondBuilder::new()),
        ],
    ));

    let field = Field::new("item", DataType::Struct(struct_fields), false);
    let mut list_builder = ListBuilder::with_field(struct_builder, field);

    for i in 0..ewkb_array.len() {
        let bytes = ewkb_array.value(i);
        let mut cursor = std::io::Cursor::new(bytes);
        let geom: DetectedGeom = detect_geom_type(bytes).map_err(|e| {
            DataFusionError::Execution(format!("Failed to detect geometry type: {e}"))
        })?;
        match (geom.base_type, geom.has_z, geom.has_m, geom.has_srid) {
            // MultipointM: treat each point as a single trajectory
            (4, false, true, true) => {
                let multipoint: MultiPointM = EwkbRead::read_ewkb(&mut cursor).map_err(|e| {
                    DataFusionError::Execution(format!("Failed to parse MultiPointM: {e}"))
                })?;
                let struct_builder = list_builder.values();
                for point in multipoint.points {
                    struct_builder
                        .field_builder::<Float64Builder>(0)
                        .unwrap()
                        .append_value(point.x);
                    struct_builder
                        .field_builder::<Float64Builder>(1)
                        .unwrap()
                        .append_value(point.y);
                    struct_builder
                        .field_builder::<TimestampMillisecondBuilder>(2)
                        .unwrap()
                        .append_value(point.m as i64);
                    struct_builder.append(true);
                }
                list_builder.append(true);
            }
            // MultiLineStringM: use only the first linestring as trajectory
            (5, false, true, true) => {
                let multilinestring: MultiLineStringM =
                    EwkbRead::read_ewkb(&mut cursor).map_err(|e| {
                        DataFusionError::Execution(format!("Failed to parse MultiLineStringM: {e}"))
                    })?;
                if let Some(first_linestring) = multilinestring.lines().next() {
                    let struct_builder = list_builder.values();
                    for point in &first_linestring.points {
                        struct_builder
                            .field_builder::<Float64Builder>(0)
                            .unwrap()
                            .append_value(point.x);
                        struct_builder
                            .field_builder::<Float64Builder>(1)
                            .unwrap()
                            .append_value(point.y);
                        struct_builder
                            .field_builder::<TimestampMillisecondBuilder>(2)
                            .unwrap()
                            .append_value(point.m as i64);
                        struct_builder.append(true);
                    }
                    list_builder.append(true);
                }
            }
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported geometry: base_type={}, has_z={}, has_m={}, has_srid={}",
                    geom.base_type, geom.has_z, geom.has_m, geom.has_srid
                )))
            }
        }
    }

    Ok(Arc::new(list_builder.finish()))
}

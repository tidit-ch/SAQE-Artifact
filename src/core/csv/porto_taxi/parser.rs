use crate::core::utils::{
    parse::{add_primitive_field_to_builders, is_datatype_primitive},
    schema::TRAJECTORY_DATATYPE,
};
use crate::utils::error::CsvError;
use arrow_schema::extension::{ExtensionType, EXTENSION_TYPE_NAME_KEY};
use csv::StringRecord;
use datafusion::arrow::{
    array::*,
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use geoarrow_schema::LineStringType;
use std::{collections::HashSet, sync::Arc};

type Result<T, E = CsvError> = std::result::Result<T, E>;

pub fn new_get_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("trip_id", DataType::Int64, false),
        Field::new("call_type", DataType::Utf8, false),
        Field::new("origin_call", DataType::Int64, true),
        Field::new("origin_stand", DataType::Int64, true),
        Field::new("taxi_id", DataType::Int64, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("day_type", DataType::Utf8, false),
        Field::new("missing_data", DataType::Boolean, false),
        Field::new("polyline", TRAJECTORY_DATATYPE.clone(), false).with_metadata(
            [(
                EXTENSION_TYPE_NAME_KEY.to_owned(),
                LineStringType::NAME.to_owned(),
            )]
            .into_iter()
            .collect(),
        ),
    ]))
}

pub struct TaxiTrajectoryData {
    projection: HashSet<usize>,
    schema: SchemaRef,
    pub builders: Vec<Box<dyn ArrayBuilder>>,
}

impl TaxiTrajectoryData {
    pub fn new(projection: Arc<Vec<usize>>, builders: Vec<Box<dyn ArrayBuilder>>) -> Self {
        TaxiTrajectoryData {
            projection: projection.iter().cloned().collect(),
            schema: new_get_schema(),
            builders,
        }
    }

    pub fn field_count() -> usize {
        9
    }

    fn parse_raw_polyline(data: String) -> Result<Vec<[f64; 2]>> {
        let mut data = data.trim_start_matches("[[").trim_end_matches("]]");
        data = data.trim_start_matches('[').trim_end_matches(']');
        if data.is_empty() {
            return Ok(Vec::new());
        }
        let points: Vec<&str> = data.split("],[").collect();
        let mut result = Vec::new();
        for point in points {
            let coords: Vec<&str> = point.split(',').collect();
            if coords.len() == 2 {
                // Raw taxi data is (lon, lat); we store it as (x=lon, y=lat) to match the
                // system-wide GeoArrow/WKT convention used by every other dataset and UDF.
                let lon: f64 = coords[0].parse::<f64>().unwrap_or(0.0);
                let lat = coords[1].parse::<f64>().unwrap_or(0.0);
                result.push([lon, lat]);
            } else {
                return Err(CsvError::CellParse {
                    expected: "[long, lat]".to_string(),
                    found: point.to_string(),
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Invalid polyline format",
                    )),
                    position: None,
                })?;
            }
        }
        Ok(result)
    }

    pub fn add_to_builder_from_record(&mut self, csv_record: &StringRecord) -> Result<()> {
        let parser_position = csv_record.position().cloned();

        let fields = self.schema.fields();
        let mut builder_index: usize = 0;

        const TIMESTAMP_INDEX: usize = 5;
        const POLYLINE_INDEX: usize = 8;

        let timestamp =
            csv_record[TIMESTAMP_INDEX]
                .parse::<i64>()
                .map_err(|e| CsvError::CellParse {
                    expected: "i64".to_string(),
                    found: csv_record[TIMESTAMP_INDEX].to_string(),
                    source: Box::new(e),
                    position: parser_position.clone(),
                })?; // this is required for trajectory timestamps, so we explicitly parse it here

        for (column_index, field) in fields.iter().enumerate() {
            if self.projection.contains(&column_index) {
                match column_index {
                    TIMESTAMP_INDEX => {
                        self.builders[builder_index]
                            .as_any_mut()
                            .downcast_mut::<Int64Builder>()
                            .unwrap()
                            .append_value(timestamp);
                    }
                    POLYLINE_INDEX => {
                        let raw_polyline = TaxiTrajectoryData::parse_raw_polyline(
                            csv_record[column_index].to_string(),
                        )?;
                        let mut start_timestamp = timestamp as f64;
                        let list_builder = self.builders[builder_index]
                            .as_any_mut()
                            .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
                            .unwrap();
                        let struct_builder = list_builder
                            .values()
                            .as_any_mut()
                            .downcast_mut::<StructBuilder>()
                            .unwrap();
                        for point in &raw_polyline {
                            struct_builder
                                .field_builder::<Float64Builder>(0)
                                .unwrap()
                                .append_value(point[0]);
                            struct_builder
                                .field_builder::<Float64Builder>(1)
                                .unwrap()
                                .append_value(point[1]);
                            struct_builder
                                .field_builder::<Float64Builder>(2)
                                .unwrap()
                                .append_value(start_timestamp);
                            start_timestamp += 15.0;
                            struct_builder.append(true);
                        }
                        list_builder.append(true);
                    }
                    _ => {
                        if is_datatype_primitive(field) {
                            add_primitive_field_to_builders(
                                csv_record,
                                &mut self.builders,
                                &builder_index,
                                field,
                                column_index,
                            )?;
                        } else {
                            Err(CsvError::CellParse {
                                expected: format!("{:?}", field.data_type()),
                                found: csv_record[column_index].to_string(),
                                source: "Unsupported non-primitive data type".into(),
                                position: parser_position.clone(),
                            })?
                        }
                    }
                }
                builder_index += 1;
            }
        }

        Ok(())
    }
}

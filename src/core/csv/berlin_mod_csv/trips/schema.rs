use arrow_schema::extension::{ExtensionType, EXTENSION_TYPE_NAME_KEY};
use chrono::NaiveDateTime;
use core::panic;
use csv::StringRecord;
use datafusion::{
    arrow::{
        array::*,
        datatypes::{DataType, Field, Schema, SchemaRef},
    },
    error::DataFusionError,
};
use geoarrow_schema::LineStringType;
use std::{sync::Arc, vec};

use crate::core::utils::schema::TRAJECTORY_DATATYPE;
use crate::utils::error::{CsvError, SaqeError};

pub fn new_get_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("trip_id", DataType::Int64, false),
        Field::new("moid", DataType::Int64, false),
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

// CSV columns: Moid,Tripid,Tstart,Tend,Xstart,Ystart,Xend,Yend
// Xstart/Xend hold the longitude, Ystart/Yend the latitude. Coordinates are stored as
// (x = longitude, y = latitude) to match the geoarrow / API ordering.
const LATITUDE_START_INDEX: usize = 5;
const LATITUDE_END_INDEX: usize = 7;
const LONGITUDE_START_INDEX: usize = 4;
const LONGITUDE_END_INDEX: usize = 6;

#[derive(Debug, Clone)]
pub struct BerlinModTripsData {
    trip_id: i64,
    moid: i64,
    polyline: Vec<(f64, f64, i64)>,
}

impl BerlinModTripsData {
    pub fn new(trip_id: i64, moid: i64, polyline: Vec<(f64, f64, i64)>) -> Self {
        BerlinModTripsData {
            trip_id,
            moid,
            polyline,
        }
    }

    pub fn field_count() -> usize {
        8
    }

    // BerlinMod trips csv has not always a complete timestamp format
    fn parse_timestamp_with_fallback(s: &str) -> Result<i64, DataFusionError> {
        let s = if s.len() == 10 {
            // Only date e.g "2007-05-27"
            format!("{s} 00:00:00")
        } else if s.len() == 13 {
            // Only hour e.g "2007-05-27 08"
            format!("{s}:00:00")
        } else if s.len() == 16 {
            // Hour + minute e.g "2007-05-27 08:36"
            format!("{s}:00")
        } else {
            s.to_string()
        };

        NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.f")
            .map(|dt| dt.and_utc().timestamp_millis())
            .map_err(|e| DataFusionError::Execution(format!("Timestamp parse error: {e}")))
    }

    // Builds a trip record from a vector of string records that consists of CSV rows
    pub fn new_from_csv_rows(buffer: Vec<StringRecord>) -> Result<BerlinModTripsData, SaqeError> {
        let mut polyline: Vec<(f64, f64, i64)> = Vec::new();

        if buffer[0].len() != Self::field_count() {
            return Err(CsvError::Validation {
                message: "mismatched number of fields".to_string(),
                position: buffer[0].position().cloned(),
            }
            .into());
        }

        // moid is the first column, trip_id is the second column in the CSV file!
        let moid: i64 = buffer[0][0].parse().map_err(|e| CsvError::CellParse {
            expected: "i64".to_string(),
            found: format!("invalid moid {}", buffer[0][0].to_string()),
            source: Box::new(e),
            position: buffer[0].position().cloned(),
        })?;
        let trip_id: i64 = buffer[0][1].parse().map_err(|e| CsvError::CellParse {
            expected: "i64".to_string(),
            found: format!("invalid trip_id {}", buffer[0][1].to_string()),
            source: Box::new(e),
            position: buffer[0].position().cloned(),
        })?;
        // Process first csv row:
        let ts_start =
            BerlinModTripsData::parse_timestamp_with_fallback(&buffer[0][2]).map_err(|e| {
                CsvError::CellParse {
                    expected: "i64".to_string(),
                    found: format!("invalid ts_start {}", buffer[0][2].to_string()),
                    source: Box::new(e),
                    position: buffer[0].position().cloned(),
                }
            })?;
        let ts_end =
            BerlinModTripsData::parse_timestamp_with_fallback(&buffer[0][3]).map_err(|e| {
                CsvError::CellParse {
                    expected: "i64".to_string(),
                    found: format!("invalid ts_end {}", buffer[0][3].to_string()),
                    source: Box::new(e),
                    position: buffer[0].position().cloned(),
                }
            })?;
        let x_start = buffer[0][LONGITUDE_START_INDEX]
            .parse::<f64>()
            .map_err(|e| CsvError::CellParse {
                expected: "f64".to_string(),
                found: format!(
                    "invalid x_start {}",
                    buffer[0][LONGITUDE_START_INDEX].to_string()
                ),
                source: Box::new(e),
                position: buffer[0].position().cloned(),
            })?;
        let y_start = buffer[0][LATITUDE_START_INDEX]
            .parse::<f64>()
            .map_err(|e| CsvError::CellParse {
                expected: "f64".to_string(),
                found: format!(
                    "invalid y_start {}",
                    buffer[0][LATITUDE_START_INDEX].to_string()
                ),
                source: Box::new(e),
                position: buffer[0].position().cloned(),
            })?;
        let x_end =
            buffer[0][LONGITUDE_END_INDEX]
                .parse::<f64>()
                .map_err(|e| CsvError::CellParse {
                    expected: "f64".to_string(),
                    found: format!(
                        "invalid x_end {}",
                        buffer[0][LONGITUDE_END_INDEX].to_string()
                    ),
                    source: Box::new(e),
                    position: buffer[0].position().cloned(),
                })?;
        let y_end =
            buffer[0][LATITUDE_END_INDEX]
                .parse::<f64>()
                .map_err(|e| CsvError::CellParse {
                    expected: "f64".to_string(),
                    found: format!(
                        "invalid y_end {}",
                        buffer[0][LATITUDE_END_INDEX].to_string()
                    ),
                    source: Box::new(e),
                    position: buffer[0].position().cloned(),
                })?;
        polyline.push((x_start, y_start, ts_start));
        polyline.push((x_end, y_end, ts_end));

        for record in buffer.iter().skip(1) {
            // First csv row is already processed -> skip it
            if record.len() != Self::field_count() {
                return Err(CsvError::Validation {
                    message: "mismatched number of fields".to_string(),
                    position: record.position().cloned(),
                }
                .into());
            }
            let record_trip_id: i64 = record[1].parse().map_err(|e| CsvError::CellParse {
                expected: "i64".to_string(),
                found: format!("invalid trip_id {}", record[1].to_string()),
                source: Box::new(e),
                position: record.position().cloned(),
            })?;
            if record_trip_id != trip_id {
                // Buffer should only contain csv rows of the same trip
                // This should be never reached!
                panic!("Trip ID mismatch: expected {trip_id}, found {record_trip_id}");
            }
            // push only end points of the trip
            let ts_end =
                BerlinModTripsData::parse_timestamp_with_fallback(&record[3]).map_err(|e| {
                    CsvError::CellParse {
                        expected: "i64".to_string(),
                        found: format!("invalid ts_end {}", record[3].to_string()),
                        source: Box::new(e),
                        position: record.position().cloned(),
                    }
                })?;
            let x_end =
                record[LONGITUDE_END_INDEX]
                    .parse::<f64>()
                    .map_err(|e| CsvError::CellParse {
                        expected: "f64".to_string(),
                        found: format!("invalid x_end {}", record[LONGITUDE_END_INDEX].to_string()),
                        source: Box::new(e),
                        position: record.position().cloned(),
                    })?;
            let y_end =
                record[LATITUDE_END_INDEX]
                    .parse::<f64>()
                    .map_err(|e| CsvError::CellParse {
                        expected: "f64".to_string(),
                        found: format!("invalid y_end {}", record[LATITUDE_END_INDEX].to_string()),
                        source: Box::new(e),
                        position: record.position().cloned(),
                    })?;
            polyline.push((x_end, y_end, ts_end)); // Note the order: (x = lon, y = lat) for geoarrow
        }

        Ok(BerlinModTripsData {
            trip_id,
            moid,
            polyline,
        })
    }

    pub fn add_data_to_builders(
        &mut self,
        builders: &mut Vec<Box<dyn ArrayBuilder>>,
        unwrapped_projection: &Vec<usize>,
    ) {
        let mut field_index: usize = 0;
        let mut builder_index: usize = 0;

        if BerlinModTripsData::should_add_this_index(
            field_index,
            builder_index,
            unwrapped_projection,
        ) {
            builders[builder_index]
                .as_any_mut()
                .downcast_mut::<Int64Builder>()
                .unwrap()
                .append_value(self.trip_id);
            builder_index += 1;
        }
        field_index += 1;

        if BerlinModTripsData::should_add_this_index(
            field_index,
            builder_index,
            unwrapped_projection,
        ) {
            builders[builder_index]
                .as_any_mut()
                .downcast_mut::<Int64Builder>()
                .unwrap()
                .append_value(self.moid);
            builder_index += 1;
        }
        field_index += 1;

        if BerlinModTripsData::should_add_this_index(
            field_index,
            builder_index,
            unwrapped_projection,
        ) {
            let list_builder = builders[builder_index]
                .as_any_mut()
                .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
                .unwrap();
            let struct_builder = list_builder
                .values()
                .as_any_mut()
                .downcast_mut::<StructBuilder>()
                .unwrap();
            for point in &self.polyline {
                let (x, y, timestamp) = point;
                // struct_builder.append(true);
                let x_builder = struct_builder.field_builder::<Float64Builder>(0).unwrap();
                x_builder.append_value(*x);
                let y_builder = struct_builder.field_builder::<Float64Builder>(1).unwrap();
                y_builder.append_value(*y);
                let timestamp_builder = struct_builder.field_builder::<Float64Builder>(2).unwrap();
                timestamp_builder.append_value(*timestamp as f64);
                struct_builder.append(true);
            }
            list_builder.append(true);
        }
    }

    pub fn should_add_this_index(
        field_index: usize,
        builder_index: usize,
        projection: &Vec<usize>,
    ) -> bool {
        if builder_index >= projection.len() {
            return false;
        }
        if projection[builder_index] != field_index {
            return false;
        }
        if projection[builder_index] == field_index {
            return true;
        }
        return false;
    }
}

impl Default for BerlinModTripsData {
    fn default() -> Self {
        BerlinModTripsData {
            trip_id: 0,
            moid: 0,
            polyline: Vec::new(),
        }
    }
}

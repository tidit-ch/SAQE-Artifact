use super::utils::{parse_optional, parse_value, ColumnKeys};
use crate::utils::error::CsvError;
use chrono::NaiveDateTime;
use csv::StringRecord;
use datafusion::arrow::array::*;

#[derive(Debug, Clone)]
pub struct OystercatcherRecordEvent {
    event_id: i64,
    location_long: Option<f64>,
    location_lat: Option<f64>,
    timestamp: Option<i64>,
    visible: bool,
    acceleration_raw_x: Option<f64>,
    acceleration_raw_y: Option<f64>,
    acceleration_raw_z: Option<f64>,
    bar_barometric_pressure: Option<f64>,
    external_temperature: Option<f64>,
    gps_dop: Option<f64>,
    gps_satellite_count: Option<i64>,
    gps_time_to_fix: Option<f64>,
    ground_speed: Option<f64>,
    heading: Option<f64>,
    height_above_msl: Option<f64>,
    location_error_numerical: Option<f64>,
    manually_marked_outlier: Option<bool>,
    tilt_x: Option<f64>,
    tilt_y: Option<f64>,
    tilt_z: Option<f64>,
    vertical_error_numerical: Option<f64>,
    sensor_type: Option<String>,
}

impl Default for OystercatcherRecordEvent {
    fn default() -> Self {
        OystercatcherRecordEvent {
            event_id: 0,
            location_long: None,
            location_lat: None,
            timestamp: None,
            visible: false,
            acceleration_raw_x: None,
            acceleration_raw_y: None,
            acceleration_raw_z: None,
            bar_barometric_pressure: None,
            external_temperature: None,
            gps_dop: None,
            gps_satellite_count: None,
            gps_time_to_fix: None,
            ground_speed: None,
            heading: None,
            height_above_msl: None,
            location_error_numerical: None,
            manually_marked_outlier: None,
            tilt_x: None,
            tilt_y: None,
            tilt_z: None,
            vertical_error_numerical: None,
            sensor_type: None,
        }
    }
}

type Result<T, E = CsvError> = std::result::Result<T, E>;

impl OystercatcherRecordEvent {
    pub fn from_csv(record: StringRecord) -> Result<OystercatcherRecordEvent> {
        let mut event = OystercatcherRecordEvent::default();

        let fields: Vec<&str> = record.iter().map(|s| s.trim()).collect();
        let parser_position = record.position();
        let parser_position = if parser_position.is_some() {
            Some(parser_position.unwrap().clone())
        } else {
            None
        };

        parse_value(fields[ColumnKeys::EventId as usize], &mut event.event_id)?;
        parse_value(fields[ColumnKeys::Visible as usize], &mut event.visible)?;
        parse_optional(
            fields[ColumnKeys::LocationLong as usize],
            &mut event.location_long,
        )?;
        parse_optional(
            fields[ColumnKeys::LocationLat as usize],
            &mut event.location_lat,
        )?;
        // start-timestamp is in the format "2018-05-25 16:11:37.000"
        let raw_start_timestamp = fields[ColumnKeys::Timestamp as usize];
        let start_timestamp = if !raw_start_timestamp.is_empty() {
            let datetime =
                NaiveDateTime::parse_from_str(&raw_start_timestamp, "%Y-%m-%d %H:%M:%S.%f")
                    .map_err(|e| CsvError::CellParse {
                        expected: "YYYY-MM-DD HH:MM:SS.sss".to_string(),
                        found: raw_start_timestamp.to_string(),
                        source: Box::new(e),
                        position: parser_position.clone(),
                    })?;
            // Convert to seconds since Unix epoch (1970-01-01 00:00:00 UTC)
            Some(datetime.and_utc().timestamp())
        } else {
            None
        };
        event.timestamp = start_timestamp;
        parse_optional(
            fields[ColumnKeys::AccelerationRawX as usize],
            &mut event.acceleration_raw_x,
        )?;
        parse_optional(
            fields[ColumnKeys::AccelerationRawY as usize],
            &mut event.acceleration_raw_y,
        )?;
        parse_optional(
            fields[ColumnKeys::AccelerationRawZ as usize],
            &mut event.acceleration_raw_z,
        )?;
        parse_optional(
            fields[ColumnKeys::BarBarometricPressure as usize],
            &mut event.bar_barometric_pressure,
        )?;
        parse_optional(
            fields[ColumnKeys::ExternalTemperature as usize],
            &mut event.external_temperature,
        )?;
        parse_optional(fields[ColumnKeys::GpsDop as usize], &mut event.gps_dop)?;
        parse_optional(
            fields[ColumnKeys::GpsSatelliteCount as usize],
            &mut event.gps_satellite_count,
        )?;
        parse_optional(
            fields[ColumnKeys::GpsTimeToFix as usize],
            &mut event.gps_time_to_fix,
        )?;
        parse_optional(
            fields[ColumnKeys::GroundSpeed as usize],
            &mut event.ground_speed,
        )?;
        parse_optional(fields[ColumnKeys::Heading as usize], &mut event.heading)?;
        parse_optional(
            fields[ColumnKeys::HeightAboveMsl as usize],
            &mut event.height_above_msl,
        )?;
        parse_optional(
            fields[ColumnKeys::LocationErrorNumerical as usize],
            &mut event.location_error_numerical,
        )?;
        parse_optional(
            fields[ColumnKeys::ManuallyMarkedOutlier as usize],
            &mut event.manually_marked_outlier,
        )?;
        parse_optional(fields[ColumnKeys::TiltX as usize], &mut event.tilt_x)?;
        parse_optional(fields[ColumnKeys::TiltY as usize], &mut event.tilt_y)?;
        parse_optional(fields[ColumnKeys::TiltZ as usize], &mut event.tilt_z)?;
        parse_optional(
            fields[ColumnKeys::VerticalErrorNumerical as usize],
            &mut event.vertical_error_numerical,
        )?;
        parse_optional(
            fields[ColumnKeys::SensorType as usize],
            &mut event.sensor_type,
        )?;

        Ok(event)
    }

    /// Returns the trajectory vertex as `(x = longitude, y = latitude, m = timestamp)`.
    pub fn get_polyline_data(&self) -> Option<(f64, f64, i64)> {
        if self.location_lat.is_some() && self.location_long.is_some() && self.timestamp.is_some() {
            Some((
                self.location_long.unwrap(),
                self.location_lat.unwrap(),
                self.timestamp.unwrap(),
            ))
        } else {
            None
        }
    }

    pub fn add_data_to_builders(&self, struct_builder: &mut StructBuilder) {
        let column_keys_to_add: Vec<ColumnKeys> = Vec::from([
            ColumnKeys::EventId,
            ColumnKeys::LocationLong,
            ColumnKeys::LocationLat,
            ColumnKeys::Timestamp,
            ColumnKeys::Visible,
            ColumnKeys::AccelerationRawX,
            ColumnKeys::AccelerationRawY,
            ColumnKeys::AccelerationRawZ,
            ColumnKeys::BarBarometricPressure,
            ColumnKeys::ExternalTemperature,
            ColumnKeys::GpsDop,
            ColumnKeys::GpsSatelliteCount,
            ColumnKeys::GpsTimeToFix,
            ColumnKeys::GroundSpeed,
            ColumnKeys::Heading,
            ColumnKeys::HeightAboveMsl,
            ColumnKeys::LocationErrorNumerical,
            ColumnKeys::ManuallyMarkedOutlier,
            ColumnKeys::TiltX,
            ColumnKeys::TiltY,
            ColumnKeys::TiltZ,
            ColumnKeys::VerticalErrorNumerical,
            ColumnKeys::SensorType,
        ]);
        for i in 0..column_keys_to_add.len() {
            match column_keys_to_add[i] {
                ColumnKeys::EventId => {
                    let typed_builder = struct_builder.field_builder::<Int64Builder>(i).unwrap();
                    typed_builder.append_value(self.event_id);
                }
                ColumnKeys::LocationLong => {
                    let typed_builder = struct_builder.field_builder::<Float64Builder>(i).unwrap();
                    if let Some(value) = self.location_long {
                        typed_builder.append_value(value);
                    } else {
                        typed_builder.append_null();
                    }
                }
                ColumnKeys::LocationLat => {
                    let typed_builder = struct_builder.field_builder::<Float64Builder>(i).unwrap();
                    if let Some(value) = self.location_lat {
                        typed_builder.append_value(value);
                    } else {
                        typed_builder.append_null();
                    }
                }
                ColumnKeys::Timestamp => {
                    let typed_builder = struct_builder.field_builder::<Int64Builder>(i).unwrap();
                    if let Some(value) = self.timestamp {
                        typed_builder.append_value(value);
                    } else {
                        typed_builder.append_null();
                    }
                }
                ColumnKeys::Visible => {
                    let typed_builder = struct_builder.field_builder::<BooleanBuilder>(i).unwrap();
                    typed_builder.append_value(self.visible);
                }
                ColumnKeys::AccelerationRawX => {
                    let typed_builder = struct_builder.field_builder::<Float64Builder>(i).unwrap();
                    if let Some(value) = self.acceleration_raw_x {
                        typed_builder.append_value(value);
                    } else {
                        typed_builder.append_null();
                    }
                }
                ColumnKeys::AccelerationRawY => {
                    let typed_builder = struct_builder.field_builder::<Float64Builder>(i).unwrap();
                    if let Some(value) = self.acceleration_raw_y {
                        typed_builder.append_value(value);
                    } else {
                        typed_builder.append_null();
                    }
                }
                ColumnKeys::AccelerationRawZ => {
                    let typed_builder = struct_builder.field_builder::<Float64Builder>(i).unwrap();
                    if let Some(value) = self.acceleration_raw_z {
                        typed_builder.append_value(value);
                    } else {
                        typed_builder.append_null();
                    }
                }
                ColumnKeys::BarBarometricPressure => {
                    let typed_builder = struct_builder.field_builder::<Float64Builder>(i).unwrap();
                    if let Some(value) = self.bar_barometric_pressure {
                        typed_builder.append_value(value);
                    } else {
                        typed_builder.append_null();
                    }
                }
                ColumnKeys::ExternalTemperature => {
                    let typed_builder = struct_builder.field_builder::<Float64Builder>(i).unwrap();
                    if let Some(value) = self.external_temperature {
                        typed_builder.append_value(value);
                    } else {
                        typed_builder.append_null();
                    }
                }
                ColumnKeys::GpsDop => {
                    let typed_builder = struct_builder.field_builder::<Float64Builder>(i).unwrap();
                    if let Some(value) = self.gps_dop {
                        typed_builder.append_value(value);
                    } else {
                        typed_builder.append_null();
                    }
                }
                ColumnKeys::GpsSatelliteCount => {
                    let typed_builder = struct_builder.field_builder::<Int64Builder>(i).unwrap();
                    if let Some(value) = self.gps_satellite_count {
                        typed_builder.append_value(value);
                    } else {
                        typed_builder.append_null();
                    }
                }
                ColumnKeys::GpsTimeToFix => {
                    let typed_builder = struct_builder.field_builder::<Float64Builder>(i).unwrap();
                    if let Some(value) = self.gps_time_to_fix {
                        typed_builder.append_value(value);
                    } else {
                        typed_builder.append_null();
                    }
                }
                ColumnKeys::GroundSpeed => {
                    let typed_builder = struct_builder.field_builder::<Float64Builder>(i).unwrap();
                    if let Some(value) = self.ground_speed {
                        typed_builder.append_value(value);
                    } else {
                        typed_builder.append_null();
                    }
                }
                ColumnKeys::Heading => {
                    let typed_builder = struct_builder.field_builder::<Float64Builder>(i).unwrap();
                    if let Some(value) = self.heading {
                        typed_builder.append_value(value);
                    } else {
                        typed_builder.append_null();
                    }
                }
                ColumnKeys::HeightAboveMsl => {
                    let typed_builder = struct_builder.field_builder::<Float64Builder>(i).unwrap();
                    if let Some(value) = self.height_above_msl {
                        typed_builder.append_value(value);
                    } else {
                        typed_builder.append_null();
                    }
                }
                ColumnKeys::LocationErrorNumerical => {
                    let typed_builder = struct_builder.field_builder::<Float64Builder>(i).unwrap();
                    if let Some(value) = self.location_error_numerical {
                        typed_builder.append_value(value);
                    } else {
                        typed_builder.append_null();
                    }
                }
                ColumnKeys::ManuallyMarkedOutlier => {
                    let typed_builder = struct_builder.field_builder::<BooleanBuilder>(i).unwrap();
                    if let Some(value) = self.manually_marked_outlier {
                        typed_builder.append_value(value);
                    } else {
                        typed_builder.append_null();
                    }
                }
                ColumnKeys::TiltX => {
                    let typed_builder = struct_builder.field_builder::<Float64Builder>(i).unwrap();
                    if let Some(value) = self.tilt_x {
                        typed_builder.append_value(value);
                    } else {
                        typed_builder.append_null();
                    }
                }
                ColumnKeys::TiltY => {
                    let typed_builder = struct_builder.field_builder::<Float64Builder>(i).unwrap();
                    if let Some(value) = self.tilt_y {
                        typed_builder.append_value(value);
                    } else {
                        typed_builder.append_null();
                    }
                }
                ColumnKeys::TiltZ => {
                    let typed_builder = struct_builder.field_builder::<Float64Builder>(i).unwrap();
                    if let Some(value) = self.tilt_z {
                        typed_builder.append_value(value);
                    } else {
                        typed_builder.append_null();
                    }
                }
                ColumnKeys::VerticalErrorNumerical => {
                    let typed_builder = struct_builder.field_builder::<Float64Builder>(i).unwrap();
                    if let Some(value) = self.vertical_error_numerical {
                        typed_builder.append_value(value);
                    } else {
                        typed_builder.append_null();
                    }
                }
                ColumnKeys::SensorType => {
                    let typed_builder = struct_builder.field_builder::<StringBuilder>(i).unwrap();
                    if let Some(value) = &self.sensor_type {
                        typed_builder.append_value(value);
                    } else {
                        typed_builder.append_null();
                    }
                }
                _ => {}
            }
        }
    }
}

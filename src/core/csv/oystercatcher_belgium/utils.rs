use std::any::type_name;
use std::error::Error as StdError;
use std::fmt::Display;
use std::str::FromStr;
use strum_macros::EnumIter;

use crate::utils::error::CsvError;

#[derive(Debug, EnumIter)]
pub enum ColumnKeys {
    EventId = 0,
    Visible = 1,
    Timestamp = 2,
    LocationLong = 3,
    LocationLat = 4,
    AccelerationRawX = 5,
    AccelerationRawY = 6,
    AccelerationRawZ = 7,
    BarBarometricPressure = 8,
    ExternalTemperature = 9,
    GpsDop = 10,
    GpsSatelliteCount = 11,
    GpsTimeToFix = 12,
    GroundSpeed = 13,
    Heading = 14,
    HeightAboveMsl = 15,
    LocationErrorNumerical = 16,
    ManuallyMarkedOutlier = 17,
    StartTimestamp = 18,
    TiltX = 19,
    TiltY = 20,
    TiltZ = 21,
    VerticalErrorNumerical = 22,
    SensorType = 23,
    IndividualTaxonCanonicalName = 24,
    TagLocalIdentifier = 25,
    IndividualLocalIdentifier = 26,
    StudyName = 27,
}

impl From<ColumnKeys> for usize {
    fn from(key: ColumnKeys) -> Self {
        key as usize
    }
}

type Result<T, E = CsvError> = std::result::Result<T, E>;

pub fn parse_value<T>(str_value: &str, save_result: &mut T) -> Result<()>
where
    T: FromStr,
    <T as FromStr>::Err: Display + StdError + Send + Sync + 'static,
{
    if str_value.is_empty() {
        Err(CsvError::CellParse {
            expected: type_name::<T>().to_string(),
            found: str_value.to_string(),
            source: String::from("value is null").into(),
            position: None,
        })?
    }
    let parsed_value = str_value.parse::<T>().map_err(|e| CsvError::CellParse {
        expected: type_name::<T>().to_string(),
        found: str_value.to_string(),
        source: Box::new(e),
        position: None,
    })?;
    *save_result = parsed_value;
    Ok(())
}

// Special version for Option<T>
pub fn parse_optional<'a, T>(str_value: &'a str, save_result: &mut Option<T>) -> Result<()>
where
    T: FromStr,
    <T as FromStr>::Err: Display + StdError + Send + Sync + 'static,
{
    if str_value.is_empty() {
        *save_result = None;
    } else {
        let parsed_value = str_value.parse::<T>().map_err(|e| CsvError::CellParse {
            expected: type_name::<T>().to_string(),
            found: str_value.to_string(),
            source: Box::new(e),
            position: None,
        })?;
        *save_result = Some(parsed_value);
    }
    Ok(())
}

use crate::utils::error::{SaqeError, SaqeResult};
use wkt::types::{
    Coord as WktCoord, Dimension as WktDimension, LineString as WktLineString,
    MultiLineString as WktMultiLineString,
};

use std::sync::LazyLock;

use super::CropFunction;
use crate::core::parser::ast::CropFunctionCall;
use crate::core::udf::doc::{UdfArgDetail, UdfDetail, UdfDoc};

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct DuringFn;

impl DuringFn {
    pub fn parse_args(&self, call: &CropFunctionCall) -> SaqeResult<(f64, f64)> {
        if call.arguments.len() != 2 {
            return Err(SaqeError::StringError(format!(
                "Function '{}' expects exactly 2 argument, but got {}",
                self.name(),
                call.arguments.len()
            )));
        }
        let start_timestamp = call.arguments[0].parse::<f64>().map_err(|_| {
            SaqeError::StringError(format!(
                "Invalid argument for '{}': expected a timestamp (float), but got '{}'",
                self.name(),
                call.arguments[0]
            ))
        })?;
        let end_timestamp = call.arguments[1].parse::<f64>().map_err(|_| {
            SaqeError::StringError(format!(
                "Invalid argument for '{}': expected a timestamp (float), but got '{}'",
                self.name(),
                call.arguments[1]
            ))
        })?;
        if start_timestamp > end_timestamp {
            return Err(SaqeError::StringError(format!(
                "Invalid arguments for '{}': start timestamp {} is greater than end timestamp {}",
                self.name(),
                start_timestamp,
                end_timestamp
            )));
        }
        Ok((start_timestamp, end_timestamp))
    }

    pub fn crop(
        &self,
        linestring: &WktLineString<f64>,
        start_timestamp: f64,
        end_timestamp: f64,
    ) -> WktLineString<f64> {
        if start_timestamp > end_timestamp {
            return WktLineString::new(Vec::new(), WktDimension::XYM);
        }

        let coords = linestring.coords();
        let Some(start_index) = self.get_start_index(coords, start_timestamp) else {
            return WktLineString::new(Vec::new(), WktDimension::XYM);
        };

        let mut coord_list: Vec<WktCoord<f64>> = Vec::new();

        for coord in &coords[start_index..] {
            let m_value = coord.m.unwrap_or_default();
            if m_value > end_timestamp {
                break;
            }
            if m_value >= start_timestamp {
                coord_list.push(coord.clone());
            }
        }
        WktLineString::new(coord_list, WktDimension::XYM)
    }

    // Returns the index of the first coordinate where m >= timestamp.
    pub fn get_start_index(&self, coords: &[WktCoord<f64>], timestamp: f64) -> Option<usize> {
        let mut left = 0;
        let mut right = coords.len();

        while left < right {
            let mid = left + (right - left) / 2;
            let m_value = coords[mid].m.unwrap_or_default();
            if m_value < timestamp {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        if left < coords.len() {
            Some(left)
        } else {
            None
        }
    }
}

impl UdfDoc for DuringFn {
    fn details(&self) -> &'static UdfDetail {
        static DETAIL: LazyLock<UdfDetail> = LazyLock::new(|| {
            UdfDetail {
            name: "during".into(),
            category: "crop".into(),
            description: "Returns the segments of the input trajectory that occur during the specified time range.".into(),
            syntax_example: "crop(column, during(start_timestamp: Float64, end_timestamp: Float64)) -> List<List<Struct{x: Float64, y: Float64, m: Float64}>>".into(),
            sql_example: Some("SELECT crop(trajectory, during(20.0, 30.0)) AS during_segments FROM trajectories;".to_string()),
            arguments: vec![
                UdfArgDetail {
                    name: "start_timestamp".into(),
                    description: "The start of the time range (inclusive).".into(),
                    data_type: "float".into(),
                },
                UdfArgDetail {
                    name: "end_timestamp".into(),
                    description: "The end of the time range (inclusive).".into(),
                    data_type: "float".into(),
                },
            ],
        }
        });
        &DETAIL
    }
}

impl CropFunction for DuringFn {
    fn name(&self) -> &'static str {
        "during"
    }

    fn eval(
        &self,
        input: &[WktMultiLineString<f64>],
        call: &CropFunctionCall,
    ) -> SaqeResult<Vec<WktMultiLineString<f64>>> {
        let (start_timestamp, end_timestamp) = self.parse_args(call)?;

        let mut result = Vec::new();
        for mls in input {
            let mut cropped_linestrings: Vec<WktLineString<f64>> = Vec::new();
            for ls in mls.line_strings() {
                let cropped_ls = self.crop(ls, start_timestamp, end_timestamp);
                if !cropped_ls.coords().is_empty() {
                    cropped_linestrings.push(cropped_ls);
                }
            }
            result.push(WktMultiLineString::new(
                cropped_linestrings,
                WktDimension::XYM,
            ));
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_linestring() -> WktLineString<f64> {
        WktLineString::new(
            vec![
                WktCoord {
                    x: 0.0,
                    y: 0.0,
                    z: None,
                    m: Some(10.0),
                },
                WktCoord {
                    x: 1.0,
                    y: 1.0,
                    z: None,
                    m: Some(20.0),
                },
                WktCoord {
                    x: 2.0,
                    y: 2.0,
                    z: None,
                    m: Some(30.0),
                },
                WktCoord {
                    x: 3.0,
                    y: 3.0,
                    z: None,
                    m: Some(40.0),
                },
            ],
            WktDimension::XYM,
        )
    }

    fn m_values(linestring: &WktLineString<f64>) -> Vec<f64> {
        linestring
            .coords()
            .iter()
            .map(|coord| coord.m.unwrap_or_default())
            .collect()
    }

    #[test]
    fn test_during_crop() {
        let during_fn = DuringFn;
        let linestring = sample_linestring();

        struct Case {
            name: &'static str,
            start_timestamp: f64,
            end_timestamp: f64,
            expected_m_values: Vec<f64>,
        }

        let cases = vec![
            Case {
                name: "range before first m",
                start_timestamp: 0.0,
                end_timestamp: 5.0,
                expected_m_values: vec![],
            },
            Case {
                name: "range after last m",
                start_timestamp: 41.0,
                end_timestamp: 50.0,
                expected_m_values: vec![],
            },
            Case {
                name: "range overlaps exact m values",
                start_timestamp: 20.0,
                end_timestamp: 30.0,
                expected_m_values: vec![20.0, 30.0],
            },
            Case {
                name: "range between m values",
                start_timestamp: 15.0,
                end_timestamp: 35.0,
                expected_m_values: vec![20.0, 30.0],
            },
        ];

        for case in cases {
            let cropped = during_fn.crop(&linestring, case.start_timestamp, case.end_timestamp);
            assert_eq!(
                m_values(&cropped),
                case.expected_m_values,
                "failed case: {}",
                case.name
            );
        }
    }
}

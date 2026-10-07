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
pub struct FollowsFn;

impl FollowsFn {
    pub fn parse_args(&self, call: &CropFunctionCall) -> SaqeResult<f64> {
        if call.arguments.len() != 1 {
            return Err(SaqeError::StringError(format!(
                "Function '{}' expects exactly 1 argument, but got {}",
                self.name(),
                call.arguments.len()
            )));
        }
        let timestamp = call.arguments[0].parse::<f64>().map_err(|_| {
            SaqeError::StringError(format!(
                "Invalid argument for '{}': expected a timestamp (float), but got '{}'",
                self.name(),
                call.arguments[0]
            ))
        })?;

        Ok(timestamp)
    }

    pub fn crop(&self, linestring: &WktLineString<f64>, timestamp: f64) -> WktLineString<f64> {
        let coords = linestring.coords();
        let crop_index = self.get_crop_index(coords, timestamp);
        if let Some(index) = crop_index {
            let cropped_coords = &coords[index..];
            WktLineString::new(cropped_coords.to_vec(), WktDimension::XYM)
        } else {
            WktLineString::new(Vec::new(), WktDimension::XYM)
        }
    }

    // [[x, y, m], ...]
    // The tuple pairs are ordered by their m values, which represent timestamps. Binary search is used to find the index of the first tuple where m > timestamp.
    pub fn get_crop_index(&self, coords: &[WktCoord<f64>], timestamp: f64) -> Option<usize> {
        let mut left = 0;
        let mut right = coords.len();

        while left < right {
            let mid = left + (right - left) / 2;
            let m_value = coords[mid].m.unwrap_or_default();
            if m_value <= timestamp {
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

impl UdfDoc for FollowsFn {
    fn details(&self) -> &'static UdfDetail {
        static DETAIL: LazyLock<UdfDetail> = LazyLock::new(|| {
            UdfDetail {
            name: "follows".into(),
            category: "crop".into(),
            description: "Returns the segments of the input trajectory that follow the specified timestamp.".into(),
            syntax_example: "crop(column, follows(timestamp: Float64)) -> List<List<Struct{x: Float64, y: Float64, m: Float64}>>".into(),
            sql_example: Some("SELECT crop(trajectory, follows(20.0)) AS following_segments FROM trajectories;".to_string()),
            arguments: vec![UdfArgDetail {
                name: "timestamp".into(),
                description: "A timestamp (float) to compare against the m values of the trajectory coordinates.".into(),
                data_type: "float".into(),
            }],
        }
        });
        &DETAIL
    }
}

impl CropFunction for FollowsFn {
    fn name(&self) -> &'static str {
        "follows"
    }

    fn eval(
        &self,
        input: &[WktMultiLineString<f64>],
        call: &CropFunctionCall,
    ) -> SaqeResult<Vec<WktMultiLineString<f64>>> {
        let timestamp = self.parse_args(call)?;

        let mut result = Vec::new();
        for mls in input {
            let mut cropped_linestrings: Vec<WktLineString<f64>> = Vec::new();
            for ls in mls.line_strings() {
                let cropped_ls = self.crop(ls, timestamp);
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
    fn test_follows_crop() {
        let follows_fn = FollowsFn;
        let linestring = sample_linestring();

        struct Case {
            name: &'static str,
            timestamp: f64,
            expected_m_values: Vec<f64>,
        }

        let cases = vec![
            Case {
                name: "timestamp > end.m",
                timestamp: 41.0,
                expected_m_values: vec![],
            },
            Case {
                name: "timestamp < start.m",
                timestamp: 9.0,
                expected_m_values: vec![10.0, 20.0, 30.0, 40.0],
            },
            Case {
                name: "timestamp overlaps existing m",
                timestamp: 20.0,
                expected_m_values: vec![30.0, 40.0],
            },
            Case {
                name: "timestamp between two m values",
                timestamp: 25.0,
                expected_m_values: vec![30.0, 40.0],
            },
        ];

        for case in cases {
            let cropped = follows_fn.crop(&linestring, case.timestamp);
            assert_eq!(
                m_values(&cropped),
                case.expected_m_values,
                "failed case: {}",
                case.name
            );
        }
    }
}

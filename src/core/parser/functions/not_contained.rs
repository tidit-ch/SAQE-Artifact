use crate::utils::error::{SaqeError, SaqeResult};
use geo::{contains::Contains, geometry::Polygon, BoundingRect, Point as GeoPoint};
use wkt::{
    types::{
        Coord as WktCoord, Dimension as WktDimension, LineString as WktLineString,
        MultiLineString as WktMultiLineString,
    },
    Wkt,
};

use std::sync::LazyLock;

use super::CropFunction;
use crate::core::parser::ast::CropFunctionCall;
use crate::core::udf::doc::{UdfArgDetail, UdfDetail, UdfDoc};

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct NotContainedFn;

impl NotContainedFn {
    pub fn crop_coords(
        &self,
        coords: &[WktCoord<f64>],
        polygon: &Polygon<f64>,
    ) -> Vec<WktLineString<f64>> {
        let Some(bounding_box) = polygon.bounding_rect() else {
            return Vec::new();
        };

        let mut result: Vec<WktLineString<f64>> = Vec::new();
        let mut ls_in_progress = false;
        let mut coord_list: Vec<WktCoord<f64>> = Vec::new();

        for coord in coords {
            let point = GeoPoint::new(coord.x, coord.y);
            let is_contained = bounding_box.contains(&point) && polygon.contains(&point);

            if !is_contained {
                ls_in_progress = true;
                coord_list.push(coord.clone());
            } else if ls_in_progress {
                ls_in_progress = false;
                result.push(WktLineString::new(coord_list, WktDimension::XYM));
                coord_list = Vec::new();
            }
        }

        if ls_in_progress {
            result.push(WktLineString::new(coord_list, WktDimension::XYM));
        }

        result
    }
}

impl UdfDoc for NotContainedFn {
    fn details(&self) -> &'static UdfDetail {
        static DETAIL: LazyLock<UdfDetail> = LazyLock::new(|| {
            UdfDetail {
            name: "not_contained".into(),
            category: "crop".into(),
            description: "Returns the segments of the input trajectory that are not contained within the specified polygon.".into(),
            syntax_example: "crop(column, not_contained(polygon_wkt: Utf8)) -> List<List<Struct{x: Float64, y: Float64, m: Float64}>>".into(),
            sql_example: Some("SELECT crop(trajectory, not_contained('POLYGON((0 0, 3 0, 3 3, 0 3, 0 0))')) AS not_contained_segments FROM trajectories;".to_string()),
            arguments: vec![UdfArgDetail {
                name: "polygon_wkt".into(),
                description: "The polygon within which to check for non-containment, specified in WKT format.".into(),
                data_type: "string".into(),
            }],
        }
        });
        &DETAIL
    }
}

impl CropFunction for NotContainedFn {
    fn name(&self) -> &'static str {
        "not_contained"
    }

    fn eval(
        &self,
        input: &[WktMultiLineString<f64>],
        call: &CropFunctionCall,
    ) -> SaqeResult<Vec<WktMultiLineString<f64>>> {
        let polygon_arg = call.arguments.get(0).ok_or_else(|| {
            SaqeError::StringError("not_contained() requires a polygon argument".into())
        })?;
        let wkt: Wkt<f64> = polygon_arg.as_str().parse()?;
        let geo_polygon: Polygon<f64> = wkt.try_into().map_err(|_| {
            SaqeError::StringError(format!(
                "Failed to parse WKT polygon: {}",
                polygon_arg.as_str()
            ))
        })?;

        let mut result = Vec::new();
        for mls in input {
            let mut current_ls: Vec<WktLineString<f64>> = vec![];
            for ls in mls.line_strings() {
                let coords: Vec<WktCoord<f64>> = ls
                    .coords()
                    .iter()
                    .map(|coord| WktCoord {
                        x: coord.x,
                        y: coord.y,
                        z: None,
                        m: coord.m,
                    })
                    .collect();
                let cropped_mls = self.crop_coords(&coords, &geo_polygon);
                current_ls.extend(cropped_mls);
            }
            result.push(WktMultiLineString::new(current_ls, WktDimension::XYM));
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn coord(x: f64, y: f64, m: f64) -> WktCoord<f64> {
        WktCoord {
            x,
            y,
            z: None,
            m: Some(m),
        }
    }

    fn sample_polygon() -> Polygon<f64> {
        let wkt: Wkt<f64> = "POLYGON((0 0, 3 0, 3 3, 0 3, 0 0))".parse().unwrap();
        wkt.try_into().unwrap()
    }

    fn segment_m_values(linestrings: &[WktLineString<f64>]) -> Vec<Vec<f64>> {
        linestrings
            .iter()
            .map(|linestring| {
                linestring
                    .coords()
                    .iter()
                    .map(|c| c.m.unwrap_or_default())
                    .collect()
            })
            .collect()
    }

    #[test]
    fn test_not_contained_crop() {
        let not_contained_fn = NotContainedFn;
        let polygon = sample_polygon();

        struct Case {
            name: &'static str,
            coords: Vec<WktCoord<f64>>,
            expected_segments_m: Vec<Vec<f64>>,
        }

        let cases = vec![
            Case {
                name: "all points are contained",
                coords: vec![
                    coord(1.0, 1.0, 10.0),
                    coord(1.5, 1.5, 20.0),
                    coord(2.0, 2.0, 30.0),
                ],
                expected_segments_m: vec![],
            },
            Case {
                name: "all points are outside in one segment",
                coords: vec![
                    coord(-1.0, -1.0, 10.0),
                    coord(-2.0, -2.0, 20.0),
                    coord(4.0, 4.0, 30.0),
                ],
                expected_segments_m: vec![vec![10.0, 20.0, 30.0]],
            },
            Case {
                name: "outside segments at start and end",
                coords: vec![
                    coord(-1.0, -1.0, 10.0),
                    coord(1.0, 1.0, 20.0),
                    coord(2.0, 2.0, 30.0),
                    coord(4.0, 4.0, 40.0),
                ],
                expected_segments_m: vec![vec![10.0], vec![40.0]],
            },
            Case {
                name: "two outside segments separated by contained points",
                coords: vec![
                    coord(1.0, 1.0, 10.0),
                    coord(4.0, 4.0, 20.0),
                    coord(2.0, 2.0, 30.0),
                    coord(-1.0, 1.0, 40.0),
                    coord(-2.0, 1.0, 50.0),
                ],
                expected_segments_m: vec![vec![20.0], vec![40.0, 50.0]],
            },
        ];

        for case in cases {
            let cropped = not_contained_fn.crop_coords(&case.coords, &polygon);
            assert_eq!(
                segment_m_values(&cropped),
                case.expected_segments_m,
                "failed case: {}",
                case.name
            );
        }
    }
}

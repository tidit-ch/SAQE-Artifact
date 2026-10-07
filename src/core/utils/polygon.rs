//! This module contains functions to check intersections of points or lines with polygons.

use arrow::array::PrimitiveArray;
use arrow::datatypes::{Float64Type, TimestampSecondType};
use geo::algorithm::contains::Contains;
use geo::geometry::Polygon;
use geo::{Coord, Intersects, Line, Point};

pub fn check_intersection_strict(
    x_array: &PrimitiveArray<Float64Type>,
    y_array: &PrimitiveArray<Float64Type>,
    polygon: &Polygon,
) -> bool {
    let mut contained = false;

    for i in 0..x_array.len() {
        let point = Point::new(x_array.value(i), y_array.value(i));
        if polygon.contains(&point) || polygon.intersects(&point) {
            contained = true;
            break;
        }
    }
    contained
}

pub fn check_intersection_relaxed(
    x_array: &PrimitiveArray<Float64Type>,
    y_array: &PrimitiveArray<Float64Type>,
    polygon: &Polygon,
) -> bool {
    if x_array.len() == 0 {
        return false;
    }

    if x_array.len() == 1 {
        let point = Point::new(x_array.value(0), y_array.value(0));
        if polygon.contains(&point) || polygon.intersects(&point) {
            return true;
        } else {
            return false;
        }
    }

    let mut contained = false;

    let mut j = 0;
    while j < x_array.len() - 1 {
        let line = Line::new(
            Coord {
                x: x_array.value(j),
                y: y_array.value(j),
            },
            Coord {
                x: x_array.value(j + 1),
                y: y_array.value(j + 1),
            },
        );
        if polygon.intersects(&line) {
            contained = true;
            break;
        }
        j += 1;
    }

    contained
}

pub fn check_polyline_properly_contained(
    x_array: &PrimitiveArray<Float64Type>,
    y_array: &PrimitiveArray<Float64Type>,
    polygon: &Polygon,
) -> bool {
    let mut contained = false;

    for i in 0..x_array.len() {
        let point = Point::new(x_array.value(i), y_array.value(i));
        if !polygon.contains(&point) {
            contained = false;
            break;
        } else {
            contained = true;
        }
    }
    contained
}

pub fn check_spatial_temporal_intersection(
    x_array: &PrimitiveArray<Float64Type>,
    y_array: &PrimitiveArray<Float64Type>,
    polygon: &Polygon,
    timestamp_array: &PrimitiveArray<TimestampSecondType>,
    start_timestamp: &i64,
    end_timestamp: &i64,
) -> bool {
    let mut contained = false;

    for i in 0..x_array.len() {
        let point = Point::new(x_array.value(i), y_array.value(i));
        if (polygon.contains(&point) || polygon.intersects(&point))
            && timestamp_array.value(i) >= *start_timestamp
            && timestamp_array.value(i) <= *end_timestamp
        {
            contained = true;
            break;
        }
    }
    contained
}

pub fn start_end_point_not_in_polygon(
    x_array: &PrimitiveArray<Float64Type>,
    y_array: &PrimitiveArray<Float64Type>,
    polygon: &Polygon,
) -> bool {
    if x_array.len() == 0 {
        return false;
    }

    let start_point = Point::new(x_array.value(0), y_array.value(0));
    let end_point = Point::new(
        x_array.value(x_array.len() - 1),
        y_array.value(x_array.len() - 1),
    );

    if !(polygon.contains(&start_point) || polygon.intersects(&start_point))
        && !(polygon.contains(&end_point) || polygon.intersects(&end_point))
    {
        return true;
    }
    return false;
}

pub fn start_end_point_properly_contained_polygon(
    x_array: &PrimitiveArray<Float64Type>,
    y_array: &PrimitiveArray<Float64Type>,
    polygon: &Polygon,
) -> bool {
    if x_array.len() == 0 {
        return false;
    }

    let start_point = Point::new(x_array.value(0), y_array.value(0));
    let end_point = Point::new(
        x_array.value(x_array.len() - 1),
        y_array.value(x_array.len() - 1),
    );

    if polygon.contains(&start_point) && polygon.contains(&end_point) {
        return true;
    }
    return false;
}

/**
 * Checks if the start point of the polyline is inside of the polygon
 * and checks if the end point of the poyline is outside of the polygon
 */
pub fn start_inside_end_outside(
    x_array: &PrimitiveArray<Float64Type>,
    y_array: &PrimitiveArray<Float64Type>,
    polygon: &Polygon,
) -> bool {
    if x_array.len() == 0 {
        return false;
    }
    let start_point = Point::new(x_array.value(0), y_array.value(0));
    let end_point = Point::new(
        x_array.value(x_array.len() - 1),
        y_array.value(x_array.len() - 1),
    );
    if polygon.contains(&start_point) && !polygon.contains(&end_point) {
        return true;
    }
    return false;
}

use geo_traits::{CoordTrait, GeometryTrait, LineStringTrait};
use geoarrow_array::scalar::LineString;

// Get the closest time range (lower, upper) for a given instant using binary search
pub fn get_time_range_for_instant(trajectory: &LineString, instant: f64) -> Option<(usize, usize)> {
    let num_coords = trajectory.num_coords();
    if num_coords == 0 {
        return None;
    }

    if trajectory.dim() != geo_traits::Dimensions::Xym {
        return None; // No timestamp dimension
    }

    let start_time = trajectory.coord(0).unwrap().nth(2).unwrap();
    let end_time = trajectory.coord(num_coords - 1).unwrap().nth(2).unwrap();

    if instant < start_time || instant > end_time {
        return None;
    }

    let mut left = 0;
    let mut right = num_coords - 1;

    while left <= right {
        let mid = left + (right - left) / 2;
        let mid_time = trajectory.coord(mid).unwrap().nth(2).unwrap();

        if mid_time == instant {
            return Some((mid, mid));
        } else if mid_time < instant {
            left = mid + 1;
        } else {
            if mid == 0 {
                break; // Prevent underflow
            }
            right = mid - 1;
        }
    }

    if right < num_coords - 1 {
        Some((right, right + 1))
    } else {
        None
    }
}

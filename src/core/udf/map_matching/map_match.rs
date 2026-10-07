//! `map_match` UDF. Design decisions are documented in `services/graphhopper/README.md`;
//! the user-facing description lives in [`MAP_MATCH_UDF_DOC`] and is surfaced via
//! `/datafusion/udf_details`.

use crate::core::utils::trajectory_arg::{as_trajectory_array, coerce_udf_args, UdfArg};
use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use arrow_schema::{
    extension::{ExtensionType, EXTENSION_TYPE_NAME_KEY},
    DataType, Field, FieldRef,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use datafusion::common::{not_impl_err, Result as DFResult};
use datafusion::logical_expr::async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl};
use datafusion::logical_expr::{
    scalar_doc_sections::DOC_SECTION_OTHER, ColumnarValue, Documentation, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use futures::stream::{self, StreamExt};
use geo_traits::{CoordTrait, LineStringTrait};
use geoarrow_array::{
    array::from_arrow_array, builder::LineStringBuilder, cast::AsGeoArrowArray, GeoArrowArray,
    GeoArrowArrayAccessor,
};
use geoarrow_schema::{Dimension, LineStringType, Metadata};
use reqwest::Client;
use serde_json::Value;
use tracing::warn;
use wkt::types::{Coord as WktCoord, Dimension as WktDimension, LineString as WktLineString};

use crate::core::utils::schema::LINESTRING_XY_DATATYPE;
use crate::utils::error::ToDataFusionError;

/// User-facing documentation for `map_match`, surfaced via `/datafusion/udf_details`.
///
/// This is the canonical description of the UDF. It cannot be reached through the trait's
/// `ScalarUDF::documentation()` because the `AsyncScalarUDF` wrapper does not forward it, so
/// [`documentation`] exposes it directly for `core::udf::get_udf_details` to pick up.
pub static MAP_MATCH_UDF_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Snaps a raw GPS trajectory onto the road network via a GraphHopper map-matching \
         sidecar, returning the matched path as a LineString. Works on-the-fly in queries and \
         at import time to persist a matched-geometry column (INSERT ... SELECT map_match(...)).",
        "map_match(trajectory) -> LineString",
    )
    .with_argument(
        "trajectory",
        "The trajectory, as the geoarrow layout, as PostGIS EWKB (Binary), or as WKT text (Utf8).",
    )
    .with_sql_example("SELECT map_match(polyline) FROM csv.porto_taxi.trips")
    .build()
});

/// Direct accessor for the `map_match` documentation (see [`MAP_MATCH_UDF_DOC`]).
pub fn documentation() -> &'static Documentation {
    &MAP_MATCH_UDF_DOC
}

/// Async UDF that map-matches trajectories via a GraphHopper sidecar.
///
/// All runtime settings are read once from `GLOBAL_CONFIG` at construction and cached on
/// the struct, alongside a reused `reqwest::Client`.
#[derive(Debug, Clone)]
pub struct MapMatch {
    signature: Signature,
    client: Client,
    endpoint: String,
    profile: String,
    gps_accuracy: f64,
    concurrency: usize,
    /// When false, `<time>` is never emitted in the GPX and GraphHopper matches purely on
    /// point geometry/order. Useful for data with degenerate timestamps (GraphHopper 400
    /// "Sequence is broken for submitted track at initial time step").
    use_timestamps: bool,
}

// `ScalarUDFImpl: DynEq + DynHash`, but the struct holds a `Client` / `f64` which are not
// `Eq`/`Hash`. Compare/hash the config identity only (the client is derived from it).
impl PartialEq for MapMatch {
    fn eq(&self, other: &Self) -> bool {
        self.endpoint == other.endpoint
            && self.profile == other.profile
            && self.gps_accuracy.to_bits() == other.gps_accuracy.to_bits()
            && self.concurrency == other.concurrency
            && self.use_timestamps == other.use_timestamps
    }
}
impl Eq for MapMatch {}
impl Hash for MapMatch {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.endpoint.hash(state);
        self.profile.hash(state);
        self.gps_accuracy.to_bits().hash(state);
        self.concurrency.hash(state);
        self.use_timestamps.hash(state);
    }
}

impl Default for MapMatch {
    fn default() -> Self {
        Self::new()
    }
}

impl MapMatch {
    pub fn new() -> Self {
        use crate::server::utils::config::GLOBAL_CONFIG;
        let endpoint = GLOBAL_CONFIG
            .get::<String>("map_matching.endpoint")
            .unwrap_or_else(|_| "http://localhost:8989".to_string());
        let profile = GLOBAL_CONFIG
            .get::<String>("map_matching.profile")
            .unwrap_or_else(|_| "car".to_string());
        let gps_accuracy = GLOBAL_CONFIG
            .get::<f64>("map_matching.gps_accuracy")
            .unwrap_or(40.0);
        let timeout_ms = GLOBAL_CONFIG
            .get::<u64>("map_matching.request_timeout_ms")
            .unwrap_or(30_000);
        let concurrency = GLOBAL_CONFIG
            .get::<usize>("map_matching.concurrency")
            .unwrap_or(16)
            .max(1);
        let use_timestamps = GLOBAL_CONFIG
            .get::<bool>("map_matching.use_timestamps")
            .unwrap_or(true);

        let client = Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .build()
            .unwrap_or_default();

        Self {
            signature: Signature::user_defined(Volatility::Volatile),
            client,
            endpoint,
            profile,
            gps_accuracy,
            concurrency,
            use_timestamps,
        }
    }

    /// Build the registrable async `ScalarUDF`.
    pub fn into_udf() -> ScalarUDF {
        AsyncScalarUDF::new(Arc::new(MapMatch::new())).into_scalar_udf()
    }
}

impl ScalarUDFImpl for MapMatch {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "map_match"
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&MAP_MATCH_UDF_DOC)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> datafusion::common::Result<Vec<DataType>> {
        coerce_udf_args(self.name(), arg_types, &[UdfArg::Trajectory])
    }

    fn return_type(&self, _args: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(LINESTRING_XY_DATATYPE.clone())
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> DFResult<FieldRef> {
        Ok(Arc::new(
            Field::new("map_match", LINESTRING_XY_DATATYPE.clone(), true).with_metadata(
                [(
                    EXTENSION_TYPE_NAME_KEY.to_string(),
                    LineStringType::NAME.to_owned(),
                )]
                .into(),
            ),
        ))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        not_impl_err!("map_match is async; it is invoked via invoke_async_with_args")
    }
}

#[async_trait]
impl AsyncScalarUDFImpl for MapMatch {
    async fn invoke_async_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let (trajectory_0, trajectory_0_field) =
            as_trajectory_array(&arrays[0], &args.arg_fields[0])?;
        let traj = from_arrow_array(&trajectory_0, &trajectory_0_field).to_df_error()?;
        let traj = traj.as_line_string();

        // 1. Build one GPX body per trajectory. `None` marks a row we skip (null/too few
        //    points) so it becomes a NULL output without a request.
        let mut bodies: Vec<Option<String>> = Vec::new();
        for maybe_ls in traj.iter() {
            match maybe_ls {
                Some(Ok(ls)) => {
                    let mut coords: Vec<(f64, f64, f64)> = Vec::new();
                    for c in ls.coords() {
                        // stored (x=lon, y=lat, m=timestamp)
                        coords.push((c.x(), c.y(), c.nth(2).unwrap_or(0.0)));
                    }
                    if coords.len() < 2 {
                        bodies.push(None);
                    } else {
                        bodies.push(Some(build_gpx(&coords, self.use_timestamps)));
                    }
                }
                _ => bodies.push(None),
            }
        }

        // 2. Fire requests with bounded, order-preserving concurrency (one track/request).
        let url = format!(
            "{}/match?profile={}&type=json&gps_accuracy={}&points_encoded=false",
            self.endpoint, self.profile, self.gps_accuracy
        );
        let tasks = bodies.into_iter().map(|maybe_body| {
            let client = self.client.clone();
            let url = url.clone();
            async move {
                let body = maybe_body?; // None -> NULL row, no request
                match match_one(&client, &url, body).await {
                    Ok(coords) => Some(coords),
                    Err(e) => {
                        warn!("[map_match] request failed, emitting NULL: {e}");
                        None
                    }
                }
            }
        });
        let results: Vec<Option<Vec<[f64; 2]>>> = stream::iter(tasks)
            .buffered(self.concurrency)
            .collect()
            .await;

        // 3. Build the LINESTRING(XY) output column. GraphHopper returns a single
        //    LineString per match; the matched path carries no timestamps, so it is XY.
        let mut builder = LineStringBuilder::new(LineStringType::new(
            Dimension::XY,
            Arc::new(Metadata::default()),
        ));
        for res in results {
            match res {
                Some(coords) if !coords.is_empty() => {
                    let wkt_coords: Vec<WktCoord<f64>> = coords
                        .iter()
                        .map(|[lon, lat]| WktCoord {
                            x: *lon,
                            y: *lat,
                            z: None,
                            m: None,
                        })
                        .collect();
                    let ls = WktLineString::new(wkt_coords, WktDimension::XY);
                    builder.push_line_string(Some(&ls)).to_df_error()?;
                }
                _ => builder
                    .push_line_string(None::<&WktLineString<f64>>)
                    .to_df_error()?,
            }
        }
        Ok(ColumnarValue::Array(builder.finish().into_array_ref()))
    }
}

/// POST one GPX track to the gphhs `/match` endpoint and extract the matched geometry.
async fn match_one(client: &Client, url: &str, gpx: String) -> Result<Vec<[f64; 2]>, String> {
    let resp = client
        .post(url)
        .header("Content-Type", "application/gpx+xml")
        .body(gpx)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("gphhs status {status}: {body}"));
    }

    let text = resp.text().await.map_err(|e| e.to_string())?;
    let json: Value = serde_json::from_str(&text).map_err(|e| e.to_string())?;
    // paths[0].points.coordinates -> [[lon, lat], ...]
    let coords = json
        .get("paths")
        .and_then(|p| p.get(0))
        .and_then(|p| p.get("points"))
        .and_then(|p| p.get("coordinates"))
        .and_then(|c| c.as_array())
        .ok_or_else(|| "response missing paths[0].points.coordinates".to_string())?;

    let mut out = Vec::with_capacity(coords.len());
    for c in coords {
        let lon = c
            .get(0)
            .and_then(Value::as_f64)
            .ok_or("bad lon in response")?;
        let lat = c
            .get(1)
            .and_then(Value::as_f64)
            .ok_or("bad lat in response")?;
        out.push([lon, lat]);
    }
    Ok(out)
}

/// Build a GPX 1.1 track from `(lon, lat, m)` points. Coordinates are reordered to
/// GraphHopper's `lat, lon`. `<time>` (M treated as epoch seconds) is emitted only when
/// `use_timestamps` is set AND timestamps are strictly increasing — GraphHopper rejects
/// non-monotonic or degenerate time steps. With no `<time>`, matching is purely spatial.
fn build_gpx(coords: &[(f64, f64, f64)], use_timestamps: bool) -> String {
    let monotonic = use_timestamps && coords.windows(2).all(|w| w[1].2 > w[0].2);
    let mut s = String::with_capacity(coords.len() * 72 + 160);
    s.push_str(
        r#"<?xml version="1.0" encoding="UTF-8"?><gpx version="1.1" creator="chameleon-datafusion" xmlns="http://www.topografix.com/GPX/1/1"><trk><trkseg>"#,
    );
    for (lon, lat, m) in coords {
        if monotonic {
            if let Some(dt) = DateTime::<Utc>::from_timestamp(*m as i64, 0) {
                s.push_str(&format!(
                    r#"<trkpt lat="{lat}" lon="{lon}"><time>{}</time></trkpt>"#,
                    dt.to_rfc3339()
                ));
                continue;
            }
        }
        s.push_str(&format!(r#"<trkpt lat="{lat}" lon="{lon}"/>"#));
    }
    s.push_str("</trkseg></trk></gpx>");
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::udf::utils::line::TrajectoryFromText;
    use datafusion::prelude::SessionContext;

    /// End-to-end check against a live GraphHopper Berlin sidecar.
    ///
    /// Start it first: `./services/graphhopper/scripts/start.sh --region berlin`
    /// Run with:       `cargo test map_match_berlin -- --ignored --nocapture`
    #[tokio::test]
    #[ignore = "needs the GraphHopper Berlin sidecar running at localhost:8989"]
    async fn map_match_berlin() {
        let ctx = SessionContext::new();
        ctx.register_udf(TrajectoryFromText::new(geoarrow_schema::CoordType::Separated).into());
        ctx.register_udf(MapMatch::into_udf());

        // Short track along Gotthardstraße, Berlin: WKT is (lon lat m), m increasing.
        let wkt = "LINESTRING M(13.33545 52.56518 0, 13.33622 52.56553 15, \
                   13.33669 52.56573 30, 13.33756 52.56605 45, 13.33882 52.56641 60)";
        let sql = format!(
            "SELECT map_match(trajectory_from_text(t.wkt)) AS matched \
             FROM (VALUES ('{wkt}')) AS t(wkt)"
        );

        let df = ctx.sql(&sql).await.unwrap();
        let results = df.collect().await.unwrap();

        assert_eq!(results.len(), 1);
        let col = results[0].column(0);
        assert_eq!(col.len(), 1);
        assert_eq!(
            col.null_count(),
            0,
            "expected a non-null matched geometry (is the Berlin sidecar running?)"
        );
        println!("matched column: {col:?}");
    }
}

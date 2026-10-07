pub mod doc;
pub mod map_matching;
pub mod misc;
pub mod postgis;
pub mod spatial_filters;
pub mod spatio_temporal_filters;
pub mod temporal_filters;
pub mod utils;

use datafusion::logical_expr::{ScalarUDF, TypeSignature};
use datafusion::prelude::SessionContext;
use geodatafusion::udf::geo::{
    measurement::{Distance as GeoDatafusionDistanceUDF, Length as GeoDatafusionLengthUDF},
    relationships::{Contains as GeoDatafusionContains, Intersects as GeoDatafusionIntersects},
};
use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

pub use doc::{UdfArgDetail, UdfDetail};

static UDF_NAME_LIST: OnceLock<Vec<String>> = OnceLock::new();
static UDF_DETAILS: OnceLock<Vec<UdfDetail>> = OnceLock::new();

pub fn get_udf_name_list() -> &'static [String] {
    UDF_NAME_LIST.get().map(|v| v.as_slice()).unwrap_or(&[])
}

pub fn get_udf_details() -> &'static [UdfDetail] {
    UDF_DETAILS.get().map(|v| v.as_slice()).unwrap_or(&[])
}

pub fn init(context: &SessionContext) {
    let temporal_udfs = temporal_filters::get_udf_list();
    let spatial_udfs = spatial_filters::get_udf_list();
    let spatio_temporal_udfs = spatio_temporal_filters::get_udf_list();
    let map_matching_udfs = map_matching::get_udf_list();
    let postgis_udfs = utils::get_udf_list();
    let geo_datafusion_udfs: Vec<ScalarUDF> = vec![
        GeoDatafusionLengthUDF::new().into(),
        GeoDatafusionDistanceUDF::new().into(),
        GeoDatafusionIntersects::new().into(),
        GeoDatafusionContains::new().into(),
    ];

    // Build category lookup sets before consuming the vecs
    let temporal_names: HashSet<String> =
        temporal_udfs.iter().map(|u| u.name().to_string()).collect();
    let spatial_names: HashSet<String> =
        spatial_udfs.iter().map(|u| u.name().to_string()).collect();
    let spatio_temporal_names: HashSet<String> = spatio_temporal_udfs
        .iter()
        .map(|u| u.name().to_string())
        .collect();
    let map_matching_names: HashSet<String> = map_matching_udfs
        .iter()
        .map(|u| u.name().to_string())
        .collect();
    let postgis_names: HashSet<String> =
        postgis_udfs.iter().map(|u| u.name().to_string()).collect();
    let geo_names: HashSet<String> = geo_datafusion_udfs
        .iter()
        .map(|u| u.name().to_string())
        .collect();

    let all_udfs: Vec<ScalarUDF> = [
        temporal_udfs,
        spatial_udfs,
        spatio_temporal_udfs,
        map_matching_udfs,
        postgis_udfs,
        geo_datafusion_udfs,
    ]
    .into_iter()
    .flatten()
    .collect();

    UDF_NAME_LIST.get_or_init(|| {
        all_udfs
            .iter()
            .map(|udf| udf.name().to_string())
            .collect::<Vec<String>>()
    });

    // Async UDFs are wrapped in `AsyncScalarUDF`, which does not forward
    // `documentation()`; look those up by name from their module instead.
    let async_docs: HashMap<&str, &datafusion::logical_expr::Documentation> =
        map_matching::get_udf_docs().into_iter().collect();

    UDF_DETAILS.get_or_init(|| {
        let mut details: Vec<UdfDetail> = all_udfs
            .iter()
            .filter_map(|udf| {
                let doc = udf
                    .documentation()
                    .or_else(|| async_docs.get(udf.name()).copied())?;

                let category = if temporal_names.contains(udf.name()) {
                    "temporal"
                } else if spatial_names.contains(udf.name()) {
                    "spatial"
                } else if spatio_temporal_names.contains(udf.name()) {
                    "spatio_temporal"
                } else if map_matching_names.contains(udf.name()) {
                    "map_matching"
                } else if postgis_names.contains(udf.name()) {
                    "postgis"
                } else if geo_names.contains(udf.name()) {
                    "geodata"
                } else {
                    "utility"
                };

                let input_types: Vec<String> = match &udf.signature().type_signature {
                    TypeSignature::Exact(types) => types.iter().map(|t| format!("{t}")).collect(),
                    TypeSignature::OneOf(sigs) => sigs
                        .iter()
                        .find_map(|s| {
                            if let TypeSignature::Exact(t) = s {
                                Some(t.iter().map(|dt| format!("{dt}")).collect::<Vec<_>>())
                            } else {
                                None
                            }
                        })
                        .unwrap_or_default(),
                    _ => vec![],
                };

                let arguments = doc
                    .arguments
                    .as_ref()
                    .map(|args| {
                        args.iter()
                            .enumerate()
                            .map(|(i, (arg_name, arg_desc))| UdfArgDetail {
                                name: arg_name.clone(),
                                description: arg_desc.clone(),
                                data_type: input_types.get(i).cloned().unwrap_or_default(),
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                Some(UdfDetail {
                    name: udf.name().to_string(),
                    category: category.to_string(),
                    description: doc.description.clone(),
                    syntax_example: doc.syntax_example.clone(),
                    sql_example: doc.sql_example.clone(),
                    arguments,
                })
            })
            .collect();

        details.sort_by(|a, b| a.name.cmp(&b.name));
        details
    });

    for udf in all_udfs {
        context.register_udf(udf);
    }
}

use crate::datafusion::utils::error::{
    arrow_error_to_datafusion_error, io_error_to_datafusion_error,
    parquet_error_to_datafusion_error,
};
use arrow::record_batch::RecordBatch;
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReader, arrow_writer::ArrowWriter},
    file::properties::WriterProperties,
};
use std::{
    collections::HashMap,
    fs::File,
    sync::{Arc, RwLock},
};
use uuid::Uuid;

use datafusion::error::DataFusionError;

type Result<T> = datafusion::common::Result<T>;

pub static DEFAULT_CACHE_PATH: &str = "tmp/datafusion_cache";

/*
    TODO:
        * Add graceful shutdown handling to clear cache
        * Add LRU cache eviction policy. https://crates.io/crates/lru
*/

pub struct CacheManager {
    cache: Arc<RwLock<HashMap<String, String>>>, // <cache_key, parquet file_path>
}

impl CacheManager {
    pub fn new() -> Self {
        CacheManager {
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn is_in_cache(&self, key: &String) -> bool {
        let cache = self.cache.read().unwrap();
        let result = cache.contains_key(key);
        tracing::info!("[CacheManager] is_in_cache: key={}, result={}", key, result);
        result
    }

    pub fn add_record_batches_to_cache(
        &self,
        cache_key: String,
        record_batches: &Vec<RecordBatch>,
    ) -> Result<()> {
        let file_path = format!("{}/{}.parquet", DEFAULT_CACHE_PATH, Uuid::new_v4());
        println!("[CacheManager] Created cache file: {}", file_path);
        let file = File::create(&file_path).map_err(io_error_to_datafusion_error)?;
        let schema = record_batches[0].schema().clone();
        let mut writer =
            ArrowWriter::try_new(file, schema, Some(WriterProperties::builder().build()))?;
        for batch in record_batches {
            writer.write(&batch)?;
        }
        writer.finish().map_err(|e| {
            DataFusionError::Execution(format!("Failed to write record batches to cache: {}", e))
        })?;
        tracing::info!(
            "[CacheManager] Added record batches to cache: key={}, file_path={}",
            cache_key,
            file_path
        );
        self.cache.write().unwrap().insert(cache_key, file_path);
        Ok(())
    }

    pub fn get_record_batches_from_cache(&self, cache_key: &String) -> Result<Vec<RecordBatch>> {
        let cache = self.cache.read().unwrap();
        if let Some(file_path) = cache.get(cache_key) {
            let file = File::open(file_path).map_err(io_error_to_datafusion_error)?;
            let mut parquet_reader = ParquetRecordBatchReader::try_new(file, 1000)
                .map_err(parquet_error_to_datafusion_error)?;
            let mut batches = Vec::new();
            while let Some(batch) = parquet_reader
                .next()
                .transpose()
                .map_err(arrow_error_to_datafusion_error)?
            {
                batches.push(batch);
            }
            tracing::info!(
                "[CacheManager] Retrieved record batches from cache: key={}, file_path={}",
                cache_key,
                file_path
            );
            return Ok(batches);
        } else {
            return Err(DataFusionError::Execution(format!(
                "Cache key '{}' not found",
                cache_key
            )));
        }
    }
}

impl Drop for CacheManager {
    fn drop(&mut self) {
        self.cache.write().unwrap().clear();
        if let Err(e) = std::fs::remove_dir_all(DEFAULT_CACHE_PATH) {
            tracing::error!("Failed to remove cache directory: {}", e);
        } else {
            tracing::info!("Cache directory removed successfully.");
        }
    }
}

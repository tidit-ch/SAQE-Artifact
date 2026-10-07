use datafusion::error::{DataFusionError, Result};

use datafusion::{
    arrow::array::*,
    arrow::datatypes::{Field, Schema, SchemaRef},
};

use std::collections::HashSet;
use std::sync::Arc;

/// Resolves a data path to the files to read.
///
/// A path to a file yields that file; a directory yields the files inside it. The decision is
/// made against the filesystem rather than the extension, so it also holds for parquet or
/// extensionless files.
pub fn list_data_files(path: &str) -> Result<Vec<String>> {
    let metadata = std::fs::metadata(path).map_err(|e| {
        DataFusionError::Execution(format!("could not read data path '{path}': {e}"))
    })?;

    if metadata.is_file() {
        return Ok(vec![path.to_string()]);
    }

    let mut files = Vec::new();
    let dir = std::fs::read_dir(path).map_err(|e| DataFusionError::External(Box::new(e)))?;

    for entry in dir {
        let entry = entry.map_err(|e| DataFusionError::External(Box::new(e)))?;
        if entry
            .file_type()
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .is_file()
        {
            files.push(entry.path().to_string_lossy().to_string());
        }
    }

    if files.is_empty() {
        return Err(DataFusionError::Execution(format!(
            "no files found in data directory '{path}'"
        )));
    }

    // read_dir order is filesystem-defined; sort so partitioning is reproducible across runs.
    files.sort();
    Ok(files)
}

pub fn get_builders_for_schema(schema: SchemaRef, batch_size: usize) -> Vec<Box<dyn ArrayBuilder>> {
    let builder = schema
        .fields()
        .iter()
        .map(|field| make_builder(field.data_type(), batch_size))
        .collect::<Vec<_>>();
    builder
}

pub fn build_batch(
    builders: &mut Vec<Box<dyn ArrayBuilder>>,
    schema: SchemaRef,
) -> Result<RecordBatch> {
    let columns = builders
        .iter_mut()
        .map(|builder| builder.finish())
        .collect::<Vec<Arc<dyn Array>>>();
    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| DataFusionError::Execution(format!("Failed to build record batch: {}", e)))
}

pub fn get_projected_schema(target_schema: SchemaRef, projection: Option<Vec<usize>>) -> SchemaRef {
    if projection.is_some() {
        let projection_set: HashSet<usize> = projection
            .unwrap()
            .iter()
            .copied()
            .collect::<HashSet<usize>>();
        let fields = target_schema.fields();
        let filtered_fields: Vec<Arc<Field>> = fields
            .into_iter()
            .enumerate()
            .filter(|(index, _)| projection_set.contains(index))
            .map(|(_, field)| field.clone())
            .collect();
        return Arc::new(Schema::new(filtered_fields));
    } else {
        return target_schema;
    }
}

#[cfg(test)]
mod list_data_files_tests {
    use super::list_data_files;
    use std::fs;

    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("chameleon_list_{name}"));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn a_directory_yields_its_files_sorted() {
        let dir = scratch("dir");
        for name in ["b.csv", "a.csv", "c.csv"] {
            fs::write(dir.join(name), "x").unwrap();
        }
        fs::create_dir(dir.join("nested")).unwrap();

        let files = list_data_files(dir.to_str().unwrap()).unwrap();
        let names: Vec<_> = files
            .iter()
            .map(|f| f.rsplit('/').next().unwrap())
            .collect();
        assert_eq!(
            names,
            ["a.csv", "b.csv", "c.csv"],
            "subdirectories excluded, order stable"
        );
    }

    #[test]
    fn a_file_yields_itself() {
        let dir = scratch("file");
        let file = dir.join("trips.csv");
        fs::write(&file, "x").unwrap();

        assert_eq!(
            list_data_files(file.to_str().unwrap()).unwrap(),
            vec![file.to_string_lossy().to_string()]
        );
    }

    /// The decision is made against the filesystem, not the extension.
    #[test]
    fn a_file_without_a_csv_extension_yields_itself() {
        let dir = scratch("noext");
        let file = dir.join("trips.parquet");
        fs::write(&file, "x").unwrap();

        assert_eq!(list_data_files(file.to_str().unwrap()).unwrap().len(), 1);
    }

    #[test]
    fn a_missing_path_names_itself_in_the_error() {
        let error = list_data_files("/nope/not/here").unwrap_err().to_string();
        assert!(error.contains("/nope/not/here"), "{error}");
    }

    #[test]
    fn an_empty_directory_is_an_error() {
        let dir = scratch("empty");
        assert!(list_data_files(dir.to_str().unwrap()).is_err());
    }
}

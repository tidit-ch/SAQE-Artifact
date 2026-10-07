use datafusion::arrow::array::RecordBatch;

pub mod config;

pub fn convert_record_batches_list_to_json(record_batches: Vec<RecordBatch>) -> Vec<u8> {
    let buf = Vec::new();
    let mut writer = datafusion::arrow::json::ArrayWriter::new(buf);
    let record_batch_refs: Vec<&datafusion::arrow::array::RecordBatch> =
        record_batches.iter().collect();
    writer.write_batches(&record_batch_refs[..]).unwrap();
    writer.finish().unwrap();
    writer.into_inner()
}

pub fn convert_record_batches_list_to_csv(record_batches: Vec<RecordBatch>) -> String {
    let buf = Vec::new();
    let mut writer = datafusion::arrow::csv::Writer::new(buf);
    for batch in record_batches {
        writer.write(&batch).unwrap();
    }
    let buf = writer.into_inner();
    let json_rows = String::from_utf8(buf).unwrap();
    json_rows
}

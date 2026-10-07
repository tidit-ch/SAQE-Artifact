use super::{event::OystercatcherRecordEvent, utils::ColumnKeys};
use crate::utils::error::CsvError;
use csv::StringRecord;
use datafusion::arrow::array::*;

type Result<T, E = CsvError> = std::result::Result<T, E>;

#[derive(Debug, Clone)]
pub struct OystercatcherRecord {
    polyline: Vec<(f64, f64, i64)>, // Vertices as (x = longitude, y = latitude, timestamp)
    individual_taxon_canonical_name: String,
    tag_local_identifier: String,
    individual_local_identifier: String,
    study_name: String,
    event_list: Vec<OystercatcherRecordEvent>,
}

impl OystercatcherRecord {
    pub fn new(
        polyline: Vec<(f64, f64, i64)>,
        individual_taxon_canonical_name: String,
        tag_local_identifier: String,
        individual_local_identifier: String,
        study_name: String,
        event_list: Vec<OystercatcherRecordEvent>,
    ) -> Self {
        OystercatcherRecord {
            polyline,
            individual_taxon_canonical_name,
            tag_local_identifier,
            individual_local_identifier,
            study_name,
            event_list,
        }
    }
}

impl Default for OystercatcherRecord {
    fn default() -> Self {
        OystercatcherRecord {
            polyline: Vec::new(),
            individual_taxon_canonical_name: String::new(),
            tag_local_identifier: String::new(),
            individual_local_identifier: String::new(),
            study_name: String::new(),
            event_list: Vec::new(),
        }
    }
}

impl OystercatcherRecord {
    pub fn belongs_to_same_individual(&self, record: StringRecord) -> Result<bool> {
        let individual_local_identifier = record.get(ColumnKeys::IndividualLocalIdentifier.into());
        match individual_local_identifier {
            Some(id) if !id.is_empty() => Ok(self.individual_local_identifier == id),
            _ => Err(CsvError::CellParse {
                expected: "string".to_string(),
                found: "".to_string(),
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Empty cell",
                )),
                position: record.position().cloned(),
            }),
        }
    }

    pub fn append_event(&mut self, record: StringRecord) -> Result<()> {
        let event = OystercatcherRecordEvent::from_csv(record)?;
        if let Some(polyline_data) = event.get_polyline_data() {
            self.polyline.push(polyline_data);
        }
        self.event_list.push(event);
        Ok(())
    }

    pub fn from_csv(record: StringRecord) -> Result<OystercatcherRecord> {
        let fields: Vec<&str> = record.iter().map(|s| s.trim()).collect();

        let mut oyestercatcher_record = OystercatcherRecord::default();
        oyestercatcher_record.individual_taxon_canonical_name =
            fields[ColumnKeys::IndividualTaxonCanonicalName as usize].to_string();
        oyestercatcher_record.tag_local_identifier =
            fields[ColumnKeys::TagLocalIdentifier as usize].to_string();
        oyestercatcher_record.individual_local_identifier =
            fields[ColumnKeys::IndividualLocalIdentifier as usize].to_string();
        oyestercatcher_record.study_name = fields[ColumnKeys::StudyName as usize].to_string();

        let event = OystercatcherRecordEvent::from_csv(record)?;
        if let Some(polyline_data) = event.get_polyline_data() {
            oyestercatcher_record.polyline.push(polyline_data);
        }
        oyestercatcher_record.event_list.push(event);

        Ok(oyestercatcher_record)
    }

    pub fn add_data_to_builders(
        &mut self,
        builders: &mut Vec<Box<dyn ArrayBuilder>>,
        projection: &Vec<usize>,
    ) -> Result<()> {
        let mut field_index: usize = 0;
        let mut builder_index: usize = 0;

        if should_add_this_index(field_index, builder_index, projection) {
            let list_builder = builders[builder_index]
                .as_any_mut()
                .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
                .unwrap();
            let struct_builder = list_builder
                .values()
                .as_any_mut()
                .downcast_mut::<StructBuilder>()
                .unwrap();
            for point in &self.polyline {
                let (x, y, timestamp) = point;
                // struct_builder.append(true);
                let x_builder = struct_builder.field_builder::<Float64Builder>(0).unwrap();
                x_builder.append_value(*x);
                let y_builder = struct_builder.field_builder::<Float64Builder>(1).unwrap();
                y_builder.append_value(*y);
                let timestamp_builder = struct_builder.field_builder::<Float64Builder>(2).unwrap();
                timestamp_builder.append_value(*timestamp as f64);
                struct_builder.append(true);
            }
            list_builder.append(true);
            builder_index += 1;
        }
        field_index += 1;

        if should_add_this_index(field_index, builder_index, projection) {
            let string_builder = builders[builder_index]
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap();
            string_builder.append_value(&self.individual_taxon_canonical_name);
            builder_index += 1;
        }
        field_index += 1;

        if should_add_this_index(field_index, builder_index, projection) {
            let string_builder = builders[builder_index]
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap();
            string_builder.append_value(&self.tag_local_identifier);
            builder_index += 1;
        }
        field_index += 1;

        if should_add_this_index(field_index, builder_index, projection) {
            let string_builder = builders[builder_index]
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap();
            string_builder.append_value(&self.individual_local_identifier);
            builder_index += 1;
        }
        field_index += 1;

        if should_add_this_index(field_index, builder_index, projection) {
            let string_builder = builders[builder_index]
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap();
            string_builder.append_value(&self.study_name);
            builder_index += 1;
        }
        field_index += 1;

        if should_add_this_index(field_index, builder_index, projection) {
            let list_builder = builders[builder_index]
                .as_any_mut()
                .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
                .unwrap();
            let struct_builder: &mut StructBuilder = list_builder
                .values()
                .as_any_mut()
                .downcast_mut::<StructBuilder>()
                .unwrap();
            for event in &self.event_list {
                event.add_data_to_builders(struct_builder);
                struct_builder.append(true);
            }
            list_builder.append(true);
        }

        Ok(())
    }
}
pub fn should_add_this_index(
    field_index: usize,
    builder_index: usize,
    projection: &Vec<usize>,
) -> bool {
    if builder_index >= projection.len() {
        return false;
    }
    if projection[builder_index] != field_index {
        return false;
    }
    if projection[builder_index] == field_index {
        return true;
    }
    return false;
}

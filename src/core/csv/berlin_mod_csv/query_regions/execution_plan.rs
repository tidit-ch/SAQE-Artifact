use crate::core::utils::common::list_data_files;
use datafusion::{
    arrow::datatypes::{Field, Schema, SchemaRef},
    common::stats::Precision,
    error::Result,
    execution::context::TaskContext,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        SendableRecordBatchStream, Statistics,
    },
};
use std::{any::Any, collections::HashSet, fmt, sync::Arc};

#[derive(Debug, Clone)]
pub struct BerlinModQueryRegionsExecutionPlan {
    file_path: String,
    file_list: Vec<String>,
    target_schema: SchemaRef,
    projected_schema: SchemaRef,
    plan_properties: PlanProperties,
    projection: Option<Vec<usize>>,
    //partition: usize,
}

impl BerlinModQueryRegionsExecutionPlan {
    pub fn new(
        file_path: String,
        target_schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Self {
        // A file path yields that file, a directory the files inside it.
        let file_list = list_data_files(&file_path).unwrap_or_else(|e| panic!("{e}"));
        //let partition = file_list.len();
        let projected_schema = BerlinModQueryRegionsExecutionPlan::get_projected_schema(
            target_schema.clone(),
            projection.clone(),
        );

        let eq_properties = EquivalenceProperties::new(projected_schema.clone());
        let partitioning = Partitioning::RoundRobinBatch(file_list.len());
        let emission_type = EmissionType::Incremental;
        let boundedness = Boundedness::Bounded;
        let plan_properties =
            PlanProperties::new(eq_properties, partitioning, emission_type, boundedness);

        Self {
            file_list,
            file_path,
            target_schema,
            projected_schema,
            plan_properties,
            projection,
            //partition,
        }
    }

    pub fn get_projected_schema(
        target_schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> SchemaRef {
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
}

impl DisplayAs for BerlinModQueryRegionsExecutionPlan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "BerlinModQueryRegionsExecutionPlan: path={}, projection={:?}",
                    self.file_path,
                    self.target_schema
                        .fields()
                        .iter()
                        .map(|f| f.name())
                        .collect::<Vec<_>>()
                )
            }
            DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "BerlinModQueryRegionsExecutionPlan\n  path: {}\n  projection: {:?}",
                    self.file_path,
                    self.target_schema
                        .fields()
                        .iter()
                        .map(|f| f.name())
                        .collect::<Vec<_>>()
                )
            }
        }
    }
}

impl ExecutionPlan for BerlinModQueryRegionsExecutionPlan {
    fn name(&self) -> &str {
        "BerlinModQueryRegionsExecutionPlan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn with_new_children(
        self: std::sync::Arc<Self>,
        _children: Vec<std::sync::Arc<dyn ExecutionPlan>>,
    ) -> Result<std::sync::Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: std::sync::Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        /* println!(
            "Executing partition: {}, file: {}, thread: {:?}",
            partition,
            self.file_list[partition],
            std::thread::current().id()
        ); */
        // Create and return your custom stream
        let stream =
            crate::core::csv::berlin_mod_csv::query_regions::csv_stream::BerlinModQueryRegionsDataStream::try_new(
                self.file_list[partition].clone(),
                self.projected_schema.clone(),
                context.session_config().batch_size(), // Desired batch size
                self.projection.clone(),
                true, // CSV header is present
            )?;

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.projected_schema.clone(),
            stream,
        )))
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics {
            num_rows: datafusion::common::stats::Precision::Exact(200), // Adjust this based on actual data -> Number of outut rows
            total_byte_size: Precision::Inexact(377450), // Adjust this based on the actual data
            column_statistics: Statistics::unknown_column(&*self.schema()),
        })
    }
}

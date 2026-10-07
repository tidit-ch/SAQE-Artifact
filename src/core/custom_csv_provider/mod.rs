//! Custom CSV Table Provider Module
//! Writing custom csv table providers for different csv datasets can be repetitive.
//! Core ideas in implementing a csv table provider can be abstracted like:
//!     - defining schema
//!     - reading csv files from a directory
//!     - streaming data in batches
//!     - getting the execution plan
//!     - getting the plan statistics
//! We abstracted these code ideas into a trait CsvBaseBehavior.
//!
//! We then created a generic CustomCsvTableProvider that takes any struct implementing the CsvBaseBehavior trait.
//! This way, to create a new csv table provider, one only needs to implement the CsvBaseBehavior trait for that specific dataset.
//!
//! Similarly, we also created a CustomCsvExecutionPlan that also takes any struct implementing the CsvBaseBehavior trait.
//! The trait provides all the necessary methods for streaming data from csv files in batches.
//!
//! The CustomCsvTableProvider can then create the CustomCsvExecutionPlan by passing down the same struct implementing CsvBaseBehavior trait for query execution.
//!
//! We also provided a CsvBaseConfig struct to hold common configuration parameters for csv table providers.
//! A builder pattern is used to easily create CsvBaseConfig instances.
//!
//! To create a new csv table provider for a specific dataset, one only needs to:
//!     - add a new struct for the dataset having a field of type CsvBaseConfig.
//!     - implement the CsvBaseBehavior trait for that struct.
//!     - create an instance of CustomCsvTableProvider by passing the struct implementing CsvBaseBehavior trait.

pub mod base_behavior;
pub mod base_config;
pub mod execution_plan;
pub mod stream;
pub mod table_provider;

/*
 * Task builder for creating compaction tasks from file groups.
 */

use compaction_common::{ExecutionConfig, FileGroup, Result};
use compaction_proto::{CommitMode, CompactionTask, FileIOConfig};
use iceberg::table::Table;
use uuid::Uuid;

/// Builds CompactionTasks from FileGroups.
pub struct TaskBuilder {
    table_identifier: String,
    snapshot_id: i64,
    schema_json: String,
    partition_spec_json: String,
    sort_order_json: Option<String>,
    output_location: String,
    file_io_config: FileIOConfig,
    execution_config: ExecutionConfig,
    commit_mode: CommitMode,
}

impl TaskBuilder {
    /// Creates a new TaskBuilder from a table.
    pub fn from_table(
        table: &Table,
        snapshot_id: i64,
        execution_config: ExecutionConfig,
    ) -> Result<Self> {
        let metadata = table.metadata();

        // Serialize schema
        let schema = metadata.current_schema();
        let schema_json =
            serde_json::to_string(schema).map_err(|e| compaction_common::CompactionError::Serialization(e.to_string()))?;

        // Serialize partition spec
        let partition_spec = metadata.default_partition_spec();
        let partition_spec_json = serde_json::to_string(partition_spec)
            .map_err(|e| compaction_common::CompactionError::Serialization(e.to_string()))?;

        // Serialize sort order if present and has fields
        let sort_order = metadata.default_sort_order();
        let sort_order_json = if sort_order.fields.is_empty() {
            None
        } else {
            Some(serde_json::to_string(sort_order.as_ref())
                .map_err(|e| compaction_common::CompactionError::Serialization(e.to_string()))?)
        };

        // Get output location from table location
        let output_location = format!("{}/data", metadata.location());

        // TODO: Extract file IO config from table properties/location
        // For now, default to local filesystem based on table location
        let file_io_config = if metadata.location().starts_with("s3://") {
            // Extract bucket from s3://bucket/path
            let path = metadata.location().trim_start_matches("s3://");
            let bucket = path.split('/').next().unwrap_or("default");
            FileIOConfig::s3(bucket, "us-east-1")
        } else if metadata.location().starts_with("gs://") {
            let path = metadata.location().trim_start_matches("gs://");
            let bucket = path.split('/').next().unwrap_or("default");
            FileIOConfig::gcs(bucket)
        } else {
            // Default to local filesystem
            FileIOConfig::local(metadata.location())
        };

        Ok(Self {
            table_identifier: table.identifier().to_string(),
            snapshot_id,
            schema_json,
            partition_spec_json,
            sort_order_json,
            output_location,
            file_io_config,
            execution_config,
            commit_mode: CommitMode::default(),
        })
    }

    /// Sets the commit mode for tasks built by this builder.
    pub fn with_commit_mode(mut self, mode: CommitMode) -> Self {
        self.commit_mode = mode;
        self
    }

    /// Builds a CompactionTask from a FileGroup.
    pub fn build_task(&self, file_group: FileGroup) -> CompactionTask {
        CompactionTask {
            task_id: Uuid::now_v7(),
            table_identifier: self.table_identifier.clone(),
            snapshot_id: self.snapshot_id,
            file_group,
            schema_json: self.schema_json.clone(),
            partition_spec_json: self.partition_spec_json.clone(),
            sort_order_json: self.sort_order_json.clone(),
            output_location: self.output_location.clone(),
            execution_config: self.execution_config.clone(),
            inline_deletes: None, // TODO: Populate for small deletes
            file_io_config: self.file_io_config.clone(),
            commit_mode: self.commit_mode,
        }
    }

    /// Builds tasks for all file groups.
    pub fn build_tasks(&self, file_groups: Vec<FileGroup>) -> Vec<CompactionTask> {
        file_groups
            .into_iter()
            .map(|group| self.build_task(group))
            .collect()
    }
}

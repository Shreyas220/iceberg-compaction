/*
 * Worker executor - orchestrates task execution.
 */

use compaction_common::{CompactionError, Result};
use compaction_proto::{CompactionTask, CompactionTaskResult, TaskStats, TaskStatus};
use std::time::Instant;

use crate::datafusion::DataFusionEngine;

/// Serializable representation of an output file.
/// (iceberg::spec::DataFile doesn't implement Serialize)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OutputFileInfo {
    pub file_path: String,
    pub file_size_bytes: u64,
    pub record_count: u64,
}

/// Executes compaction tasks.
pub struct WorkerExecutor {
    engine: DataFusionEngine,
}

impl WorkerExecutor {
    /// Creates a new worker executor.
    pub fn new() -> Self {
        Self {
            engine: DataFusionEngine::new(),
        }
    }

    /// Executes a compaction task.
    pub async fn execute(&self, task: CompactionTask) -> CompactionTaskResult {
        let start = Instant::now();
        let task_id = task.task_id;

        match self.execute_inner(task).await {
            Ok((output_files_json, stats)) => CompactionTaskResult {
                task_id,
                status: TaskStatus::Success,
                output_files_json,
                stats: TaskStats {
                    execution_time_ms: start.elapsed().as_millis() as u64,
                    ..stats
                },
                error: None,
            },
            Err(e) => CompactionTaskResult {
                task_id,
                status: TaskStatus::Failed,
                output_files_json: vec![],
                stats: TaskStats {
                    execution_time_ms: start.elapsed().as_millis() as u64,
                    ..Default::default()
                },
                error: Some(e.to_string()),
            },
        }
    }

    async fn execute_inner(
        &self,
        task: CompactionTask,
    ) -> Result<(Vec<String>, TaskStats)> {
        // Record input stats
        let input_bytes = task.file_group.input_total_bytes();
        let input_file_count = task.file_group.input_files_count();

        // Execute the compaction
        let (output_files, rows_processed) = self.engine.execute_compaction(&task).await?;

        // Convert to serializable format and serialize
        let output_files_json: Vec<String> = output_files
            .iter()
            .map(|f| {
                let info = OutputFileInfo {
                    file_path: f.file_path().to_string(),
                    file_size_bytes: f.file_size_in_bytes() as u64,
                    record_count: f.record_count() as u64,
                };
                serde_json::to_string(&info)
            })
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| CompactionError::Serialization(e.to_string()))?;

        let output_bytes: u64 = output_files.iter().map(|f| f.file_size_in_bytes()).sum();
        let output_file_count = output_files.len();

        Ok((
            output_files_json,
            TaskStats {
                input_bytes,
                output_bytes: output_bytes as u64,
                input_file_count,
                output_file_count,
                rows_processed,
                execution_time_ms: 0, // Filled in by caller
            },
        ))
    }
}

impl Default for WorkerExecutor {
    fn default() -> Self {
        Self::new()
    }
}

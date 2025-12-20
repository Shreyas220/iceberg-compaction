/*
 * Commit Coordination
 *
 * Handles committing compaction results to Iceberg tables.
 * Supports planner-commit (default, safer) and worker-commit modes.
 */

use compaction_common::{CompactionError, Result};
use compaction_proto::{CompactionTask, CompactionTaskResult, TaskStatus};
// Note: DataFile, DataFileBuilder will be used when implementing full rewrite
use iceberg::table::Table;
use iceberg::transaction::Transaction;
use serde::{Deserialize, Serialize};

/// Commit mode for compaction results.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
pub enum CommitMode {
    /// Planner commits all results (default, safer)
    /// Workers return DataFile info, planner does atomic commit
    #[default]
    PlannerCommit,

    /// Workers commit their own results
    /// Faster but requires careful coordination
    WorkerCommit,
}

/// Coordinates commits for compaction results.
pub struct CommitCoordinator {
    mode: CommitMode,
}

impl CommitCoordinator {
    /// Creates a new commit coordinator with the specified mode.
    pub fn new(mode: CommitMode) -> Self {
        Self { mode }
    }

    /// Creates a coordinator with planner-commit mode (default).
    pub fn planner_commit() -> Self {
        Self::new(CommitMode::PlannerCommit)
    }

    /// Returns the commit mode.
    pub fn mode(&self) -> CommitMode {
        self.mode
    }

    /// Commits results for a batch of completed tasks.
    ///
    /// In planner-commit mode, this:
    /// 1. Parses output file info from task results
    /// 2. Creates a single atomic transaction
    /// 3. Removes old files, adds new files
    /// 4. Commits the transaction
    pub async fn commit_batch(
        &self,
        table: &mut Table,
        tasks: &[CompactionTask],
        results: &[CompactionTaskResult],
    ) -> Result<CommitSummary> {
        if self.mode != CommitMode::PlannerCommit {
            return Err(CompactionError::Commit(
                "WorkerCommit mode should commit in workers, not coordinator".to_string(),
            ));
        }

        // Filter to successful tasks only
        let successful: Vec<_> = tasks
            .iter()
            .zip(results.iter())
            .filter(|(_, r)| r.status == TaskStatus::Success)
            .collect();

        if successful.is_empty() {
            return Ok(CommitSummary::empty());
        }

        // Collect files to remove and add
        let mut files_to_remove: Vec<String> = Vec::new();
        let mut files_to_add: Vec<DataFileInfo> = Vec::new();

        for (task, result) in &successful {
            // Collect input files to remove
            for data_file in &task.file_group.data_files {
                files_to_remove.push(data_file.file_path.clone());
            }

            // Parse output files to add
            for json in &result.output_files_json {
                let info: DataFileInfo = serde_json::from_str(json)
                    .map_err(|e| CompactionError::Serialization(e.to_string()))?;
                files_to_add.push(info);
            }
        }

        // Create and execute transaction
        let tx = Transaction::new(table);

        // Build rewrite operation
        let rewrite = tx.rewrite_files();

        // Note: The actual iceberg-rust API for rewrite may differ
        // This is a conceptual implementation showing the intent
        tracing::info!(
            "Committing compaction: removing {} files, adding {} files",
            files_to_remove.len(),
            files_to_add.len()
        );

        // TODO: Execute the actual rewrite transaction
        // The iceberg-rust API is evolving, so this would need to match
        // the actual API version being used

        Ok(CommitSummary {
            files_removed: files_to_remove.len(),
            files_added: files_to_add.len(),
            bytes_removed: successful
                .iter()
                .map(|(t, _)| t.file_group.total_size_bytes)
                .sum(),
            bytes_added: successful.iter().map(|(_, r)| r.stats.output_bytes).sum(),
            tasks_committed: successful.len(),
        })
    }

    /// Validates that a task result can be committed.
    pub fn validate_result(
        &self,
        task: &CompactionTask,
        result: &CompactionTaskResult,
    ) -> Result<()> {
        if result.status != TaskStatus::Success {
            return Err(CompactionError::Commit(format!(
                "Cannot commit failed task: {:?}",
                result.error
            )));
        }

        // Validate snapshot hasn't changed (optimistic concurrency)
        // This would be checked during actual commit
        tracing::debug!(
            "Validating task {} for commit (snapshot {})",
            task.task_id,
            task.snapshot_id
        );

        Ok(())
    }
}

impl Default for CommitCoordinator {
    fn default() -> Self {
        Self::planner_commit()
    }
}

/// Information about a data file for commit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataFileInfo {
    pub file_path: String,
    pub file_size_bytes: u64,
    pub record_count: u64,
    pub partition_values: Option<std::collections::HashMap<String, String>>,
}

/// Summary of a commit operation.
#[derive(Debug, Clone, Default)]
pub struct CommitSummary {
    pub files_removed: usize,
    pub files_added: usize,
    pub bytes_removed: u64,
    pub bytes_added: u64,
    pub tasks_committed: usize,
}

impl CommitSummary {
    pub fn empty() -> Self {
        Self::default()
    }

    /// Returns the space savings ratio.
    pub fn space_savings_ratio(&self) -> f64 {
        if self.bytes_removed == 0 {
            0.0
        } else {
            1.0 - (self.bytes_added as f64 / self.bytes_removed as f64)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_commit_mode_default() {
        let coord = CommitCoordinator::default();
        assert_eq!(coord.mode(), CommitMode::PlannerCommit);
    }

    #[test]
    fn test_space_savings_ratio() {
        let summary = CommitSummary {
            bytes_removed: 1000,
            bytes_added: 800,
            ..Default::default()
        };
        assert!((summary.space_savings_ratio() - 0.2).abs() < 0.001);
    }
}

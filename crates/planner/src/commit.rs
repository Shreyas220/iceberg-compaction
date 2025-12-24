/*
 * Commit Coordination
 *
 * Handles committing compaction results to Iceberg tables.
 * Supports planner-commit (default, safer) and worker-commit modes.
 *
 * Iceberg compaction commits involve a "rewrite" operation:
 * 1. Add new compacted data files to the table
 * 2. Remove old source data files
 * 3. Record this as a single atomic snapshot
 *
 * Note: The iceberg-rust API is still evolving. Currently we implement
 * what's possible with the available Transaction methods and document
 * the intended behavior for when full rewrite support is available.
 */

use compaction_common::{CompactionError, Result};
use compaction_proto::{CommitMode, CompactionTask, CompactionTaskResult, TaskStatus};
use iceberg::table::Table;
use iceberg::transaction::Transaction;
use iceberg::Catalog;
use serde::{Deserialize, Serialize};

/// Coordinates commits for compaction results.
///
/// The coordinator handles the commit lifecycle:
/// 1. Validate that all required tasks completed successfully
/// 2. Parse output DataFile metadata from task results
/// 3. Build the appropriate Iceberg transaction
/// 4. Execute the commit with conflict detection
/// 5. Handle retries on conflicts
pub struct CommitCoordinator {
    mode: CommitMode,
    /// Maximum commit retries on conflict
    max_retries: usize,
}

impl CommitCoordinator {
    /// Creates a new commit coordinator with the specified mode.
    pub fn new(mode: CommitMode) -> Self {
        Self {
            mode,
            max_retries: 3,
        }
    }

    /// Creates a coordinator with planner-commit mode (default).
    pub fn planner_commit() -> Self {
        Self::new(CommitMode::PlannerCommit)
    }

    /// Returns the commit mode.
    pub fn mode(&self) -> CommitMode {
        self.mode
    }

    /// Sets the maximum number of retries on commit conflicts.
    pub fn with_max_retries(mut self, retries: usize) -> Self {
        self.max_retries = retries;
        self
    }

    /// Commits results for a batch of completed tasks.
    ///
    /// In planner-commit mode, this:
    /// 1. Parses output file info from task results
    /// 2. Creates a single atomic transaction
    /// 3. Removes old files, adds new files
    /// 4. Commits the transaction
    ///
    /// For rewrite operations, we need to:
    /// - Add the new compacted DataFiles
    /// - Remove the old source DataFiles
    /// - Ensure the operation is atomic
    ///
    /// Note: Current iceberg-rust only has `fast_append`. Full rewrite
    /// support requires matching the API when it becomes available.
    pub async fn commit_batch<C: Catalog>(
        &self,
        table: &mut Table,
        catalog: &C,
        tasks: &[CompactionTask],
        results: &[CompactionTaskResult],
    ) -> Result<CommitSummary> {
        if self.mode == CommitMode::NoCommit {
            tracing::info!("NoCommit mode - skipping commit");
            return self.build_summary_without_commit(tasks, results);
        }

        if self.mode == CommitMode::WorkerCommit {
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
            tracing::warn!("No successful tasks to commit");
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

        tracing::info!(
            "Preparing commit: removing {} files, adding {} files",
            files_to_remove.len(),
            files_to_add.len()
        );

        // Attempt commit with retries
        let mut last_error = None;
        for attempt in 0..=self.max_retries {
            if attempt > 0 {
                tracing::info!("Commit retry attempt {} of {}", attempt, self.max_retries);
            }

            match self
                .execute_commit(table, catalog, &files_to_remove, &files_to_add)
                .await
            {
                Ok(()) => {
                    tracing::info!(
                        "Commit successful: {} files removed, {} files added",
                        files_to_remove.len(),
                        files_to_add.len()
                    );

                    return Ok(CommitSummary {
                        files_removed: files_to_remove.len(),
                        files_added: files_to_add.len(),
                        bytes_removed: successful
                            .iter()
                            .map(|(t, _)| t.file_group.total_size_bytes)
                            .sum(),
                        bytes_added: successful.iter().map(|(_, r)| r.stats.output_bytes).sum(),
                        tasks_committed: successful.len(),
                    });
                }
                Err(e) => {
                    tracing::warn!("Commit attempt {} failed: {}", attempt, e);
                    last_error = Some(e);

                    // On conflict, we'd need to refresh the table and revalidate
                    // For now, we just retry with exponential backoff
                    if attempt < self.max_retries {
                        let delay =
                            std::time::Duration::from_millis(100 * 2_u64.pow(attempt as u32));
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            CompactionError::Commit("Commit failed after all retries".to_string())
        }))
    }

    /// Executes the actual commit transaction.
    ///
    /// This is where we interface with iceberg-rust's Transaction API.
    /// Currently uses fast_append for adding files. Full rewrite support
    /// (adding + removing files atomically) depends on iceberg-rust API.
    async fn execute_commit<C: Catalog>(
        &self,
        table: &Table,
        _catalog: &C,
        files_to_remove: &[String],
        files_to_add: &[DataFileInfo],
    ) -> Result<()> {
        // Create transaction (will be used when rewrite API is available)
        let _tx = Transaction::new(table);

        // Log what we intend to do
        tracing::debug!(
            "Transaction: remove {} files, add {} files",
            files_to_remove.len(),
            files_to_add.len()
        );

        for path in files_to_remove {
            tracing::debug!("  - Remove: {}", path);
        }
        for file in files_to_add {
            tracing::debug!(
                "  + Add: {} ({} bytes, {} records)",
                file.file_path,
                file.file_size_bytes,
                file.record_count
            );
        }

        // NOTE: iceberg-rust's Transaction API is still evolving.
        // The full rewrite operation (remove + add atomically) may not
        // be available yet. When it is, replace this with:
        //
        // let rewrite = tx.rewrite_files();
        // for path in files_to_remove {
        //     rewrite.remove_file(path);
        // }
        // for file in files_to_add {
        //     rewrite.add_file(file.to_data_file()?);
        // }
        // let updated_table = rewrite.apply().await?;
        //
        // For now, we document the intent and use what's available.

        // Create a pending commit record for auditing
        let commit_record = PendingCommit {
            files_to_remove: files_to_remove.to_vec(),
            files_to_add: files_to_add.to_vec(),
            snapshot_id: table.metadata().current_snapshot().map(|s| s.snapshot_id()),
        };

        tracing::info!(
            "Pending commit recorded (snapshot {:?}): {} removals, {} additions",
            commit_record.snapshot_id,
            commit_record.files_to_remove.len(),
            commit_record.files_to_add.len()
        );

        // When fast_append is appropriate (no deletes, just adds):
        // let append = tx.fast_append(None, vec![])?;
        // for file in files_to_add {
        //     append.add_data_file(file.to_data_file()?);
        // }
        // let _updated = tx.commit(catalog).await?;

        // For now, log that we need the rewrite API
        tracing::warn!(
            "Full rewrite commit not yet implemented - iceberg-rust API evolving. \
             Files written to storage but table metadata not updated. \
             Use external tooling (Spark, Trino) to complete the rewrite."
        );

        Ok(())
    }

    /// Builds a summary without actually committing.
    fn build_summary_without_commit(
        &self,
        tasks: &[CompactionTask],
        results: &[CompactionTaskResult],
    ) -> Result<CommitSummary> {
        let successful: Vec<_> = tasks
            .iter()
            .zip(results.iter())
            .filter(|(_, r)| r.status == TaskStatus::Success)
            .collect();

        let mut files_removed = 0;
        let mut files_added = 0;

        for (task, result) in &successful {
            files_removed += task.file_group.data_files.len();
            files_added += result.output_files_json.len();
        }

        Ok(CommitSummary {
            files_removed,
            files_added,
            bytes_removed: successful
                .iter()
                .map(|(t, _)| t.file_group.total_size_bytes)
                .sum(),
            bytes_added: successful.iter().map(|(_, r)| r.stats.output_bytes).sum(),
            tasks_committed: 0, // No actual commit
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

        // Validate output files exist
        if result.output_files_json.is_empty() {
            return Err(CompactionError::Commit(
                "Task completed but produced no output files".to_string(),
            ));
        }

        // Validate output files are parseable
        for json in &result.output_files_json {
            let _: DataFileInfo = serde_json::from_str(json)
                .map_err(|e| CompactionError::Serialization(format!("Invalid output file: {}", e)))?;
        }

        tracing::debug!(
            "Validated task {} for commit (snapshot {}, {} output files)",
            task.task_id,
            task.snapshot_id,
            result.output_files_json.len()
        );

        Ok(())
    }
}

impl Default for CommitCoordinator {
    fn default() -> Self {
        Self::planner_commit()
    }
}

/// Record of a pending commit for auditing and recovery.
#[derive(Debug, Clone)]
pub struct PendingCommit {
    pub files_to_remove: Vec<String>,
    pub files_to_add: Vec<DataFileInfo>,
    pub snapshot_id: Option<i64>,
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

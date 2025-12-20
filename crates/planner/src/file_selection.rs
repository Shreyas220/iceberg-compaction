/*
 * File selection strategies for compaction.
 *
 * Adapted from iceberg-compaction/core/src/file_selection/
 */

use compaction_common::{
    CompactionStrategy, FileGroup, FileMetadata, GroupingStrategy, PlanningConfig, Result,
};
use iceberg::scan::FileScanTask;
use iceberg::spec::DataContentType;
use iceberg::table::Table;
use std::collections::HashMap;

use crate::packer::ListPacker;

/// Selects files for compaction from an Iceberg table.
pub struct FileSelector;

impl FileSelector {
    /// Selects files for compaction based on strategy and config.
    pub async fn select_files(
        table: &Table,
        snapshot_id: i64,
        strategy: SelectionStrategy,
        config: &PlanningConfig,
    ) -> Result<Vec<FileGroup>> {
        // Scan for files
        let scan = table.scan().snapshot_id(snapshot_id).build()?;
        let file_scan_stream = scan.plan_files().await?;

        // Collect data files
        use futures::TryStreamExt;
        let all_tasks: Vec<FileScanTask> = file_scan_stream.try_collect().await?;

        let data_files: Vec<FileScanTask> = all_tasks
            .into_iter()
            .filter(|task| matches!(task.data_file_content, DataContentType::Data))
            .collect();

        // Apply selection strategy
        let filtered_files = strategy.filter(data_files, config);

        // Group files
        let groups = Self::group_files(filtered_files, config);

        // Calculate parallelism for each group
        let groups_with_parallelism: Vec<FileGroup> = groups
            .into_iter()
            .map(|g| Self::calculate_parallelism(g, config))
            .collect();

        Ok(groups_with_parallelism)
    }

    /// Groups files based on configuration.
    fn group_files(files: Vec<FileScanTask>, config: &PlanningConfig) -> Vec<FileGroup> {
        match &config.grouping_strategy {
            GroupingStrategy::Single => {
                if files.is_empty() {
                    vec![]
                } else {
                    vec![Self::create_file_group(files)]
                }
            }
            GroupingStrategy::BinPack {
                target_group_size_bytes,
            } => {
                let packer = ListPacker::new(*target_group_size_bytes);
                let bins = packer.pack(files, |task| task.length);

                bins.into_iter()
                    .map(Self::create_file_group)
                    .filter(|g| !g.is_empty())
                    .collect()
            }
        }
    }

    /// Creates a FileGroup from FileScanTasks, deduplicating delete files.
    fn create_file_group(data_files: Vec<FileScanTask>) -> FileGroup {
        let mut position_delete_map: HashMap<String, FileMetadata> = HashMap::new();
        let mut equality_delete_map: HashMap<String, FileMetadata> = HashMap::new();

        // Extract and deduplicate delete files
        for task in &data_files {
            for delete in &task.deletes {
                let metadata = FileMetadata {
                    file_path: delete.data_file_path.clone(),
                    file_size_bytes: delete.length,
                    record_count: delete.record_count,
                    sequence_number: Some(delete.sequence_number),
                    project_field_ids: delete.project_field_ids.clone(),
                    equality_ids: delete.equality_ids.clone(),
                };

                match delete.data_file_content {
                    DataContentType::PositionDeletes => {
                        position_delete_map
                            .entry(delete.data_file_path.clone())
                            .or_insert(metadata);
                    }
                    DataContentType::EqualityDeletes => {
                        equality_delete_map
                            .entry(delete.data_file_path.clone())
                            .or_insert(metadata);
                    }
                    _ => {}
                }
            }
        }

        // Convert data files to metadata
        let data_metadata: Vec<FileMetadata> = data_files
            .iter()
            .map(|task| FileMetadata {
                file_path: task.data_file_path.clone(),
                file_size_bytes: task.length,
                record_count: task.record_count,
                sequence_number: Some(task.sequence_number),
                project_field_ids: task.project_field_ids.clone(),
                equality_ids: None,
            })
            .collect();

        FileGroup::new(data_metadata)
            .with_position_deletes(position_delete_map.into_values().collect())
            .with_equality_deletes(equality_delete_map.into_values().collect())
    }

    /// Calculates executor and output parallelism for a file group.
    fn calculate_parallelism(mut group: FileGroup, config: &PlanningConfig) -> FileGroup {
        let total_bytes = group.total_size_bytes;
        let file_count = group.data_file_count;

        let partition_by_size =
            (total_bytes as f64 / config.min_size_per_partition as f64).ceil() as usize;
        let partition_by_count =
            (file_count as f64 / config.max_file_count_per_partition as f64).ceil() as usize;

        let executor_parallelism = partition_by_size
            .max(partition_by_count)
            .min(config.max_parallelism)
            .max(1);

        let output_parallelism =
            (total_bytes as f64 / config.target_file_size_bytes as f64).ceil() as usize;
        let output_parallelism = output_parallelism.min(config.max_parallelism).max(1);

        group.executor_parallelism = executor_parallelism;
        group.output_parallelism = output_parallelism;
        group
    }
}

/// Strategy for selecting which files to compact.
pub enum SelectionStrategy {
    /// Select files smaller than threshold
    SmallFiles { threshold_bytes: u64 },
    /// Select all files
    Full,
    /// Select files with delete files
    FilesWithDeletes { min_delete_count: usize },
}

impl SelectionStrategy {
    /// Creates strategy from CompactionStrategy enum.
    pub fn from_config(strategy: &CompactionStrategy, config: &PlanningConfig) -> Self {
        match strategy {
            CompactionStrategy::SmallFiles => SelectionStrategy::SmallFiles {
                threshold_bytes: config.small_file_threshold_bytes,
            },
            CompactionStrategy::Full => SelectionStrategy::Full,
            CompactionStrategy::FilesWithDeletes => SelectionStrategy::FilesWithDeletes {
                min_delete_count: config.min_delete_file_count_threshold,
            },
        }
    }

    /// Filters files based on strategy.
    fn filter(&self, files: Vec<FileScanTask>, _config: &PlanningConfig) -> Vec<FileScanTask> {
        match self {
            SelectionStrategy::SmallFiles { threshold_bytes } => files
                .into_iter()
                .filter(|task| task.length <= *threshold_bytes)
                .collect(),
            SelectionStrategy::Full => files,
            SelectionStrategy::FilesWithDeletes { min_delete_count } => files
                .into_iter()
                .filter(|task| task.deletes.len() >= *min_delete_count)
                .collect(),
        }
    }
}

/*
 * Configuration for distributed compaction.
 */

use derive_builder::Builder;
use serde::{Deserialize, Serialize};

/// Configuration for compaction planning.
#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
#[builder(setter(into), default)]
pub struct PlanningConfig {
    /// Target file size for output files (default: 1GB)
    pub target_file_size_bytes: u64,

    /// Small file threshold for SmallFiles strategy (default: 32MB)
    pub small_file_threshold_bytes: u64,

    /// Minimum delete file count threshold for FilesWithDeletes strategy
    pub min_delete_file_count_threshold: usize,

    /// Maximum parallelism for execution
    pub max_parallelism: usize,

    /// Minimum bytes per partition for parallelism calculation
    pub min_size_per_partition: u64,

    /// Maximum file count per partition
    pub max_file_count_per_partition: usize,

    /// Grouping strategy
    pub grouping_strategy: GroupingStrategy,
}

impl Default for PlanningConfig {
    fn default() -> Self {
        Self {
            target_file_size_bytes: 1024 * 1024 * 1024, // 1GB
            small_file_threshold_bytes: 32 * 1024 * 1024, // 32MB
            min_delete_file_count_threshold: 1,
            max_parallelism: 16,
            min_size_per_partition: 256 * 1024 * 1024, // 256MB
            max_file_count_per_partition: 100,
            grouping_strategy: GroupingStrategy::BinPack {
                target_group_size_bytes: 1024 * 1024 * 1024, // 1GB
            },
        }
    }
}

/// Configuration for worker execution.
#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
#[builder(setter(into), default)]
pub struct ExecutionConfig {
    /// Target file size for output files
    pub target_file_size_bytes: u64,

    /// Maximum rows per record batch
    pub max_record_batch_rows: usize,

    /// Maximum concurrent file closes
    pub max_concurrent_closes: usize,

    /// Enable dynamic size estimation for rolling writer
    pub enable_dynamic_size_estimation: bool,

    /// Size estimation smoothing factor
    pub size_estimation_smoothing_factor: f64,

    /// Sort columns (if sorting is enabled)
    pub sort_columns: Option<Vec<SortColumn>>,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            target_file_size_bytes: 1024 * 1024 * 1024, // 1GB
            max_record_batch_rows: 1024,
            max_concurrent_closes: 4,
            enable_dynamic_size_estimation: true,
            size_estimation_smoothing_factor: 0.2,
            sort_columns: None,
        }
    }
}

/// Grouping strategy for file selection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GroupingStrategy {
    /// All files in a single group
    Single,
    /// Bin-pack files into groups of target size
    BinPack { target_group_size_bytes: u64 },
}

/// Sort column specification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortColumn {
    pub name: String,
    pub descending: bool,
    pub nulls_first: bool,
}

/// Compaction strategy type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompactionStrategy {
    /// Compact small files below threshold
    SmallFiles,
    /// Compact all files
    Full,
    /// Compact files with delete files
    FilesWithDeletes,
}

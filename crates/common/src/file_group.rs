/*
 * FileGroup: A bundle of data files and associated delete files for compaction.
 *
 * This is adapted from the original iceberg-compaction repo's FileGroup,
 * but made serializable for distributed execution.
 */

use serde::{Deserialize, Serialize};

/// Metadata for a file to be compacted.
/// This is a serializable representation of iceberg::scan::FileScanTask.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadata {
    /// Path to the file
    pub file_path: String,
    /// File size in bytes
    pub file_size_bytes: u64,
    /// Record count (if known)
    pub record_count: Option<u64>,
    /// Sequence number for ordering (None for delete files without explicit seq num)
    pub sequence_number: Option<i64>,
    /// Field IDs to project (for equality deletes)
    pub project_field_ids: Vec<i32>,
    /// Equality IDs (for equality delete files)
    pub equality_ids: Option<Vec<i32>>,
}

/// A group of files to be compacted together.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileGroup {
    /// Data files to compact
    pub data_files: Vec<FileMetadata>,
    /// Position delete files (deduplicated)
    pub position_delete_files: Vec<FileMetadata>,
    /// Equality delete files (deduplicated)
    pub equality_delete_files: Vec<FileMetadata>,
    /// Total size of data files
    pub total_size_bytes: u64,
    /// Number of data files
    pub data_file_count: usize,
    /// Calculated executor parallelism
    pub executor_parallelism: usize,
    /// Calculated output parallelism
    pub output_parallelism: usize,
}

impl FileGroup {
    /// Creates a new FileGroup from data file metadata.
    pub fn new(data_files: Vec<FileMetadata>) -> Self {
        let total_size_bytes = data_files.iter().map(|f| f.file_size_bytes).sum();
        let data_file_count = data_files.len();

        Self {
            data_files,
            position_delete_files: Vec::new(),
            equality_delete_files: Vec::new(),
            total_size_bytes,
            data_file_count,
            executor_parallelism: 1,
            output_parallelism: 1,
        }
    }

    /// Sets position delete files.
    pub fn with_position_deletes(mut self, deletes: Vec<FileMetadata>) -> Self {
        self.position_delete_files = deletes;
        self
    }

    /// Sets equality delete files.
    pub fn with_equality_deletes(mut self, deletes: Vec<FileMetadata>) -> Self {
        self.equality_delete_files = deletes;
        self
    }

    /// Sets parallelism values.
    pub fn with_parallelism(mut self, executor: usize, output: usize) -> Self {
        self.executor_parallelism = executor;
        self.output_parallelism = output;
        self
    }

    /// Returns true if the group is empty.
    pub fn is_empty(&self) -> bool {
        self.data_files.is_empty()
    }

    /// Returns total input bytes including delete files.
    pub fn input_total_bytes(&self) -> u64 {
        self.data_files
            .iter()
            .chain(&self.position_delete_files)
            .chain(&self.equality_delete_files)
            .map(|f| f.file_size_bytes)
            .sum()
    }

    /// Returns total input file count including delete files.
    pub fn input_files_count(&self) -> usize {
        self.data_files.len() + self.position_delete_files.len() + self.equality_delete_files.len()
    }
}

/// For inline small deletes: actual delete data sent to workers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InlineDeleteData {
    /// Equality delete values: column names and their values
    pub equality_deletes: Vec<EqualityDeleteBatch>,
    /// Position deletes: (file_path, positions)
    pub position_deletes: Vec<PositionDeleteBatch>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EqualityDeleteBatch {
    /// Column names for equality matching
    pub columns: Vec<String>,
    /// Values to delete (each inner vec is one row's values as JSON)
    pub values: Vec<Vec<serde_json::Value>>,
    /// Sequence number of this delete batch
    pub sequence_number: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionDeleteBatch {
    /// File path these deletes apply to
    pub file_path: String,
    /// Row positions to delete
    pub positions: Vec<i64>,
}

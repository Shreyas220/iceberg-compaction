/*
 * Rolling Parquet Writer
 *
 * Streams record batches to Parquet files, rolling to new files
 * when the target size is reached. This enables bounded memory
 * usage during compaction.
 */

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use compaction_common::{CompactionError, Result};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::PathBuf;
use uuid::Uuid;

/// Information about a written file.
#[derive(Debug, Clone)]
pub struct WrittenFile {
    pub path: String,
    pub size_bytes: u64,
    pub record_count: u64,
}

/// Configuration for the rolling writer.
#[derive(Debug, Clone)]
pub struct RollingWriterConfig {
    /// Target size per file in bytes
    pub target_file_size_bytes: u64,
    /// Output directory
    pub output_dir: String,
    /// File prefix
    pub file_prefix: String,
    /// Compression codec
    pub compression: Compression,
    /// Enable dynamic size estimation
    pub enable_dynamic_estimation: bool,
    /// Smoothing factor for size estimation (0.0-1.0)
    pub estimation_smoothing: f64,
}

impl Default for RollingWriterConfig {
    fn default() -> Self {
        Self {
            target_file_size_bytes: 1024 * 1024 * 1024, // 1GB
            output_dir: "/tmp".to_string(),
            file_prefix: "compacted".to_string(),
            compression: Compression::ZSTD(Default::default()),
            enable_dynamic_estimation: true,
            estimation_smoothing: 0.2,
        }
    }
}

/// Rolling writer that streams batches to Parquet files.
pub struct RollingWriter {
    config: RollingWriterConfig,
    schema: SchemaRef,
    current_writer: Option<ActiveWriter>,
    completed_files: Vec<WrittenFile>,
    bytes_per_row_estimate: f64,
    total_rows_written: u64,
}

struct ActiveWriter {
    writer: ArrowWriter<File>,
    path: PathBuf,
    rows_written: u64,
    estimated_size: u64,
}

impl RollingWriter {
    /// Creates a new rolling writer.
    pub fn new(schema: SchemaRef, config: RollingWriterConfig) -> Self {
        Self {
            config,
            schema,
            current_writer: None,
            completed_files: Vec::new(),
            bytes_per_row_estimate: 0.0,
            total_rows_written: 0,
        }
    }

    /// Writes a record batch, potentially rolling to a new file.
    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        // Start new file if needed
        if self.current_writer.is_none() {
            self.start_new_file()?;
        }

        let writer = self.current_writer.as_mut().unwrap();

        // Write the batch
        writer.writer.write(batch)
            .map_err(|e| CompactionError::Execution(format!("Failed to write batch: {}", e)))?;

        writer.rows_written += batch.num_rows() as u64;

        // Update size estimate
        if self.config.enable_dynamic_estimation && self.bytes_per_row_estimate > 0.0 {
            writer.estimated_size = (writer.rows_written as f64 * self.bytes_per_row_estimate) as u64;
        }

        // Check if we should roll to new file
        if writer.estimated_size >= self.config.target_file_size_bytes {
            self.close_current_file()?;
        }

        Ok(())
    }

    /// Closes the current file and starts a new one.
    fn start_new_file(&mut self) -> Result<()> {
        let file_id = Uuid::now_v7();
        let filename = format!("{}_{}.parquet", self.config.file_prefix, file_id);
        let path = PathBuf::from(&self.config.output_dir).join(&filename);

        // Create output directory if needed
        std::fs::create_dir_all(&self.config.output_dir)
            .map_err(|e| CompactionError::Execution(format!("Failed to create output dir: {}", e)))?;

        let file = File::create(&path)
            .map_err(|e| CompactionError::Execution(format!("Failed to create file: {}", e)))?;

        let props = WriterProperties::builder()
            .set_compression(self.config.compression)
            .build();

        let writer = ArrowWriter::try_new(file, self.schema.clone(), Some(props))
            .map_err(|e| CompactionError::Execution(format!("Failed to create writer: {}", e)))?;

        self.current_writer = Some(ActiveWriter {
            writer,
            path,
            rows_written: 0,
            estimated_size: 0,
        });

        tracing::debug!("Started new file: {}", filename);

        Ok(())
    }

    /// Closes the current file.
    fn close_current_file(&mut self) -> Result<()> {
        if let Some(mut active) = self.current_writer.take() {
            // Close the writer
            active.writer.close()
                .map_err(|e| CompactionError::Execution(format!("Failed to close writer: {}", e)))?;

            // Get actual file size
            let metadata = std::fs::metadata(&active.path)
                .map_err(|e| CompactionError::Execution(format!("Failed to get file metadata: {}", e)))?;

            let actual_size = metadata.len();

            // Update bytes per row estimate
            if active.rows_written > 0 && self.config.enable_dynamic_estimation {
                let actual_bytes_per_row = actual_size as f64 / active.rows_written as f64;
                if self.bytes_per_row_estimate == 0.0 {
                    self.bytes_per_row_estimate = actual_bytes_per_row;
                } else {
                    // Exponential moving average
                    self.bytes_per_row_estimate = self.config.estimation_smoothing * actual_bytes_per_row
                        + (1.0 - self.config.estimation_smoothing) * self.bytes_per_row_estimate;
                }
            }

            self.total_rows_written += active.rows_written;

            self.completed_files.push(WrittenFile {
                path: active.path.to_string_lossy().to_string(),
                size_bytes: actual_size,
                record_count: active.rows_written,
            });

            tracing::debug!(
                "Closed file: {} ({} bytes, {} rows)",
                active.path.display(),
                actual_size,
                active.rows_written
            );
        }

        Ok(())
    }

    /// Finishes writing and returns all completed files.
    pub fn finish(mut self) -> Result<Vec<WrittenFile>> {
        self.close_current_file()?;
        Ok(self.completed_files)
    }

    /// Returns the total rows written so far.
    pub fn total_rows_written(&self) -> u64 {
        self.total_rows_written
            + self.current_writer.as_ref().map(|w| w.rows_written).unwrap_or(0)
    }

    /// Returns the number of completed files.
    pub fn completed_file_count(&self) -> usize {
        self.completed_files.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use tempfile::tempdir;

    #[test]
    fn test_rolling_writer_basic() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let dir = tempdir().unwrap();
        let config = RollingWriterConfig {
            output_dir: dir.path().to_string_lossy().to_string(),
            target_file_size_bytes: 1024, // Small for testing
            ..Default::default()
        };

        let mut writer = RollingWriter::new(schema.clone(), config);

        // Write some batches
        for _ in 0..5 {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec!["a", "b", "c"])),
                ],
            ).unwrap();

            writer.write_batch(&batch).unwrap();
        }

        let files = writer.finish().unwrap();
        assert!(!files.is_empty());

        // Verify files exist
        for file in &files {
            assert!(std::path::Path::new(&file.path).exists());
        }
    }
}

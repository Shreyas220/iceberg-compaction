/*
 * Production Rolling Parquet Writer
 *
 * Streams record batches to Parquet files on remote storage (S3/GCS/Azure),
 * rolling to new files when target size is reached.
 *
 * Features:
 * - opendal-backed writes (any storage backend)
 * - Async streaming writes
 * - Dynamic size estimation
 * - Retry logic with exponential backoff
 * - Proper resource cleanup
 */

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use compaction_common::{CompactionError, CompactionMetrics, Result};
use opendal::Operator;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

/// Information about a written file.
#[derive(Debug, Clone)]
pub struct WrittenFile {
    pub path: String,
    pub size_bytes: u64,
    pub record_count: u64,
    pub column_sizes: Option<Vec<(String, u64)>>,
    pub null_counts: Option<Vec<(String, u64)>>,
    pub min_values: Option<Vec<(String, Vec<u8>)>>,
    pub max_values: Option<Vec<(String, Vec<u8>)>>,
}

/// Configuration for the rolling writer.
#[derive(Debug, Clone)]
pub struct RollingWriterConfig {
    /// Target size per file in bytes
    pub target_file_size_bytes: u64,
    /// Output directory/prefix (e.g., "s3://bucket/warehouse/table/data/")
    pub output_prefix: String,
    /// File prefix for generated files
    pub file_prefix: String,
    /// Compression codec
    pub compression: Compression,
    /// Enable dynamic size estimation
    pub enable_dynamic_estimation: bool,
    /// Smoothing factor for size estimation (0.0-1.0)
    pub estimation_smoothing: f64,
    /// Max retries for write operations
    pub max_retries: u32,
    /// Base delay for retry backoff
    pub retry_base_delay: Duration,
    /// Row group size in rows
    pub row_group_size: usize,
}

impl Default for RollingWriterConfig {
    fn default() -> Self {
        Self {
            target_file_size_bytes: 512 * 1024 * 1024, // 512MB (leaves room for growth)
            output_prefix: String::new(),
            file_prefix: "compacted".to_string(),
            compression: Compression::UNCOMPRESSED,
            enable_dynamic_estimation: true,
            estimation_smoothing: 0.2,
            max_retries: 3,
            retry_base_delay: Duration::from_millis(100),
            row_group_size: 100_000, // 100K rows per row group (reduces memory)
        }
    }
}

/// Rolling writer that streams batches to Parquet files on remote storage.
pub struct RollingWriter {
    operator: Operator,
    config: RollingWriterConfig,
    schema: SchemaRef,
    current_writer: Option<InMemoryWriter>,
    completed_files: Vec<WrittenFile>,
    bytes_per_row_estimate: f64,
    total_rows_written: u64,
    metrics: Option<Arc<CompactionMetrics>>,
}

struct InMemoryWriter {
    writer: ArrowWriter<Cursor<Vec<u8>>>,
    /// Path relative to bucket for opendal operations
    storage_path: String,
    /// Full path including s3://bucket/ prefix for metadata
    metadata_path: String,
    rows_written: u64,
}

impl RollingWriter {
    /// Creates a new rolling writer with the given operator.
    pub fn new(operator: Operator, schema: SchemaRef, config: RollingWriterConfig) -> Self {
        Self {
            operator,
            config,
            schema,
            current_writer: None,
            completed_files: Vec::new(),
            bytes_per_row_estimate: 0.0,
            total_rows_written: 0,
            metrics: None,
        }
    }

    /// Sets the metrics collector.
    pub fn with_metrics(mut self, metrics: Arc<CompactionMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Writes a record batch, potentially rolling to a new file.
    pub async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        // Start new file if needed
        if self.current_writer.is_none() {
            self.start_new_file()?;
        }

        let writer = self.current_writer.as_mut().unwrap();

        // Write the batch to in-memory buffer
        writer
            .writer
            .write(batch)
            .map_err(|e| CompactionError::Execution(format!("Failed to write batch: {}", e)))?;

        writer.rows_written += batch.num_rows() as u64;

        // Roll file based on row count to limit memory usage
        // With ~50 bytes per row uncompressed, 500K rows ≈ 25MB
        // This limits ArrowWriter internal buffering
        const MAX_ROWS_PER_FILE: u64 = 500_000;

        let should_roll = if writer.rows_written >= MAX_ROWS_PER_FILE {
            true
        } else if self.config.enable_dynamic_estimation && self.bytes_per_row_estimate > 0.0 {
            let estimated_size = (writer.rows_written as f64 * self.bytes_per_row_estimate) as u64;
            estimated_size >= self.config.target_file_size_bytes
        } else {
            false
        };

        if should_roll {
            self.close_current_file().await?;
        }

        Ok(())
    }

    /// Starts a new file.
    fn start_new_file(&mut self) -> Result<()> {
        let file_id = Uuid::now_v7();
        let filename = format!("{}_{}.parquet", self.config.file_prefix, file_id);

        // Handle both full S3 URLs and relative paths
        // If output_prefix is s3://bucket/path, we need to extract just the path portion
        // for opendal (since the operator is already configured for the bucket)
        let (storage_path, metadata_path) = Self::build_paths(&self.config.output_prefix, &filename);

        let props = WriterProperties::builder()
            .set_compression(self.config.compression)
            .set_max_row_group_size(self.config.row_group_size)
            .set_write_batch_size(8192)
            .build();

        // Start with smaller buffer - it will grow as needed
        // With 500K rows per file at ~50 bytes/row, expect ~25MB files
        let write_buffer = Cursor::new(Vec::with_capacity(4 * 1024 * 1024)); // 4MB initial

        let writer = ArrowWriter::try_new(write_buffer, self.schema.clone(), Some(props))
            .map_err(|e| CompactionError::Execution(format!("Failed to create writer: {}", e)))?;

        self.current_writer = Some(InMemoryWriter {
            writer,
            storage_path: storage_path.clone(),
            metadata_path,
            rows_written: 0,
        });

        tracing::debug!("Started new file: {}", storage_path);

        Ok(())
    }

    /// Builds both storage path (for opendal) and metadata path (for DataFile)
    fn build_paths(output_prefix: &str, filename: &str) -> (String, String) {
        if output_prefix.starts_with("s3://") {
            // Parse s3://bucket/path
            let without_scheme = &output_prefix[5..];  // Remove "s3://"
            if let Some(slash_pos) = without_scheme.find('/') {
                let bucket = &without_scheme[..slash_pos];
                let path_part = &without_scheme[slash_pos + 1..];  // Path after bucket

                let storage_path = if path_part.ends_with('/') {
                    format!("{}{}", path_part, filename)
                } else if path_part.is_empty() {
                    filename.to_string()
                } else {
                    format!("{}/{}", path_part, filename)
                };

                let metadata_path = format!("s3://{}/{}", bucket, storage_path);
                (storage_path, metadata_path)
            } else {
                // Just bucket, no path
                (filename.to_string(), format!("{}/{}", output_prefix, filename))
            }
        } else {
            // Local or relative path
            let storage_path = if output_prefix.ends_with('/') {
                format!("{}{}", output_prefix, filename)
            } else if output_prefix.is_empty() {
                filename.to_string()
            } else {
                format!("{}/{}", output_prefix, filename)
            };
            (storage_path.clone(), storage_path)
        }
    }

    /// Closes the current file and uploads to storage.
    async fn close_current_file(&mut self) -> Result<()> {
        if let Some(active) = self.current_writer.take() {
            // Close the Parquet writer and get the buffer
            let buffer = active
                .writer
                .into_inner()
                .map_err(|e| CompactionError::Execution(format!("Failed to close writer: {}", e)))?;

            let data = buffer.into_inner();
            let actual_size = data.len() as u64;

            // Upload with retry using storage_path (relative to bucket)
            self.upload_with_retry(&active.storage_path, data).await?;

            // Update bytes per row estimate
            if active.rows_written > 0 && self.config.enable_dynamic_estimation {
                let actual_bytes_per_row = actual_size as f64 / active.rows_written as f64;
                if self.bytes_per_row_estimate == 0.0 {
                    self.bytes_per_row_estimate = actual_bytes_per_row;
                } else {
                    self.bytes_per_row_estimate = self.config.estimation_smoothing * actual_bytes_per_row
                        + (1.0 - self.config.estimation_smoothing) * self.bytes_per_row_estimate;
                }
            }

            self.total_rows_written += active.rows_written;

            // Record metrics
            if let Some(ref metrics) = self.metrics {
                metrics.record_bytes_written(actual_size);
                metrics.files_written.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }

            // Use metadata_path (full S3 URL) for the WrittenFile
            self.completed_files.push(WrittenFile {
                path: active.metadata_path.clone(),
                size_bytes: actual_size,
                record_count: active.rows_written,
                column_sizes: None,  // TODO: Extract from Parquet metadata
                null_counts: None,
                min_values: None,
                max_values: None,
            });

            tracing::info!(
                "Uploaded file: {} ({} bytes, {} rows)",
                active.metadata_path,
                actual_size,
                active.rows_written
            );
        }

        Ok(())
    }

    /// Uploads data with exponential backoff retry.
    async fn upload_with_retry(&self, path: &str, data: Vec<u8>) -> Result<()> {
        let bytes = Bytes::from(data);
        let mut attempt = 0;
        let mut delay = self.config.retry_base_delay;

        loop {
            match self.operator.write(path, bytes.clone()).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    attempt += 1;
                    if attempt > self.config.max_retries {
                        return Err(CompactionError::Storage(format!(
                            "Failed to upload {} after {} attempts: {}",
                            path, self.config.max_retries, e
                        )));
                    }

                    tracing::warn!(
                        "Upload attempt {} failed for {}: {}. Retrying in {:?}",
                        attempt,
                        path,
                        e,
                        delay
                    );

                    tokio::time::sleep(delay).await;
                    delay = std::cmp::min(delay * 2, Duration::from_secs(30));
                }
            }
        }
    }

    /// Finishes writing and returns all completed files.
    pub async fn finish(mut self) -> Result<Vec<WrittenFile>> {
        self.close_current_file().await?;
        Ok(self.completed_files)
    }

    /// Returns the total rows written so far.
    pub fn total_rows_written(&self) -> u64 {
        self.total_rows_written
            + self
                .current_writer
                .as_ref()
                .map(|w| w.rows_written)
                .unwrap_or(0)
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
    #[allow(unused_imports)]
    use opendal::Builder; // Trait must be in scope even if not called directly
    use std::sync::Arc;

    #[tokio::test]
    async fn test_rolling_writer_local() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        // Use local filesystem via opendal
        let temp_dir = tempfile::tempdir().unwrap();
        let fs = opendal::services::Fs::default()
            .root(temp_dir.path().to_str().unwrap());
        let operator = opendal::Operator::new(fs)
            .unwrap()
            .finish();

        let config = RollingWriterConfig {
            output_prefix: "/".to_string(),
            target_file_size_bytes: 1024, // Small for testing
            ..Default::default()
        };

        let mut writer = RollingWriter::new(operator, schema.clone(), config);

        // Write some batches
        for _ in 0..5 {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec!["a", "b", "c"])),
                ],
            )
            .unwrap();

            writer.write_batch(&batch).await.unwrap();
        }

        let files = writer.finish().await.unwrap();
        assert!(!files.is_empty());

        // Verify files exist
        for file in &files {
            let path = temp_dir.path().join(file.path.trim_start_matches('/'));
            assert!(path.exists(), "File should exist: {:?}", path);
        }
    }
}

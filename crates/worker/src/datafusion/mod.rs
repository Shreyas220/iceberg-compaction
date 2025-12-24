/*
 * DataFusion execution engine for compaction.
 *
 * This module handles:
 * 1. Registering data files and delete files as DataFusion tables
 * 2. Building SQL queries with ANTI JOINs for delete application
 * 3. Streaming execution and writing output files via opendal
 *
 * Adapted from iceberg-compaction/core/src/executor/datafusion/
 */

mod file_scan;
pub mod logical_plan_sketch;
mod sql_builder;

use arrow::datatypes::Schema as ArrowSchema;
use compaction_common::{CompactionError, CompactionMetrics, Result};
use compaction_proto::{CompactionTask, StorageCredentials};
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::StreamExt;
use iceberg::spec::{DataContentType, DataFile, DataFileBuilder, DataFileFormat};
use object_store::aws::AmazonS3Builder;
use parquet::basic::Compression;
use std::sync::Arc;
use url::Url;

pub use file_scan::FileScanProvider;
pub use sql_builder::SqlBuilder;

use crate::writer::{RollingWriter, RollingWriterConfig, WrittenFile};

// System hidden columns used for merge-on-read operations
pub const SYS_HIDDEN_SEQ_NUM: &str = "sys_hidden_seq_num";
pub const SYS_HIDDEN_FILE_PATH: &str = "sys_hidden_file_path";
pub const SYS_HIDDEN_POS: &str = "sys_hidden_pos";

/// DataFusion-based compaction engine.
#[allow(dead_code)]
pub struct DataFusionEngine {
    /// Maximum batch size for execution (reserved for future use)
    batch_size: usize,
    /// Optional metrics collector
    metrics: Option<Arc<CompactionMetrics>>,
}

impl DataFusionEngine {
    /// Creates a new DataFusion engine.
    pub fn new() -> Self {
        Self {
            batch_size: 8192,
            metrics: None,
        }
    }

    /// Creates an engine with custom batch size.
    pub fn with_batch_size(batch_size: usize) -> Self {
        Self {
            batch_size,
            metrics: None,
        }
    }

    /// Sets the metrics collector.
    pub fn with_metrics(mut self, metrics: Arc<CompactionMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Executes a compaction task using DataFusion.
    ///
    /// Returns the output DataFiles and total rows processed.
    #[tracing::instrument(skip(self, task), fields(task_id = %task.task_id))]
    pub async fn execute_compaction(
        &self,
        task: &CompactionTask,
    ) -> Result<(Vec<DataFile>, u64)> {
        let executor_parallelism = task.file_group.executor_parallelism;

        // Record metrics
        if let Some(ref metrics) = self.metrics {
            metrics.record_task_start();
        }

        // Build storage operator from task configuration (for writing output files)
        let operator = task.file_io_config.build_operator()?;

        // Create DataFusion session with memory limits
        // Limit batch size to 4096 to reduce memory spikes
        let batch_size = std::cmp::min(task.execution_config.max_record_batch_rows, 4096);

        let session_config = SessionConfig::new()
            .with_target_partitions(executor_parallelism)
            .with_batch_size(batch_size);

        // Create a memory pool with 256MB limit to control DataFusion memory usage
        // FairSpillPool will spill to disk if memory is exceeded (though we aim to stay under)
        let memory_pool = Arc::new(FairSpillPool::new(256 * 1024 * 1024)); // 256MB limit

        let runtime_env = RuntimeEnvBuilder::new()
            .with_memory_pool(memory_pool)
            .build()
            .map_err(|e| CompactionError::Execution(format!("Failed to build runtime: {}", e)))?;

        let ctx = SessionContext::new_with_config_rt(session_config, Arc::new(runtime_env));

        // Register S3 object store for DataFusion to read input files
        self.register_object_store(&ctx, task)?;

        // Deserialize schema
        let iceberg_schema: iceberg::spec::Schema = serde_json::from_str(&task.schema_json)
            .map_err(|e| CompactionError::Serialization(e.to_string()))?;

        // Convert to Arrow schema
        let arrow_schema = iceberg_schema_to_arrow(&iceberg_schema)?;
        let schema_ref = Arc::new(arrow_schema);

        // Determine what hidden columns we need
        let has_position_deletes = !task.file_group.position_delete_files.is_empty();
        let has_equality_deletes = !task.file_group.equality_delete_files.is_empty();

        // Register data files as a table
        FileScanProvider::register_data_table(
            &ctx,
            "data_table",
            &task.file_group.data_files,
            schema_ref.clone(),
            has_position_deletes,
            has_equality_deletes,
        ).await?;

        // Register position delete files if present
        if has_position_deletes {
            FileScanProvider::register_position_delete_table(
                &ctx,
                "position_delete_table",
                &task.file_group.position_delete_files,
            ).await?;
        }

        // Register equality delete files if present
        let _eq_table_names = if has_equality_deletes {
            FileScanProvider::register_equality_delete_tables(
                &ctx,
                "equality_delete_table",
                &task.file_group.equality_delete_files,
                &schema_ref,
            ).await?
        } else {
            Vec::new()
        };

        // Build the SQL query
        let project_columns: Vec<String> = iceberg_schema
            .as_struct()
            .fields()
            .iter()
            .map(|f| f.name.clone())
            .collect();

        let sql = SqlBuilder::new()
            .with_project_columns(project_columns)
            .with_data_table("data_table")
            .with_position_deletes(has_position_deletes, "position_delete_table")
            .with_equality_deletes(
                has_equality_deletes,
                self.extract_equality_columns(task)?,
            )
            .build()?;

        tracing::debug!("Generated SQL: {}", sql);

        // Execute the query
        let df = ctx.sql(&sql).await
            .map_err(|e| CompactionError::Execution(format!("SQL execution failed: {}", e)))?;

        // Get the result schema (without hidden columns)
        let output_schema: Arc<arrow::datatypes::Schema> = df.schema().clone().into();

        // Create rolling writer with opendal operator
        // Use smaller file size (64MB) to reduce memory usage - flushes more frequently
        let target_file_size = std::cmp::min(
            task.execution_config.target_file_size_bytes,
            64 * 1024 * 1024, // 64MB max to limit memory
        );
        let writer_config = RollingWriterConfig {
            target_file_size_bytes: target_file_size,
            output_prefix: task.output_location.clone(),
            file_prefix: format!("compacted_{}", task.task_id),
            compression: Compression::UNCOMPRESSED,
            enable_dynamic_estimation: task.execution_config.enable_dynamic_size_estimation,
            estimation_smoothing: task.execution_config.size_estimation_smoothing_factor,
            row_group_size: 10_000, // 10K rows per row group for lower memory
            ..Default::default()
        };

        let mut writer = RollingWriter::new(operator, output_schema, writer_config);

        // Attach metrics if available
        if let Some(ref metrics) = self.metrics {
            writer = writer.with_metrics(metrics.clone());
        }

        // Stream results through writer
        let mut stream = df.execute_stream().await
            .map_err(|e| CompactionError::Execution(format!("Failed to execute stream: {}", e)))?;

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result
                .map_err(|e| CompactionError::Execution(format!("Batch error: {}", e)))?;

            // Async write to remote storage
            writer.write_batch(&batch).await?;
        }

        let total_rows = writer.total_rows_written();

        // Async finish - uploads final file
        let written_files = writer.finish().await?;

        // Convert written files to DataFiles
        let data_files = self.convert_to_data_files(written_files, task)?;

        // Record completion metrics
        if let Some(ref metrics) = self.metrics {
            metrics.record_rows_processed(total_rows);
            metrics.record_task_complete(true);
        }

        tracing::info!(
            "Compaction complete: {} files written, {} rows processed",
            data_files.len(),
            total_rows
        );

        Ok((data_files, total_rows))
    }

    /// Extracts equality column names from equality delete files.
    fn extract_equality_columns(&self, task: &CompactionTask) -> Result<Vec<Vec<String>>> {
        let schema: iceberg::spec::Schema = serde_json::from_str(&task.schema_json)
            .map_err(|e| CompactionError::Serialization(e.to_string()))?;

        let mut result = Vec::new();

        for delete_file in &task.file_group.equality_delete_files {
            if let Some(equality_ids) = &delete_file.equality_ids {
                let columns: Vec<String> = equality_ids
                    .iter()
                    .filter_map(|id| schema.field_by_id(*id).map(|f| f.name.clone()))
                    .collect();
                if !columns.is_empty() {
                    result.push(columns);
                }
            }
        }

        Ok(result)
    }

    /// Converts written files to Iceberg DataFiles.
    ///
    /// Creates proper DataFile instances with partition values inherited from input files,
    /// and basic column statistics extracted from the written Parquet files.
    fn convert_to_data_files(
        &self,
        written_files: Vec<WrittenFile>,
        task: &CompactionTask,
    ) -> Result<Vec<DataFile>> {
        let partition_spec: iceberg::spec::PartitionSpec = serde_json::from_str(&task.partition_spec_json)
            .map_err(|e| CompactionError::Serialization(e.to_string()))?;

        let iceberg_schema: iceberg::spec::Schema = serde_json::from_str(&task.schema_json)
            .map_err(|e| CompactionError::Serialization(e.to_string()))?;

        // Inherit partition values from the first input file (all files in a task should share partition)
        let partition_values = if let Some(first_data_file) = task.file_group.data_files.first() {
            first_data_file.partition_values.clone()
        } else {
            std::collections::HashMap::new()
        };

        // Convert partition values to Iceberg Struct format
        let partition_struct = self.build_partition_struct(&partition_spec, &partition_values)?;

        let mut data_files = Vec::with_capacity(written_files.len());

        for written in written_files {
            // Build column size map if available
            let column_sizes: std::collections::HashMap<i32, u64> = written
                .column_sizes
                .as_ref()
                .map(|sizes| {
                    sizes.iter()
                        .filter_map(|(name, size)| {
                            iceberg_schema
                                .field_by_name(name)
                                .map(|f| (f.id, *size))
                        })
                        .collect()
                })
                .unwrap_or_default();

            // Build null count map if available
            let null_counts: std::collections::HashMap<i32, u64> = written
                .null_counts
                .as_ref()
                .map(|counts| {
                    counts.iter()
                        .filter_map(|(name, count)| {
                            iceberg_schema
                                .field_by_name(name)
                                .map(|f| (f.id, *count))
                        })
                        .collect()
                })
                .unwrap_or_default();

            // Build value bounds if available
            let lower_bounds: std::collections::HashMap<i32, iceberg::spec::Datum> = written
                .min_values
                .as_ref()
                .map(|mins| {
                    mins.iter()
                        .filter_map(|(name, bytes)| {
                            iceberg_schema.field_by_name(name).and_then(|f| {
                                self.bytes_to_datum(&f.field_type, bytes)
                                    .map(|d| (f.id, d))
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();

            let upper_bounds: std::collections::HashMap<i32, iceberg::spec::Datum> = written
                .max_values
                .as_ref()
                .map(|maxs| {
                    maxs.iter()
                        .filter_map(|(name, bytes)| {
                            iceberg_schema.field_by_name(name).and_then(|f| {
                                self.bytes_to_datum(&f.field_type, bytes)
                                    .map(|d| (f.id, d))
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();

            // Build the DataFile
            let data_file = DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(written.path.clone())
                .file_format(DataFileFormat::Parquet)
                .partition(partition_struct.clone())
                .record_count(written.record_count)
                .file_size_in_bytes(written.size_bytes)
                .column_sizes(column_sizes)
                .null_value_counts(null_counts)
                .lower_bounds(lower_bounds)
                .upper_bounds(upper_bounds)
                .build()
                .map_err(|e| CompactionError::Execution(format!("Failed to build DataFile: {}", e)))?;

            data_files.push(data_file);
        }

        tracing::info!(
            "Created {} DataFiles from written files",
            data_files.len()
        );

        Ok(data_files)
    }

    /// Builds a partition Struct from partition spec and values.
    fn build_partition_struct(
        &self,
        spec: &iceberg::spec::PartitionSpec,
        values: &std::collections::HashMap<String, String>,
    ) -> Result<iceberg::spec::Struct> {
        use iceberg::spec::Literal;

        let literals: Vec<Option<Literal>> = spec
            .fields()
            .iter()
            .map(|field| {
                values.get(&field.name).map(|v| {
                    // Parse as string literal - production would need type-aware parsing
                    Literal::string(v.clone())
                })
            })
            .collect();

        Ok(iceberg::spec::Struct::from_iter(literals))
    }

    /// Converts raw bytes to an Iceberg Datum based on the field type.
    fn bytes_to_datum(&self, field_type: &iceberg::spec::Type, bytes: &[u8]) -> Option<iceberg::spec::Datum> {
        use iceberg::spec::{Datum, PrimitiveType, Type};

        match field_type {
            Type::Primitive(prim) => match prim {
                PrimitiveType::Boolean if bytes.len() >= 1 => {
                    Some(Datum::bool(bytes[0] != 0))
                }
                PrimitiveType::Int if bytes.len() >= 4 => {
                    let val = i32::from_le_bytes(bytes[..4].try_into().ok()?);
                    Some(Datum::int(val))
                }
                PrimitiveType::Long if bytes.len() >= 8 => {
                    let val = i64::from_le_bytes(bytes[..8].try_into().ok()?);
                    Some(Datum::long(val))
                }
                PrimitiveType::Float if bytes.len() >= 4 => {
                    let val = f32::from_le_bytes(bytes[..4].try_into().ok()?);
                    Some(Datum::float(val))
                }
                PrimitiveType::Double if bytes.len() >= 8 => {
                    let val = f64::from_le_bytes(bytes[..8].try_into().ok()?);
                    Some(Datum::double(val))
                }
                PrimitiveType::String => {
                    let s = std::str::from_utf8(bytes).ok()?;
                    Some(Datum::string(s))
                }
                PrimitiveType::Binary => {
                    Some(Datum::binary(bytes.to_vec()))
                }
                _ => None, // Complex types would need more handling
            },
            _ => None, // Struct/List/Map types not handled for bounds
        }
    }

    /// Registers the object store (S3/GCS/etc) with DataFusion's runtime.
    ///
    /// This enables DataFusion to read Parquet files from cloud storage.
    fn register_object_store(&self, ctx: &SessionContext, task: &CompactionTask) -> Result<()> {
        // Build the S3 object store from task credentials
        let config = &task.file_io_config;

        // Parse the bucket from the first data file path
        let first_file = task.file_group.data_files.first()
            .ok_or_else(|| CompactionError::Execution("No data files in task".to_string()))?;

        // Parse the S3 URL to get the bucket name
        let url = Url::parse(&first_file.file_path)
            .map_err(|e| CompactionError::Execution(format!("Invalid file path URL: {}", e)))?;

        let bucket = url.host_str()
            .ok_or_else(|| CompactionError::Execution("Cannot extract bucket from URL".to_string()))?;

        tracing::debug!("Registering S3 object store for bucket: {}", bucket);

        // Build S3 object store based on credentials type
        let region = config.region.clone().unwrap_or_else(|| "us-east-1".to_string());
        let mut s3_builder = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region(&region);

        // Set endpoint if provided (for MinIO/LocalStack)
        if let Some(endpoint) = &config.endpoint {
            s3_builder = s3_builder
                .with_endpoint(endpoint)
                .with_allow_http(true)  // MinIO typically uses HTTP locally
                .with_virtual_hosted_style_request(false);  // Use path-style for MinIO
        }

        // Set credentials
        if let Some(creds) = &config.credentials {
            match creds {
                StorageCredentials::AwsAccessKey {
                    access_key_id,
                    secret_access_key,
                    session_token,
                } => {
                    s3_builder = s3_builder
                        .with_access_key_id(access_key_id)
                        .with_secret_access_key(secret_access_key);

                    if let Some(token) = session_token {
                        s3_builder = s3_builder.with_token(token);
                    }
                }
                StorageCredentials::GcpServiceAccount { .. } => {
                    // GCP credentials not applicable for S3
                    return Err(CompactionError::Execution(
                        "GCP credentials cannot be used with S3".to_string()
                    ));
                }
                StorageCredentials::AzureStorageKey { .. }
                | StorageCredentials::AzureSasToken { .. } => {
                    // Azure credentials not applicable for S3
                    return Err(CompactionError::Execution(
                        "Azure credentials cannot be used with S3".to_string()
                    ));
                }
            }
        }

        // Build the object store
        let s3_store = s3_builder.build()
            .map_err(|e| CompactionError::Storage(format!("Failed to build S3 object store: {}", e)))?;

        // Register with DataFusion's runtime
        let s3_url = Url::parse(&format!("s3://{}", bucket))
            .map_err(|e| CompactionError::Execution(format!("Invalid S3 URL: {}", e)))?;

        ctx.runtime_env()
            .register_object_store(&s3_url, Arc::new(s3_store));

        tracing::debug!("Registered S3 object store for bucket: {}", bucket);

        Ok(())
    }
}

impl Default for DataFusionEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Converts an Iceberg schema to Arrow schema.
fn iceberg_schema_to_arrow(schema: &iceberg::spec::Schema) -> Result<ArrowSchema> {
    use arrow::datatypes::Field;

    let fields: Vec<Field> = schema
        .as_struct()
        .fields()
        .iter()
        .map(|f| {
            let data_type = iceberg_type_to_arrow(&f.field_type);
            Field::new(&f.name, data_type, !f.required)
        })
        .collect();

    Ok(ArrowSchema::new(fields))
}

/// Converts an Iceberg type to Arrow DataType.
fn iceberg_type_to_arrow(ty: &iceberg::spec::Type) -> arrow::datatypes::DataType {
    use arrow::datatypes::DataType;
    use iceberg::spec::{PrimitiveType, Type};

    match ty {
        Type::Primitive(prim) => match prim {
            PrimitiveType::Boolean => DataType::Boolean,
            PrimitiveType::Int => DataType::Int32,
            PrimitiveType::Long => DataType::Int64,
            PrimitiveType::Float => DataType::Float32,
            PrimitiveType::Double => DataType::Float64,
            PrimitiveType::Date => DataType::Date32,
            PrimitiveType::Timestamp => DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                None,
            ),
            PrimitiveType::Timestamptz => DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                Some("UTC".into()),
            ),
            PrimitiveType::String => DataType::Utf8,
            PrimitiveType::Binary => DataType::Binary,
            PrimitiveType::Uuid => DataType::FixedSizeBinary(16),
            PrimitiveType::Fixed(len) => DataType::FixedSizeBinary(*len as i32),
            PrimitiveType::Decimal { precision, scale } => {
                DataType::Decimal128(*precision as u8, *scale as i8)
            }
            _ => DataType::Binary, // Fallback for time types
        },
        Type::Struct(s) => {
            let fields: Vec<_> = s
                .fields()
                .iter()
                .map(|f| {
                    arrow::datatypes::Field::new(
                        &f.name,
                        iceberg_type_to_arrow(&f.field_type),
                        !f.required,
                    )
                })
                .collect();
            DataType::Struct(fields.into())
        }
        Type::List(l) => {
            let elem_type = iceberg_type_to_arrow(&l.element_field.field_type);
            DataType::List(Arc::new(arrow::datatypes::Field::new(
                "element",
                elem_type,
                !l.element_field.required,
            )))
        }
        Type::Map(m) => {
            let key_type = iceberg_type_to_arrow(&m.key_field.field_type);
            let value_type = iceberg_type_to_arrow(&m.value_field.field_type);
            DataType::Map(
                Arc::new(arrow::datatypes::Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            arrow::datatypes::Field::new("key", key_type, false),
                            arrow::datatypes::Field::new("value", value_type, !m.value_field.required),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            )
        }
    }
}

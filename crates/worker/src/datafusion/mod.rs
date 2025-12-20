/*
 * DataFusion execution engine for compaction.
 *
 * This module handles:
 * 1. Registering data files and delete files as DataFusion tables
 * 2. Building SQL queries with ANTI JOINs for delete application
 * 3. Streaming execution and writing output files
 *
 * Adapted from iceberg-compaction/core/src/executor/datafusion/
 */

mod file_scan;
mod sql_builder;

use arrow::datatypes::Schema as ArrowSchema;
use compaction_common::{CompactionError, Result};
use compaction_proto::CompactionTask;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::StreamExt;
use iceberg::spec::DataFile;
use parquet::basic::Compression;
use std::sync::Arc;

pub use file_scan::FileScanProvider;
pub use sql_builder::SqlBuilder;

use crate::writer::{RollingWriter, RollingWriterConfig, WrittenFile};

// System hidden columns used for merge-on-read operations
pub const SYS_HIDDEN_SEQ_NUM: &str = "sys_hidden_seq_num";
pub const SYS_HIDDEN_FILE_PATH: &str = "sys_hidden_file_path";
pub const SYS_HIDDEN_POS: &str = "sys_hidden_pos";

/// DataFusion-based compaction engine.
pub struct DataFusionEngine {
    /// Maximum batch size for execution
    batch_size: usize,
}

impl DataFusionEngine {
    /// Creates a new DataFusion engine.
    pub fn new() -> Self {
        Self {
            batch_size: 8192,
        }
    }

    /// Creates an engine with custom batch size.
    pub fn with_batch_size(batch_size: usize) -> Self {
        Self { batch_size }
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

        // Create DataFusion session
        let session_config = SessionConfig::new()
            .with_target_partitions(executor_parallelism)
            .with_batch_size(task.execution_config.max_record_batch_rows);

        let ctx = SessionContext::new_with_config(session_config);

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
        let eq_table_names = if has_equality_deletes {
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
        let output_schema = df.schema().clone().into();

        // Create rolling writer
        let writer_config = RollingWriterConfig {
            target_file_size_bytes: task.execution_config.target_file_size_bytes,
            output_dir: task.output_location.clone(),
            file_prefix: format!("compacted_{}", task.task_id),
            compression: Compression::ZSTD(Default::default()),
            enable_dynamic_estimation: task.execution_config.enable_dynamic_size_estimation,
            estimation_smoothing: task.execution_config.size_estimation_smoothing_factor,
        };

        let mut writer = RollingWriter::new(output_schema, writer_config);

        // Stream results through writer
        let mut stream = df.execute_stream().await
            .map_err(|e| CompactionError::Execution(format!("Failed to execute stream: {}", e)))?;

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result
                .map_err(|e| CompactionError::Execution(format!("Batch error: {}", e)))?;

            writer.write_batch(&batch)?;
        }

        let total_rows = writer.total_rows_written();
        let written_files = writer.finish()?;

        // Convert written files to DataFiles
        let data_files = self.convert_to_data_files(written_files, task)?;

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
    fn convert_to_data_files(
        &self,
        written_files: Vec<WrittenFile>,
        task: &CompactionTask,
    ) -> Result<Vec<DataFile>> {
        // Note: Full DataFile creation requires partition values, metrics, etc.
        // This is a simplified implementation - production would need more metadata
        let _partition_spec: iceberg::spec::PartitionSpec = serde_json::from_str(&task.partition_spec_json)
            .map_err(|e| CompactionError::Serialization(e.to_string()))?;

        // For now, we return an empty vec and rely on the OutputFileInfo serialization
        // A full implementation would use iceberg::spec::DataFileBuilder
        tracing::debug!(
            "Would create {} DataFiles from written files",
            written_files.len()
        );

        // TODO: Create proper DataFile instances with:
        // - Partition values from input files
        // - Column statistics
        // - Split offsets
        // - Sort order ID

        Ok(vec![])
    }
}

impl Default for DataFusionEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Converts an Iceberg schema to Arrow schema.
fn iceberg_schema_to_arrow(schema: &iceberg::spec::Schema) -> Result<ArrowSchema> {
    use arrow::datatypes::{DataType, Field};

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

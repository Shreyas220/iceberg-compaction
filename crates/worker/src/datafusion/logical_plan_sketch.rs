/*
 * Logical plan + streaming writer sketch for equality deletes.
 *
 * This is intentionally minimal and opinionated:
 * - Equality deletes only
 * - LogicalPlanBuilder (no SQL)
 * - Streaming Parquet writer using OpenDAL writer_with + AsyncArrowWriter
 * - Size-based rolling at row-group boundaries
 */

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use compaction_common::{CompactionError, Result};
use datafusion::datasource::TableProvider;
use datafusion::datasource::provider_as_source;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{Expr, JoinType, LogicalPlan, LogicalPlanBuilder, SortExpr};
use datafusion::prelude::col;
use futures::{FutureExt, StreamExt};
use opendal::Operator;
use parquet::arrow::AsyncArrowWriter;
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::basic::Compression;
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;

/// Minimal logical plan builder for equality deletes.
pub struct EqualityDeletePlanBuilder;

impl EqualityDeletePlanBuilder {
    /// Build a logical plan that applies equality deletes using LEFT ANTI join.
    ///
    /// Join predicate:
    ///   (data.key = del.key ...) AND (data.seq < del.seq)
    pub fn build(
        data_table: Arc<dyn TableProvider>,
        delete_table: Arc<dyn TableProvider>,
        data_alias: &str,
        delete_alias: &str,
        key_columns: &[&str],
        data_seq_col: &str,
        delete_seq_col: &str,
        sort_exprs: Option<Vec<SortExpr>>,
    ) -> Result<LogicalPlan> {
        let data_scan = LogicalPlanBuilder::scan(data_alias, provider_as_source(data_table), None)
            .map_err(|e| CompactionError::Execution(e.to_string()))?
            .build()
            .map_err(|e| CompactionError::Execution(e.to_string()))?;

        let delete_scan =
            LogicalPlanBuilder::scan(delete_alias, provider_as_source(delete_table), None)
                .map_err(|e| CompactionError::Execution(e.to_string()))?
                .build()
                .map_err(|e| CompactionError::Execution(e.to_string()))?;

        let mut join_exprs: Vec<Expr> = key_columns
            .iter()
            .map(|col_name| {
                col(format!("{}.{}", data_alias, col_name))
                    .eq(col(format!("{}.{}", delete_alias, col_name)))
            })
            .collect();

        join_exprs.push(
            col(format!("{}.{}", data_alias, data_seq_col))
                .lt(col(format!("{}.{}", delete_alias, delete_seq_col))),
        );

        let plan = LogicalPlanBuilder::from(data_scan)
            .join_on(delete_scan, JoinType::LeftAnti, join_exprs)
            .map_err(|e| CompactionError::Execution(e.to_string()))?
            .build()
            .map_err(|e| CompactionError::Execution(e.to_string()))?;

        let plan = if let Some(sort_exprs) = sort_exprs {
            LogicalPlanBuilder::from(plan)
                .sort(sort_exprs)
                .map_err(|e| CompactionError::Execution(e.to_string()))?
                .build()
                .map_err(|e| CompactionError::Execution(e.to_string()))?
        } else {
            plan
        };

        Ok(plan)
    }
}

/// Rolling writer defaults (size-based, no compression, no spill).
#[derive(Debug, Clone)]
pub struct RollingWriterConfig {
    /// Output prefix within the bucket (e.g. "sketch/output")
    pub output_prefix: String,
    /// File name prefix (e.g. "part")
    pub file_prefix: String,
    /// Target file size in bytes
    pub target_file_size_bytes: u64,
    /// Parquet row group size in rows
    pub row_group_rows: usize,
    /// Multipart chunk size in bytes (must be >= 5MB for S3)
    pub multipart_chunk_bytes: usize,
    /// Compression codec
    pub compression: Compression,
}

impl Default for RollingWriterConfig {
    fn default() -> Self {
        Self {
            output_prefix: "".to_string(),
            file_prefix: "part".to_string(),
            target_file_size_bytes: 256 * 1024 * 1024,
            row_group_rows: 300_000,
            multipart_chunk_bytes: 8 * 1024 * 1024,
            compression: Compression::UNCOMPRESSED,
        }
    }
}

/// Minimal size-based rolling writer that streams parquet to object storage.
pub struct RollingParquetWriter {
    operator: Operator,
    schema: SchemaRef,
    config: RollingWriterConfig,
    file_index: u64,
    current: Option<AsyncArrowWriter<OpendalFileWriter>>,
    current_path: Option<String>,
    completed_paths: Vec<String>,
}

impl RollingParquetWriter {
    pub fn new(operator: Operator, schema: SchemaRef, config: RollingWriterConfig) -> Self {
        Self {
            operator,
            schema,
            config,
            file_index: 0,
            current: None,
            current_path: None,
            completed_paths: Vec::new(),
        }
    }

    pub async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.current.is_none() {
            self.start_new_file().await?;
        }

        let writer = self.current.as_mut().expect("writer must exist");
        writer
            .write(batch)
            .await
            .map_err(|e| CompactionError::Execution(e.to_string()))?;

        if writer.bytes_written() as u64 >= self.config.target_file_size_bytes {
            self.finish_current_file().await?;
        }

        Ok(())
    }

    pub async fn finish(mut self) -> Result<Vec<String>> {
        self.finish_current_file().await?;
        Ok(self.completed_paths)
    }

    async fn start_new_file(&mut self) -> Result<()> {
        let filename = format!("{}-{:06}.parquet", self.config.file_prefix, self.file_index);
        self.file_index += 1;

        let path = if self.config.output_prefix.is_empty() {
            filename
        } else {
            format!(
                "{}/{}",
                self.config.output_prefix.trim_end_matches('/'),
                filename
            )
        };

        let writer = self
            .operator
            .writer_with(&path)
            .chunk(self.config.multipart_chunk_bytes)
            .await
            .map_err(|e| CompactionError::Storage(e.to_string()))?;

        let async_write = OpendalFileWriter { inner: writer };

        let props = WriterProperties::builder()
            .set_compression(self.config.compression)
            .set_max_row_group_size(self.config.row_group_rows)
            .build();

        let parquet_writer =
            AsyncArrowWriter::try_new(async_write, self.schema.clone(), Some(props))
                .map_err(|e| CompactionError::Execution(e.to_string()))?;

        self.current = Some(parquet_writer);
        self.current_path = Some(path);

        Ok(())
    }

    async fn finish_current_file(&mut self) -> Result<()> {
        if let Some(mut writer) = self.current.take() {
            let _metadata = writer
                .finish()
                .await
                .map_err(|e| CompactionError::Execution(e.to_string()))?;

            if let Some(path) = self.current_path.take() {
                self.completed_paths.push(path);
            }
        }
        Ok(())
    }
}

/// Execute a logical plan and stream the results into a rolling parquet writer.
pub async fn execute_plan_to_parquet(
    ctx: &SessionContext,
    plan: LogicalPlan,
    writer: &mut RollingParquetWriter,
) -> Result<()> {
    let df = ctx
        .execute_logical_plan(plan)
        .await
        .map_err(|e| CompactionError::Execution(e.to_string()))?;

    let mut stream = df
        .execute_stream()
        .await
        .map_err(|e| CompactionError::Execution(e.to_string()))?;

    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(|e| CompactionError::Execution(e.to_string()))?;
        writer.write_batch(&batch).await?;
    }

    Ok(())
}

struct OpendalFileWriter {
    inner: opendal::Writer,
}

impl AsyncFileWriter for OpendalFileWriter {
    fn write(&mut self, bs: Bytes) -> futures::future::BoxFuture<'_, parquet::errors::Result<()>> {
        async move {
            self.inner
                .write(bs)
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))?;
            Ok(())
        }
        .boxed()
    }

    fn complete(&mut self) -> futures::future::BoxFuture<'_, parquet::errors::Result<()>> {
        async move {
            self.inner
                .close()
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))?;
            Ok(())
        }
        .boxed()
    }
}

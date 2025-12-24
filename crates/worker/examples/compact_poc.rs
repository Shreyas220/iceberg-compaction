/*
 * Compaction POC - S3 Storage with Benchmarking
 *
 * Run with: cargo run --example compact_poc --release
 *
 * This demonstrates:
 * 1. Session/schema reuse (the "plan reuse" optimization)
 * 2. Streaming scan via DataFusion from S3
 * 3. Rolling Parquet writer with multipart upload to S3
 * 4. Benchmarking with memory tracking and multiple runs
 *
 * MinIO Configuration:
 * - Endpoint: http://localhost:9000
 * - Access Key: admin
 * - Secret Key: password
 * - Bucket: warehouse
 */

// Use jemalloc for better handling of large allocations
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::*;
use futures::{FutureExt, StreamExt, TryStreamExt};
use memory_stats::memory_stats;
use object_store::aws::AmazonS3Builder;
use opendal::Operator;
use parquet::arrow::AsyncArrowWriter;
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::basic::Compression;
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use url::Url;

// ============================================================================
// S3 Configuration
// ============================================================================

const S3_ENDPOINT: &str = "http://localhost:9000";
const S3_ACCESS_KEY: &str = "admin";
const S3_SECRET_KEY: &str = "password";
const S3_BUCKET: &str = "warehouse";
const S3_REGION: &str = "us-east-1";

// ============================================================================
// Benchmark Configuration
// ============================================================================

const NUM_BENCHMARK_RUNS: usize = 3;
const NUM_FILE_SETS: usize = 1;
const FILES_PER_SET: usize = 5;
const ROWS_PER_FILE: usize = 350_000;
const ROW_GROUP_SIZE: usize = 300_000; // Large row groups - jemalloc should handle this
const RECREATE_ENGINE_EACH_RUN: bool = false;

// ============================================================================
// Memory Tracker
// ============================================================================

#[derive(Debug, Clone)]
pub struct MemorySnapshot {
    pub physical_mem_bytes: u64,
    pub virtual_mem_bytes: u64,
    pub timestamp: Instant,
}

impl MemorySnapshot {
    pub fn capture() -> Self {
        let (physical, virtual_mem) = if let Some(usage) = memory_stats() {
            (usage.physical_mem as u64, usage.virtual_mem as u64)
        } else {
            (0, 0)
        };

        Self {
            physical_mem_bytes: physical,
            virtual_mem_bytes: virtual_mem,
            timestamp: Instant::now(),
        }
    }

    pub fn physical_mb(&self) -> f64 {
        self.physical_mem_bytes as f64 / (1024.0 * 1024.0)
    }
}

#[derive(Debug)]
pub struct BenchmarkMetrics {
    pub run_id: usize,
    pub file_set_id: usize,
    pub input_files: usize,
    pub input_rows: u64,
    pub input_bytes: u64,
    pub output_files: usize,
    pub output_rows: u64,
    pub output_bytes: u64,

    // Timing
    pub total_duration: Duration,
    pub read_duration: Duration,
    pub write_duration: Duration,

    // Memory
    pub mem_before: MemorySnapshot,
    pub mem_peak: MemorySnapshot,
    pub mem_after: MemorySnapshot,
}

impl BenchmarkMetrics {
    pub fn throughput_mb_per_sec(&self) -> f64 {
        let total_mb = self.input_bytes as f64 / (1024.0 * 1024.0);
        let secs = self.total_duration.as_secs_f64();
        if secs > 0.0 { total_mb / secs } else { 0.0 }
    }

    pub fn peak_memory_mb(&self) -> f64 {
        self.mem_peak.physical_mb()
    }

    pub fn memory_delta_mb(&self) -> f64 {
        self.mem_peak.physical_mb() - self.mem_before.physical_mb()
    }
}

#[derive(Debug)]
pub struct BenchmarkSummary {
    pub runs: Vec<BenchmarkMetrics>,
}

impl BenchmarkSummary {
    pub fn new() -> Self {
        Self { runs: Vec::new() }
    }

    pub fn add(&mut self, metrics: BenchmarkMetrics) {
        self.runs.push(metrics);
    }

    pub fn print_summary(&self) {
        if self.runs.is_empty() {
            println!("No benchmark runs recorded.");
            return;
        }

        println!();
        println!(
            "╔═══════════════════════════════════════════════════════════════════════════════╗"
        );
        println!(
            "║                           BENCHMARK SUMMARY                                    ║"
        );
        println!(
            "╠═══════════════════════════════════════════════════════════════════════════════╣"
        );

        // Per-run details
        println!(
            "║                                                                                 ║"
        );
        println!(
            "║  Individual Runs:                                                               ║"
        );
        println!(
            "║  ─────────────────────────────────────────────────────────────────────────────  ║"
        );

        for m in &self.runs {
            println!(
                "║  Run {}-{}: {:>6.2}s | {:>6.1} MB/s | Peak: {:>6.1} MB | Delta: {:>+6.1} MB        ║",
                m.file_set_id,
                m.run_id,
                m.total_duration.as_secs_f64(),
                m.throughput_mb_per_sec(),
                m.peak_memory_mb(),
                m.memory_delta_mb()
            );
        }

        // Aggregated stats
        let durations: Vec<f64> = self
            .runs
            .iter()
            .map(|m| m.total_duration.as_secs_f64())
            .collect();
        let throughputs: Vec<f64> = self
            .runs
            .iter()
            .map(|m| m.throughput_mb_per_sec())
            .collect();
        let peak_mems: Vec<f64> = self.runs.iter().map(|m| m.peak_memory_mb()).collect();
        let delta_mems: Vec<f64> = self.runs.iter().map(|m| m.memory_delta_mb()).collect();

        let (dur_min, dur_max, dur_avg) = stats(&durations);
        let (tp_min, tp_max, tp_avg) = stats(&throughputs);
        let (mem_min, mem_max, mem_avg) = stats(&peak_mems);
        let (delta_min, delta_max, delta_avg) = stats(&delta_mems);

        println!(
            "║                                                                                 ║"
        );
        println!(
            "║  Aggregated Statistics ({} runs):                                               ║",
            self.runs.len()
        );
        println!(
            "║  ─────────────────────────────────────────────────────────────────────────────  ║"
        );
        println!(
            "║                              Min         Max         Avg                        ║"
        );
        println!(
            "║  Duration (s):           {:>8.2}    {:>8.2}    {:>8.2}                       ║",
            dur_min, dur_max, dur_avg
        );
        println!(
            "║  Throughput (MB/s):      {:>8.1}    {:>8.1}    {:>8.1}                       ║",
            tp_min, tp_max, tp_avg
        );
        println!(
            "║  Peak Memory (MB):       {:>8.1}    {:>8.1}    {:>8.1}                       ║",
            mem_min, mem_max, mem_avg
        );
        println!(
            "║  Memory Delta (MB):      {:>8.1}    {:>8.1}    {:>8.1}                       ║",
            delta_min, delta_max, delta_avg
        );

        // Data processed
        let total_input_bytes: u64 = self.runs.iter().map(|m| m.input_bytes).sum();
        let total_output_bytes: u64 = self.runs.iter().map(|m| m.output_bytes).sum();
        let total_rows: u64 = self.runs.iter().map(|m| m.input_rows).sum();

        println!(
            "║                                                                                 ║"
        );
        println!(
            "║  Total Data Processed:                                                          ║"
        );
        println!(
            "║  ─────────────────────────────────────────────────────────────────────────────  ║"
        );
        println!(
            "║  Input:  {:>10.2} MB across {} runs                                          ║",
            total_input_bytes as f64 / (1024.0 * 1024.0),
            self.runs.len()
        );
        println!(
            "║  Output: {:>10.2} MB                                                          ║",
            total_output_bytes as f64 / (1024.0 * 1024.0)
        );
        println!(
            "║  Rows:   {:>10} total                                                        ║",
            total_rows
        );

        println!(
            "║                                                                                 ║"
        );
        println!(
            "╚═══════════════════════════════════════════════════════════════════════════════╝"
        );
    }
}

fn stats(values: &[f64]) -> (f64, f64, f64) {
    if values.is_empty() {
        return (0.0, 0.0, 0.0);
    }
    let min = values.iter().cloned().fold(f64::INFINITY, f64::min);
    let max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let avg = values.iter().sum::<f64>() / values.len() as f64;
    (min, max, avg)
}

// ============================================================================
// Compaction Configuration
// ============================================================================

#[derive(Debug, Clone)]
pub struct CompactionConfig {
    pub target_file_size_bytes: u64,
    pub row_group_size: usize,
    pub multipart_chunk_bytes: usize,
    pub compression: Compression,
    pub batch_size: usize,
    pub memory_limit_bytes: usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            target_file_size_bytes: 256 * 1024 * 1024, // 256 MB
            row_group_size: 300_000,
            multipart_chunk_bytes: 8 * 1024 * 1024, // 8 MB
            compression: Compression::UNCOMPRESSED,
            batch_size: 8192,
            memory_limit_bytes: 512 * 1024 * 1024, // 512 MB
        }
    }
}

// ============================================================================
// Compaction Engine with S3 Support
// ============================================================================

pub struct CompactionEngine {
    ctx: SessionContext,
    schema: SchemaRef,
    write_operator: Operator,
    config: CompactionConfig,
}

impl CompactionEngine {
    pub async fn new_s3(
        schema: SchemaRef,
        endpoint: &str,
        access_key: &str,
        secret_key: &str,
        bucket: &str,
        region: &str,
        config: CompactionConfig,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let session_config = SessionConfig::new()
            .with_batch_size(config.batch_size)
            .with_target_partitions(1);

        let memory_pool = Arc::new(FairSpillPool::new(config.memory_limit_bytes));
        let runtime_env = RuntimeEnvBuilder::new()
            .with_memory_pool(memory_pool)
            .build()?;

        let ctx = SessionContext::new_with_config_rt(session_config, Arc::new(runtime_env));
        Self::register_s3_for_datafusion(&ctx, endpoint, access_key, secret_key, bucket, region)?;
        let write_operator =
            Self::build_opendal_s3(endpoint, access_key, secret_key, bucket, region)?;

        Ok(Self {
            ctx,
            schema,
            write_operator,
            config,
        })
    }

    fn register_s3_for_datafusion(
        ctx: &SessionContext,
        endpoint: &str,
        access_key: &str,
        secret_key: &str,
        bucket: &str,
        region: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let store = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region(region)
            .with_access_key_id(access_key)
            .with_secret_access_key(secret_key)
            .with_endpoint(endpoint)
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false)
            .build()?;

        let url = Url::parse(&format!("s3://{}", bucket))?;
        ctx.runtime_env()
            .register_object_store(&url, Arc::new(store));
        Ok(())
    }

    fn build_opendal_s3(
        endpoint: &str,
        access_key: &str,
        secret_key: &str,
        bucket: &str,
        region: &str,
    ) -> Result<Operator, Box<dyn std::error::Error>> {
        let builder = opendal::services::S3::default()
            .bucket(bucket)
            .region(region)
            .access_key_id(access_key)
            .secret_access_key(secret_key)
            .endpoint(endpoint)
            .disable_config_load()
            .disable_ec2_metadata();

        Ok(Operator::new(builder)?.finish())
    }

    /// Executes compaction with detailed metrics.
    pub async fn compact_with_metrics(
        &self,
        input_files: &[String],
        output_prefix: &str,
        file_prefix: &str,
        run_id: usize,
        file_set_id: usize,
    ) -> Result<BenchmarkMetrics, Box<dyn std::error::Error>> {
        let mem_before = MemorySnapshot::capture();
        let mut mem_peak = mem_before.clone();

        let start_time = Instant::now();
        let read_start = Instant::now();

        // Build scan plan
        let df = self
            .ctx
            .read_parquet(
                input_files.to_vec(),
                ParquetReadOptions::default().schema(&self.schema),
            )
            .await?;

        let column_names: Vec<&str> = self
            .schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        let df = df.select_columns(&column_names)?;

        // Start streaming
        let mut writer = RollingParquetWriter::new(
            self.write_operator.clone(),
            self.schema.clone(),
            output_prefix.to_string(),
            file_prefix.to_string(),
            &self.config,
        );

        let mut stream = df.execute_stream().await?;
        let read_duration = read_start.elapsed();

        let write_start = Instant::now();
        let mut total_rows = 0u64;

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            total_rows += batch.num_rows() as u64;
            writer.write_batch(&batch).await?;

            // Track peak memory periodically
            let current_mem = MemorySnapshot::capture();
            if current_mem.physical_mem_bytes > mem_peak.physical_mem_bytes {
                mem_peak = current_mem;
            }
        }

        let output_files = writer.finish().await?;
        let write_duration = write_start.elapsed();
        let total_duration = start_time.elapsed();
        let mem_after = MemorySnapshot::capture();

        // Calculate input bytes (estimate: 100 bytes/row for our schema)
        let input_bytes = total_rows * 100;
        let output_bytes: u64 = output_files.iter().map(|f| f.size_bytes).sum();

        Ok(BenchmarkMetrics {
            run_id,
            file_set_id,
            input_files: input_files.len(),
            input_rows: total_rows,
            input_bytes,
            output_files: output_files.len(),
            output_rows: total_rows,
            output_bytes,
            total_duration,
            read_duration,
            write_duration,
            mem_before,
            mem_peak,
            mem_after,
        })
    }

    pub fn operator(&self) -> &Operator {
        &self.write_operator
    }
}

// ============================================================================
// Rolling Parquet Writer
// ============================================================================

#[derive(Debug, Clone)]
pub struct WrittenFile {
    pub path: String,
    pub size_bytes: u64,
    pub record_count: u64,
}

pub struct RollingParquetWriter {
    operator: Operator,
    schema: SchemaRef,
    output_prefix: String,
    file_prefix: String,
    config: CompactionConfig,
    current_writer: Option<AsyncArrowWriter<OpendalAsyncWriter>>,
    current_path: Option<String>,
    current_rows: u64,
    file_index: u64,
    completed: Vec<WrittenFile>,
}

impl RollingParquetWriter {
    pub fn new(
        operator: Operator,
        schema: SchemaRef,
        output_prefix: String,
        file_prefix: String,
        config: &CompactionConfig,
    ) -> Self {
        Self {
            operator,
            schema,
            output_prefix,
            file_prefix,
            config: config.clone(),
            current_writer: None,
            current_path: None,
            current_rows: 0,
            file_index: 0,
            completed: Vec::new(),
        }
    }

    pub async fn write_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if self.current_writer.is_none() {
            self.start_new_file().await?;
        }

        let writer = self.current_writer.as_mut().unwrap();
        writer.write(batch).await?;
        self.current_rows += batch.num_rows() as u64;

        if writer.bytes_written() as u64 >= self.config.target_file_size_bytes {
            self.finish_current_file().await?;
        }

        Ok(())
    }

    pub async fn finish(mut self) -> Result<Vec<WrittenFile>, Box<dyn std::error::Error>> {
        self.finish_current_file().await?;
        Ok(self.completed)
    }

    async fn start_new_file(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let filename = format!("{}-{:06}.parquet", self.file_prefix, self.file_index);
        self.file_index += 1;

        let path = if self.output_prefix.is_empty() {
            filename
        } else {
            format!("{}/{}", self.output_prefix.trim_end_matches('/'), filename)
        };

        let opendal_writer = self
            .operator
            .writer_with(&path)
            .chunk(self.config.multipart_chunk_bytes)
            .await?;

        let async_writer = OpendalAsyncWriter {
            inner: opendal_writer,
        };

        let props = WriterProperties::builder()
            .set_compression(self.config.compression)
            .set_max_row_group_size(self.config.row_group_size)
            .build();

        let parquet_writer =
            AsyncArrowWriter::try_new(async_writer, self.schema.clone(), Some(props))?;

        self.current_writer = Some(parquet_writer);
        self.current_path = Some(path);
        self.current_rows = 0;

        Ok(())
    }

    async fn finish_current_file(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(mut writer) = self.current_writer.take() {
            let metadata = writer.finish().await?;

            if let Some(path) = self.current_path.take() {
                let size_bytes: u64 = metadata
                    .row_groups
                    .iter()
                    .map(|rg| rg.total_byte_size as u64)
                    .sum();

                self.completed.push(WrittenFile {
                    path,
                    size_bytes,
                    record_count: self.current_rows,
                });
            }
        }
        Ok(())
    }
}

struct OpendalAsyncWriter {
    inner: opendal::Writer,
}

impl AsyncFileWriter for OpendalAsyncWriter {
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

// ============================================================================
// Test Data Generation
// ============================================================================

fn create_test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
    ]))
}

fn create_large_batch(schema: &SchemaRef, start_id: i64, num_rows: usize) -> RecordBatch {
    let ids: Vec<i64> = (start_id..start_id + num_rows as i64).collect();
    let timestamps: Vec<i64> = ids.iter().map(|i| 1700000000000 + i * 1000).collect();
    let values: Vec<f64> = ids.iter().map(|i| (*i as f64) * 0.123456789).collect();
    let categories: Vec<String> = ids
        .iter()
        .map(|i| format!("cat_{:05}", i % 10000))
        .collect();
    let descriptions: Vec<String> = ids
        .iter()
        .map(|i| {
            format!(
                "Description for record {} with some padding text here_{:010}",
                i,
                i % 1000000
            )
        })
        .collect();

    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(Float64Array::from(values)),
            Arc::new(StringArray::from(categories)),
            Arc::new(StringArray::from(descriptions)),
        ],
    )
    .unwrap()
}

async fn create_test_file_on_s3(
    operator: &Operator,
    path: &str,
    schema: &SchemaRef,
    start_id: i64,
    rows: usize,
) -> Result<(u64, u64), Box<dyn std::error::Error>> {
    let opendal_writer = operator.writer_with(path).chunk(8 * 1024 * 1024).await?;
    let async_writer = OpendalAsyncWriter {
        inner: opendal_writer,
    };

    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_max_row_group_size(100_000)
        .build();

    let mut writer = AsyncArrowWriter::try_new(async_writer, schema.clone(), Some(props))?;

    let rows_per_batch = 10_000;
    let num_batches = rows / rows_per_batch;
    let mut total_rows = 0u64;

    for i in 0..num_batches {
        let batch = create_large_batch(
            schema,
            start_id + (i * rows_per_batch) as i64,
            rows_per_batch,
        );
        writer.write(&batch).await?;
        total_rows += rows_per_batch as u64;
    }

    let metadata = writer.finish().await?;
    let size_bytes: u64 = metadata
        .row_groups
        .iter()
        .map(|rg| rg.total_byte_size as u64)
        .sum();

    Ok((total_rows, size_bytes))
}

async fn cleanup_s3_path(
    operator: &Operator,
    prefix: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // List and delete all files under the prefix
    let mut lister = operator.lister(prefix).await?;
    while let Some(entry) = lister.try_next().await? {
        let path = entry.path();
        if entry.metadata().is_file() {
            operator.delete(path).await?;
        }
    }
    Ok(())
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔═══════════════════════════════════════════════════════════════════════════════╗");
    println!("║         Compaction POC - Benchmarking with jemalloc Allocator                  ║");
    println!("╚═══════════════════════════════════════════════════════════════════════════════╝");
    println!();
    println!("Configuration:");
    println!("  Allocator:       jemalloc (better large allocation handling)");
    println!("  S3 Endpoint:     {}", S3_ENDPOINT);
    println!("  Bucket:          {}", S3_BUCKET);
    println!("  File Sets:       {}", NUM_FILE_SETS);
    println!("  Files per Set:   {}", FILES_PER_SET);
    println!(
        "  Rows per File:   {} (~{:.0} MB each)",
        ROWS_PER_FILE,
        ROWS_PER_FILE as f64 * 100.0 / (1024.0 * 1024.0)
    );
    println!("  Row Group Size:  {} rows", ROW_GROUP_SIZE);
    println!("  Runs per Set:    {}", NUM_BENCHMARK_RUNS);
    println!();

    let schema = create_test_schema();

    // Create engine once (this is the "reuse" we're measuring)
    println!("Creating CompactionEngine (one-time setup)...");
    let setup_start = Instant::now();

    let config = CompactionConfig {
        target_file_size_bytes: 256 * 1024 * 1024,
        row_group_size: ROW_GROUP_SIZE, // Using constant (100,000 instead of 300,000)
        multipart_chunk_bytes: 8 * 1024 * 1024,
        batch_size: 8192,
        ..Default::default()
    };

    let engine = CompactionEngine::new_s3(
        schema.clone(),
        S3_ENDPOINT,
        S3_ACCESS_KEY,
        S3_SECRET_KEY,
        S3_BUCKET,
        S3_REGION,
        config.clone(),
    )
    .await?;

    println!(
        "   ✓ Engine created in {:.2}ms",
        setup_start.elapsed().as_secs_f64() * 1000.0
    );
    println!();

    let mut summary = BenchmarkSummary::new();

    // Run benchmarks for each file set
    for file_set_id in 0..NUM_FILE_SETS {
        println!(
            "════════════════════════════════════════════════════════════════════════════════"
        );
        println!("FILE SET {} of {}", file_set_id + 1, NUM_FILE_SETS);
        println!(
            "════════════════════════════════════════════════════════════════════════════════"
        );
        println!();

        // Reuse existing test files in MinIO
        let input_prefix = format!("benchmark/set_{}/input", file_set_id);
        let output_prefix = format!("benchmark/set_{}/output", file_set_id);

        // // Cleanup any existing files
        // println!("Cleaning up previous test files...");
        // let _ = cleanup_s3_path(engine.operator(), &format!("benchmark/set_{}/", file_set_id)).await;
        //
        // println!("Creating {} test files (~{:.0} MB each)...", FILES_PER_SET, ROWS_PER_FILE as f64 * 100.0 / (1024.0 * 1024.0));
        //
        // let mut input_files: Vec<String> = Vec::new();
        // let mut total_input_bytes = 0u64;
        //
        // for i in 0..FILES_PER_SET {
        //     let path = format!("{}/data_{:03}.parquet", input_prefix, i);
        //     let start_id = (file_set_id * FILES_PER_SET + i) as i64 * ROWS_PER_FILE as i64;
        //
        //     let (rows, bytes) = create_test_file_on_s3(
        //         engine.operator(),
        //         &path,
        //         &schema,
        //         start_id,
        //         ROWS_PER_FILE,
        //     ).await?;
        //
        //     println!("   ✓ {} ({:.2} MB, {} rows)", path, bytes as f64 / (1024.0 * 1024.0), rows);
        //
        //     input_files.push(format!("s3://{}/{}", S3_BUCKET, path));
        //     total_input_bytes += bytes;
        // }
        //
        // println!();
        // println!("   Total input: {:.2} MB", total_input_bytes as f64 / (1024.0 * 1024.0));
        // println!();

        // Use existing files from MinIO
        println!("Using existing files from MinIO...");
        let input_files: Vec<String> = (0..FILES_PER_SET)
            .map(|i| format!("s3://{}/{}/data_{:03}.parquet", S3_BUCKET, input_prefix, i))
            .collect();

        for f in &input_files {
            println!("   • {}", f);
        }
        println!();

        // Run multiple benchmark iterations
        for run_id in 0..NUM_BENCHMARK_RUNS {
            // Memory before this run
            let pre_run_mem = MemorySnapshot::capture();
            println!(
                "─── Run {} of {} ─── (pre-run memory: {:.1} MB) [recreate_engine={}]",
                run_id + 1,
                NUM_BENCHMARK_RUNS,
                pre_run_mem.physical_mb(),
                RECREATE_ENGINE_EACH_RUN
            );

            // Optionally recreate engine to test if it holds the leak
            let run_engine: CompactionEngine;
            let engine_ref = if RECREATE_ENGINE_EACH_RUN {
                run_engine = CompactionEngine::new_s3(
                    schema.clone(),
                    S3_ENDPOINT,
                    S3_ACCESS_KEY,
                    S3_SECRET_KEY,
                    S3_BUCKET,
                    S3_REGION,
                    config.clone(),
                )
                .await?;
                &run_engine
            } else {
                &engine
            };

            // Clean output directory before each run
            let _ = cleanup_s3_path(engine_ref.operator(), &output_prefix).await;

            let file_prefix = format!("compacted_run{}", run_id);

            let metrics = engine_ref
                .compact_with_metrics(
                    &input_files,
                    &output_prefix,
                    &file_prefix,
                    run_id,
                    file_set_id,
                )
                .await?;

            println!(
                "   Duration: {:.2}s | Throughput: {:.1} MB/s | Peak Mem: {:.1} MB | Delta: {:+.1} MB",
                metrics.total_duration.as_secs_f64(),
                metrics.throughput_mb_per_sec(),
                metrics.peak_memory_mb(),
                metrics.memory_delta_mb()
            );
            println!(
                "   Output: {} file(s), {:.2} MB, {} rows",
                metrics.output_files,
                metrics.output_bytes as f64 / (1024.0 * 1024.0),
                metrics.output_rows
            );

            // Post-run memory and brief pause to let allocator settle
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            let post_run_mem = MemorySnapshot::capture();
            println!(
                "   Post-run memory: {:.1} MB (retained: {:+.1} MB from baseline)",
                post_run_mem.physical_mb(),
                post_run_mem.physical_mb() - pre_run_mem.physical_mb()
            );
            println!();

            summary.add(metrics);
        }

        // // Cleanup test files for this set (commented out - reusing files)
        // println!("Cleaning up file set {}...", file_set_id);
        // let _ = cleanup_s3_path(engine.operator(), &format!("benchmark/set_{}/", file_set_id)).await;
        println!();
    }

    // Print final summary
    summary.print_summary();

    println!();
    println!("Benchmark complete!");

    Ok(())
}

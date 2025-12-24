/*
 * Dynamic Iceberg Data Generator
 *
 * Creates test data files in Iceberg with configurable:
 * - Total data size (1GB to 100GB+)
 * - Target file size (for creating many small files to compact)
 * - Parallelism (concurrent writers)
 * - Commit frequency (files per commit)
 *
 * Usage:
 *   cargo run --example data_generator -- \
 *     --table test_db.compaction_test \
 *     --total-size 1GB \
 *     --file-size 32MB \
 *     --parallelism 4 \
 *     --files-per-commit 10
 */

use arrow::array::{
    ArrayRef, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
// futures StreamExt unused for now but may be needed for streaming APIs
#[allow(unused_imports)]
use futures::StreamExt;
use iceberg::io::FileIOBuilder;
use iceberg::CatalogBuilder;
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, NestedField, PrimitiveType,
    Schema, Type,
};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::Catalog;
use iceberg::{Namespace, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogBuilder, REST_CATALOG_PROP_URI};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;
use uuid::Uuid;

/// Configuration for data generation
#[derive(Debug, Clone)]
struct GeneratorConfig {
    /// Table identifier (namespace.table)
    table_name: String,
    /// Total data size to generate
    total_size_bytes: u64,
    /// Target size per file
    file_size_bytes: u64,
    /// Number of parallel writers
    parallelism: usize,
    /// Number of files per commit
    files_per_commit: usize,
    /// REST catalog URI
    catalog_uri: String,
    /// S3/MinIO endpoint
    s3_endpoint: String,
    /// S3 access key
    s3_access_key: String,
    /// S3 secret key
    s3_secret_key: String,
    /// S3 bucket
    s3_bucket: String,
    /// Create table if not exists
    create_table: bool,
}

impl Default for GeneratorConfig {
    fn default() -> Self {
        Self {
            table_name: "test_db.compaction_test".to_string(),
            total_size_bytes: 1024 * 1024 * 1024, // 1GB
            file_size_bytes: 32 * 1024 * 1024,    // 32MB
            parallelism: 4,
            files_per_commit: 10,
            catalog_uri: "http://localhost:8181".to_string(),
            s3_endpoint: "http://localhost:9000".to_string(),
            s3_access_key: "admin".to_string(),
            s3_secret_key: "password".to_string(),
            s3_bucket: "warehouse".to_string(),
            create_table: true,
        }
    }
}

/// Statistics for tracking progress
struct GeneratorStats {
    bytes_written: AtomicU64,
    files_written: AtomicU64,
    commits_completed: AtomicU64,
    start_time: Instant,
}

impl GeneratorStats {
    fn new() -> Self {
        Self {
            bytes_written: AtomicU64::new(0),
            files_written: AtomicU64::new(0),
            commits_completed: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    fn record_file(&self, bytes: u64) {
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
        self.files_written.fetch_add(1, Ordering::Relaxed);
    }

    fn record_commit(&self) {
        self.commits_completed.fetch_add(1, Ordering::Relaxed);
    }

    fn print_progress(&self, total_bytes: u64) {
        let written = self.bytes_written.load(Ordering::Relaxed);
        let files = self.files_written.load(Ordering::Relaxed);
        let commits = self.commits_completed.load(Ordering::Relaxed);
        let elapsed = self.start_time.elapsed();

        let progress = (written as f64 / total_bytes as f64) * 100.0;
        let throughput = written as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0);

        tracing::info!(
            "Progress: {:.1}% | Files: {} | Commits: {} | Throughput: {:.1} MB/s | Written: {} MB",
            progress,
            files,
            commits,
            throughput,
            written / (1024 * 1024)
        );
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("data_generator=info".parse()?)
                .add_directive("iceberg=warn".parse()?),
        )
        .init();

    // Parse configuration from command line
    let config = parse_args()?;

    tracing::info!("=== Iceberg Data Generator ===");
    tracing::info!("Table: {}", config.table_name);
    tracing::info!(
        "Total size: {} MB",
        config.total_size_bytes / (1024 * 1024)
    );
    tracing::info!(
        "File size: {} MB",
        config.file_size_bytes / (1024 * 1024)
    );
    tracing::info!("Parallelism: {}", config.parallelism);
    tracing::info!("Files per commit: {}", config.files_per_commit);

    let num_files = (config.total_size_bytes / config.file_size_bytes).max(1);
    let num_commits = (num_files as usize / config.files_per_commit).max(1);
    tracing::info!("Expected files: {}", num_files);
    tracing::info!("Expected commits: {}", num_commits);
    tracing::info!("================================");

    // Connect to catalog
    tracing::info!("Connecting to REST catalog at {}...", config.catalog_uri);
    let catalog = create_catalog(&config).await?;

    // Create or get table
    let table = get_or_create_table(&catalog, &config).await?;
    let table_location = table.metadata().location().to_string();
    tracing::info!("Table location: {}", table_location);

    // Create stats tracker
    let stats = Arc::new(GeneratorStats::new());

    // Create file IO for writing
    let file_io = create_file_io(&config)?;

    // Generate data files in parallel
    let semaphore = Arc::new(Semaphore::new(config.parallelism));
    let config = Arc::new(config);

    // Calculate files per commit batch
    let files_per_commit = config.files_per_commit;
    let mut all_files: Vec<DataFile> = Vec::new();

    // Generate files in batches for commits
    let mut file_id = 0u64;
    let mut bytes_remaining = config.total_size_bytes;

    while bytes_remaining > 0 {
        let batch_size = files_per_commit.min(
            ((bytes_remaining / config.file_size_bytes) as usize).max(1),
        );

        // Generate batch of files in parallel
        let batch_futures: Vec<_> = (0..batch_size)
            .map(|_| {
                let permit = semaphore.clone().acquire_owned();
                let config = config.clone();
                let stats = stats.clone();
                let file_io = file_io.clone();
                let table_location = table_location.clone();
                let current_file_id = file_id;
                file_id += 1;

                let target_size = config.file_size_bytes.min(bytes_remaining);
                bytes_remaining = bytes_remaining.saturating_sub(target_size);

                async move {
                    let _permit = permit.await?;
                    generate_data_file(
                        &file_io,
                        &table_location,
                        current_file_id,
                        target_size,
                        &stats,
                    )
                    .await
                }
            })
            .collect();

        // Wait for batch to complete
        let batch_results: Vec<Result<DataFile, Box<dyn std::error::Error + Send + Sync>>> =
            futures::future::join_all(batch_futures).await;

        // Collect successful files
        let mut batch_files = Vec::new();
        for result in batch_results {
            match result {
                Ok(file) => batch_files.push(file),
                Err(e) => {
                    tracing::error!("Failed to generate file: {}", e);
                }
            }
        }

        all_files.extend(batch_files);

        // Print progress
        stats.print_progress(config.total_size_bytes);

        if bytes_remaining == 0 {
            break;
        }
    }

    // Now commit all files in batches
    tracing::info!("Committing {} files in batches...", all_files.len());

    let mut table = table;
    for (commit_idx, chunk) in all_files.chunks(files_per_commit).enumerate() {
        tracing::info!(
            "Commit {}: adding {} files ({} bytes)...",
            commit_idx + 1,
            chunk.len(),
            chunk.iter().map(|f| f.file_size_in_bytes()).sum::<u64>()
        );

        for file in chunk {
            tracing::debug!("  - {}", file.file_path());
        }

        // Create transaction and commit files
        let tx = Transaction::new(&table);
        let action = tx.fast_append();

        // Add all data files in this batch
        let files_to_add: Vec<_> = chunk.to_vec();
        let action = action.add_data_files(files_to_add);

        // Apply the action to the transaction (synchronous)
        let tx = action.apply(tx)?;

        // Commit to the catalog
        table = tx.commit(&catalog).await?;

        tracing::info!("  Commit {} succeeded!", commit_idx + 1);
        stats.record_commit();
    }

    // Final stats
    let elapsed = stats.start_time.elapsed();
    let total_bytes = stats.bytes_written.load(Ordering::Relaxed);
    let total_files = stats.files_written.load(Ordering::Relaxed);
    let total_commits = stats.commits_completed.load(Ordering::Relaxed);

    tracing::info!("=== Generation Complete ===");
    tracing::info!("Total time: {:.2}s", elapsed.as_secs_f64());
    tracing::info!("Files written: {}", total_files);
    tracing::info!("Commits: {}", total_commits);
    tracing::info!(
        "Data written: {} MB",
        total_bytes / (1024 * 1024)
    );
    tracing::info!(
        "Throughput: {:.1} MB/s",
        total_bytes as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0)
    );
    tracing::info!("===========================");

    tracing::info!(
        "Note: Files written to storage. Run compaction with:"
    );
    tracing::info!(
        "  cargo run --example local_compaction -- --table {}",
        config.table_name
    );

    Ok(())
}

/// Generates a single data file with random data
async fn generate_data_file(
    file_io: &iceberg::io::FileIO,
    table_location: &str,
    file_id: u64,
    target_size: u64,
    stats: &GeneratorStats,
) -> Result<DataFile, Box<dyn std::error::Error + Send + Sync>> {
    let file_uuid = Uuid::now_v7();
    let file_path = format!(
        "{}/data/{:05}-{}.parquet",
        table_location, file_id, file_uuid
    );

    // Create Arrow schema matching our Iceberg schema
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("event_time", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("user_id", DataType::Int64, false),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("value", DataType::Float64, true),
        Field::new("metadata", DataType::Utf8, true),
    ]));

    // Estimate rows needed for target size
    // With Parquet + SNAPPY compression, actual size is ~15-20 bytes per row
    // Use 15 bytes to ensure we hit the target compressed size
    let rows_per_batch = 10_000;
    let bytes_per_row_compressed = 15;
    let target_rows = (target_size / bytes_per_row_compressed) as usize;
    let num_batches = (target_rows / rows_per_batch).max(1);

    // Write parquet to memory first, then upload
    let mut buffer = Vec::new();
    {
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let mut writer = ArrowWriter::try_new(&mut buffer, arrow_schema.clone(), Some(props))?;

        let mut total_rows = 0;
        for batch_idx in 0..num_batches {
            let batch = generate_record_batch(&arrow_schema, rows_per_batch, file_id, batch_idx)?;
            total_rows += batch.num_rows();
            writer.write(&batch)?;
        }

        writer.close()?;
        tracing::debug!(
            "Generated {} rows, {} bytes for file {}",
            total_rows,
            buffer.len(),
            file_id
        );
    }

    let file_size = buffer.len() as u64;
    let record_count = (num_batches * rows_per_batch) as u64;

    // Upload to storage
    let output = file_io.new_output(&file_path)?;
    output.write(Bytes::from(buffer)).await?;

    stats.record_file(file_size);

    // Create DataFile metadata
    let data_file = DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(file_path)
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(file_size)
        .record_count(record_count)
        .build()?;

    Ok(data_file)
}

/// Generates a record batch with random data
fn generate_record_batch(
    schema: &Arc<ArrowSchema>,
    num_rows: usize,
    file_id: u64,
    batch_idx: usize,
) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
    let base_id = (file_id * 1_000_000 + batch_idx as u64 * 10_000) as i64;
    let base_time = 1700000000_000_000i64; // ~2023-11-14

    // Generate arrays
    let ids: Vec<i64> = (0..num_rows).map(|i| base_id + i as i64).collect();
    let times: Vec<i64> = (0..num_rows)
        .map(|i| base_time + (i as i64 * 1000))
        .collect();
    let user_ids: Vec<i64> = (0..num_rows)
        .map(|i| ((base_id + i as i64) % 10000) + 1)
        .collect();
    let event_types: Vec<&str> = (0..num_rows)
        .map(|i| match i % 5 {
            0 => "click",
            1 => "view",
            2 => "purchase",
            3 => "add_to_cart",
            _ => "search",
        })
        .collect();
    let values: Vec<f64> = (0..num_rows)
        .map(|i| (i as f64 * 0.1) + (file_id as f64 * 0.01))
        .collect();
    let metadata: Vec<String> = (0..num_rows)
        .map(|i| format!("{{\"batch\":{},\"idx\":{}}}", batch_idx, i))
        .collect();

    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(ids)),
        Arc::new(TimestampMicrosecondArray::from(times)),
        Arc::new(Int64Array::from(user_ids)),
        Arc::new(StringArray::from(event_types)),
        Arc::new(Float64Array::from(values)),
        Arc::new(StringArray::from(
            metadata.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        )),
    ];

    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}

/// Creates the REST catalog connection
async fn create_catalog(config: &GeneratorConfig) -> Result<RestCatalog, Box<dyn std::error::Error>>
{
    // Build properties for the catalog
    // These properties need to work for both the catalog API and the FileIO it uses internally
    let mut props = HashMap::new();

    // REST catalog endpoint
    props.insert(REST_CATALOG_PROP_URI.to_string(), config.catalog_uri.clone());

    // S3 configuration - these are passed to FileIO for metadata operations
    props.insert("s3.endpoint".to_string(), config.s3_endpoint.clone());
    props.insert("s3.access-key-id".to_string(), config.s3_access_key.clone());
    props.insert("s3.secret-access-key".to_string(), config.s3_secret_key.clone());
    props.insert("s3.region".to_string(), "us-east-1".to_string());
    props.insert("s3.path-style-access".to_string(), "true".to_string());

    // Disable AWS SDK credential chain to force static credentials
    props.insert("s3.disable-ec2-metadata".to_string(), "true".to_string());
    props.insert("s3.disable-config-load".to_string(), "true".to_string());

    // OpenDAL uses slightly different property names under the hood
    // Add client.* prefixed properties as well
    props.insert("client.region".to_string(), "us-east-1".to_string());

    // The iceberg FileIO may use these AWS-style property names
    props.insert("aws.region".to_string(), "us-east-1".to_string());
    props.insert("aws.access-key-id".to_string(), config.s3_access_key.clone());
    props.insert("aws.secret-access-key".to_string(), config.s3_secret_key.clone());
    props.insert("aws.s3.endpoint".to_string(), config.s3_endpoint.clone());
    props.insert("aws.s3.path-style-access".to_string(), "true".to_string());

    let catalog = RestCatalogBuilder::default()
        .load("generator", props)
        .await?;

    Ok(catalog)
}

/// Creates FileIO for writing files
fn create_file_io(config: &GeneratorConfig) -> Result<iceberg::io::FileIO, Box<dyn std::error::Error>> {
    // Note: iceberg-rust uses opendal under the hood
    // The properties need to match opendal's S3 operator config
    let file_io = FileIOBuilder::new("s3")
        .with_prop("s3.endpoint", &config.s3_endpoint)
        .with_prop("s3.access-key-id", &config.s3_access_key)
        .with_prop("s3.secret-access-key", &config.s3_secret_key)
        .with_prop("s3.region", "us-east-1")
        .with_prop("s3.path-style-access", "true")
        // Disable STS/session credentials - use static credentials
        .with_prop("s3.disable-ec2-metadata", "true")
        .with_prop("s3.disable-config-load", "true")
        .with_prop("client.region", "us-east-1")
        .build()?;

    Ok(file_io)
}

/// Gets existing table or creates a new one
async fn get_or_create_table(
    catalog: &RestCatalog,
    config: &GeneratorConfig,
) -> Result<iceberg::table::Table, Box<dyn std::error::Error>> {
    let parts: Vec<&str> = config.table_name.split('.').collect();
    if parts.len() != 2 {
        return Err("Table name must be in format 'namespace.table'".into());
    }

    let namespace_name = parts[0];
    let table_name = parts[1];

    let namespace_ident = NamespaceIdent::new(namespace_name.to_string());
    let table_ident = TableIdent::new(namespace_ident.clone(), table_name.to_string());

    // Try to load existing table
    match catalog.load_table(&table_ident).await {
        Ok(table) => {
            tracing::info!("Loaded existing table: {}", config.table_name);
            return Ok(table);
        }
        Err(_) if config.create_table => {
            tracing::info!("Table not found, creating...");
        }
        Err(e) => {
            return Err(format!("Failed to load table: {}", e).into());
        }
    }

    // Create namespace if needed
    match catalog.get_namespace(&namespace_ident).await {
        Ok(_) => {
            tracing::info!("Namespace '{}' exists", namespace_name);
        }
        Err(_) => {
            tracing::info!("Creating namespace '{}'...", namespace_name);
            let ns = Namespace::with_properties(
                namespace_ident.clone(),
                HashMap::from([("location".to_string(), format!("s3://{}/{}", config.s3_bucket, namespace_name))]),
            );
            catalog.create_namespace(&namespace_ident, ns.properties().clone()).await?;
        }
    }

    // Create schema
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "event_time", Type::Primitive(PrimitiveType::Timestamp)).into(),
            NestedField::required(3, "user_id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(4, "event_type", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(5, "value", Type::Primitive(PrimitiveType::Double)).into(),
            NestedField::optional(6, "metadata", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()?;

    // Create unpartitioned table for simplicity
    // Partitioning can be added later if needed
    let table_location = format!(
        "s3://{}/{}/{}",
        config.s3_bucket, namespace_name, table_name
    );

    let table_creation = TableCreation::builder()
        .name(table_name.to_string())
        .schema(schema)
        .location(table_location)
        .build();

    let table = catalog.create_table(&namespace_ident, table_creation).await?;
    tracing::info!("Created table: {}", config.table_name);

    Ok(table)
}

/// Parses command line arguments
fn parse_args() -> Result<GeneratorConfig, Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let mut config = GeneratorConfig::default();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--table" => {
                i += 1;
                config.table_name = args.get(i).cloned().unwrap_or_default();
            }
            "--total-size" => {
                i += 1;
                config.total_size_bytes = parse_size(&args.get(i).cloned().unwrap_or_default())?;
            }
            "--file-size" => {
                i += 1;
                config.file_size_bytes = parse_size(&args.get(i).cloned().unwrap_or_default())?;
            }
            "--parallelism" => {
                i += 1;
                config.parallelism = args.get(i).and_then(|s| s.parse().ok()).unwrap_or(4);
            }
            "--files-per-commit" => {
                i += 1;
                config.files_per_commit = args.get(i).and_then(|s| s.parse().ok()).unwrap_or(10);
            }
            "--catalog-uri" => {
                i += 1;
                config.catalog_uri = args.get(i).cloned().unwrap_or_default();
            }
            "--s3-endpoint" => {
                i += 1;
                config.s3_endpoint = args.get(i).cloned().unwrap_or_default();
            }
            "--s3-access-key" => {
                i += 1;
                config.s3_access_key = args.get(i).cloned().unwrap_or_default();
            }
            "--s3-secret-key" => {
                i += 1;
                config.s3_secret_key = args.get(i).cloned().unwrap_or_default();
            }
            "--s3-bucket" => {
                i += 1;
                config.s3_bucket = args.get(i).cloned().unwrap_or_default();
            }
            "--no-create" => {
                config.create_table = false;
            }
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            _ => {}
        }
        i += 1;
    }

    Ok(config)
}

/// Parses a size string like "1GB", "100MB", "512KB"
fn parse_size(s: &str) -> Result<u64, Box<dyn std::error::Error>> {
    let s = s.trim().to_uppercase();

    if s.ends_with("GB") {
        let num: f64 = s.trim_end_matches("GB").parse()?;
        Ok((num * 1024.0 * 1024.0 * 1024.0) as u64)
    } else if s.ends_with("MB") {
        let num: f64 = s.trim_end_matches("MB").parse()?;
        Ok((num * 1024.0 * 1024.0) as u64)
    } else if s.ends_with("KB") {
        let num: f64 = s.trim_end_matches("KB").parse()?;
        Ok((num * 1024.0) as u64)
    } else {
        // Assume bytes
        Ok(s.parse()?)
    }
}

fn print_help() {
    println!(
        r#"
Iceberg Data Generator - Create test data for compaction

USAGE:
    cargo run --example data_generator -- [OPTIONS]

OPTIONS:
    --table <NAME>           Table name (namespace.table) [default: test_db.compaction_test]
    --total-size <SIZE>      Total data size to generate (e.g., 1GB, 100MB) [default: 1GB]
    --file-size <SIZE>       Target size per file (e.g., 32MB) [default: 32MB]
    --parallelism <N>        Number of parallel writers [default: 4]
    --files-per-commit <N>   Files to batch per commit [default: 10]
    --catalog-uri <URI>      REST catalog URI [default: http://localhost:8181]
    --s3-endpoint <URI>      S3/MinIO endpoint [default: http://localhost:9000]
    --s3-access-key <KEY>    S3 access key [default: admin]
    --s3-secret-key <KEY>    S3 secret key [default: password]
    --s3-bucket <BUCKET>     S3 bucket name [default: warehouse]
    --no-create              Don't create table if it doesn't exist
    -h, --help               Print this help

EXAMPLES:
    # Generate 1GB with 32MB files (creates ~32 files)
    cargo run --example data_generator -- --total-size 1GB --file-size 32MB

    # Generate 10GB with many small 8MB files (creates ~1280 files for compaction)
    cargo run --example data_generator -- --total-size 10GB --file-size 8MB --parallelism 8

    # Generate 100GB for stress testing
    cargo run --example data_generator -- --total-size 100GB --file-size 64MB --parallelism 16
"#
    );
}

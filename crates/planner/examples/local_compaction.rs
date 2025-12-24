/*
 * Integration test with local MinIO and Iceberg REST catalog.
 *
 * Prerequisites:
 * - MinIO running at localhost:9000 (access_key: admin, secret_key: password)
 * - Iceberg REST catalog at localhost:8181
 * - A test table with some data files
 *
 * Usage:
 *   cargo run --example local_compaction -- --table default.test_table
 *
 * Optional flags:
 *   --s3-endpoint <URI>     S3/MinIO endpoint [default: http://localhost:9000]
 *   --s3-access-key <KEY>   S3 access key [default: admin]
 *   --s3-secret-key <KEY>   S3 secret key [default: password]
 *   --s3-bucket <BUCKET>    S3 bucket name [default: warehouse]
 *   --verify-multipart      Verify output files are multipart (ETag contains '-')
 */

use compaction_common::{
    build_operator, CompactionMetrics, ExecutionConfig, FileGroup, FileMetadata, StorageConfig,
};
use compaction_planner::{IcebergTableProvider, TableProvider};
use compaction_proto::{CompactionTask, FileIOConfig, StorageCredentials};
use compaction_worker::WorkerExecutor;
use iceberg::CatalogBuilder;
use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, RestCatalog, RestCatalogBuilder};
use opendal::EntryMode;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "dhat-heap")]
    let _dhat_profiler = dhat::Profiler::new_heap();

    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("compaction=debug".parse()?)
                .add_directive("iceberg=info".parse()?),
        )
        .init();

    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();
    let table_name = get_arg(&args, "--table").unwrap_or("default.test_table");
    let s3_endpoint = get_arg(&args, "--s3-endpoint").unwrap_or("http://localhost:9000");
    let s3_access_key = get_arg(&args, "--s3-access-key").unwrap_or("admin");
    let s3_secret_key = get_arg(&args, "--s3-secret-key").unwrap_or("password");
    let s3_bucket = get_arg(&args, "--s3-bucket").unwrap_or("warehouse");
    let verify_multipart = args.iter().any(|a| a == "--verify-multipart");

    tracing::info!("Starting local compaction test");
    tracing::info!("  REST Catalog: http://localhost:8181");
    tracing::info!("  MinIO: {}", s3_endpoint);
    tracing::info!("  Table: {}", table_name);
    tracing::info!("  Bucket: {}", s3_bucket);

    // Step 1: Connect to REST catalog
    tracing::info!("Connecting to REST catalog...");
    let catalog = create_rest_catalog(s3_endpoint, s3_access_key, s3_secret_key).await?;
    let provider = IcebergTableProvider::new(catalog);

    // Step 2: List available tables
    let namespace = table_name.split('.').next().unwrap_or("default");
    tracing::info!("Listing tables in namespace '{}'...", namespace);

    match provider.list_tables(namespace).await {
        Ok(tables) => {
            tracing::info!("Found {} tables:", tables.len());
            for t in &tables {
                tracing::info!("  - {}", t);
            }
        }
        Err(e) => {
            tracing::warn!("Could not list tables: {}", e);
        }
    }

    // Step 3: Load the target table
    tracing::info!("Loading table '{}'...", table_name);
    let table = match provider.load_table(table_name).await {
        Ok(t) => t,
        Err(e) => {
            tracing::error!("Failed to load table: {}", e);
            tracing::info!("Make sure the table exists. You can create one with:");
            tracing::info!(
                "  spark-sql --conf ... -e \"CREATE TABLE {} ...\"",
                table_name
            );
            return Err(e.into());
        }
    };

    tracing::info!("Table loaded successfully!");
    tracing::info!("  Location: {}", table.metadata().location());

    // Step 4: Get current snapshot and scan for files
    let current_snapshot = table.metadata().current_snapshot();
    if current_snapshot.is_none() {
        tracing::warn!("Table has no snapshots (no data). Exiting.");
        return Ok(());
    }

    let snapshot = current_snapshot.unwrap();
    let snapshot_id = snapshot.snapshot_id();
    tracing::info!("Current snapshot: {}", snapshot_id);

    // Step 5: Scan for data files
    tracing::info!("Scanning for data files...");
    let scan = table.scan().snapshot_id(snapshot_id).build()?;

    use futures::TryStreamExt;
    let file_tasks: Vec<_> = scan.plan_files().await?.try_collect().await?;

    tracing::info!("Found {} file scan tasks", file_tasks.len());

    if file_tasks.is_empty() {
        tracing::info!("No files to compact. Exiting.");
        return Ok(());
    }

    // Convert to our FileMetadata format
    let file_metadata: Vec<FileMetadata> = file_tasks
        .iter()
        .map(|task| FileMetadata {
            file_path: task.data_file_path.clone(),
            file_size_bytes: task.length,
            record_count: task.record_count,
            sequence_number: Some(task.sequence_number),
            project_field_ids: task.project_field_ids.clone(),
            equality_ids: None,
            partition_values: HashMap::new(),
        })
        .collect();

    let total_size: u64 = file_metadata.iter().map(|f| f.file_size_bytes).sum();
    let total_records: u64 = file_metadata.iter().filter_map(|f| f.record_count).sum();

    tracing::info!("Files to compact:");
    for f in &file_metadata {
        tracing::info!(
            "  - {} ({} bytes, {:?} records)",
            f.file_path,
            f.file_size_bytes,
            f.record_count
        );
    }
    tracing::info!("Total: {} bytes, {} records", total_size, total_records);

    // Step 6: Create a compaction task
    tracing::info!("Creating compaction task...");

    let file_group = FileGroup::new(file_metadata).with_parallelism(4, 1);

    let schema_json = serde_json::to_string(table.metadata().current_schema())?;
    let partition_spec_json = serde_json::to_string(table.metadata().default_partition_spec())?;

    // Determine output location
    let output_location = format!("{}/data/compacted", table.metadata().location());

    let task = CompactionTask {
        task_id: Uuid::now_v7(),
        table_identifier: table_name.to_string(),
        snapshot_id,
        file_group,
        schema_json,
        partition_spec_json,
        sort_order_json: None,
        output_location: output_location.clone(),
        execution_config: ExecutionConfig::default(),
        inline_deletes: None,
        file_io_config: FileIOConfig::s3(s3_bucket, "us-east-1")
            .with_endpoint(s3_endpoint)
            .with_credentials(StorageCredentials::AwsAccessKey {
                access_key_id: s3_access_key.to_string(),
                secret_access_key: s3_secret_key.to_string(),
                session_token: None,
            }),
        // Use NoCommit for testing - files are written but table isn't updated
        commit_mode: compaction_proto::CommitMode::NoCommit,
    };

    tracing::info!("Task created:");
    tracing::info!("  Task ID: {}", task.task_id);
    tracing::info!("  Output: {}", output_location);

    // Step 7: Execute the compaction
    tracing::info!("Executing compaction...");

    let metrics = Arc::new(CompactionMetrics::new());
    let executor = WorkerExecutor::with_metrics(metrics.clone());

    let result = executor.execute(task).await;

    // Step 8: Report results
    tracing::info!("Compaction result:");
    tracing::info!("  Status: {:?}", result.status);
    tracing::info!("  Execution time: {} ms", result.stats.execution_time_ms);
    tracing::info!("  Input bytes: {}", result.stats.input_bytes);
    tracing::info!("  Output bytes: {}", result.stats.output_bytes);
    tracing::info!("  Rows processed: {}", result.stats.rows_processed);
    tracing::info!("  Output files: {}", result.stats.output_file_count);

    if let Some(error) = &result.error {
        tracing::error!("Error: {}", error);
    }

    for output_json in &result.output_files_json {
        tracing::info!("  Output file: {}", output_json);
    }

    if verify_multipart {
        if let Err(err) = verify_multipart_outputs(
            &table.metadata().location(),
            s3_endpoint,
            s3_access_key,
            s3_secret_key,
            s3_bucket,
        )
        .await
        {
            tracing::error!("Multipart verification failed: {}", err);
        }
    }

    // Print metrics
    let snapshot = metrics.snapshot();
    tracing::info!("Metrics:");
    tracing::info!("  Bytes read: {}", snapshot.bytes_read);
    tracing::info!("  Bytes written: {}", snapshot.bytes_written);
    tracing::info!("  Files written: {}", snapshot.files_written);

    tracing::info!("Compaction test completed!");

    Ok(())
}

/// Creates a REST catalog connected to localhost:8181 with MinIO storage.
async fn create_rest_catalog(
    s3_endpoint: &str,
    s3_access_key: &str,
    s3_secret_key: &str,
) -> Result<RestCatalog, Box<dyn std::error::Error>> {
    use iceberg::Catalog;

    // Build properties for the catalog
    let mut props = HashMap::new();

    // REST catalog endpoint
    props.insert(
        REST_CATALOG_PROP_URI.to_string(),
        "http://localhost:8181".to_string(),
    );

    // S3/MinIO configuration
    props.insert("s3.endpoint".to_string(), s3_endpoint.to_string());
    props.insert("s3.access-key-id".to_string(), s3_access_key.to_string());
    props.insert("s3.secret-access-key".to_string(), s3_secret_key.to_string());
    props.insert("s3.region".to_string(), "us-east-1".to_string());
    props.insert("s3.path-style-access".to_string(), "true".to_string());

    // Disable AWS SDK credential chain to force static credentials
    props.insert("s3.disable-ec2-metadata".to_string(), "true".to_string());
    props.insert("s3.disable-config-load".to_string(), "true".to_string());

    // AWS-style property names (some iceberg-rust code paths use these)
    props.insert("aws.region".to_string(), "us-east-1".to_string());
    props.insert("aws.access-key-id".to_string(), "admin".to_string());
    props.insert("aws.secret-access-key".to_string(), "password".to_string());
    props.insert("aws.s3.endpoint".to_string(), s3_endpoint.to_string());
    props.insert("aws.s3.path-style-access".to_string(), "true".to_string());

    let catalog = RestCatalogBuilder::default().load("local", props).await?;

    // Test the connection by listing namespaces
    tracing::debug!("Testing catalog connection...");
    let namespaces = catalog.list_namespaces(None).await?;
    tracing::debug!("Found {} namespaces", namespaces.len());

    Ok(catalog)
}

fn get_arg<'a>(args: &'a [String], name: &str) -> Option<&'a str> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .map(|s| s.as_str())
}

async fn verify_multipart_outputs(
    table_location: &str,
    s3_endpoint: &str,
    s3_access_key: &str,
    s3_secret_key: &str,
    s3_bucket: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let location = url::Url::parse(table_location)?;
    let prefix = location
        .path()
        .trim_start_matches('/')
        .trim_end_matches('/');
    let output_prefix = format!("{}/data/compacted/", prefix);

    let operator = build_operator(
        &StorageConfig::s3(s3_bucket)
            .with_endpoint(s3_endpoint)
            .with_region("us-east-1")
            .with_aws_credentials(s3_access_key, s3_secret_key, None::<String>),
    )?;

    let entries = operator.list(&output_prefix).await?;
    if entries.is_empty() {
        tracing::warn!("No output files found under {}", output_prefix);
        return Ok(());
    }

    let mut saw_large = false;
    let mut saw_multipart_etag = false;

    for entry in entries {
        if entry.metadata().mode() != EntryMode::FILE {
            continue;
        }
        let path = entry.path();
        let meta = operator.stat(path).await?;
        let size = meta.content_length();
        let etag = meta.etag().unwrap_or_default();

        tracing::info!("Output file: {} ({} bytes, etag={})", path, size, etag);

        if size >= 5 * 1024 * 1024 {
            saw_large = true;
        }
        if etag.contains('-') {
            saw_multipart_etag = true;
        }
    }

    if !saw_large {
        return Err("no output file >= 5MB; multipart unlikely".into());
    }
    if !saw_multipart_etag {
        return Err("no multipart-style etag detected".into());
    }

    Ok(())
}

/// Test the storage connection directly
#[allow(dead_code)]
async fn test_storage_connection() -> Result<(), Box<dyn std::error::Error>> {
    tracing::info!("Testing MinIO connection...");

    let config = StorageConfig::s3("warehouse")
        .with_endpoint("http://localhost:9000")
        .with_region("us-east-1")
        .with_aws_credentials("admin", "password", None);

    let operator = build_operator(&config)?;

    // Try to list the root
    let entries = operator.list("/").await?;
    tracing::info!("MinIO root contents:");
    for entry in entries {
        tracing::info!("  - {}", entry.path());
    }

    Ok(())
}

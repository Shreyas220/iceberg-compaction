# Technical Deep Dive: Iceberg Compaction Implementation

## Core Algorithms and Data Structures

### 1. File Selection Algorithm (from Nimtable)

```rust
// Nimtable's approach to file selection
pub struct FileGroup {
    pub data_files: Vec<DataFile>,
    pub position_delete_files: Vec<DeleteFile>,
    pub equality_delete_files: Vec<DeleteFile>,
    pub total_size: u64,
    pub data_file_count: usize,
}

impl FileGroup {
    pub fn should_compact(&self, config: &Config) -> bool {
        // Compact if:
        // 1. Too many small files
        if self.data_file_count > config.max_file_count_per_partition {
            return true;
        }

        // 2. Average file size below threshold
        let avg_size = self.total_size / self.data_file_count as u64;
        if avg_size < config.small_file_threshold {
            return true;
        }

        // 3. Too many delete files
        let delete_ratio = (self.position_delete_files.len() +
                           self.equality_delete_files.len()) as f64 /
                           self.data_file_count as f64;
        if delete_ratio > config.max_delete_ratio {
            return true;
        }

        false
    }
}
```

### 2. Bin Packing Algorithm (from Amoro)

```java
// Amoro's bin packing approach
public class BinPackingStrategy {
    private final long targetFileSize;
    private final long minFileSize;
    private final long maxFileSize;

    public List<FileGroup> packFiles(List<DataFile> files) {
        // Sort files by size (largest first)
        files.sort((a, b) -> Long.compare(b.fileSizeInBytes(), a.fileSizeInBytes()));

        List<FileGroup> groups = new ArrayList<>();
        FileGroup currentGroup = new FileGroup();
        long currentSize = 0;

        for (DataFile file : files) {
            if (currentSize + file.fileSizeInBytes() > maxFileSize && !currentGroup.isEmpty()) {
                // Start new group
                groups.add(currentGroup);
                currentGroup = new FileGroup();
                currentSize = 0;
            }

            currentGroup.add(file);
            currentSize += file.fileSizeInBytes();

            // Check if we've reached target size
            if (currentSize >= targetFileSize) {
                groups.add(currentGroup);
                currentGroup = new FileGroup();
                currentSize = 0;
            }
        }

        // Add remaining files
        if (!currentGroup.isEmpty()) {
            groups.add(currentGroup);
        }

        return groups;
    }
}
```

### 3. Partition Scoring (from Amoro)

```java
// How Amoro scores partitions for compaction priority
public class PartitionEvaluator {

    public double scorePartition(Partition partition) {
        double score = 0.0;

        // Factor 1: File count (more files = higher score)
        double fileCountScore = Math.min(partition.fileCount / 100.0, 1.0) * 0.3;
        score += fileCountScore;

        // Factor 2: Fragment ratio (more fragments = higher score)
        double fragmentRatio = calculateFragmentRatio(partition);
        double fragmentScore = fragmentRatio * 0.3;
        score += fragmentScore;

        // Factor 3: Time since last compaction
        long hoursSinceCompaction = partition.getHoursSinceLastCompaction();
        double timeScore = Math.min(hoursSinceCompaction / 24.0, 1.0) * 0.2;
        score += timeScore;

        // Factor 4: Small file ratio
        double smallFileRatio = partition.smallFileCount / (double) partition.fileCount;
        double smallFileScore = smallFileRatio * 0.2;
        score += smallFileScore;

        return score;
    }

    private double calculateFragmentRatio(Partition partition) {
        int deleteFiles = partition.positionDeleteFiles + partition.equalityDeleteFiles;
        return deleteFiles / (double) Math.max(partition.dataFiles, 1);
    }
}
```

## DataFusion Integration (from Nimtable)

### Reading Iceberg Files with DataFusion

```rust
use datafusion::prelude::*;
use iceberg::scan::FileScanTask;

pub struct IcebergTableProvider {
    scan_tasks: Vec<FileScanTask>,
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for IcebergTableProvider {
    async fn scan(
        &self,
        ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Convert Iceberg scan tasks to DataFusion execution plan
        let mut file_groups = vec![];

        for task in &self.scan_tasks {
            let file_scan = FileScanConfig {
                object_store_url: ObjectStoreUrl::parse(&task.data_file().file_path())?,
                file_schema: self.schema.clone(),
                file_groups: vec![vec![task.data_file().file_path().into()]],
                statistics: Statistics::default(),
                projection: projection.cloned(),
                limit,
                table_partition_cols: vec![],
                output_ordering: vec![],
            };

            file_groups.push(file_scan);
        }

        // Create Parquet execution plan
        let exec = ParquetExec::new(file_groups[0].clone(), None, None);

        Ok(Arc::new(exec))
    }
}
```

### Writing Compacted Files

```rust
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;

pub struct CompactionWriter {
    target_file_size: usize,
    current_writer: Option<ArrowWriter<File>>,
    current_size: usize,
    output_files: Vec<DataFile>,
}

impl CompactionWriter {
    pub async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        // Check if we need to roll to a new file
        let batch_size = batch.get_array_memory_size();

        if self.current_size + batch_size > self.target_file_size {
            self.close_current_file().await?;
            self.start_new_file().await?;
        }

        // Write batch to current file
        if let Some(writer) = &mut self.current_writer {
            writer.write(&batch)?;
            self.current_size += batch_size;
        }

        Ok(())
    }

    async fn close_current_file(&mut self) -> Result<()> {
        if let Some(mut writer) = self.current_writer.take() {
            let metadata = writer.close()?;

            // Create Iceberg DataFile
            let data_file = DataFile::builder()
                .file_path(self.current_path.clone())
                .file_format(FileFormat::Parquet)
                .record_count(metadata.num_rows)
                .file_size_in_bytes(metadata.file_size)
                .build()?;

            self.output_files.push(data_file);
            self.current_size = 0;
        }

        Ok(())
    }
}
```

## Commit Protocol (from both)

### Transaction Management

```rust
// How to safely commit compaction results
pub struct CompactionCommitter {
    table: Table,
    retry_policy: RetryPolicy,
}

impl CompactionCommitter {
    pub async fn commit(&self, result: CompactionResult) -> Result<()> {
        // Build the rewrite transaction
        let transaction = self.table.new_transaction();

        // Remove old files
        for file in &result.replaced_files {
            transaction.delete_file(file.clone());
        }

        // Add new compacted files
        for file in &result.new_files {
            transaction.add_file(file.clone());
        }

        // Commit with retries
        retry(self.retry_policy.clone())
            .retry(|| async {
                match transaction.commit().await {
                    Ok(_) => Ok(()),
                    Err(e) if e.is_retriable() => Err(e),
                    Err(e) => {
                        // Non-retriable error, fail immediately
                        return Err(BackoffError::permanent(e));
                    }
                }
            })
            .await?;

        Ok(())
    }
}
```

### Conflict Resolution

```rust
// Handle concurrent modifications
pub async fn commit_with_conflict_resolution(
    table: &Table,
    result: CompactionResult,
) -> Result<()> {
    let max_retries = 5;
    let mut attempt = 0;

    loop {
        attempt += 1;

        // Refresh table metadata
        table.refresh().await?;

        // Check if our input files still exist
        let current_files = table.current_snapshot()?.data_files();
        let input_still_valid = result.replaced_files.iter()
            .all(|f| current_files.contains(f));

        if !input_still_valid {
            // Files were already compacted by another process
            return Ok(());
        }

        // Try to commit
        match commit_compaction(table, &result).await {
            Ok(_) => return Ok(()),
            Err(e) if e.is_conflict() && attempt < max_retries => {
                // Retry with exponential backoff
                tokio::time::sleep(Duration::from_millis(100 * 2_u64.pow(attempt))).await;
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}
```

## Performance Optimizations

### 1. Memory Management

```rust
// Streaming processing to handle large files
pub async fn compact_partition_streaming(
    files: Vec<DataFile>,
    target_size: usize,
) -> Result<Vec<DataFile>> {
    // Create streaming reader
    let mut reader = StreamingReader::new(files);
    let mut writer = RollingWriter::new(target_size);

    // Process in chunks to control memory usage
    const CHUNK_SIZE: usize = 10000; // rows

    while let Some(chunk) = reader.next_chunk(CHUNK_SIZE).await? {
        // Apply any transformations (sorting, filtering)
        let processed = process_chunk(chunk)?;

        // Write to output files
        writer.write_chunk(processed).await?;

        // Force garbage collection if memory pressure is high
        if memory_pressure_high() {
            writer.flush().await?;
        }
    }

    writer.finalize().await
}
```

### 2. Parallelization Strategy

```rust
// Parallel compaction execution
pub async fn parallel_compact(
    groups: Vec<FileGroup>,
    parallelism: usize,
) -> Result<Vec<CompactionResult>> {
    use futures::stream::{self, StreamExt};

    // Create semaphore to limit parallelism
    let semaphore = Arc::new(Semaphore::new(parallelism));

    let results = stream::iter(groups)
        .map(|group| {
            let sem = semaphore.clone();
            async move {
                // Acquire permit
                let _permit = sem.acquire().await.unwrap();

                // Compact this group
                compact_file_group(group).await
            }
        })
        .buffer_unordered(parallelism)
        .collect::<Vec<_>>()
        .await;

    // Check for any failures
    let mut successful = vec![];
    for result in results {
        successful.push(result?);
    }

    Ok(successful)
}
```

### 3. Caching Strategy

```rust
// Cache frequently accessed metadata
pub struct MetadataCache {
    file_stats: Arc<RwLock<HashMap<String, FileStatistics>>>,
    partition_info: Arc<RwLock<HashMap<String, PartitionInfo>>>,
    ttl: Duration,
}

impl MetadataCache {
    pub async fn get_file_stats(&self, file_path: &str) -> Option<FileStatistics> {
        // Try cache first
        {
            let cache = self.file_stats.read().await;
            if let Some(stats) = cache.get(file_path) {
                if stats.is_fresh(self.ttl) {
                    return Some(stats.clone());
                }
            }
        }

        // Cache miss - fetch and update
        let stats = fetch_file_statistics(file_path).await.ok()?;

        {
            let mut cache = self.file_stats.write().await;
            cache.insert(file_path.to_string(), stats.clone());
        }

        Some(stats)
    }
}
```

## Cloud Cost Optimization

### 1. S3 Request Optimization

```rust
// Minimize S3 API calls
pub struct S3Optimizer {
    // Batch multiple small files into single requests
    pub async fn batch_read_small_files(
        files: Vec<DataFile>,
    ) -> Result<Vec<RecordBatch>> {
        // Group files that can be read together
        let groups = group_by_size(files, MAX_MULTIPART_SIZE);

        let mut all_batches = vec![];

        for group in groups {
            // Use S3 Select to read multiple files in one request
            let request = S3SelectRequest {
                bucket: "data-lake",
                keys: group.iter().map(|f| &f.path).collect(),
                expression: "SELECT * FROM S3Object[*]",
            };

            let batches = execute_s3_select(request).await?;
            all_batches.extend(batches);
        }

        Ok(all_batches)
    }

    // Use multipart upload for large files
    pub async fn write_with_multipart(
        data: Vec<RecordBatch>,
        path: &str,
    ) -> Result<()> {
        let parts = split_into_parts(data, PART_SIZE);

        let upload_id = initiate_multipart_upload(path).await?;

        let mut part_etags = vec![];
        for (i, part) in parts.enumerate() {
            let etag = upload_part(upload_id, i + 1, part).await?;
            part_etags.push(etag);
        }

        complete_multipart_upload(upload_id, part_etags).await?;

        Ok(())
    }
}
```

### 2. Lambda Cost Optimization

```rust
// Optimize for Lambda pricing model
pub struct LambdaCostOptimizer {
    pub fn calculate_optimal_memory(data_size: usize) -> u32 {
        // Lambda pricing is linear with memory
        // But execution time decreases with more memory (more CPU)
        // Find the sweet spot

        const MB: usize = 1024 * 1024;

        match data_size {
            0..100*MB => 512,        // Small files: minimal memory
            100*MB..500*MB => 1024,  // Medium files: balanced
            500*MB..1000*MB => 2048, // Large files: more CPU needed
            _ => 3008,               // Max Lambda memory for best CPU
        }
    }

    pub fn should_use_lambda(task: &CompactionTask) -> bool {
        // Lambda is cost-effective for:
        // - Tasks under 15 minutes
        // - Sporadic workloads
        // - Small to medium file sizes

        let estimated_duration = estimate_duration(task);
        let is_sporadic = task.frequency < Duration::from_hours(1);
        let size_appropriate = task.total_size < 10 * 1024 * 1024 * 1024; // 10GB

        estimated_duration < Duration::from_mins(15) &&
        (is_sporadic || size_appropriate)
    }
}
```

## Monitoring and Observability

### 1. Metrics Collection

```rust
use prometheus::{Counter, Histogram, Gauge};

pub struct CompactionMetrics {
    // Counters
    pub files_compacted: Counter,
    pub bytes_read: Counter,
    pub bytes_written: Counter,
    pub compactions_succeeded: Counter,
    pub compactions_failed: Counter,

    // Histograms
    pub compaction_duration: Histogram,
    pub file_size_distribution: Histogram,
    pub compression_ratio: Histogram,

    // Gauges
    pub pending_compactions: Gauge,
    pub active_compactions: Gauge,
    pub last_compaction_timestamp: Gauge,
}

impl CompactionMetrics {
    pub fn record_compaction(&self, result: &CompactionResult) {
        self.files_compacted.inc_by(result.files_processed as f64);
        self.bytes_read.inc_by(result.bytes_read as f64);
        self.bytes_written.inc_by(result.bytes_written as f64);

        let compression_ratio = result.bytes_read as f64 / result.bytes_written as f64;
        self.compression_ratio.observe(compression_ratio);

        self.compaction_duration.observe(result.duration.as_secs_f64());

        if result.success {
            self.compactions_succeeded.inc();
        } else {
            self.compactions_failed.inc();
        }
    }
}
```

### 2. Distributed Tracing

```rust
use tracing::{instrument, span, Level};
use opentelemetry::trace::Tracer;

#[instrument(skip(table))]
pub async fn compact_table(
    table: &Table,
    strategy: CompactionStrategy,
) -> Result<CompactionResult> {
    let span = span!(Level::INFO, "compact_table",
        table_name = %table.name(),
        strategy = %strategy
    );

    let _enter = span.enter();

    // Planning phase
    let plan_span = span!(Level::DEBUG, "planning");
    let plan = {
        let _enter = plan_span.enter();
        create_compaction_plan(table, strategy).await?
    };

    // Execution phase
    let exec_span = span!(Level::DEBUG, "execution",
        partitions = plan.partitions.len(),
        total_files = plan.total_files
    );

    let result = {
        let _enter = exec_span.enter();
        execute_compaction(plan).await?
    };

    // Commit phase
    let commit_span = span!(Level::DEBUG, "commit");
    {
        let _enter = commit_span.enter();
        commit_results(table, result).await?
    };

    Ok(result)
}
```

## Testing Strategies

### 1. Property-Based Testing

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn test_bin_packing_properties(
        files in prop::collection::vec(
            (1usize..1000000000), // File sizes
            1..100 // Number of files
        ),
        target_size in 100000000usize..2000000000, // 100MB - 2GB
    ) {
        let groups = bin_pack(files.clone(), target_size);

        // Property 1: All files are included
        let total_files: usize = groups.iter()
            .map(|g| g.files.len())
            .sum();
        prop_assert_eq!(total_files, files.len());

        // Property 2: No group exceeds max size (2x target)
        for group in &groups {
            prop_assert!(group.total_size <= target_size * 2);
        }

        // Property 3: Groups are reasonably balanced
        let avg_size = groups.iter()
            .map(|g| g.total_size)
            .sum::<usize>() / groups.len();

        for group in &groups {
            prop_assert!(group.total_size >= avg_size / 2);
            prop_assert!(group.total_size <= avg_size * 2);
        }
    }
}
```

### 2. Chaos Testing

```rust
// Test resilience to failures
pub async fn chaos_test_compaction() {
    let chaos_config = ChaosConfig {
        network_failure_rate: 0.1,
        storage_failure_rate: 0.05,
        random_delays: true,
        max_delay: Duration::from_secs(5),
    };

    let table = create_test_table_with_chaos(chaos_config).await;

    // Should eventually succeed despite failures
    let result = retry_with_backoff(|| async {
        compact_table(&table).await
    }, 10, Duration::from_secs(1)).await;

    assert!(result.is_ok());

    // Verify data integrity
    verify_no_data_loss(&table).await.unwrap();
}
```

## Integration Patterns

### 1. Airflow Integration

```python
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator

dag = DAG(
    'iceberg_compaction',
    schedule_interval='0 */6 * * *',  # Every 6 hours
    default_args={
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    }
)

compact_sales = SimpleHttpOperator(
    task_id='compact_sales_table',
    http_conn_id='compaction_service',
    endpoint='/api/v1/compact',
    method='POST',
    data=json.dumps({
        'table': 'sales',
        'strategy': 'time-series',
        'options': {
            'target_file_size': '1GB',
            'max_cost': 10.0
        }
    }),
    headers={"Content-Type": "application/json"},
    dag=dag,
)
```

### 2. Kubernetes CronJob

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: hourly-compaction
spec:
  schedule: "0 * * * *"
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: compactor
            image: iceberg-compaction:latest
            command: ["iceberg-compact"]
            args:
              - "--catalog-uri=$(CATALOG_URI)"
              - "--table=events"
              - "--strategy=small-files"
            env:
              - name: CATALOG_URI
                valueFrom:
                  secretKeyRef:
                    name: catalog-secret
                    key: uri
            resources:
              limits:
                memory: "4Gi"
                cpu: "2"
              requests:
                memory: "2Gi"
                cpu: "1"
          restartPolicy: OnFailure
```

This technical deep dive provides the actual implementation details and code patterns needed to build a production-grade Iceberg compaction service.
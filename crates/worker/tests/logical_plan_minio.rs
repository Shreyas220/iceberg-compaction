use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use compaction_common::{CompactionError, Result, StorageConfig, build_operator};
use compaction_worker::datafusion::logical_plan_sketch::{
    EqualityDeletePlanBuilder, RollingParquetWriter, RollingWriterConfig, execute_plan_to_parquet,
};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::context::SessionConfig;
use datafusion::prelude::SessionContext;
use object_store::aws::AmazonS3Builder;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use url::Url;

const MINIO_ENDPOINT: &str = "http://localhost:9000";
const MINIO_ACCESS_KEY: &str = "minioadmin";
const MINIO_SECRET_KEY: &str = "minioadmin";
const MINIO_BUCKET: &str = "warehouse";
const MINIO_REGION: &str = "us-east-1";

#[tokio::test]
#[ignore = "Requires MinIO running locally with a 'warehouse' bucket"]
async fn logical_plan_minio_equality_delete_smoke() -> Result<()> {
    let storage = StorageConfig::s3(MINIO_BUCKET)
        .with_endpoint(MINIO_ENDPOINT)
        .with_region(MINIO_REGION)
        .with_aws_credentials(MINIO_ACCESS_KEY, MINIO_SECRET_KEY, None::<String>);
    let operator = build_operator(&storage)?;

    // Build input parquet files in-memory (larger payload to exercise size-based rolling)
    let data_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, true),
        Field::new("seq", DataType::Int64, false),
    ]));

    let rows_per_batch: usize = 50_000;
    let batch_count: usize = 2;
    let payload = "x".repeat(200);

    let mut data_batches = Vec::with_capacity(batch_count);
    for batch_idx in 0..batch_count {
        let start = (batch_idx * rows_per_batch) as i64;
        data_batches.push(make_data_batch(start, rows_per_batch, &payload, data_schema.clone())?);
    }

    let delete_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("seq", DataType::Int64, false),
    ]));

    let delete_batch = RecordBatch::try_new(
        delete_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![25_000])),
            Arc::new(Int64Array::from(vec![10])),
        ],
    )
    .map_err(|e| CompactionError::Execution(e.to_string()))?;

    let data_bytes = build_parquet_bytes(data_schema.clone(), &data_batches)?;
    let delete_bytes = build_parquet_bytes(delete_schema.clone(), &[delete_batch])?;

    let data_path = "sketch/input/data.parquet";
    let delete_path = "sketch/input/delete.parquet";

    operator
        .write(data_path, data_bytes)
        .await
        .map_err(|e| CompactionError::Storage(e.to_string()))?;
    operator
        .write(delete_path, delete_bytes)
        .await
        .map_err(|e| CompactionError::Storage(e.to_string()))?;

    // Register object store for DataFusion
    let s3_store = AmazonS3Builder::new()
        .with_bucket_name(MINIO_BUCKET)
        .with_region(MINIO_REGION)
        .with_access_key_id(MINIO_ACCESS_KEY)
        .with_secret_access_key(MINIO_SECRET_KEY)
        .with_endpoint(MINIO_ENDPOINT)
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .build()
        .map_err(|e| CompactionError::Storage(e.to_string()))?;

    let s3_url = Url::parse(&format!("s3://{}", MINIO_BUCKET))
        .map_err(|e| CompactionError::Execution(e.to_string()))?;

    let session_config = SessionConfig::new().with_batch_size(1024);
    let ctx = SessionContext::new_with_config(session_config);
    ctx.runtime_env()
        .register_object_store(&s3_url, Arc::new(s3_store));

    let format = Arc::new(ParquetFormat::default());
    let listing_options = ListingOptions::new(format).with_file_extension(".parquet");

    let data_url = ListingTableUrl::parse(&format!("s3://{}/{}", MINIO_BUCKET, data_path))
        .map_err(|e| CompactionError::Execution(e.to_string()))?;
    let delete_url = ListingTableUrl::parse(&format!("s3://{}/{}", MINIO_BUCKET, delete_path))
        .map_err(|e| CompactionError::Execution(e.to_string()))?;

    let data_table = ListingTable::try_new(
        ListingTableConfig::new_with_multi_paths(vec![data_url])
            .with_listing_options(listing_options.clone())
            .with_schema(data_schema.clone()),
    )
    .map_err(|e| CompactionError::Execution(e.to_string()))?;

    let delete_table = ListingTable::try_new(
        ListingTableConfig::new_with_multi_paths(vec![delete_url])
            .with_listing_options(listing_options)
            .with_schema(delete_schema.clone()),
    )
    .map_err(|e| CompactionError::Execution(e.to_string()))?;

    let plan = EqualityDeletePlanBuilder::build(
        Arc::new(data_table),
        Arc::new(delete_table),
        "data",
        "deletes",
        &["id"],
        "seq",
        "seq",
        None,
    )?;

    let writer_config = RollingWriterConfig {
        output_prefix: "sketch/output".to_string(),
        file_prefix: "part".to_string(),
        target_file_size_bytes: 16 * 1024 * 1024, // ~16MB
        row_group_rows: 300_000,
        multipart_chunk_bytes: 8 * 1024 * 1024,
        ..RollingWriterConfig::default()
    };

    let mut writer =
        RollingParquetWriter::new(operator.clone(), data_schema.clone(), writer_config);
    execute_plan_to_parquet(&ctx, plan, &mut writer).await?;
    let outputs = writer.finish().await?;

    assert!(!outputs.is_empty());

    // Validate output rows (id=2 should be deleted)
    let mut total_rows = 0;
    let mut max_size = 0u64;
    for path in outputs {
        let meta = operator
            .stat(&path)
            .await
            .map_err(|e| CompactionError::Storage(e.to_string()))?;
        max_size = max_size.max(meta.content_length());

        let bytes = operator
            .read(&path)
            .await
            .map_err(|e| CompactionError::Storage(e.to_string()))?
            .to_bytes();
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| CompactionError::Execution(e.to_string()))?
            .build()
            .map_err(|e| CompactionError::Execution(e.to_string()))?;

        for batch in reader {
            let batch = batch.map_err(|e| CompactionError::Execution(e.to_string()))?;
            total_rows += batch.num_rows();
        }
    }

    let expected_rows = (rows_per_batch * batch_count - 1) as usize;
    assert_eq!(total_rows, expected_rows);
    assert!(
        max_size >= 5 * 1024 * 1024,
        "expected at least one output file >= 5MB, got {} bytes",
        max_size
    );

    Ok(())
}

fn build_parquet_bytes(schema: Arc<Schema>, batches: &[RecordBatch]) -> Result<Vec<u8>> {
    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, schema, None)
        .map_err(|e| CompactionError::Execution(e.to_string()))?;

    for batch in batches {
        writer
            .write(batch)
            .map_err(|e| CompactionError::Execution(e.to_string()))?;
    }

    writer
        .close()
        .map_err(|e| CompactionError::Execution(e.to_string()))?;

    Ok(buffer)
}

fn make_data_batch(
    start: i64,
    len: usize,
    payload: &str,
    schema: Arc<Schema>,
) -> Result<RecordBatch> {
    let ids = Int64Array::from_iter_values(start..start + len as i64);
    let seq = Int64Array::from_iter_values(std::iter::repeat(1_i64).take(len));
    let mut values = Vec::with_capacity(len);
    for id in start..start + len as i64 {
        values.push(format!("value_{:016x}_{}", id, payload));
    }
    let value_array = StringArray::from_iter_values(values);

    RecordBatch::try_new(
        schema,
        vec![Arc::new(ids), Arc::new(value_array), Arc::new(seq)],
    )
    .map_err(|e| CompactionError::Execution(e.to_string()))
}

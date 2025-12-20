/*
 * Task definitions for distributed compaction.
 *
 * A CompactionTask is what the planner sends to a worker.
 * It contains everything needed to execute compaction independently.
 */

use compaction_common::{ExecutionConfig, FileGroup, InlineDeleteData};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A compaction task to be executed by a worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionTask {
    /// Unique task ID
    pub task_id: Uuid,

    /// Table identifier (catalog.database.table)
    pub table_identifier: String,

    /// Snapshot ID this task is based on
    pub snapshot_id: i64,

    /// Files to compact
    pub file_group: FileGroup,

    /// Schema as JSON (Iceberg schema serialized)
    pub schema_json: String,

    /// Partition spec as JSON
    pub partition_spec_json: String,

    /// Sort order as JSON (from table metadata)
    pub sort_order_json: Option<String>,

    /// Output location for new files
    pub output_location: String,

    /// Execution configuration
    pub execution_config: ExecutionConfig,

    /// Inline delete data (for small deletes, sent directly)
    pub inline_deletes: Option<InlineDeleteData>,

    /// File IO configuration (S3 credentials, etc.)
    pub file_io_config: FileIOConfig,
}

/// File IO configuration for accessing storage.
///
/// This is designed to be serializable for transmission between planner and worker,
/// and can be converted to an opendal Operator for actual I/O operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileIOConfig {
    /// Storage backend type
    pub storage_type: StorageType,

    /// Root path/bucket for this storage (e.g., "my-bucket/warehouse")
    pub root: String,

    /// Endpoint override (for S3-compatible services, custom GCS endpoints, etc.)
    pub endpoint: Option<String>,

    /// Region (primarily for S3/Azure)
    pub region: Option<String>,

    /// Access credentials
    pub credentials: Option<StorageCredentials>,

    /// Additional configuration properties (provider-specific options)
    pub properties: std::collections::HashMap<String, String>,
}

impl FileIOConfig {
    /// Creates a new FileIOConfig for local filesystem.
    pub fn local(root: impl Into<String>) -> Self {
        Self {
            storage_type: StorageType::Local,
            root: root.into(),
            endpoint: None,
            region: None,
            credentials: None,
            properties: std::collections::HashMap::new(),
        }
    }

    /// Creates a new FileIOConfig for S3.
    pub fn s3(bucket: impl Into<String>, region: impl Into<String>) -> Self {
        Self {
            storage_type: StorageType::S3,
            root: bucket.into(),
            endpoint: None,
            region: Some(region.into()),
            credentials: None,
            properties: std::collections::HashMap::new(),
        }
    }

    /// Creates a new FileIOConfig for GCS.
    pub fn gcs(bucket: impl Into<String>) -> Self {
        Self {
            storage_type: StorageType::Gcs,
            root: bucket.into(),
            endpoint: None,
            region: None,
            credentials: None,
            properties: std::collections::HashMap::new(),
        }
    }

    /// Creates a new FileIOConfig for Azure Blob Storage.
    pub fn azure(container: impl Into<String>, account: impl Into<String>) -> Self {
        let mut props = std::collections::HashMap::new();
        props.insert("account".to_string(), account.into());
        Self {
            storage_type: StorageType::Azure,
            root: container.into(),
            endpoint: None,
            region: None,
            credentials: None,
            properties: props,
        }
    }

    /// Sets the endpoint (for S3-compatible services like MinIO).
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Sets credentials.
    pub fn with_credentials(mut self, credentials: StorageCredentials) -> Self {
        self.credentials = Some(credentials);
        self
    }

    /// Adds a custom property.
    pub fn with_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }
}

/// Storage backend type.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum StorageType {
    /// Amazon S3 or S3-compatible (MinIO, LocalStack, etc.)
    S3,
    /// Google Cloud Storage
    Gcs,
    /// Azure Blob Storage
    Azure,
    /// Local filesystem
    Local,
}

/// Storage credentials (serializable for task transmission).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StorageCredentials {
    /// AWS-style access key credentials
    AwsAccessKey {
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
    },
    /// GCP service account JSON key
    GcpServiceAccount {
        /// The service account JSON as a string
        service_account_json: String,
    },
    /// Azure storage account key
    AzureStorageKey {
        account_name: String,
        account_key: String,
    },
    /// Azure SAS token
    AzureSasToken {
        account_name: String,
        sas_token: String,
    },
}

impl FileIOConfig {
    /// Converts this FileIOConfig to a StorageConfig for opendal.
    pub fn to_storage_config(&self) -> compaction_common::StorageConfig {
        use compaction_common::{StorageBackend, StorageConfig};

        let backend = match self.storage_type {
            StorageType::S3 => StorageBackend::S3,
            StorageType::Gcs => StorageBackend::Gcs,
            StorageType::Azure => StorageBackend::Azure,
            StorageType::Local => StorageBackend::Local,
        };

        let mut config = StorageConfig {
            backend,
            root: self.root.clone(),
            endpoint: self.endpoint.clone(),
            region: self.region.clone(),
            options: self.properties.clone(),
        };

        // Apply credentials to options
        if let Some(creds) = &self.credentials {
            match creds {
                StorageCredentials::AwsAccessKey {
                    access_key_id,
                    secret_access_key,
                    session_token,
                } => {
                    config.options.insert("access_key_id".to_string(), access_key_id.clone());
                    config.options.insert("secret_access_key".to_string(), secret_access_key.clone());
                    if let Some(token) = session_token {
                        config.options.insert("session_token".to_string(), token.clone());
                    }
                }
                StorageCredentials::GcpServiceAccount { service_account_json } => {
                    config.options.insert("credential".to_string(), service_account_json.clone());
                }
                StorageCredentials::AzureStorageKey { account_name, account_key } => {
                    config.options.insert("account_name".to_string(), account_name.clone());
                    config.options.insert("account_key".to_string(), account_key.clone());
                }
                StorageCredentials::AzureSasToken { account_name, sas_token } => {
                    config.options.insert("account_name".to_string(), account_name.clone());
                    config.options.insert("sas_token".to_string(), sas_token.clone());
                }
            }
        }

        config
    }

    /// Builds an opendal Operator directly from this FileIOConfig.
    pub fn build_operator(&self) -> compaction_common::Result<opendal::Operator> {
        compaction_common::build_operator(&self.to_storage_config())
    }
}

/// Result of a compaction task execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionTaskResult {
    /// Task ID
    pub task_id: Uuid,

    /// Status of the task
    pub status: TaskStatus,

    /// Output files created (as JSON DataFile representations)
    pub output_files_json: Vec<String>,

    /// Statistics
    pub stats: TaskStats,

    /// Error message if failed
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskStatus {
    Success,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TaskStats {
    /// Input bytes read
    pub input_bytes: u64,
    /// Output bytes written
    pub output_bytes: u64,
    /// Input file count
    pub input_file_count: usize,
    /// Output file count
    pub output_file_count: usize,
    /// Rows processed
    pub rows_processed: u64,
    /// Execution time in milliseconds
    pub execution_time_ms: u64,
}

/// Request to register a worker with the planner.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerRegistration {
    /// Worker ID
    pub worker_id: Uuid,
    /// Worker address (host:port)
    pub address: String,
    /// Maximum concurrent tasks
    pub max_concurrent_tasks: usize,
    /// Available memory in bytes
    pub available_memory_bytes: u64,
}

/// Heartbeat from worker to planner.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerHeartbeat {
    /// Worker ID
    pub worker_id: Uuid,
    /// Currently running task IDs
    pub running_tasks: Vec<Uuid>,
    /// Current load (0.0 - 1.0)
    pub current_load: f64,
}

/// Task assignment from planner to worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAssignment {
    /// The task to execute
    pub task: CompactionTask,
    /// Deadline for task completion (Unix timestamp)
    pub deadline_timestamp: Option<i64>,
}

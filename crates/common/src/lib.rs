/*
 * Distributed Iceberg Compaction - Common Types
 *
 * Shared types, errors, and configuration used across planner and worker.
 */

pub mod config;
pub mod error;
pub mod file_group;
pub mod metrics;
pub mod retry;
pub mod storage;

pub use config::*;
pub use error::{CompactionError, Result, ResultExt};
pub use file_group::{
    EqualityDeleteBatch, FileGroup, FileMetadata, InlineDeleteData, PositionDeleteBatch,
};
pub use metrics::{CompactionMetrics, MetricsSnapshot, Timer};
pub use retry::{retry_async, RetryBuilder, RetryConfig};
pub use storage::{build_operator, StorageBackend, StorageConfig};

/*
 * Error types for distributed compaction.
 */

use thiserror::Error;

#[derive(Error, Debug)]
pub enum CompactionError {
    #[error("Planning error: {0}")]
    Planning(String),

    #[error("Execution error: {0}")]
    Execution(String),

    #[error("Communication error: {0}")]
    Communication(String),

    #[error("Iceberg error: {0}")]
    Iceberg(#[from] iceberg::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Worker error: {0}")]
    Worker(String),

    #[error("Commit error: {0}")]
    Commit(String),

    #[error("Unexpected error: {0}")]
    Unexpected(String),
}

pub type Result<T> = std::result::Result<T, CompactionError>;

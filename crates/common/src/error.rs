/*
 * Error types for distributed compaction.
 *
 * Errors are categorized by:
 * - Source: where the error originated (storage, worker, commit, etc.)
 * - Retryability: whether the operation can be retried
 * - Severity: how the error should be handled (retry, skip, abort)
 */

use std::time::Duration;
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

    #[error("Timeout: operation exceeded {0:?}")]
    Timeout(Duration),

    #[error("Resource exhausted: {0}")]
    ResourceExhausted(String),

    #[error("Conflict: {0}")]
    Conflict(String),

    #[error("Unexpected error: {0}")]
    Unexpected(String),
}

impl CompactionError {
    /// Returns true if this error is likely transient and the operation can be retried.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            CompactionError::Communication(_)
                | CompactionError::Storage(_)
                | CompactionError::Timeout(_)
                | CompactionError::ResourceExhausted(_)
                | CompactionError::Conflict(_)
        )
    }

    /// Returns a suggested retry delay for this error type.
    pub fn suggested_retry_delay(&self) -> Option<Duration> {
        match self {
            CompactionError::Communication(_) => Some(Duration::from_millis(100)),
            CompactionError::Storage(_) => Some(Duration::from_millis(200)),
            CompactionError::Timeout(_) => Some(Duration::from_secs(1)),
            CompactionError::ResourceExhausted(_) => Some(Duration::from_secs(5)),
            CompactionError::Conflict(_) => Some(Duration::from_millis(50)),
            _ => None,
        }
    }

    /// Wraps this error with additional context.
    pub fn with_context(self, context: impl Into<String>) -> Self {
        let ctx = context.into();
        match self {
            CompactionError::Planning(msg) => CompactionError::Planning(format!("{}: {}", ctx, msg)),
            CompactionError::Execution(msg) => {
                CompactionError::Execution(format!("{}: {}", ctx, msg))
            }
            CompactionError::Communication(msg) => {
                CompactionError::Communication(format!("{}: {}", ctx, msg))
            }
            CompactionError::Storage(msg) => CompactionError::Storage(format!("{}: {}", ctx, msg)),
            CompactionError::Worker(msg) => CompactionError::Worker(format!("{}: {}", ctx, msg)),
            CompactionError::Commit(msg) => CompactionError::Commit(format!("{}: {}", ctx, msg)),
            CompactionError::Serialization(msg) => {
                CompactionError::Serialization(format!("{}: {}", ctx, msg))
            }
            CompactionError::Unexpected(msg) => {
                CompactionError::Unexpected(format!("{}: {}", ctx, msg))
            }
            CompactionError::ResourceExhausted(msg) => {
                CompactionError::ResourceExhausted(format!("{}: {}", ctx, msg))
            }
            CompactionError::Conflict(msg) => CompactionError::Conflict(format!("{}: {}", ctx, msg)),
            // For errors with structured data, wrap differently
            e @ CompactionError::Iceberg(_) => {
                CompactionError::Unexpected(format!("{}: {}", ctx, e))
            }
            e @ CompactionError::Timeout(_) => CompactionError::Unexpected(format!("{}: {}", ctx, e)),
        }
    }
}

pub type Result<T> = std::result::Result<T, CompactionError>;

/// Extension trait for adding context to Results.
pub trait ResultExt<T> {
    /// Adds context to an error result.
    fn context(self, context: impl Into<String>) -> Result<T>;

    /// Adds context lazily (only evaluated on error).
    fn with_context<F, S>(self, f: F) -> Result<T>
    where
        F: FnOnce() -> S,
        S: Into<String>;
}

impl<T> ResultExt<T> for Result<T> {
    fn context(self, context: impl Into<String>) -> Result<T> {
        self.map_err(|e| e.with_context(context))
    }

    fn with_context<F, S>(self, f: F) -> Result<T>
    where
        F: FnOnce() -> S,
        S: Into<String>,
    {
        self.map_err(|e| e.with_context(f()))
    }
}

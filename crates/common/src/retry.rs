/*
 * Retry utilities with exponential backoff and jitter.
 *
 * Provides configurable retry logic for transient failures,
 * commonly needed for storage operations, network calls, and
 * catalog interactions.
 */

use crate::{CompactionError, Result};
use std::future::Future;
use std::time::Duration;
use tracing::{debug, warn};

/// Configuration for retry behavior.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts (0 = no retries, just the initial attempt)
    pub max_retries: usize,
    /// Initial delay before first retry
    pub initial_delay: Duration,
    /// Maximum delay between retries
    pub max_delay: Duration,
    /// Multiplier for exponential backoff (typically 2.0)
    pub backoff_multiplier: f64,
    /// Add random jitter to prevent thundering herd (0.0-1.0)
    pub jitter_factor: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            backoff_multiplier: 2.0,
            jitter_factor: 0.1,
        }
    }
}

impl RetryConfig {
    /// Creates a retry config with no retries (fail fast).
    pub fn no_retry() -> Self {
        Self {
            max_retries: 0,
            ..Default::default()
        }
    }

    /// Creates a retry config suitable for storage operations.
    pub fn for_storage() -> Self {
        Self {
            max_retries: 5,
            initial_delay: Duration::from_millis(200),
            max_delay: Duration::from_secs(30),
            backoff_multiplier: 2.0,
            jitter_factor: 0.2,
        }
    }

    /// Creates a retry config suitable for network/RPC operations.
    pub fn for_network() -> Self {
        Self {
            max_retries: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            backoff_multiplier: 2.0,
            jitter_factor: 0.15,
        }
    }

    /// Creates a retry config for catalog/commit operations.
    pub fn for_commit() -> Self {
        Self {
            max_retries: 5,
            initial_delay: Duration::from_millis(50),
            max_delay: Duration::from_secs(5),
            backoff_multiplier: 1.5,
            jitter_factor: 0.25, // Higher jitter for commits to reduce conflicts
        }
    }

    /// Calculates the delay for a given attempt number (0-indexed).
    pub fn delay_for_attempt(&self, attempt: usize) -> Duration {
        let base_delay = self.initial_delay.as_secs_f64()
            * self.backoff_multiplier.powi(attempt as i32);
        let capped_delay = base_delay.min(self.max_delay.as_secs_f64());

        // Add jitter
        let jitter = if self.jitter_factor > 0.0 {
            let jitter_range = capped_delay * self.jitter_factor;
            // Simple pseudo-random jitter based on attempt
            let jitter_value = (attempt as f64 * 0.618033988749895) % 1.0;
            jitter_range * jitter_value
        } else {
            0.0
        };

        Duration::from_secs_f64(capped_delay + jitter)
    }
}

/// Executes an async operation with retry logic.
///
/// The operation will be retried if it fails with a retryable error
/// (as determined by `CompactionError::is_retryable()`).
///
/// # Example
/// ```ignore
/// let result = retry_async(&RetryConfig::for_storage(), || async {
///     storage.write(path, data).await
/// }).await?;
/// ```
pub async fn retry_async<F, Fut, T>(config: &RetryConfig, operation: F) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut last_error = None;

    for attempt in 0..=config.max_retries {
        match operation().await {
            Ok(value) => {
                if attempt > 0 {
                    debug!("Operation succeeded on attempt {}", attempt + 1);
                }
                return Ok(value);
            }
            Err(e) => {
                let is_retryable = e.is_retryable();
                let attempts_remaining = config.max_retries.saturating_sub(attempt);

                if !is_retryable || attempts_remaining == 0 {
                    if !is_retryable {
                        debug!("Non-retryable error: {}", e);
                    } else {
                        warn!("All {} retries exhausted: {}", config.max_retries, e);
                    }
                    return Err(e);
                }

                let delay = e
                    .suggested_retry_delay()
                    .unwrap_or_else(|| config.delay_for_attempt(attempt));

                warn!(
                    "Attempt {} failed ({}), retrying in {:?}: {}",
                    attempt + 1,
                    attempts_remaining,
                    delay,
                    e
                );

                tokio::time::sleep(delay).await;
                last_error = Some(e);
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        CompactionError::Unexpected("Retry loop exited without result".to_string())
    }))
}

/// A builder for constructing retry operations with custom configuration.
pub struct RetryBuilder<'a> {
    config: &'a RetryConfig,
    operation_name: Option<String>,
}

impl<'a> RetryBuilder<'a> {
    /// Creates a new retry builder with the given configuration.
    pub fn new(config: &'a RetryConfig) -> Self {
        Self {
            config,
            operation_name: None,
        }
    }

    /// Sets a name for the operation (used in logging).
    pub fn operation(mut self, name: impl Into<String>) -> Self {
        self.operation_name = Some(name.into());
        self
    }

    /// Executes the operation with retry logic.
    pub async fn run<F, Fut, T>(self, operation: F) -> Result<T>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let mut last_error = None;
        let op_name = self.operation_name.as_deref().unwrap_or("operation");

        for attempt in 0..=self.config.max_retries {
            match operation().await {
                Ok(value) => {
                    if attempt > 0 {
                        debug!("{} succeeded on attempt {}", op_name, attempt + 1);
                    }
                    return Ok(value);
                }
                Err(e) => {
                    let is_retryable = e.is_retryable();
                    let attempts_remaining = self.config.max_retries.saturating_sub(attempt);

                    if !is_retryable || attempts_remaining == 0 {
                        if !is_retryable {
                            debug!("{}: non-retryable error: {}", op_name, e);
                        } else {
                            warn!(
                                "{}: all {} retries exhausted: {}",
                                op_name, self.config.max_retries, e
                            );
                        }
                        return Err(e);
                    }

                    let delay = e
                        .suggested_retry_delay()
                        .unwrap_or_else(|| self.config.delay_for_attempt(attempt));

                    warn!(
                        "{}: attempt {} failed ({} remaining), retrying in {:?}: {}",
                        op_name,
                        attempt + 1,
                        attempts_remaining,
                        delay,
                        e
                    );

                    tokio::time::sleep(delay).await;
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            CompactionError::Unexpected(format!("{}: retry loop exited without result", op_name))
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn test_delay_calculation() {
        let config = RetryConfig {
            initial_delay: Duration::from_millis(100),
            backoff_multiplier: 2.0,
            max_delay: Duration::from_secs(10),
            jitter_factor: 0.0,
            ..Default::default()
        };

        assert_eq!(config.delay_for_attempt(0), Duration::from_millis(100));
        assert_eq!(config.delay_for_attempt(1), Duration::from_millis(200));
        assert_eq!(config.delay_for_attempt(2), Duration::from_millis(400));
        assert_eq!(config.delay_for_attempt(3), Duration::from_millis(800));
    }

    #[test]
    fn test_delay_capped_at_max() {
        let config = RetryConfig {
            initial_delay: Duration::from_secs(1),
            backoff_multiplier: 10.0,
            max_delay: Duration::from_secs(5),
            jitter_factor: 0.0,
            ..Default::default()
        };

        // 1 * 10^5 = 100000 seconds, but should be capped at 5
        assert_eq!(config.delay_for_attempt(5), Duration::from_secs(5));
    }

    #[tokio::test]
    async fn test_retry_success_first_attempt() {
        let config = RetryConfig::default();
        let attempts = AtomicUsize::new(0);

        let result: Result<i32> = retry_async(&config, || {
            attempts.fetch_add(1, Ordering::SeqCst);
            async { Ok(42) }
        })
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_retry_success_after_retries() {
        let config = RetryConfig {
            initial_delay: Duration::from_millis(1),
            ..Default::default()
        };
        let attempts = AtomicUsize::new(0);

        let result: Result<i32> = retry_async(&config, || {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            async move {
                if attempt < 2 {
                    Err(CompactionError::Storage("transient".to_string()))
                } else {
                    Ok(42)
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_retry_non_retryable_fails_immediately() {
        let config = RetryConfig::default();
        let attempts = AtomicUsize::new(0);

        let result: Result<i32> = retry_async(&config, || {
            attempts.fetch_add(1, Ordering::SeqCst);
            async { Err(CompactionError::Serialization("bad data".to_string())) }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }
}

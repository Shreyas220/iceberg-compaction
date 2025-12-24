/*
 * Health Checks and Graceful Shutdown
 *
 * Provides production-ready health endpoints and graceful shutdown
 * handling for the worker service.
 *
 * Health checks expose:
 * - /health/live - Liveness probe (is the process running?)
 * - /health/ready - Readiness probe (can the worker accept tasks?)
 *
 * Graceful shutdown:
 * - Stops accepting new tasks
 * - Waits for in-flight tasks to complete (with timeout)
 * - Cleanly drains connections
 */

use compaction_common::CompactionMetrics;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

/// Health state for the worker.
#[derive(Debug, Clone)]
pub struct HealthState {
    /// Whether the worker is alive (process healthy)
    live: Arc<AtomicBool>,
    /// Whether the worker is ready to accept tasks
    ready: Arc<AtomicBool>,
    /// Number of tasks currently in flight
    in_flight_tasks: Arc<AtomicUsize>,
    /// Maximum concurrent tasks allowed
    max_concurrent_tasks: usize,
    /// Metrics collector for detailed health info
    metrics: Arc<CompactionMetrics>,
    /// Shutdown signal sender
    shutdown_tx: watch::Sender<bool>,
    /// Shutdown signal receiver
    shutdown_rx: watch::Receiver<bool>,
}

impl HealthState {
    /// Creates a new health state.
    pub fn new(max_concurrent_tasks: usize, metrics: Arc<CompactionMetrics>) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        Self {
            live: Arc::new(AtomicBool::new(true)),
            ready: Arc::new(AtomicBool::new(true)),
            in_flight_tasks: Arc::new(AtomicUsize::new(0)),
            max_concurrent_tasks,
            metrics,
            shutdown_tx,
            shutdown_rx,
        }
    }

    /// Returns whether the worker is alive.
    pub fn is_live(&self) -> bool {
        self.live.load(Ordering::SeqCst)
    }

    /// Returns whether the worker is ready to accept tasks.
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst) && !self.is_shutting_down()
    }

    /// Returns whether shutdown has been initiated.
    pub fn is_shutting_down(&self) -> bool {
        *self.shutdown_rx.borrow()
    }

    /// Returns the current number of in-flight tasks.
    pub fn in_flight_count(&self) -> usize {
        self.in_flight_tasks.load(Ordering::SeqCst)
    }

    /// Sets the ready state.
    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::SeqCst);
        tracing::info!("Worker ready state changed to: {}", ready);
    }

    /// Marks the worker as unhealthy (not live).
    pub fn mark_unhealthy(&self) {
        self.live.store(false, Ordering::SeqCst);
        self.ready.store(false, Ordering::SeqCst);
        tracing::error!("Worker marked as unhealthy");
    }

    /// Increments the in-flight task count.
    /// Returns false if the worker is at capacity or shutting down.
    pub fn try_accept_task(&self) -> bool {
        if self.is_shutting_down() {
            tracing::debug!("Rejecting task: worker is shutting down");
            return false;
        }

        let current = self.in_flight_tasks.fetch_add(1, Ordering::SeqCst);
        if current >= self.max_concurrent_tasks {
            self.in_flight_tasks.fetch_sub(1, Ordering::SeqCst);
            tracing::debug!(
                "Rejecting task: at capacity ({}/{})",
                current,
                self.max_concurrent_tasks
            );
            return false;
        }

        tracing::debug!(
            "Accepted task ({}/{})",
            current + 1,
            self.max_concurrent_tasks
        );
        true
    }

    /// Decrements the in-flight task count.
    pub fn task_completed(&self) {
        let prev = self.in_flight_tasks.fetch_sub(1, Ordering::SeqCst);
        tracing::debug!(
            "Task completed ({}/{})",
            prev.saturating_sub(1),
            self.max_concurrent_tasks
        );
    }

    /// Gets a receiver for shutdown signals.
    pub fn shutdown_receiver(&self) -> watch::Receiver<bool> {
        self.shutdown_rx.clone()
    }

    /// Initiates graceful shutdown.
    pub fn initiate_shutdown(&self) {
        tracing::info!("Initiating graceful shutdown");
        self.ready.store(false, Ordering::SeqCst);
        let _ = self.shutdown_tx.send(true);
    }

    /// Waits for all in-flight tasks to complete.
    pub async fn wait_for_drain(&self, timeout: Duration) -> bool {
        let start = std::time::Instant::now();

        while self.in_flight_count() > 0 {
            if start.elapsed() > timeout {
                tracing::warn!(
                    "Drain timeout: {} tasks still in flight",
                    self.in_flight_count()
                );
                return false;
            }

            tracing::debug!(
                "Waiting for {} tasks to complete...",
                self.in_flight_count()
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        tracing::info!("All tasks drained successfully");
        true
    }

    /// Returns detailed health information.
    pub fn detailed_status(&self) -> HealthStatus {
        let metrics = self.metrics.snapshot();

        HealthStatus {
            live: self.is_live(),
            ready: self.is_ready(),
            shutting_down: self.is_shutting_down(),
            in_flight_tasks: self.in_flight_count(),
            max_concurrent_tasks: self.max_concurrent_tasks,
            total_tasks_completed: metrics.tasks_completed,
            total_tasks_failed: metrics.tasks_failed,
            bytes_read: metrics.bytes_read,
            bytes_written: metrics.bytes_written,
        }
    }
}

/// Detailed health status for monitoring.
#[derive(Debug, Clone, serde::Serialize)]
pub struct HealthStatus {
    pub live: bool,
    pub ready: bool,
    pub shutting_down: bool,
    pub in_flight_tasks: usize,
    pub max_concurrent_tasks: usize,
    pub total_tasks_completed: u64,
    pub total_tasks_failed: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

/// Simple HTTP health check server.
///
/// Provides endpoints compatible with Kubernetes probes:
/// - GET /health/live - Returns 200 if alive, 503 otherwise
/// - GET /health/ready - Returns 200 if ready, 503 otherwise
/// - GET /health/status - Returns detailed JSON status
pub struct HealthServer {
    state: Arc<HealthState>,
    addr: SocketAddr,
}

impl HealthServer {
    /// Creates a new health server.
    pub fn new(state: Arc<HealthState>, addr: SocketAddr) -> Self {
        Self { state, addr }
    }

    /// Starts the health server.
    ///
    /// Returns a handle that can be used to wait for shutdown.
    pub async fn serve(
        self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind(self.addr).await?;
        tracing::info!("Health server listening on {}", self.addr);

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((stream, _)) => {
                            let state = self.state.clone();
                            tokio::spawn(async move {
                                let (reader, mut writer) = stream.into_split();
                                let mut reader = BufReader::new(reader);
                                let mut line = String::new();

                                if reader.read_line(&mut line).await.is_err() {
                                    return;
                                }

                                let response = Self::handle_request(&line, &state);
                                let _ = writer.write_all(response.as_bytes()).await;
                            });
                        }
                        Err(e) => {
                            tracing::warn!("Failed to accept connection: {}", e);
                        }
                    }
                }
                _ = shutdown.changed() => {
                    tracing::info!("Health server shutting down");
                    break;
                }
            }
        }

        Ok(())
    }

    fn handle_request(request_line: &str, state: &HealthState) -> String {
        let parts: Vec<&str> = request_line.split_whitespace().collect();
        let path = parts.get(1).unwrap_or(&"/");

        match *path {
            "/health/live" | "/healthz" => {
                if state.is_live() {
                    Self::ok_response("OK")
                } else {
                    Self::error_response("NOT LIVE")
                }
            }
            "/health/ready" | "/readyz" => {
                if state.is_ready() {
                    Self::ok_response("OK")
                } else {
                    Self::error_response("NOT READY")
                }
            }
            "/health/status" | "/health" => {
                let status = state.detailed_status();
                let body =
                    serde_json::to_string_pretty(&status).unwrap_or_else(|_| "{}".to_string());

                if status.live && status.ready {
                    Self::json_ok_response(&body)
                } else {
                    Self::json_error_response(&body)
                }
            }
            _ => Self::not_found_response(),
        }
    }

    fn ok_response(body: &str) -> String {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        )
    }

    fn error_response(body: &str) -> String {
        format!(
            "HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        )
    }

    fn json_ok_response(body: &str) -> String {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        )
    }

    fn json_error_response(body: &str) -> String {
        format!(
            "HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        )
    }

    fn not_found_response() -> String {
        "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n".to_string()
    }
}

/// Sets up graceful shutdown handling for Unix signals.
pub async fn setup_graceful_shutdown(state: Arc<HealthState>) {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut sigterm = signal(SignalKind::terminate()).expect("Failed to setup SIGTERM handler");
        let mut sigint = signal(SignalKind::interrupt()).expect("Failed to setup SIGINT handler");

        tokio::select! {
            _ = sigterm.recv() => {
                tracing::info!("Received SIGTERM");
            }
            _ = sigint.recv() => {
                tracing::info!("Received SIGINT");
            }
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to setup Ctrl+C handler");
        tracing::info!("Received Ctrl+C");
    }

    state.initiate_shutdown();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_state_creation() {
        let metrics = Arc::new(CompactionMetrics::new());
        let state = HealthState::new(4, metrics);

        assert!(state.is_live());
        assert!(state.is_ready());
        assert!(!state.is_shutting_down());
        assert_eq!(state.in_flight_count(), 0);
    }

    #[test]
    fn test_task_acceptance() {
        let metrics = Arc::new(CompactionMetrics::new());
        let state = HealthState::new(2, metrics);

        // Should accept up to max tasks
        assert!(state.try_accept_task());
        assert_eq!(state.in_flight_count(), 1);
        assert!(state.try_accept_task());
        assert_eq!(state.in_flight_count(), 2);

        // Should reject at capacity
        assert!(!state.try_accept_task());
        assert_eq!(state.in_flight_count(), 2);

        // Complete a task
        state.task_completed();
        assert_eq!(state.in_flight_count(), 1);

        // Should accept again
        assert!(state.try_accept_task());
        assert_eq!(state.in_flight_count(), 2);
    }

    #[test]
    fn test_shutdown() {
        let metrics = Arc::new(CompactionMetrics::new());
        let state = HealthState::new(4, metrics);

        assert!(state.is_ready());
        assert!(!state.is_shutting_down());

        state.initiate_shutdown();

        assert!(!state.is_ready());
        assert!(state.is_shutting_down());

        // Should reject new tasks during shutdown
        assert!(!state.try_accept_task());
    }

    #[test]
    fn test_detailed_status() {
        let metrics = Arc::new(CompactionMetrics::new());
        metrics.record_bytes_read(1000);
        metrics.record_bytes_written(800);

        let state = HealthState::new(4, metrics);
        let _ = state.try_accept_task();

        let status = state.detailed_status();
        assert!(status.live);
        assert!(status.ready);
        assert!(!status.shutting_down);
        assert_eq!(status.in_flight_tasks, 1);
        assert_eq!(status.max_concurrent_tasks, 4);
        assert_eq!(status.bytes_read, 1000);
        assert_eq!(status.bytes_written, 800);
    }
}

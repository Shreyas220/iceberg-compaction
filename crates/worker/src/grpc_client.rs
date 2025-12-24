/*
 * gRPC Client for Worker
 *
 * Connects to the planner to:
 * - Register the worker
 * - Send heartbeats
 * - Report task completion
 */

use compaction_common::{CompactionError, Result};
use compaction_proto::grpc::compaction_service_client::CompactionServiceClient;
use compaction_proto::grpc::{
    HeartbeatRequest, WorkerMetrics, WorkerRegistrationRequest,
};
use std::time::Duration;
use tokio::time::interval;
use tonic::transport::Channel;

/// Client for communicating with the planner.
pub struct WorkerClient {
    /// The gRPC client
    client: CompactionServiceClient<Channel>,
    /// Worker ID
    worker_id: String,
    /// Worker address (for registration)
    worker_address: String,
    /// Maximum concurrent tasks
    max_concurrent_tasks: usize,
    /// Heartbeat interval
    heartbeat_interval: Duration,
}

impl WorkerClient {
    /// Connects to the planner at the given address.
    pub async fn connect(
        planner_address: &str,
        worker_id: String,
        worker_address: String,
        max_concurrent_tasks: usize,
    ) -> Result<Self> {
        let channel = Channel::from_shared(planner_address.to_string())
            .map_err(|e| CompactionError::Unexpected(format!("Invalid address: {}", e)))?
            .connect()
            .await
            .map_err(|e| CompactionError::Unexpected(format!("Connection failed: {}", e)))?;

        let client = CompactionServiceClient::new(channel);

        Ok(Self {
            client,
            worker_id,
            worker_address,
            max_concurrent_tasks,
            heartbeat_interval: Duration::from_secs(10),
        })
    }

    /// Registers this worker with the planner.
    pub async fn register(&mut self) -> Result<Duration> {
        let request = WorkerRegistrationRequest {
            worker_id: self.worker_id.clone(),
            address: self.worker_address.clone(),
            max_concurrent_tasks: self.max_concurrent_tasks as u32,
            available_memory_bytes: Self::get_available_memory(),
            capabilities: vec!["parquet".to_string(), "iceberg".to_string()],
        };

        let response = self
            .client
            .register_worker(request)
            .await
            .map_err(|e| CompactionError::Unexpected(format!("Registration failed: {}", e)))?;

        let resp = response.into_inner();
        if !resp.accepted {
            return Err(CompactionError::Unexpected(format!(
                "Registration rejected: {}",
                resp.message
            )));
        }

        self.heartbeat_interval = Duration::from_millis(resp.heartbeat_interval_ms as u64);

        tracing::info!(
            "Registered with planner, heartbeat interval: {:?}",
            self.heartbeat_interval
        );

        Ok(self.heartbeat_interval)
    }

    /// Sends a heartbeat to the planner.
    pub async fn send_heartbeat(
        &mut self,
        running_task_ids: Vec<String>,
        current_load: f64,
        metrics: WorkerMetrics,
    ) -> Result<Vec<String>> {
        let request = HeartbeatRequest {
            worker_id: self.worker_id.clone(),
            running_task_ids,
            current_load,
            metrics: Some(metrics),
        };

        let response = self
            .client
            .heartbeat(request)
            .await
            .map_err(|e| CompactionError::Unexpected(format!("Heartbeat failed: {}", e)))?;

        let resp = response.into_inner();
        if !resp.acknowledged {
            tracing::warn!("Heartbeat not acknowledged by planner");
        }

        Ok(resp.tasks_to_cancel)
    }

    /// Starts the heartbeat loop in the background.
    pub fn start_heartbeat_loop(
        mut self,
        running_tasks: std::sync::Arc<tokio::sync::RwLock<Vec<String>>>,
        metrics_provider: std::sync::Arc<dyn Fn() -> WorkerMetrics + Send + Sync>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = interval(self.heartbeat_interval);

            loop {
                ticker.tick().await;

                let tasks = running_tasks.read().await.clone();
                let load = tasks.len() as f64 / self.max_concurrent_tasks as f64;
                let metrics = metrics_provider();

                match self.send_heartbeat(tasks, load, metrics).await {
                    Ok(tasks_to_cancel) => {
                        if !tasks_to_cancel.is_empty() {
                            tracing::warn!("Planner requested cancellation of tasks: {:?}", tasks_to_cancel);
                            // TODO: Actually cancel the tasks
                        }
                    }
                    Err(e) => {
                        tracing::error!("Heartbeat failed: {}", e);
                        // Could implement reconnection logic here
                    }
                }
            }
        })
    }

    /// Gets available system memory (placeholder implementation).
    fn get_available_memory() -> u64 {
        // In production, use sysinfo crate or similar
        8 * 1024 * 1024 * 1024 // 8GB default
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_available_memory() {
        let mem = WorkerClient::get_available_memory();
        assert!(mem > 0);
    }
}

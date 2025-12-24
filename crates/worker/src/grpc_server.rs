/*
 * gRPC Server for Worker
 *
 * Implements the CompactionService to receive tasks from the planner
 * and report progress/results back.
 */

use compaction_common::{CompactionMetrics, Result};
use compaction_proto::grpc::compaction_service_server::{CompactionService, CompactionServiceServer};
use compaction_proto::grpc::{
    CancelTaskRequest, CancelTaskResponse, GetTaskStatusRequest, GetTaskStatusResponse,
    HeartbeatRequest, HeartbeatResponse, SubmitTaskRequest, SubmitTaskResponse, TaskMetrics,
    TaskProgressUpdate, TaskResultResponse, TaskState, WorkerRegistrationRequest,
    WorkerRegistrationResponse,
};
use compaction_proto::{CompactionTask, TaskStatus};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status, Streaming};

use crate::executor::WorkerExecutor;

/// State of a running task
#[derive(Debug, Clone)]
struct TaskExecution {
    task_id: String,
    state: TaskState,
    progress: f64,
    metrics: TaskMetrics,
}

/// Worker gRPC server that receives and executes compaction tasks.
#[allow(dead_code)]
pub struct WorkerGrpcServer {
    /// The worker executor that runs tasks
    executor: Arc<WorkerExecutor>,
    /// Metrics collector
    metrics: Arc<CompactionMetrics>,
    /// Currently running tasks
    running_tasks: Arc<RwLock<HashMap<String, TaskExecution>>>,
    /// Worker ID
    worker_id: String,
    /// Maximum concurrent tasks
    max_concurrent_tasks: usize,
}

impl WorkerGrpcServer {
    /// Creates a new worker gRPC server.
    pub fn new(worker_id: String, max_concurrent_tasks: usize) -> Self {
        let metrics = Arc::new(CompactionMetrics::new());
        let executor = Arc::new(WorkerExecutor::with_metrics(metrics.clone()));

        Self {
            executor,
            metrics,
            running_tasks: Arc::new(RwLock::new(HashMap::new())),
            worker_id,
            max_concurrent_tasks,
        }
    }

    /// Starts the gRPC server on the given address.
    pub async fn serve(self, addr: SocketAddr) -> Result<()> {
        tracing::info!("Starting worker gRPC server on {}", addr);

        tonic::transport::Server::builder()
            .add_service(CompactionServiceServer::new(self))
            .serve(addr)
            .await
            .map_err(|e| compaction_common::CompactionError::Unexpected(e.to_string()))?;

        Ok(())
    }

    /// Returns the metrics collector.
    pub fn metrics(&self) -> Arc<CompactionMetrics> {
        self.metrics.clone()
    }
}

#[tonic::async_trait]
impl CompactionService for WorkerGrpcServer {
    /// Handle worker registration (workers don't register with other workers, but needed for interface).
    async fn register_worker(
        &self,
        _request: Request<WorkerRegistrationRequest>,
    ) -> std::result::Result<Response<WorkerRegistrationResponse>, Status> {
        // Workers don't accept registrations from other workers
        Ok(Response::new(WorkerRegistrationResponse {
            accepted: false,
            message: "This is a worker, not a planner".to_string(),
            heartbeat_interval_ms: 0,
        }))
    }

    /// Handle heartbeat (workers don't process heartbeats from other workers).
    async fn heartbeat(
        &self,
        _request: Request<HeartbeatRequest>,
    ) -> std::result::Result<Response<HeartbeatResponse>, Status> {
        Ok(Response::new(HeartbeatResponse {
            acknowledged: false,
            tasks_to_cancel: vec![],
        }))
    }

    /// Submit a new compaction task.
    async fn submit_task(
        &self,
        request: Request<SubmitTaskRequest>,
    ) -> std::result::Result<Response<SubmitTaskResponse>, Status> {
        let req = request.into_inner();
        let task_id = req.task_id.clone();

        // Check capacity
        let current_tasks = self.running_tasks.read().await.len();
        if current_tasks >= self.max_concurrent_tasks {
            return Ok(Response::new(SubmitTaskResponse {
                accepted: false,
                message: format!(
                    "Worker at capacity ({}/{})",
                    current_tasks, self.max_concurrent_tasks
                ),
                estimated_start_time: 0,
            }));
        }

        // Deserialize task
        let task: CompactionTask = serde_json::from_slice(&req.task_payload)
            .map_err(|e| Status::invalid_argument(format!("Invalid task payload: {}", e)))?;

        // Register task as running
        {
            let mut tasks = self.running_tasks.write().await;
            tasks.insert(
                task_id.clone(),
                TaskExecution {
                    task_id: task_id.clone(),
                    state: TaskState::Running,
                    progress: 0.0,
                    metrics: TaskMetrics::default(),
                },
            );
        }

        // Spawn task execution using spawn_local or spawn_blocking approach
        // Note: We use a channel to communicate results since the executor
        // may contain non-Send types (like Parquet's ArrowWriter)
        let executor = self.executor.clone();
        let running_tasks = self.running_tasks.clone();
        let metrics = self.metrics.clone();
        let task_id_clone = task_id.clone();

        // Use a separate runtime thread for the execution
        std::thread::spawn(move || {
            // Create a new runtime for this thread
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to create runtime");

            rt.block_on(async move {
                let result = executor.execute(task).await;

                // Update task state
                let mut tasks = running_tasks.write().await;
                if let Some(execution) = tasks.get_mut(&task_id_clone) {
                    match result.status {
                        TaskStatus::Success => {
                            execution.state = TaskState::Completed;
                            execution.progress = 1.0;
                        }
                        TaskStatus::Failed => {
                            execution.state = TaskState::Failed;
                        }
                        TaskStatus::Cancelled => {
                            execution.state = TaskState::Cancelled;
                        }
                    }
                    execution.metrics = TaskMetrics {
                        input_bytes_read: result.stats.input_bytes,
                        output_bytes_written: result.stats.output_bytes,
                        rows_processed: result.stats.rows_processed,
                        files_written: result.stats.output_file_count as u32,
                        elapsed_time_ms: result.stats.execution_time_ms,
                    };

                    // Record overall metrics
                    metrics.record_bytes_written(result.stats.output_bytes);
                }

                tracing::info!(
                    "Task {} completed with status: {:?}",
                    task_id_clone,
                    result.status
                );
            });
        });

        Ok(Response::new(SubmitTaskResponse {
            accepted: true,
            message: "Task accepted".to_string(),
            estimated_start_time: chrono::Utc::now().timestamp(),
        }))
    }

    /// Get the status of a task.
    async fn get_task_status(
        &self,
        request: Request<GetTaskStatusRequest>,
    ) -> std::result::Result<Response<GetTaskStatusResponse>, Status> {
        let req = request.into_inner();
        let tasks = self.running_tasks.read().await;

        if let Some(execution) = tasks.get(&req.task_id) {
            Ok(Response::new(GetTaskStatusResponse {
                task_id: execution.task_id.clone(),
                state: execution.state.into(),
                progress: execution.progress,
                message: String::new(),
                metrics: Some(execution.metrics.clone()),
            }))
        } else {
            Ok(Response::new(GetTaskStatusResponse {
                task_id: req.task_id,
                state: TaskState::Unknown.into(),
                progress: 0.0,
                message: "Task not found".to_string(),
                metrics: None,
            }))
        }
    }

    /// Cancel a running task.
    async fn cancel_task(
        &self,
        request: Request<CancelTaskRequest>,
    ) -> std::result::Result<Response<CancelTaskResponse>, Status> {
        let req = request.into_inner();
        let mut tasks = self.running_tasks.write().await;

        if let Some(execution) = tasks.get_mut(&req.task_id) {
            // Mark as cancelled (actual cancellation requires tokio_util::sync::CancellationToken)
            execution.state = TaskState::Cancelled;

            Ok(Response::new(CancelTaskResponse {
                cancelled: true,
                message: format!("Task {} marked for cancellation: {}", req.task_id, req.reason),
            }))
        } else {
            Ok(Response::new(CancelTaskResponse {
                cancelled: false,
                message: "Task not found".to_string(),
            }))
        }
    }

    /// Stream task results (bidirectional streaming).
    async fn stream_task_result(
        &self,
        _request: Request<Streaming<TaskProgressUpdate>>,
    ) -> std::result::Result<Response<TaskResultResponse>, Status> {
        // This would be used for streaming progress from worker to planner
        // For now, return a placeholder
        Err(Status::unimplemented("Streaming not yet implemented"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_server_creation() {
        let server = WorkerGrpcServer::new("test-worker".to_string(), 4);
        assert_eq!(server.worker_id, "test-worker");
        assert_eq!(server.max_concurrent_tasks, 4);
    }
}

/*
 * Distributed Iceberg Compaction - Worker
 *
 * The worker is responsible for:
 * 1. Receiving compaction tasks from the planner (via gRPC)
 * 2. Executing DataFusion queries to apply deletes
 * 3. Writing output files in streaming fashion (via opendal)
 * 4. Reporting results back to the planner
 *
 * IMPORTANT: For production, use jemalloc in your main binary:
 * ```
 * #[global_allocator]
 * static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
 * ```
 * This prevents memory fragmentation with large Parquet row groups.
 */

// Re-export jemalloc for easy use in binaries
pub use tikv_jemallocator::Jemalloc;

pub mod datafusion;
pub mod executor;
pub mod grpc_client;
pub mod grpc_server;
pub mod health;
pub mod writer;

pub use datafusion::{DataFusionEngine, FileScanProvider, SqlBuilder};
pub use executor::{OutputFileInfo, WorkerExecutor};
pub use grpc_client::WorkerClient;
pub use grpc_server::WorkerGrpcServer;
pub use health::{setup_graceful_shutdown, HealthServer, HealthState, HealthStatus};
pub use writer::{RollingWriter, RollingWriterConfig, WrittenFile};

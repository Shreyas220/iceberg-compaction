/*
 * Distributed Iceberg Compaction - Worker
 *
 * The worker is responsible for:
 * 1. Receiving compaction tasks from the planner
 * 2. Executing DataFusion queries to apply deletes
 * 3. Writing output files in streaming fashion
 * 4. Reporting results back to the planner
 */

pub mod datafusion;
pub mod executor;
pub mod writer;

pub use datafusion::{DataFusionEngine, FileScanProvider, SqlBuilder};
pub use executor::{OutputFileInfo, WorkerExecutor};
pub use writer::{RollingWriter, RollingWriterConfig, WrittenFile};

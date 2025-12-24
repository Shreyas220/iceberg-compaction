/*
 * Distributed Iceberg Compaction - Planner
 *
 * The planner is responsible for:
 * 1. Scanning tables to find files needing compaction
 * 2. Grouping files into compaction tasks
 * 3. Distributing tasks to workers
 * 4. Coordinating commits after workers complete
 */

pub mod commit;
pub mod file_selection;
pub mod metadata;
pub mod packer;
pub mod table_provider;
pub mod task_builder;

pub use commit::{CommitCoordinator, CommitSummary, DataFileInfo, PendingCommit};
// Re-export CommitMode from proto for convenience
pub use compaction_proto::CommitMode;
pub use file_selection::{FileSelector, SelectionStrategy};
pub use metadata::{AnchorStore, InMemoryAnchorStore, ManifestCache, SnapshotAnchor};
pub use table_provider::{IcebergTableProvider, TableProvider};
pub use task_builder::TaskBuilder;

// Re-export RestCatalog for convenience
pub use iceberg_catalog_rest::RestCatalog;

/*
 * Protocol definitions for planner <-> worker communication.
 *
 * This module contains:
 * - Task definitions (JSON serializable for flexibility)
 * - gRPC service definitions (for production distributed communication)
 */

pub mod task;

// Include the generated gRPC code
pub mod generated {
    include!("generated/compaction.rs");
}

// Re-export task types at the crate root
pub use task::*;

// Re-export gRPC service traits and types under a grpc namespace
pub mod grpc {
    pub use super::generated::*;
}

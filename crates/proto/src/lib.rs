/*
 * Protocol definitions for planner <-> worker communication.
 *
 * This defines the task format that planners create and workers execute.
 */

pub mod task;

pub use task::*;

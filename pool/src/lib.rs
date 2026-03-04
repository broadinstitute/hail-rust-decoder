//! genohype-pool: Generic distributed worker pool library
//!
//! This crate provides the infrastructure for distributed task execution,
//! including worker management, coordinator orchestration, and cloud deployment.
//!
//! # Core Traits
//!
//! - [`TaskHandler`](traits::TaskHandler) - Implement to define how tasks are executed
//! - [`CoordinatorPlugin`](traits::CoordinatorPlugin) - Implement to define work scheduling
//!
//! # Distributed Messages
//!
//! The [`distributed`] module provides generic message types for coordinator/worker
//! communication that use `serde_json::Value` for job payloads.
//!
//! # Cloud Orchestration
//!
//! The [`cloud`] module provides abstractions for managing worker pools across
//! different cloud providers.

pub mod cloud;
pub mod distributed;
pub mod traits;

// Re-export async_trait for implementors
pub use async_trait::async_trait;

// Re-export main types at crate root for convenience
pub use traits::{CoordinatorPlugin, TaskHandler, TaskResult, WorkAssignment};

// Re-export cloud types
pub use cloud::{CloudPoolManager, LocalPoolManager, PoolStatus, SubmitOptions};

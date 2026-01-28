//! Cloud infrastructure abstraction for distributed processing.
//!
//! This module provides traits and implementations for managing cloud VMs
//! for distributed hail-decoder workloads.

pub mod gcp;
pub mod pool;
pub mod startup;

use crate::Result;
use std::path::Path;
use std::process::Command;

/// Configuration for a worker pool.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Name of the pool (used for tagging and identification)
    pub name: String,
    /// Number of worker VMs to create
    pub worker_count: usize,
    /// GCP machine type (e.g., "c3-highcpu-22")
    pub machine_type: String,
    /// GCP zone (e.g., "us-central1-a")
    pub zone: String,
    /// Use spot/preemptible instances for cost savings
    pub spot: bool,
    /// GCP project ID
    pub project_id: String,
    /// VPC network name (None = default)
    pub network: Option<String>,
    /// Subnet name (required for custom networks)
    pub subnet: Option<String>,
}

/// Information about a cloud instance.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct Instance {
    /// Instance name
    pub name: String,
    /// Zone where instance is located
    pub zone: String,
    /// Network interfaces (for IP addresses)
    #[serde(rename = "networkInterfaces", default)]
    pub network_interfaces: Vec<NetworkInterface>,
    /// Instance status (RUNNING, TERMINATED, etc.)
    pub status: String,
}

/// Network interface information.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct NetworkInterface {
    /// Internal IP address
    #[serde(rename = "networkIP")]
    pub network_ip: String,
}

impl Instance {
    /// Get the internal IP address of the instance.
    pub fn ip(&self) -> Option<&str> {
        self.network_interfaces
            .first()
            .map(|ni| ni.network_ip.as_str())
    }

    /// Check if the instance is running.
    pub fn is_running(&self) -> bool {
        self.status == "RUNNING"
    }
}

/// Abstract interface for cloud infrastructure operations.
///
/// This trait allows swapping out the underlying cloud provider implementation
/// (e.g., gcloud CLI wrapper vs native SDK) without changing the orchestration logic.
pub trait CloudProvider {
    /// Provision a cluster of worker VMs.
    ///
    /// Creates `config.worker_count` VMs with names `{pool_name}-worker-{i}`.
    fn create_pool(&self, config: &PoolConfig) -> Result<()>;

    /// List active instances in the pool.
    ///
    /// Returns instances tagged with `pool-{pool_name}`.
    fn list_instances(&self, pool_name: &str) -> Result<Vec<Instance>>;

    /// Destroy all instances in the pool.
    fn destroy_pool(&self, pool_name: &str, zone: &str) -> Result<()>;

    /// Upload a file to a specific instance via SCP.
    fn upload_file(
        &self,
        local_path: &Path,
        remote_path: &str,
        instance: &str,
        zone: &str,
    ) -> Result<()>;

    /// Get a Command struct ready to execute SSH into an instance.
    ///
    /// The caller is responsible for spawning and managing I/O.
    /// This allows streaming stdout/stderr for real-time progress.
    fn get_ssh_command(&self, instance: &str, zone: &str, command: &str) -> Command;
}

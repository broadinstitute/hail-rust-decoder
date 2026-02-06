//! Worker pool management for distributed processing.
//!
//! This module provides the `PoolManager` which orchestrates:
//! - Creating worker VMs
//! - Deploying the hail-decoder binary
//! - Submitting distributed jobs
//! - Streaming logs and aggregating metrics
//! - Cleaning up resources

use crate::benchmark::BenchmarkReport;
use crate::cloud::{CloudProvider, Instance, PoolConfig, ProgressUpdate};
use crate::HailError;
use crate::Result;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use owo_colors::OwoColorize;
use rayon::prelude::*;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Instant;

/// Manages distributed worker pools for parallel processing.
pub struct PoolManager<P: CloudProvider> {
    provider: P,
}

impl<P: CloudProvider + Sync> PoolManager<P> {
    /// Create a new pool manager with the given cloud provider.
    pub fn new(provider: P) -> Self {
        Self { provider }
    }

    /// Create a new worker pool.
    ///
    /// Provisions `config.worker_count` VMs in parallel.
    /// If `wait` is true, polls until all VMs have completed their startup scripts.
    /// Automatically builds Linux binary if on macOS (unless `skip_build` is true).
    /// If `with_coordinator` is true, also starts the coordinator in idle mode.
    pub fn create(&self, config: &PoolConfig, wait: bool, skip_build: bool) -> Result<()> {
        // Build Linux binary first (needed for deployment)
        if !skip_build {
            Self::build_linux_binary()?;
        } else {
            println!("{}", "Skipping binary build (--skip-build)".dimmed());
        }

        println!(
            "{} pool '{}' with {} workers ({}, {})...",
            "Creating".green(),
            config.name.bright_white(),
            config.worker_count.to_string().bright_white(),
            config.machine_type.dimmed(),
            if config.spot { "spot" } else { "on-demand" }
        );

        self.provider.create_pool(config)?;

        println!(
            "{} Pool '{}' created successfully.",
            "OK".green().bold(),
            config.name
        );

        // Always wait for pool ready when with_coordinator, so we can deploy and start
        let should_wait = wait || config.with_coordinator;

        if should_wait {
            println!("{}", "Waiting for VMs to be ready...".dimmed());
            self.wait_for_pool_ready(&config.name, &config.zone, 300)?;
            println!("{} All workers are ready.", "OK".green().bold());
        } else {
            println!(
                "   Workers will be ready in ~60 seconds (or use --wait)"
            );
        }

        // If with_coordinator, deploy binary and start coordinator in idle mode
        if config.with_coordinator {
            self.start_idle_coordinator(&config.name, &config.zone, skip_build)?;
        }

        Ok(())
    }

    /// Deploy binary and start coordinator in idle mode.
    fn start_idle_coordinator(&self, pool_name: &str, zone: &str, _skip_build: bool) -> Result<()> {
        // Get coordinator instance
        let instances = self.provider.list_instances(pool_name)?;
        let coordinator = instances
            .iter()
            .find(|i| i.name.ends_with("-coordinator"))
            .ok_or_else(|| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!(
                        "No coordinator instance found for pool '{}'. Pool was not created with --with-coordinator?",
                        pool_name
                    ),
                ))
            })?;

        // Locate binary
        let binary = self.locate_binary(None)?;
        println!(
            "{} Deploying binary to coordinator...",
            "Setup:".cyan()
        );

        // Deploy to coordinator only
        self.deploy_binary(&binary, &[coordinator.clone()], zone)?;

        // Start coordinator in idle mode (no job args)
        println!(
            "{} Starting coordinator in idle mode on {}...",
            "Setup:".cyan(),
            coordinator.name.cyan()
        );

        let coord_cmd = format!(
            "nohup /usr/local/bin/hail-decoder service start-coordinator \
             --port 3000 \
             > /tmp/coordinator.log 2>&1 & echo $! > /tmp/coordinator.pid"
        );

        let status = self
            .provider
            .get_ssh_command(&coordinator.name, zone, &coord_cmd)
            .status()
            .map_err(HailError::Io)?;

        if !status.success() {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to start coordinator service in idle mode",
            )));
        }

        // Give it a moment to start
        std::thread::sleep(std::time::Duration::from_secs(2));

        // Verify it's running
        let coord_ip = coordinator.ip().unwrap_or("localhost");
        println!(
            "{} Coordinator started in idle mode",
            "OK".green().bold()
        );
        println!(
            "  Dashboard: http://{}:3000/dashboard",
            coord_ip
        );
        println!(
            "  Submit jobs with: hail-decoder pool submit {} -- <command>",
            pool_name
        );

        Ok(())
    }

    /// Check if coordinator service is already running and reachable.
    fn check_coordinator_status(&self, coordinator: &Instance, zone: &str) -> bool {
        // Try to reach the coordinator's /status endpoint via SSH
        let mut cmd = self.provider.get_ssh_command(
            &coordinator.name,
            zone,
            "curl -s --connect-timeout 2 http://localhost:3000/status",
        );
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::null());

        if let Ok(output) = cmd.output() {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                // Check if it looks like a valid JSON response
                return stdout.contains("\"pending\"") || stdout.contains("\"completed\"");
            }
        }
        false
    }

    /// Submit job configuration to an already-running coordinator via its API.
    fn submit_job_via_api(
        &self,
        coordinator: &Instance,
        zone: &str,
        input_path: &str,
        job_spec: &crate::distributed::message::JobSpec,
        total_partitions: usize,
        force: bool,
        batch_size: Option<usize>,
        filters: &[String],
        intervals: &[String],
    ) -> Result<bool> {
        use crate::distributed::message::{JobConfigRequest, JobConfigResponse};

        // Use provided batch_size or fall back to sensible defaults per job type
        // Larger batches let workers parallelize with rayon
        let batch_size = batch_size.or_else(|| match job_spec {
            crate::distributed::message::JobSpec::Manhattan(_) => Some(40),
            crate::distributed::message::JobSpec::ExportParquet { .. } => Some(100),
            _ => Some(50),
        });

        let request = JobConfigRequest {
            input_path: input_path.to_string(),
            job_spec: job_spec.clone(),
            total_partitions,
            batch_size,
            force,
            filters: filters.to_vec(),
            intervals: intervals.to_vec(),
        };

        let json_payload = serde_json::to_string(&request)
            .map_err(|e| HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to serialize job config: {}", e),
            )))?;

        // Submit via curl through SSH
        let curl_cmd = format!(
            "curl -s -X POST -H 'Content-Type: application/json' -d '{}' http://localhost:3000/api/job",
            json_payload.replace('\'', "'\\''") // Escape single quotes for shell
        );

        let mut cmd = self.provider.get_ssh_command(&coordinator.name, zone, &curl_cmd);
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());

        let output = cmd.output().map_err(HailError::Io)?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            eprintln!("Failed to submit job via API: {}", stderr);
            return Ok(false);
        }

        let stdout = String::from_utf8_lossy(&output.stdout);

        // Parse response
        match serde_json::from_str::<JobConfigResponse>(&stdout) {
            Ok(response) => {
                if response.acknowledged {
                    Ok(true)
                } else {
                    if let Some(err) = response.error {
                        eprintln!("Coordinator rejected job: {}", err);
                    }
                    Ok(false)
                }
            }
            Err(e) => {
                eprintln!("Failed to parse coordinator response: {} (raw: {})", e, stdout);
                Ok(false)
            }
        }
    }

    /// Wait for all instances in a pool to complete their startup scripts.
    fn wait_for_pool_ready(&self, pool_name: &str, zone: &str, timeout_secs: u64) -> Result<()> {
        use std::time::{Duration, Instant};

        let start = Instant::now();
        let timeout = Duration::from_secs(timeout_secs);

        // Get list of instances
        let instances = self.provider.list_instances(pool_name)?;
        if instances.is_empty() {
            return Ok(());
        }

        let total = instances.len();
        let mut ready_count = 0;

        println!("   Waiting for {} instances...", total);

        while ready_count < total {
            if start.elapsed() > timeout {
                return Err(crate::HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("Timeout waiting for pool '{}' to be ready", pool_name),
                )));
            }

            ready_count = 0;
            for inst in &instances {
                // Check if the ready marker file exists
                let mut cmd = self.provider.get_ssh_command(
                    &inst.name,
                    zone,
                    "test -f /tmp/hail-decoder-ready && echo ready",
                );
                cmd.stdout(std::process::Stdio::piped());
                cmd.stderr(std::process::Stdio::null());

                if let Ok(output) = cmd.output() {
                    if output.status.success() {
                        let stdout = String::from_utf8_lossy(&output.stdout);
                        if stdout.contains("ready") {
                            ready_count += 1;
                        }
                    }
                }
            }

            println!("   {}/{} ready", ready_count, total);

            if ready_count < total {
                std::thread::sleep(Duration::from_secs(5));
            }
        }

        Ok(())
    }

    /// Destroy a worker pool.
    ///
    /// Deletes all VMs tagged with the pool name.
    /// If `metrics_bucket` is provided, exports metrics to GCS before deletion.
    /// Otherwise, attempts to download metrics via SSH (may timeout).
    pub fn destroy(&self, name: &str, zone: &str, metrics_bucket: Option<&str>) -> Result<()> {
        println!("{} pool '{}'...", "Destroying".red(), name.bright_white());

        // First list to show what we're deleting
        let instances = self.provider.list_instances(name)?;
        if instances.is_empty() {
            println!("   No instances found for pool '{}'", name);
            return Ok(());
        }

        println!("   Found {} instances to delete", instances.len());
        for inst in &instances {
            println!("   - {}", inst.name.dimmed());
        }

        // Try to export/download metrics database from coordinator before destroying
        if let Some(coordinator) = instances.iter().find(|i| i.name.ends_with("-coordinator")) {
            if let Some(bucket) = metrics_bucket {
                // Export to GCS via API (fast and reliable)
                self.export_metrics_to_gcs(name, coordinator, zone, bucket);
            } else {
                // Fall back to SSH download (may timeout)
                self.download_metrics_db(name, &coordinator.name, zone);
            }
        }

        self.provider.destroy_pool(name, zone)?;

        println!("{} Pool '{}' destroyed.", "OK".green().bold(), name);

        Ok(())
    }

    /// Export metrics database to GCS via coordinator API.
    /// Best-effort: failures are logged but don't block pool destruction.
    fn export_metrics_to_gcs(
        &self,
        pool_name: &str,
        coordinator: &Instance,
        zone: &str,
        bucket_path: &str,
    ) {
        use crate::distributed::message::{ExportMetricsRequest, ExportMetricsResponse};
        use std::time::{SystemTime, UNIX_EPOCH};

        // Generate timestamp for unique filename
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        // Build the destination path
        let bucket_path = bucket_path.trim_end_matches('/');
        let destination = format!("{}/{}-{}-metrics.db", bucket_path, pool_name, timestamp);

        println!(
            "{} Exporting metrics to GCS...",
            "Saving:".cyan()
        );

        let request = ExportMetricsRequest {
            destination: destination.clone(),
        };

        let json_payload = match serde_json::to_string(&request) {
            Ok(j) => j,
            Err(e) => {
                println!(
                    "   {} Failed to serialize request: {}",
                    "Warning:".yellow(),
                    e
                );
                return;
            }
        };

        // Call the API via SSH curl
        let curl_cmd = format!(
            "curl -s -X POST -H 'Content-Type: application/json' -d '{}' http://localhost:3000/api/export-metrics",
            json_payload.replace('\'', "'\\''")
        );

        let mut cmd = self.provider.get_ssh_command(&coordinator.name, zone, &curl_cmd);
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());

        let output = match cmd.output() {
            Ok(o) => o,
            Err(e) => {
                println!(
                    "   {} Failed to call export API: {}",
                    "Warning:".yellow(),
                    e
                );
                return;
            }
        };

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            println!(
                "   {} Export API call failed: {}",
                "Warning:".yellow(),
                stderr.trim()
            );
            return;
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        match serde_json::from_str::<ExportMetricsResponse>(&stdout) {
            Ok(response) => {
                if response.success {
                    println!(
                        "   {} Metrics exported to {}",
                        "OK".green().bold(),
                        response.path.unwrap_or(destination).bright_white()
                    );
                } else {
                    println!(
                        "   {} {}",
                        "Warning:".yellow(),
                        response.error.unwrap_or_else(|| "Unknown error".to_string())
                    );
                }
            }
            Err(e) => {
                println!(
                    "   {} Failed to parse response: {} (raw: {})",
                    "Warning:".yellow(),
                    e,
                    stdout.trim()
                );
            }
        }
    }

    /// Download metrics database from coordinator VM.
    /// Best-effort: failures are logged but don't block pool destruction.
    /// Saves to ~/.local/share/hail-decoder/<pool-name>-<timestamp>-metrics.db
    fn download_metrics_db(&self, pool_name: &str, coordinator_name: &str, zone: &str) {
        use std::time::{SystemTime, UNIX_EPOCH};

        // Use XDG data directory: ~/.local/share/hail-decoder/
        let data_dir = dirs::data_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("hail-decoder");

        // Create directory if it doesn't exist
        if let Err(e) = std::fs::create_dir_all(&data_dir) {
            println!(
                "   {} Failed to create data directory: {}",
                "Warning:".yellow(),
                e
            );
            return;
        }

        // Generate timestamp for unique filename
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let local_path = data_dir.join(format!("{}-{}-metrics.db", pool_name, timestamp));

        println!(
            "{} Downloading metrics database...",
            "Saving:".cyan()
        );

        // Use gcloud compute scp to download the file
        let result = std::process::Command::new("gcloud")
            .args([
                "compute",
                "scp",
                &format!("{}:/tmp/hail-coordinator-metrics.db", coordinator_name),
                local_path.to_str().unwrap_or("metrics.db"),
                "--zone",
                zone,
                "--quiet",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .output();

        match result {
            Ok(output) if output.status.success() => {
                println!(
                    "   {} Metrics saved to {}",
                    "OK".green().bold(),
                    local_path.display().to_string().bright_white()
                );
            }
            Ok(output) => {
                let stderr = String::from_utf8_lossy(&output.stderr);
                if stderr.contains("No such file") {
                    println!("   {} No metrics database found on coordinator", "Note:".yellow());
                } else {
                    println!(
                        "   {} Failed to download metrics: {}",
                        "Warning:".yellow(),
                        stderr.trim()
                    );
                }
            }
            Err(e) => {
                println!(
                    "   {} Failed to download metrics: {}",
                    "Warning:".yellow(),
                    e
                );
            }
        }
    }

    /// Cancel a distributed job running on the pool.
    pub fn cancel(&self, name: &str, zone: &str) -> Result<()> {
        let instances = self.provider.list_instances(name)?;
        let coordinator = instances.iter().find(|i| i.name.ends_with("-coordinator"));

        if let Some(coord) = coordinator {
            println!("Sending cancel request to {}...", coord.name);

            use crate::distributed::message::{CancelRequest, CancelResponse};
            let request = CancelRequest {
                reason: Some("CLI cancel command".to_string()),
            };
            let json_payload = serde_json::to_string(&request).unwrap();

            let cmd_str = format!(
                "curl -s -X POST -H 'Content-Type: application/json' -d '{}' http://localhost:3000/api/cancel",
                json_payload
            );

            let mut cmd = self
                .provider
                .get_ssh_command(&coord.name, zone, &cmd_str);
            cmd.stdout(std::process::Stdio::piped());

            if let Ok(output) = cmd.output() {
                if output.status.success() {
                    let stdout = String::from_utf8_lossy(&output.stdout);
                    if let Ok(response) = serde_json::from_str::<CancelResponse>(&stdout) {
                        if response.success {
                            println!("{} {}", "Success:".green().bold(), response.message);
                        } else {
                            println!("{} {}", "Failed:".red().bold(), response.message);
                        }
                        return Ok(());
                    }
                }
            }
            println!("Failed to communicate with coordinator");
        } else {
            println!("No coordinator found for pool '{}'", name);
        }
        Ok(())
    }

    /// Get status of a distributed job running on the pool.
    pub fn status(&self, name: &str, zone: &str) -> Result<()> {
        let instances = self.provider.list_instances(name)?;
        let coordinator = instances.iter().find(|i| i.name.ends_with("-coordinator"));

        if let Some(coord) = coordinator {
            println!("Fetching status from {}...", coord.name);
            let mut cmd = self.provider.get_ssh_command(
                &coord.name,
                zone,
                "curl -s http://localhost:3000/status",
            );
            cmd.stdout(std::process::Stdio::piped());

            if let Ok(output) = cmd.output() {
                if output.status.success() {
                    let json_str = String::from_utf8_lossy(&output.stdout);
                    if let Ok(status) = serde_json::from_str::<
                        crate::distributed::message::StatusResponse,
                    >(&json_str)
                    {
                        println!();
                        println!("{}", "Job Status".bold().underline());
                        println!(
                            "  Progress:    {}/{} partitions ({:.1}%)",
                            status.completed,
                            status.total,
                            if status.total > 0 {
                                (status.completed as f64 / status.total as f64) * 100.0
                            } else {
                                0.0
                            }
                        );
                        println!("  Processing:  {} workers active", status.processing);
                        println!("  Pending:     {} partitions", status.pending);
                        if status.failed > 0 {
                            println!(
                                "  {} {} partitions",
                                "Failed:".red(),
                                status.failed
                            );
                        }
                        println!("  Rows:        {}", status.total_rows);
                        return Ok(());
                    }
                }
            }
            println!("Could not connect to coordinator service. Is the job running?");
        } else {
            println!("No coordinator found for pool '{}'", name);
        }
        Ok(())
    }

    /// List instances in a pool.
    pub fn list(&self, name: &str) -> Result<Vec<Instance>> {
        let instances = self.provider.list_instances(name)?;

        if instances.is_empty() {
            println!("No instances found for pool '{}'", name);
        } else {
            println!(
                "Pool '{}' has {} instances:",
                name.bright_white(),
                instances.len().to_string().bright_white()
            );
            for inst in &instances {
                let status_color = if inst.is_running() {
                    inst.status.green().to_string()
                } else {
                    inst.status.yellow().to_string()
                };
                println!(
                    "  {} {} ({})",
                    inst.name.cyan(),
                    status_color,
                    inst.ip().unwrap_or("no IP")
                );
            }
        }

        Ok(instances)
    }

    /// Scale the number of workers in a pool.
    ///
    /// This will:
    /// - Scale up: create new worker VMs, wait for startup, deploy binary
    /// - Scale down: delete workers with highest indices
    ///
    /// The coordinator is never affected by scaling operations.
    pub fn scale(
        &self,
        name: &str,
        target_workers: usize,
        zone: &str,
        binary_path: Option<String>,
        skip_build: bool,
        config: &crate::cloud::ScalingConfig,
    ) -> Result<()> {
        println!(
            "{} pool '{}' to {} workers...",
            "Scaling".green(),
            name.bright_white(),
            target_workers.to_string().bright_white()
        );

        // 1. Get current status
        let instances = self.provider.list_instances(name)?;
        let workers: Vec<&Instance> = instances
            .iter()
            .filter(|i| i.name.contains("-worker-"))
            .collect();

        let current_count = workers.len();

        if current_count == target_workers {
            println!(
                "{} Pool already has {} workers.",
                "OK".green().bold(),
                current_count
            );
            return Ok(());
        }

        // 2. Identify coordinator (needed for deploying binary to new workers)
        let coordinator = instances
            .iter()
            .find(|i| i.name.ends_with("-coordinator"));
        if coordinator.is_none() && config.with_coordinator {
            println!(
                "{} Coordinator not found, but configuration expects one.",
                "Warning:".yellow()
            );
        }

        // Get project ID for operations
        let project_id = config
            .project
            .clone()
            .unwrap_or_else(|| "default".to_string());

        if target_workers > current_count {
            // SCALE UP
            let to_add = target_workers - current_count;
            println!(
                "{} Adding {} workers...",
                "Scaling up:".cyan(),
                to_add.to_string().bright_white()
            );

            // Build binary first if needed (fail fast before creating VMs)
            if !skip_build {
                Self::build_linux_binary()?;
            }
            let binary = self.locate_binary(binary_path.clone())?;

            // Determine indices for new workers
            // Find existing indices and create new workers at gaps or at the end
            let mut existing_indices: Vec<usize> = workers
                .iter()
                .filter_map(|w| {
                    w.name
                        .split("-worker-")
                        .nth(1)
                        .and_then(|s| s.parse().ok())
                })
                .collect();
            existing_indices.sort();

            let mut new_instances = Vec::new();
            let mut next_idx = 0;

            for _ in 0..to_add {
                // Find the next available index
                while existing_indices.contains(&next_idx) {
                    next_idx += 1;
                }

                let instance_name = format!("{}-worker-{}", name, next_idx);
                let tags = format!(
                    "hail-decoder-worker,pool-{},role-worker",
                    name
                );

                new_instances.push(crate::cloud::InstanceSetup {
                    name: instance_name,
                    machine_type: config.machine_type.clone(),
                    zone: zone.to_string(),
                    tags: vec![tags],
                    startup_script: super::startup::generate_startup_script(),
                    spot: config.spot,
                    network: config.network.clone(),
                    subnet: config.subnet.clone(),
                    project_id: project_id.clone(),
                });

                existing_indices.push(next_idx);
            }

            // Create instances
            self.provider.create_instances(&new_instances)?;
            println!(
                "{} Created {} new instances.",
                "OK".green().bold(),
                to_add
            );

            // Wait for readiness
            println!("{}", "Waiting for new instances to be ready...".dimmed());
            for inst in &new_instances {
                self.wait_for_startup_complete(&inst.name, zone, 300)?;
            }

            // Get updated instance list to get IPs
            let updated_instances = self.provider.list_instances(name)?;
            let new_worker_instances: Vec<Instance> = updated_instances
                .into_iter()
                .filter(|i| new_instances.iter().any(|n| n.name == i.name))
                .collect();

            // Deploy binary
            if let Some(coord) = coordinator {
                if let Some(coord_ip) = coord.ip() {
                    // Coordinator exists, check if it's running to serve binary
                    if self.check_coordinator_status(coord, zone) {
                        println!(
                            "{}",
                            "Deploying binary via coordinator...".dimmed()
                        );
                        self.propagate_binary_from_coordinator(
                            coord_ip,
                            &new_worker_instances,
                            zone,
                        )?;
                    } else {
                        // Coordinator not running, deploy via SCP
                        println!(
                            "{}",
                            "Coordinator not running, deploying via SCP...".dimmed()
                        );
                        self.deploy_binary(&binary, &new_worker_instances, zone)?;
                    }
                } else {
                    self.deploy_binary(&binary, &new_worker_instances, zone)?;
                }
            } else {
                // No coordinator, direct SCP
                self.deploy_binary(&binary, &new_worker_instances, zone)?;
            }

            println!(
                "{} Scaled up to {} workers.",
                "OK".green().bold(),
                target_workers
            );
        } else {
            // SCALE DOWN
            let to_remove = current_count - target_workers;
            println!(
                "{} Removing {} workers...",
                "Scaling down:".cyan(),
                to_remove.to_string().bright_white()
            );

            // Sort workers by index descending to remove highest indices first
            let mut sorted_workers: Vec<(usize, &Instance)> = workers
                .iter()
                .filter_map(|w| {
                    w.name
                        .split("-worker-")
                        .nth(1)
                        .and_then(|s| s.parse().ok())
                        .map(|idx| (idx, *w))
                })
                .collect();

            // Sort descending by index
            sorted_workers.sort_by(|a, b| b.0.cmp(&a.0));

            let instances_to_delete: Vec<String> = sorted_workers
                .iter()
                .take(to_remove)
                .map(|(_, w)| w.name.clone())
                .collect();

            // Show which instances are being deleted
            for name in &instances_to_delete {
                println!("  {} {}", "Deleting:".dimmed(), name.yellow());
            }

            self.provider
                .delete_instances(&instances_to_delete, zone, &project_id)?;

            println!(
                "{} Scaled down to {} workers.",
                "OK".green().bold(),
                target_workers
            );
        }

        Ok(())
    }

    /// Wait for startup script to complete on a specific instance.
    fn wait_for_startup_complete(
        &self,
        instance_name: &str,
        zone: &str,
        timeout_secs: u64,
    ) -> Result<()> {
        use std::time::{Duration, Instant};

        let start = Instant::now();
        let timeout = Duration::from_secs(timeout_secs);

        loop {
            if start.elapsed() > timeout {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!(
                        "Timeout waiting for startup script on instance {}",
                        instance_name
                    ),
                )));
            }

            // Check if the ready marker file exists
            let mut cmd = self.provider.get_ssh_command(
                instance_name,
                zone,
                "test -f /tmp/hail-decoder-ready && echo ready",
            );
            cmd.stdout(std::process::Stdio::piped());
            cmd.stderr(std::process::Stdio::null());

            if let Ok(output) = cmd.output() {
                if output.status.success() {
                    let stdout = String::from_utf8_lossy(&output.stdout);
                    if stdout.contains("ready") {
                        println!("  {} {}", "Ready:".dimmed(), instance_name.cyan());
                        return Ok(());
                    }
                }
            }

            std::thread::sleep(Duration::from_secs(5));
        }
    }

    /// Update the binary on a running pool.
    ///
    /// This will:
    /// 1. Build the Linux binary (unless skip_build is true)
    /// 2. Upload the binary to the coordinator
    /// 3. Ensure coordinator is running (to serve /api/binary)
    /// 4. Have all workers pull the new binary from the coordinator
    ///
    /// This is useful for updating code on a long-running pool without
    /// destroying and recreating it.
    pub fn update_binary(
        &self,
        name: &str,
        zone: &str,
        binary_path: Option<String>,
        skip_build: bool,
    ) -> Result<()> {
        // Build Linux binary first (unless skipped)
        if !skip_build {
            Self::build_linux_binary()?;
        } else {
            println!("{}", "Skipping binary build (--skip-build)".dimmed());
        }

        // Locate the binary
        let binary = self.locate_binary(binary_path)?;
        println!(
            "{} {}",
            "Binary:".cyan(),
            binary.display().to_string().bright_white()
        );

        // Get running instances
        println!("{}", "Fetching instance list...".dimmed());
        let instances = self.provider.list_instances(name)?;
        let running: Vec<_> = instances.into_iter().filter(|i| i.is_running()).collect();

        if running.is_empty() {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "No running instances found for pool '{}'. Is the pool running?",
                    name
                ),
            )));
        }

        // Separate coordinator from workers
        let (coordinators, workers): (Vec<_>, Vec<_>) = running
            .into_iter()
            .partition(|i| i.name.ends_with("-coordinator"));

        let coordinator = coordinators.into_iter().next().ok_or_else(|| {
            HailError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "No coordinator found for pool '{}'. This command requires a coordinator.\n\
                     Create pool with: hail-decoder pool create {} --with-coordinator",
                    name, name
                ),
            ))
        })?;

        let coord_ip = coordinator.ip().ok_or_else(|| {
            HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Coordinator {} has no internal IP", coordinator.name),
            ))
        })?;

        println!(
            "{} coordinator: {} ({}), {} workers",
            "Found".green(),
            coordinator.name.cyan(),
            coord_ip,
            workers.len().to_string().bright_white()
        );

        // Check if coordinator is running (we'll need to restart it)
        let coord_was_running = self.check_coordinator_status(&coordinator, zone);

        // Stop coordinator if running (so we can update the binary)
        if coord_was_running {
            println!(
                "{}",
                "Stopping coordinator service for update...".dimmed()
            );
            let stop_cmd = "pkill -f 'hail-decoder service start-coordinator' || true";
            let _ = self
                .provider
                .get_ssh_command(&coordinator.name, zone, stop_cmd)
                .status();

            // Give it a moment to stop
            std::thread::sleep(std::time::Duration::from_secs(1));
        }

        // Deploy binary to coordinator
        println!("{}", "Uploading binary to coordinator...".dimmed());
        self.deploy_binary(&binary, &[coordinator.clone()], zone)?;
        println!(
            "{} Binary uploaded to coordinator.",
            "OK".green().bold()
        );

        // Start/restart coordinator service (to serve /api/binary and new dashboard)
        println!(
            "{}",
            "Starting coordinator service with new binary...".dimmed()
        );
        let coord_cmd =
            "nohup /usr/local/bin/hail-decoder service start-coordinator \
             --port 3000 \
             > /tmp/coordinator.log 2>&1 & echo $! > /tmp/coordinator.pid";
        let status = self
            .provider
            .get_ssh_command(&coordinator.name, zone, coord_cmd)
            .status()
            .map_err(HailError::Io)?;

        if !status.success() {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to start coordinator service",
            )));
        }

        // Wait for coordinator to be ready
        std::thread::sleep(std::time::Duration::from_secs(2));

        // Verify coordinator is back up
        if !self.check_coordinator_status(&coordinator, zone) {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Coordinator failed to start after binary update. Check /tmp/coordinator.log",
            )));
        }
        println!(
            "{} Coordinator restarted with new binary.",
            "OK".green().bold()
        );

        // Have workers pull binary from coordinator
        if workers.is_empty() {
            println!("{}", "No workers to update.".yellow());
        } else {
            println!(
                "{}",
                format!("Workers pulling binary from coordinator ({})...", coord_ip).dimmed()
            );
            self.propagate_binary_from_coordinator(coord_ip, &workers, zone)?;
            println!(
                "{} Binary updated on {} workers.",
                "OK".green().bold(),
                workers.len()
            );
        }

        println!();
        println!(
            "{} Binary updated on pool '{}'",
            "Done!".green().bold(),
            name.bright_white()
        );

        Ok(())
    }

    /// Submit a job to the worker pool.
    ///
    /// This will:
    /// 1. Locate or validate the Linux binary
    /// 2. Upload the binary to all workers in parallel
    /// 3. Execute the command on each worker with partition slicing
    /// 4. Stream logs and aggregate benchmark results
    ///
    /// Automatically uses coordinator/worker pattern when a coordinator exists,
    /// providing resilient distributed processing with automatic retry on Spot
    /// instance preemption.
    ///
    /// If `autoscale` is true and `config` is provided, automatically scales
    /// workers up before the job and down to 0 after the job completes.
    pub fn submit(
        &self,
        name: &str,
        zone: &str,
        binary_path: Option<String>,
        auto_stop: bool,
        force_redeploy: bool,
        force: bool,
        autoscale: bool,
        batch_size: Option<usize>,
        config: Option<&crate::cloud::ScalingConfig>,
        command: &[String],
    ) -> Result<()> {
        // Build binary if redeploying (ensures latest code is used)
        if force_redeploy {
            Self::build_linux_binary()?;
        }

        // Handle autoscaling
        if autoscale {
            let pool_config = config.ok_or_else(|| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Autoscaling requires pool configuration. Ensure pool is defined in config.toml",
                ))
            })?;

            // Scale up to target workers
            let target = pool_config.workers;
            println!(
                "{} Autoscaling up to {} workers...",
                "Setup:".cyan(),
                target.to_string().bright_white()
            );

            // Build binary once (skip if already built for redeploy), then scale
            if !force_redeploy {
                Self::build_linux_binary()?;
            }
            self.scale(name, target, zone, binary_path.clone(), true, pool_config)?;
        }

        // Run the actual job
        let result = self.submit_internal(
            name,
            zone,
            binary_path.clone(),
            auto_stop,
            force_redeploy,
            force,
            batch_size,
            command,
        );

        // Handle autoscaling down
        if autoscale {
            if let Some(pool_config) = config {
                println!(
                    "\n{} Autoscaling down to 0 workers...",
                    "Cleanup:".cyan()
                );
                // Ignore errors during scale down to ensure we return the job result
                if let Err(e) = self.scale(name, 0, zone, binary_path, true, pool_config) {
                    eprintln!("{} Failed to scale down: {}", "Warning:".yellow(), e);
                }
            }
        }

        result
    }

    /// Internal submit implementation (called by submit, handles the actual job).
    fn submit_internal(
        &self,
        name: &str,
        zone: &str,
        binary_path: Option<String>,
        auto_stop: bool,
        force_redeploy: bool,
        force: bool,
        batch_size: Option<usize>,
        command: &[String],
    ) -> Result<()> {
        // 1. Locate the Linux binary (will check if needed after seeing coordinator status)
        let binary = self.locate_binary(binary_path).ok();

        // 2. Get running instances
        println!("{}", "Fetching instance list...".dimmed());
        let instances = self.provider.list_instances(name)?;
        let running: Vec<_> = instances.into_iter().filter(|i| i.is_running()).collect();

        if running.is_empty() {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "No running instances found for pool '{}'. Create with: hail-decoder pool create {}",
                    name, name
                ),
            )));
        }

        // Separate coordinator from workers
        let (coordinators, workers): (Vec<_>, Vec<_>) = running
            .into_iter()
            .partition(|i| i.name.ends_with("-coordinator"));

        let coordinator = coordinators.into_iter().next();
        let total_workers = workers.len();

        // Auto-detect distributed mode: use coordinator/worker pattern when coordinator exists
        let use_distributed = coordinator.is_some();

        // Validate we have workers for distributed mode
        if use_distributed && total_workers == 0 {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "No workers available for pool '{}'. Either:\n\
                     - Scale up workers: hail-decoder pool scale {} --workers N\n\
                     - Use --autoscale to automatically scale workers for the job",
                    name, name
                ),
            )));
        }

        println!(
            "{} {} running worker(s){}",
            "Found".green(),
            total_workers.to_string().bright_white(),
            if let Some(ref c) = coordinator {
                format!(", 1 coordinator ({})", c.name.cyan())
            } else {
                String::new()
            }
        );

        // 3. Deploy binary - auto-skip if coordinator is already running (binary was deployed earlier)
        let should_deploy = if use_distributed {
            let coord = coordinator.as_ref().unwrap();
            let coord_running = self.check_coordinator_status(coord, zone);
            if coord_running && !force_redeploy {
                // Coordinator already running = binary already deployed
                println!(
                    "{} Coordinator already running, skipping binary deployment",
                    "Note:".cyan()
                );
                println!(
                    "{}",
                    "      (use --redeploy-binary or 'pool update-binary' to redeploy)".dimmed()
                );
                false
            } else {
                true // Deploy if coordinator not running, or if force_redeploy
            }
        } else {
            true // Non-distributed always deploys
        };

        if should_deploy {
            let binary = binary.as_ref().ok_or_else(|| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "Linux binary not found. Build with: cargo linux --release",
                ))
            })?;
            if use_distributed {
                // Distributed mode: upload to coordinator only, workers pull from coordinator
                let coord = coordinator.as_ref().unwrap();
                let coord_ip = coord.ip().ok_or_else(|| {
                    HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Coordinator {} has no internal IP", coord.name),
                    ))
                })?;

                // Stop any existing coordinator first (to avoid "Address already in use")
                println!("{}", "Stopping existing coordinator...".dimmed());
                let stop_cmd = "pkill -f 'hail-decoder service start-coordinator' || true";
                let _ = self
                    .provider
                    .get_ssh_command(&coord.name, zone, stop_cmd)
                    .status();
                std::thread::sleep(std::time::Duration::from_secs(1));

                // Deploy binary to coordinator
                println!("{}", "Deploying binary to coordinator...".dimmed());
                self.deploy_binary(binary, &[coord.clone()], zone)?;
                println!(
                    "{} Binary deployed to coordinator.",
                    "OK".green().bold()
                );

                // Start coordinator service (so /api/binary is available)
                println!(
                    "{}",
                    "Starting coordinator service to serve binary...".dimmed()
                );
                let coord_cmd =
                    "nohup /usr/local/bin/hail-decoder service start-coordinator \
                     --port 3000 \
                     > /tmp/coordinator.log 2>&1 & echo $! > /tmp/coordinator.pid";
                let status = self
                    .provider
                    .get_ssh_command(&coord.name, zone, coord_cmd)
                    .status()
                    .map_err(HailError::Io)?;

                if !status.success() {
                    return Err(HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Failed to start coordinator service for binary serving",
                    )));
                }

                // Wait for coordinator to be ready
                std::thread::sleep(std::time::Duration::from_secs(2));

                // Have workers pull binary from coordinator over GCP internal network
                println!(
                    "{}",
                    "Workers pulling binary from coordinator...".dimmed()
                );
                self.propagate_binary_from_coordinator(coord_ip, &workers, zone)?;
                println!(
                    "{} Binary propagated to {} workers.",
                    "OK".green().bold(),
                    workers.len()
                );
            } else {
                // Non-distributed mode: upload to all nodes via SCP
                let all_nodes: Vec<_> = if let Some(ref c) = coordinator {
                    let mut nodes = workers.clone();
                    nodes.push(c.clone());
                    nodes
                } else {
                    workers.clone()
                };

                println!("{}", "Deploying binary to nodes...".dimmed());
                self.deploy_binary(binary, &all_nodes, zone)?;
                println!("{} Binary deployed to all nodes.", "OK".green().bold());
            }
        }

        // 4. Branch based on mode - use coordinator/worker pattern when coordinator exists
        if use_distributed {
            return self.submit_distributed(
                name,
                zone,
                coordinator.as_ref().unwrap(),
                &workers,
                command,
                auto_stop,
                force,
                batch_size,
            );
        }

        // Legacy mode: submit jobs with progress tracking
        println!("{}", "Submitting jobs (legacy mode)...".dimmed());
        let base_args = command.join(" ");
        let start_time = Instant::now();

        // Setup multi-progress display
        let multi_progress = MultiProgress::new();
        let progress_style = ProgressStyle::default_bar()
            .template("{prefix:.cyan} [{bar:30.cyan/blue}] {pos}/{len} partitions ({eta})")
            .unwrap()
            .progress_chars("█▓░");

        // Create progress bars for each worker (initially with length 0, updated on first progress)
        let worker_bars: Vec<ProgressBar> = (0..total_workers)
            .map(|i| {
                let pb = multi_progress.add(ProgressBar::new(0));
                pb.set_style(progress_style.clone());
                pb.set_prefix(format!("worker-{}", i));
                pb
            })
            .collect();

        // Total progress bar
        let total_bar = multi_progress.add(ProgressBar::new(0));
        total_bar.set_style(
            ProgressStyle::default_bar()
                .template("{prefix:.green.bold} [{bar:30.green/white}] {pos}/{len} partitions | {msg}")
                .unwrap()
                .progress_chars("█▓░"),
        );
        total_bar.set_prefix("TOTAL");

        // Atomic counters for aggregate tracking
        let total_rows = Arc::new(AtomicUsize::new(0));
        let total_partitions_done = Arc::new(AtomicUsize::new(0));
        let total_partitions_expected = Arc::new(AtomicUsize::new(0));

        // Channel for receiving results from workers
        let (tx, rx) = mpsc::channel();

        // Spawn threads for each worker (legacy mode uses workers list)
        let handles: Vec<_> = workers
            .iter()
            .enumerate()
            .map(|(worker_id, inst)| {
                let inst_name = inst.name.clone();
                let inst_zone = inst.zone.clone();
                // Add --progress-json flag for machine-readable progress
                let args = format!(
                    "{} --worker-id {} --total-workers {} --progress-json",
                    base_args, worker_id, total_workers
                );
                let tx = tx.clone();

                // Build SSH command
                let remote_cmd = format!("/usr/local/bin/hail-decoder {}", args);
                let mut cmd = self.provider.get_ssh_command(&inst_name, &inst_zone, &remote_cmd);
                cmd.stdout(std::process::Stdio::piped());
                cmd.stderr(std::process::Stdio::piped());

                std::thread::spawn(move || {
                    let result = Self::run_worker_job(worker_id, cmd, &tx);
                    if let Err(e) = result {
                        let _ = tx.send(WorkerMessage::Error {
                            worker_id,
                            message: e.to_string(),
                        });
                    }
                })
            })
            .collect();

        // Drop our sender so the channel closes when all workers are done
        drop(tx);

        // Process messages from workers
        let mut aggregate_report = BenchmarkReport::empty();
        let mut completed = 0;
        let mut errors = 0;
        let mut worker_partition_totals: Vec<usize> = vec![0; total_workers];

        for msg in rx {
            match msg {
                WorkerMessage::Log { worker_id, line } => {
                    // Use suspend to avoid interfering with progress bars
                    multi_progress.suspend(|| {
                        println!("[worker-{}] {}", worker_id, line.dimmed());
                    });
                }
                WorkerMessage::Progress { worker_id, update } => {
                    // Update worker's progress bar
                    if worker_id < worker_bars.len() {
                        let pb = &worker_bars[worker_id];
                        // Set total on first update (partitions_total might not be known initially)
                        if pb.length() != Some(update.partitions_total as u64) {
                            pb.set_length(update.partitions_total as u64);
                            // Track totals for overall progress
                            let old_total = worker_partition_totals[worker_id];
                            worker_partition_totals[worker_id] = update.partitions_total;
                            total_partitions_expected.fetch_add(
                                update.partitions_total.saturating_sub(old_total),
                                Ordering::Relaxed,
                            );
                            // Update total bar length
                            total_bar.set_length(
                                total_partitions_expected.load(Ordering::Relaxed) as u64,
                            );
                        }
                        pb.set_position(update.partitions_done as u64);
                    }

                    // Update totals
                    total_rows.store(
                        total_rows.load(Ordering::Relaxed).max(update.rows),
                        Ordering::Relaxed,
                    );
                    total_partitions_done.store(
                        worker_bars.iter().map(|pb| pb.position() as usize).sum(),
                        Ordering::Relaxed,
                    );
                    total_bar.set_position(total_partitions_done.load(Ordering::Relaxed) as u64);

                    // Update throughput message
                    let elapsed = start_time.elapsed().as_secs_f64();
                    let rows = total_rows.load(Ordering::Relaxed);
                    if elapsed > 0.0 && rows > 0 {
                        total_bar.set_message(format!("{:.0} rows/sec", rows as f64 / elapsed));
                    }
                }
                WorkerMessage::Report { worker_id, report } => {
                    // Mark worker's bar as finished
                    if worker_id < worker_bars.len() {
                        worker_bars[worker_id].finish_with_message("done");
                    }
                    aggregate_report.merge(report);
                    completed += 1;
                }
                WorkerMessage::Error { worker_id, message } => {
                    if worker_id < worker_bars.len() {
                        worker_bars[worker_id].abandon_with_message("error");
                    }
                    multi_progress.suspend(|| {
                        eprintln!("[worker-{}] {} {}", worker_id, "Error:".red(), message);
                    });
                    errors += 1;
                }
                WorkerMessage::Complete { worker_id } => {
                    if worker_id < worker_bars.len() {
                        worker_bars[worker_id].finish_with_message("done");
                    }
                    completed += 1;
                }
            }
        }

        // Wait for all threads to finish
        for handle in handles {
            let _ = handle.join();
        }

        // Finish total bar
        total_bar.finish_with_message("complete");

        let elapsed = start_time.elapsed();

        // Print summary
        println!();
        println!("{}", "Cluster Job Complete".green().bold());
        println!("  {} {:.1}s", "Duration:".cyan(), elapsed.as_secs_f64());
        println!(
            "  {} {}/{}",
            "Workers:".cyan(),
            completed.to_string().green(),
            total_workers
        );
        if errors > 0 {
            println!("  {} {}", "Errors:".cyan(), errors.to_string().red());
        }

        // Print aggregate metrics if available
        if aggregate_report.total_rows > 0 {
            println!();
            println!("{}", "Aggregate Results:".green().bold());
            println!(
                "  {} {}",
                "Total rows:".cyan(),
                aggregate_report.total_rows.to_string().bright_white()
            );
            println!(
                "  {} {}",
                "Total partitions:".cyan(),
                aggregate_report.total_partitions.to_string().bright_white()
            );
            println!(
                "  {} {:.0} rows/sec",
                "Throughput:".cyan(),
                aggregate_report.total_rows as f64 / elapsed.as_secs_f64()
            );
        }

        if errors > 0 {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("{} workers failed", errors),
            )));
        }

        Ok(())
    }

    /// Submit a distributed job using the coordinator/worker pattern.
    ///
    /// This method:
    /// 1. Parses the command to extract input/output paths and job type
    /// 2. Calculates total partitions from the input table
    /// 3. Checks if coordinator is already running (idle mode), submits via API
    /// 4. Or starts the coordinator service on the coordinator VM (legacy)
    /// 5. Starts worker services on all worker VMs
    /// 6. Streams coordinator logs for progress monitoring
    fn submit_distributed(
        &self,
        _pool_name: &str,
        zone: &str,
        coordinator: &Instance,
        workers: &[Instance],
        command: &[String],
        auto_stop: bool,
        force: bool,
        batch_size: Option<usize>,
    ) -> Result<()> {
        use crate::query::QueryEngine;

        println!("{}", "Preparing distributed job...".dimmed());

        // Parse command into JobSpec
        // Supported formats:
        //   export parquet <input> <output> [--where ...]
        //   export json <input> <output> [--where ...]
        let (input_path, mut job_spec, filters, intervals) = Self::parse_command_to_job_spec(command)?;

        // Calculate total partitions by reading metadata locally
        println!("Reading metadata from {}...", input_path.bright_white());
        let engine = QueryEngine::open_path(&input_path)?;
        let total_partitions = engine.num_partitions();
        println!(
            "  {} {} partitions to process",
            "Found".green(),
            total_partitions.to_string().bright_white()
        );
        println!(
            "  {} {}",
            "Job type:".cyan(),
            job_spec.description().bright_white()
        );
        if let Some(out) = job_spec.output_path() {
            println!("  {} {}", "Output:".cyan(), out.bright_white());
        }

        // For Manhattan jobs, compute the layout and partition counts for all tables
        if let crate::distributed::message::JobSpec::Manhattan(ref mut spec) = job_spec {
            use crate::manhattan::layout::{ChromosomeLayout, YScale};
            use crate::manhattan::reference::get_contig_lengths;

            println!("  {} Computing chromosome layout...", "Setup:".cyan());
            let contigs = get_contig_lengths(&engine);
            let layout = ChromosomeLayout::new(&contigs, spec.width, 4);
            // Use a reasonable max -log10(p) for initial Y scale (will cover most GWAS hits)
            // Use high max to avoid cutting off extreme p-values (height can have -log10(p) > 100)
            let y_scale = YScale::new(spec.height, 300.0);
            spec.layout = Some(layout);
            spec.y_scale = Some(y_scale);

            // Count partitions for each table
            if let Some(ref exome_path) = spec.exome {
                if let Ok(exome_engine) = QueryEngine::open_path(exome_path) {
                    let exome_parts = exome_engine.num_partitions();
                    println!("  {} {} exome partitions", "Found".green(), exome_parts);
                    spec.exome_partitions = Some(exome_parts);
                }
            }
            if let Some(ref genome_path) = spec.genome {
                if let Ok(genome_engine) = QueryEngine::open_path(genome_path) {
                    let genome_parts = genome_engine.num_partitions();
                    println!("  {} {} genome partitions", "Found".green(), genome_parts);
                    spec.genome_partitions = Some(genome_parts);
                }
            }
        }

        drop(engine);

        // Get coordinator's internal IP
        let coord_ip = coordinator.ip().ok_or_else(|| {
            HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Coordinator {} has no internal IP", coordinator.name),
            ))
        })?;

        // Check if coordinator is already running (started in idle mode during pool create)
        let coordinator_already_running = self.check_coordinator_status(coordinator, zone);

        if coordinator_already_running {
            println!(
                "{} Coordinator already running on {} ({})",
                "Found".green(),
                coordinator.name.cyan(),
                coord_ip
            );

            // Start workers FIRST so they're connected when we submit the job
            println!(
                "Starting {} worker(s)...",
                workers.len().to_string().bright_white()
            );
            self.start_worker_services(workers, coord_ip, zone)?;

            // Give workers a moment to connect to coordinator
            println!("{}", "Waiting for workers to connect...".dimmed());
            std::thread::sleep(std::time::Duration::from_secs(3));

            // Submit job via API
            println!("{}", "Submitting job via API...".dimmed());
            if !self.submit_job_via_api(
                coordinator,
                zone,
                &input_path,
                &job_spec,
                total_partitions,
                force,
                batch_size,
                &filters,
                &intervals,
            )? {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Failed to submit job to coordinator via API",
                )));
            }
            println!("{} Job submitted via API", "OK".green().bold());
        } else {
            // Legacy mode: start coordinator fresh (only supports basic parquet export)
            let output_path = job_spec.output_path().ok_or_else(|| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Legacy coordinator mode only supports jobs with output paths",
                ))
            })?;

            println!(
                "Starting coordinator on {} ({})...",
                coordinator.name.cyan(),
                coord_ip
            );

            // Start coordinator service and save PID
            let coord_cmd = format!(
                "nohup /usr/local/bin/hail-decoder service start-coordinator \
                 --port 3000 \
                 --input '{}' \
                 --output '{}' \
                 --total-partitions {} \
                 > /tmp/coordinator.log 2>&1 & echo $! > /tmp/coordinator.pid",
                input_path, output_path, total_partitions
            );

            let status = self
                .provider
                .get_ssh_command(&coordinator.name, zone, &coord_cmd)
                .status()
                .map_err(HailError::Io)?;

            if !status.success() {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Failed to start coordinator service",
                )));
            }

            // Give coordinator a moment to bind its port
            std::thread::sleep(std::time::Duration::from_secs(2));

            // Start workers
            println!(
                "Starting {} worker(s)...",
                workers.len().to_string().bright_white()
            );
            self.start_worker_services(workers, coord_ip, zone)?;
        }

        println!();
        println!(
            "{} Distributed job submitted!",
            "OK".green().bold()
        );
        println!("  {} {}", "Coordinator:".cyan(), coordinator.name);
        println!("  {} {}", "Workers:".cyan(), workers.len());
        println!("  {} {}", "Total partitions:".cyan(), total_partitions);
        println!();
        println!("{}", "Streaming coordinator logs (Ctrl+C to exit)...".dimmed());
        println!();

        // Stream coordinator logs, exiting when the coordinator process exits
        let mut log_cmd = self.provider.get_ssh_command(
            &coordinator.name,
            zone,
            "tail -f --pid=$(cat /tmp/coordinator.pid) /tmp/coordinator.log",
        );

        // This blocks until coordinator exits or user interrupts
        let _ = log_cmd.status();

        // Fetch and display aggregated results for Summary jobs
        if matches!(job_spec, crate::distributed::message::JobSpec::Summary) {
            println!();
            println!("{}", "Fetching aggregated results...".dimmed());
            if let Err(e) = self.fetch_and_display_summary_results(coordinator, zone) {
                eprintln!("{} Failed to fetch results: {}", "Warning:".yellow(), e);
            }
        }

        if auto_stop {
            println!(
                "{}",
                "Job finished. Stopping pool instances (--auto-stop)..."
                    .yellow()
            );
            let mut stop_cmd = std::process::Command::new("gcloud");
            stop_cmd.args(["compute", "instances", "stop"]);

            let mut instance_names = vec![coordinator.name.as_str()];
            for w in workers {
                instance_names.push(&w.name);
            }

            stop_cmd.args(&instance_names);
            stop_cmd.args(["--zone", zone, "--quiet"]);

            match stop_cmd.status() {
                Ok(s) if s.success() => {
                    println!("{} Instances stopped.", "OK".green().bold())
                }
                _ => eprintln!("{} Failed to stop instances.", "Error:".red()),
            }
        }

        Ok(())
    }

    /// Start worker services on the given instances.
    fn start_worker_services(&self, workers: &[Instance], coord_ip: &str, zone: &str) -> Result<()> {
        use rayon::prelude::*;

        let worker_results: Vec<Result<()>> = workers
            .par_iter()
            .map(|worker| {
                let worker_cmd = format!(
                    "nohup /usr/local/bin/hail-decoder service start-worker \
                     --url http://{}:3000 \
                     --worker-id {} \
                     > /tmp/worker.log 2>&1 &",
                    coord_ip, worker.name
                );

                let status = self
                    .provider
                    .get_ssh_command(&worker.name, zone, &worker_cmd)
                    .status()
                    .map_err(HailError::Io)?;

                if !status.success() {
                    return Err(HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to start worker on {}", worker.name),
                    )));
                }

                println!("  {} started on {}", "Worker".dimmed(), worker.name.cyan());
                Ok(())
            })
            .collect();

        // Check for any startup failures
        for result in worker_results {
            result?;
        }

        Ok(())
    }

    /// Fetch aggregated summary results from coordinator and display them.
    fn fetch_and_display_summary_results(&self, coordinator: &Instance, zone: &str) -> Result<()> {
        use crate::summary::stats::StatsAccumulator;

        // Fetch results file saved by coordinator before exit
        let fetch_cmd = "cat /tmp/job_result.json";
        let output = self
            .provider
            .get_ssh_command(&coordinator.name, zone, fetch_cmd)
            .output()
            .map_err(HailError::Io)?;

        if !output.status.success() {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to fetch results file from coordinator",
            )));
        }

        // Parse the response
        let response: serde_json::Value = serde_json::from_slice(&output.stdout)
            .map_err(|e| HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to parse result JSON: {}", e),
            )))?;

        // Check if results are available
        if !response.get("available").and_then(|v| v.as_bool()).unwrap_or(false) {
            let error = response.get("error").and_then(|v| v.as_str()).unwrap_or("Unknown error");
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Results not available: {}", error),
            )));
        }

        // Get the array of partial results from workers
        let results = response.get("result").and_then(|v| v.as_array()).ok_or_else(|| {
            HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "No result array in response",
            ))
        })?;

        // Merge all partial StatsAccumulators
        let mut merged = StatsAccumulator::new();
        let mut total_rows = 0usize;

        for partial in results {
            if let Ok(acc) = serde_json::from_value::<StatsAccumulator>(partial.clone()) {
                total_rows += acc.stats.values().map(|s| s.count).max().unwrap_or(0);
                merged.merge(acc);
            }
        }

        // Display results
        println!();
        println!("{} {}", "Row Count:".green(), total_rows.to_string().bright_white().bold());
        println!();

        // Print field statistics
        println!("{}", "Field Statistics:".green().bold());
        println!("{:<50} | {:>10} | {:>10} | {:>20} | {:>20}",
            "Field".cyan(), "Count".cyan(), "Nulls".cyan(), "Min".cyan(), "Max".cyan());
        println!("{}", "-".repeat(120).dimmed());

        for key in merged.sorted_fields() {
            let s = &merged.stats[key];

            // Truncate field name if too long
            let field_display = if key.len() > 48 {
                format!("...{}", &key[key.len() - 45..])
            } else {
                key.clone()
            };

            // Truncate min/max if too long
            let min_display = match &s.min {
                Some(m) if m.len() > 18 => format!("{}...", &m[..15]),
                Some(m) => m.clone(),
                None => String::new(),
            };
            let max_display = match &s.max {
                Some(m) if m.len() > 18 => format!("{}...", &m[..15]),
                Some(m) => m.clone(),
                None => String::new(),
            };

            println!("{:<50} | {:>10} | {:>10} | {:>20} | {:>20}",
                field_display,
                s.count,
                s.null_count,
                min_display,
                max_display
            );
        }

        Ok(())
    }

    /// Deploy binary to instances via SCP upload.
    fn deploy_binary(&self, binary: &Path, instances: &[Instance], zone: &str) -> Result<()> {
        instances.par_iter().try_for_each(|inst| {
            // Upload to /tmp first (user writable)
            self.provider
                .upload_file(binary, "/tmp/hail-decoder", &inst.name, zone)?;

            // Make executable and move to /usr/local/bin (needs sudo)
            let setup_cmd =
                "chmod +x /tmp/hail-decoder && sudo mv /tmp/hail-decoder /usr/local/bin/hail-decoder";
            let status = self
                .provider
                .get_ssh_command(&inst.name, zone, setup_cmd)
                .status()
                .map_err(HailError::Io)?;

            if !status.success() {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to install binary on {}", inst.name),
                )));
            }

            println!("   {} {}", "Deployed to".dimmed(), inst.name.cyan());
            Ok(())
        })
    }

    /// Have workers pull the binary from the coordinator over GCP internal network.
    ///
    /// This is much faster than uploading to each worker via SCP from the client machine,
    /// since it leverages the high-bandwidth GCP internal network.
    fn propagate_binary_from_coordinator(
        &self,
        coordinator_ip: &str,
        workers: &[Instance],
        zone: &str,
    ) -> Result<()> {
        workers.par_iter().try_for_each(|worker| {
            // Download binary from coordinator, install it, and restart worker process
            // The worker process must be restarted to pick up the new binary!
            let curl_cmd = format!(
                "curl -sL --retry 3 --retry-delay 2 http://{}:3000/api/binary -o /tmp/hail-decoder && \
                 chmod +x /tmp/hail-decoder && \
                 sudo mv /tmp/hail-decoder /usr/local/bin/hail-decoder && \
                 pkill -f 'hail-decoder service start-worker' || true && \
                 sleep 1 && \
                 nohup /usr/local/bin/hail-decoder service start-worker --url http://{}:3000 --worker-id {} > /tmp/worker.log 2>&1 &",
                coordinator_ip, coordinator_ip, worker.name
            );

            let status = self
                .provider
                .get_ssh_command(&worker.name, zone, &curl_cmd)
                .status()
                .map_err(HailError::Io)?;

            if !status.success() {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!(
                        "Failed to pull binary from coordinator on {}",
                        worker.name
                    ),
                )));
            }

            println!(
                "   {} {} (from coordinator)",
                "Deployed to".dimmed(),
                worker.name.cyan()
            );
            Ok(())
        })
    }

    /// Run a job on a single worker, streaming output.
    fn run_worker_job(
        worker_id: usize,
        mut cmd: std::process::Command,
        tx: &mpsc::Sender<WorkerMessage>,
    ) -> Result<()> {
        let mut child = cmd.spawn().map_err(HailError::Io)?;

        // Stream stdout
        if let Some(stdout) = child.stdout.take() {
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                if let Ok(l) = line {
                    // Check if line is JSON
                    if l.trim().starts_with('{') {
                        // Try to parse as progress update first
                        if l.contains("\"type\":\"progress\"") {
                            if let Ok(update) = serde_json::from_str::<ProgressUpdate>(&l) {
                                let _ = tx.send(WorkerMessage::Progress { worker_id, update });
                                continue;
                            }
                        }
                        // Try to parse as benchmark report
                        if l.contains("\"total_rows\"") {
                            if let Ok(report) = serde_json::from_str::<BenchmarkReport>(&l) {
                                let _ = tx.send(WorkerMessage::Report { worker_id, report });
                                continue;
                            }
                        }
                    }
                    // Otherwise send as log line
                    let _ = tx.send(WorkerMessage::Log {
                        worker_id,
                        line: l,
                    });
                }
            }
        }

        let status = child.wait().map_err(HailError::Io)?;
        if !status.success() {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Worker {} exited with status: {}", worker_id, status),
            )));
        }

        let _ = tx.send(WorkerMessage::Complete { worker_id });
        Ok(())
    }

    /// Build the Linux binary for deployment to workers.
    ///
    /// On macOS, uses `cargo linux` (cargo-zigbuild) to cross-compile.
    /// On Linux, uses regular `cargo build`.
    fn build_linux_binary() -> Result<()> {
        let is_macos = cfg!(target_os = "macos");

        if is_macos {
            println!("{}", "Building Linux binary (cross-compiling)...".dimmed());

            // Use shell to set ulimit first (fixes "too many open files" during linking)
            // cargo linux is an alias for cargo-zigbuild
            // Suppress compiler warnings (already seen during local build) with RUSTFLAGS
            // Include duckdb-bundled for aggregate phase (annotation joins)
            // Note: duckdb-bundled requires high ulimit due to many object files
            let status = std::process::Command::new("sh")
                .args(["-c", "ulimit -n 16384 2>/dev/null || ulimit -n 8192 2>/dev/null; RUSTFLAGS='-Awarnings' cargo linux --release --features duckdb-bundled"])
                .status()
                .map_err(|e| {
                    HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!(
                            "Failed to run 'cargo linux'. Is cargo-zigbuild installed?\n\
                             Install with: cargo install cargo-zigbuild\n\
                             Error: {}",
                            e
                        ),
                    ))
                })?;

            if !status.success() {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Failed to build Linux binary. Check cargo output above.",
                )));
            }

            // Verify the binary was created
            let binary_path = PathBuf::from("target/x86_64-unknown-linux-gnu/release/hail-decoder");
            if !binary_path.exists() {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Linux binary not found at: {}", binary_path.display()),
                )));
            }

            println!(
                "{} Linux binary built: {}",
                "OK".green().bold(),
                binary_path.display().to_string().dimmed()
            );
        } else {
            println!("{}", "Building release binary...".dimmed());

            let status = std::process::Command::new("cargo")
                .args(["build", "--release", "--bin", "hail-decoder"])
                .status()
                .map_err(HailError::Io)?;

            if !status.success() {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Failed to build release binary",
                )));
            }

            println!("{} Release binary built", "OK".green().bold());
        }

        Ok(())
    }

    /// Locate the Linux binary for deployment.
    fn locate_binary(&self, path: Option<String>) -> Result<PathBuf> {
        if let Some(p) = path {
            let path = PathBuf::from(&p);
            if path.exists() {
                return Ok(path);
            }
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Binary not found at: {}", p),
            )));
        }

        // Try default cross-compile path
        let default_path = PathBuf::from("target/x86_64-unknown-linux-gnu/release/hail-decoder");
        if default_path.exists() {
            return Ok(default_path);
        }

        // Try release path (if running on Linux)
        let release_path = PathBuf::from("target/release/hail-decoder");
        if release_path.exists() {
            return Ok(release_path);
        }

        Err(HailError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Linux binary not found.\n\
             \n\
             If on macOS, cross-compile for Linux:\n\
               cargo install cross\n\
               cross build --release --target x86_64-unknown-linux-gnu\n\
             \n\
             Or specify path with --binary",
        )))
    }

    /// Parse a command array into a JobSpec and input path.
    ///
    /// Supported formats:
    /// - `export parquet <input> <output> [--where ...] [--interval ...]`
    /// - `export json <input> <output> [--where ...] [--interval ...]`
    ///
    /// Returns (input_path, job_spec, filters, intervals)
    fn parse_command_to_job_spec(
        command: &[String],
    ) -> Result<(String, crate::distributed::message::JobSpec, Vec<String>, Vec<String>)> {
        use crate::distributed::message::JobSpec;

        if command.is_empty() {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Empty command",
            )));
        }

        let cmd = command.get(0).map(|s| s.as_str()).unwrap_or("<empty>");

        // Handle 'summary <input>' command
        if cmd == "summary" {
            if command.len() < 2 {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Summary command requires: summary <input>\n\
                     Example: summary gs://bucket/input.ht",
                )));
            }
            let input_path = command[1].clone();
            return Ok((input_path, JobSpec::Summary, Vec::new(), Vec::new()));
        }

        // Handle 'manhattan' command
        if cmd == "manhattan" {
            return Self::parse_manhattan_command(&command[1..]);
        }

        // Expect: export <type> <input> <output> [args...]
        if cmd != "export" {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Distributed mode supports: export, summary, manhattan. Got: '{}'\n\
                     Examples:\n  \
                     pool submit mypool -- export parquet gs://bucket/input.ht gs://bucket/output/\n  \
                     pool submit mypool -- summary gs://bucket/input.ht\n  \
                     pool submit mypool -- manhattan --exome gs://bucket/exome.ht --output gs://bucket/out/",
                    cmd
                ),
            )));
        }

        if command.len() < 4 {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Export command requires: export <type> <input> <output>\n\
                 Example: export parquet gs://bucket/input.ht gs://bucket/output/",
            )));
        }

        let export_type = &command[1];
        let input_path = command[2].clone();
        let output_path = command[3].clone();

        // Parse optional arguments (--where, --interval)
        let mut filters = Vec::new();
        let mut intervals = Vec::new();
        let mut i = 4;
        while i < command.len() {
            match command[i].as_str() {
                "--where" => {
                    if i + 1 < command.len() {
                        filters.push(command[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--interval" => {
                    if i + 1 < command.len() {
                        intervals.push(command[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                _ => {
                    i += 1;
                }
            }
        }

        let job_spec = match export_type.as_str() {
            "parquet" => JobSpec::ExportParquet {
                output_path,
            },
            "json" => JobSpec::ExportJson {
                output_path,
                group_by: None,
            },
            other => {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "Unsupported export type for distributed mode: '{}'\n\
                         Supported types: parquet, json",
                        other
                    ),
                )));
            }
        };

        Ok((input_path, job_spec, filters, intervals))
    }

    /// Parse a `manhattan` command into a ManhattanSpec job.
    ///
    /// Supports: manhattan --exome <path> --genome <path> --output <path> [--threshold ...] ...
    fn parse_manhattan_command(
        args: &[String],
    ) -> Result<(String, crate::distributed::message::JobSpec, Vec<String>, Vec<String>)> {
        use crate::distributed::message::{JobSpec, ManhattanSpec};

        // Parse named arguments
        let mut exome: Option<String> = None;
        let mut exome_annotations: Option<String> = None;
        let mut genome: Option<String> = None;
        let mut genome_annotations: Option<String> = None;
        let mut gene_burden: Option<String> = None;
        let mut genes: Option<String> = None;
        let mut output: Option<String> = None;
        let mut threshold: f64 = 5e-8;
        let mut gene_threshold: f64 = 2.5e-6;
        let mut locus_threshold: f64 = 0.01;
        let mut locus_window: i32 = 1_000_000;
        let mut locus_plots = false;
        let mut skip_composite = false;
        let mut width: u32 = 3000;
        let mut height: u32 = 800;
        let mut y_field = "Pvalue".to_string();

        let mut i = 0;
        while i < args.len() {
            match args[i].as_str() {
                "--exome" => {
                    if i + 1 < args.len() {
                        exome = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--exome-annotations" => {
                    if i + 1 < args.len() {
                        exome_annotations = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--genome" => {
                    if i + 1 < args.len() {
                        genome = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--genome-annotations" => {
                    if i + 1 < args.len() {
                        genome_annotations = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--gene-burden" => {
                    if i + 1 < args.len() {
                        gene_burden = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--genes" => {
                    if i + 1 < args.len() {
                        genes = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--output" => {
                    if i + 1 < args.len() {
                        output = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--threshold" | "--variant-threshold" => {
                    if i + 1 < args.len() {
                        threshold = args[i + 1].parse().unwrap_or(5e-8);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--gene-threshold" => {
                    if i + 1 < args.len() {
                        gene_threshold = args[i + 1].parse().unwrap_or(2.5e-6);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--locus-threshold" => {
                    if i + 1 < args.len() {
                        locus_threshold = args[i + 1].parse().unwrap_or(0.01);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--locus-window" => {
                    if i + 1 < args.len() {
                        locus_window = args[i + 1].parse().unwrap_or(1_000_000);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--locus-plots" => {
                    locus_plots = true;
                    i += 1;
                }
                "--no-composite" => {
                    skip_composite = true;
                    i += 1;
                }
                "--width" => {
                    if i + 1 < args.len() {
                        width = args[i + 1].parse().unwrap_or(3000);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--height" => {
                    if i + 1 < args.len() {
                        height = args[i + 1].parse().unwrap_or(800);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--y-field" => {
                    if i + 1 < args.len() {
                        y_field = args[i + 1].clone();
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                _ => {
                    i += 1;
                }
            }
        }

        // Validate: need at least one input table and an output
        let output_path = output.ok_or_else(|| {
            HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Manhattan command requires --output <path>",
            ))
        })?;

        // Determine primary input for partition counting
        let input_path = exome
            .as_ref()
            .or(genome.as_ref())
            .or(gene_burden.as_ref())
            .cloned()
            .ok_or_else(|| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Manhattan command requires at least one input table: --exome, --genome, or --gene-burden",
                ))
            })?;

        let spec = ManhattanSpec {
            exome,
            exome_annotations,
            genome,
            genome_annotations,
            gene_burden,
            genes,
            threshold,
            gene_threshold,
            locus_threshold,
            locus_window,
            locus_plots,
            width,
            height,
            y_field,
            output_path,
            layout: None,  // Computed by coordinator before dispatch
            y_scale: None, // Computed by coordinator before dispatch
            skip_composite,
            exome_partitions: None, // Computed by submit_distributed
            genome_partitions: None, // Computed by submit_distributed
        };

        Ok((input_path, JobSpec::Manhattan(spec), Vec::new(), Vec::new()))
    }
}

/// Messages sent from worker threads to the coordinator.
enum WorkerMessage {
    /// A log line from the worker
    Log { worker_id: usize, line: String },
    /// A progress update from the worker
    Progress {
        worker_id: usize,
        update: ProgressUpdate,
    },
    /// A benchmark report from the worker
    Report {
        worker_id: usize,
        report: BenchmarkReport,
    },
    /// Worker completed successfully
    Complete { worker_id: usize },
    /// Worker encountered an error
    Error { worker_id: usize, message: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_locate_binary_not_found() {
        struct MockProvider;
        impl CloudProvider for MockProvider {
            fn create_pool(&self, _: &PoolConfig) -> Result<()> {
                Ok(())
            }
            fn list_instances(&self, _: &str) -> Result<Vec<Instance>> {
                Ok(vec![])
            }
            fn destroy_pool(&self, _: &str, _: &str) -> Result<()> {
                Ok(())
            }
            fn create_instances(&self, _: &[super::super::InstanceSetup]) -> Result<()> {
                Ok(())
            }
            fn delete_instances(&self, _: &[String], _: &str, _: &str) -> Result<()> {
                Ok(())
            }
            fn upload_file(&self, _: &Path, _: &str, _: &str, _: &str) -> Result<()> {
                Ok(())
            }
            fn get_ssh_command(
                &self,
                _: &str,
                _: &str,
                _: &str,
            ) -> std::process::Command {
                std::process::Command::new("echo")
            }
        }

        let manager = PoolManager::new(MockProvider);
        let result = manager.locate_binary(Some("/nonexistent/path".to_string()));
        assert!(result.is_err());
    }
}

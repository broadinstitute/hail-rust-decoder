//! Worker pool management for distributed processing.
//!
//! This module provides the `PoolManager` which orchestrates:
//! - Creating worker VMs
//! - Deploying the hail-decoder binary
//! - Submitting distributed jobs
//! - Streaming logs and aggregating metrics
//! - Cleaning up resources

use crate::benchmark::BenchmarkReport;
use crate::cloud::{CloudProvider, Instance, PoolConfig};
use crate::HailError;
use crate::Result;
use owo_colors::OwoColorize;
use rayon::prelude::*;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
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
    pub fn create(&self, config: &PoolConfig, wait: bool) -> Result<()> {
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

        if wait {
            println!("{}", "Waiting for VMs to be ready...".dimmed());
            self.wait_for_pool_ready(&config.name, &config.zone, 300)?;
            println!("{} All workers are ready.", "OK".green().bold());
        } else {
            println!(
                "   Workers will be ready in ~60 seconds (or use --wait)"
            );
        }

        Ok(())
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
    pub fn destroy(&self, name: &str, zone: &str) -> Result<()> {
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

        self.provider.destroy_pool(name, zone)?;

        println!("{} Pool '{}' destroyed.", "OK".green().bold(), name);

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

    /// Submit a job to the worker pool.
    ///
    /// This will:
    /// 1. Locate or validate the Linux binary
    /// 2. Upload the binary to all workers in parallel
    /// 3. Execute the command on each worker with partition slicing
    /// 4. Stream logs and aggregate benchmark results
    pub fn submit(
        &self,
        name: &str,
        zone: &str,
        binary_path: Option<String>,
        command: &[String],
    ) -> Result<()> {
        // 1. Locate the Linux binary
        let binary = self.locate_binary(binary_path)?;
        println!(
            "{} {}",
            "Binary:".cyan(),
            binary.display().to_string().bright_white()
        );

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

        let total_workers = running.len();
        println!(
            "{} {} running workers",
            "Found".green(),
            total_workers.to_string().bright_white()
        );

        // 3. Deploy binary to all workers in parallel
        println!("{}", "Deploying binary to workers...".dimmed());
        self.deploy_binary(&binary, &running, zone)?;
        println!("{} Binary deployed to all workers.", "OK".green().bold());

        // 4. Submit jobs
        println!("{}", "Submitting jobs...".dimmed());
        let base_args = command.join(" ");
        let start_time = Instant::now();

        // Channel for receiving results from workers
        let (tx, rx) = mpsc::channel();

        // Spawn threads for each worker
        let handles: Vec<_> = running
            .iter()
            .enumerate()
            .map(|(worker_id, inst)| {
                let inst_name = inst.name.clone();
                let inst_zone = inst.zone.clone();
                let args = format!(
                    "{} --worker-id {} --total-workers {}",
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

        for msg in rx {
            match msg {
                WorkerMessage::Log { worker_id, line } => {
                    println!("[worker-{}] {}", worker_id, line.dimmed());
                }
                WorkerMessage::Report { worker_id, report } => {
                    println!(
                        "[worker-{}] {} Processed {} rows, {} partitions",
                        worker_id,
                        "Done:".green(),
                        report.total_rows,
                        report.total_partitions
                    );
                    aggregate_report.merge(report);
                    completed += 1;
                }
                WorkerMessage::Error { worker_id, message } => {
                    eprintln!("[worker-{}] {} {}", worker_id, "Error:".red(), message);
                    errors += 1;
                }
                WorkerMessage::Complete { worker_id } => {
                    println!("[worker-{}] {}", worker_id, "Finished".green());
                    completed += 1;
                }
            }
        }

        // Wait for all threads to finish
        for handle in handles {
            let _ = handle.join();
        }

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

    /// Deploy binary to all workers in parallel.
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
                    // Check if line is a JSON benchmark report
                    if l.trim().starts_with('{') && l.contains("\"total_rows\"") {
                        if let Ok(report) = serde_json::from_str::<BenchmarkReport>(&l) {
                            let _ = tx.send(WorkerMessage::Report { worker_id, report });
                            continue;
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
}

/// Messages sent from worker threads to the coordinator.
enum WorkerMessage {
    /// A log line from the worker
    Log { worker_id: usize, line: String },
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

//! Configuration file support for hail-decoder.
//!
//! Loads configuration from TOML files, supporting:
//! - Global defaults (project, zone, network)
//! - Export defaults (bigquery staging bucket, clickhouse URL)
//! - Named pool profiles with optional WireGuard configuration
//!
//! Configuration is loaded from (in order of precedence):
//! 1. Path specified via `--config` CLI flag
//! 2. `$XDG_CONFIG_HOME/hail-decoder/config.toml`
//! 3. `~/.config/hail-decoder/config.toml`
//! 4. `./hail-decoder.toml` (current directory)

use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;

/// Root configuration structure.
#[derive(Debug, Deserialize, Default, Clone)]
pub struct Config {
    /// Global default settings
    #[serde(default)]
    pub defaults: Defaults,

    /// Export command defaults
    #[serde(default)]
    pub export: ExportDefaults,

    /// Named pool profiles
    #[serde(default)]
    pub pools: HashMap<String, PoolProfile>,
}

/// Global default settings.
#[derive(Debug, Deserialize, Default, Clone)]
pub struct Defaults {
    /// GCP project ID
    pub project: Option<String>,
    /// GCP zone
    pub zone: Option<String>,
    /// VPC network name
    pub network: Option<String>,
    /// Subnet name
    pub subnet: Option<String>,
    /// SSH user (defaults to current user)
    pub ssh_user: Option<String>,
}

/// Export command defaults.
#[derive(Debug, Deserialize, Default, Clone)]
pub struct ExportDefaults {
    /// BigQuery export settings
    #[serde(default)]
    pub bigquery: BigQueryDefaults,
    /// ClickHouse export settings
    #[serde(default)]
    pub clickhouse: ClickHouseDefaults,
}

/// BigQuery export defaults.
#[derive(Debug, Deserialize, Default, Clone)]
pub struct BigQueryDefaults {
    /// GCS bucket for staging parquet files
    pub staging_bucket: Option<String>,
    /// Dataset prefix for auto-generated table names
    pub dataset_prefix: Option<String>,
}

/// ClickHouse export defaults.
#[derive(Debug, Deserialize, Default, Clone)]
pub struct ClickHouseDefaults {
    /// ClickHouse HTTP URL
    pub url: Option<String>,
    /// ClickHouse user
    pub user: Option<String>,
}

/// A named pool profile with machine and network configuration.
#[derive(Debug, Deserialize, Clone)]
pub struct PoolProfile {
    /// GCP machine type (e.g., "c3-highcpu-22")
    pub machine_type: Option<String>,
    /// Number of workers to create initially (default: 0)
    pub starting_workers: Option<usize>,
    /// Target number of workers for autoscaling (default: 4)
    pub workers: Option<usize>,
    /// Use spot/preemptible instances
    pub spot: Option<bool>,
    /// GCP zone (overrides defaults)
    pub zone: Option<String>,
    /// VPC network name (overrides defaults)
    pub network: Option<String>,
    /// Subnet name (overrides defaults)
    pub subnet: Option<String>,
    /// Create a dedicated coordinator node
    pub with_coordinator: Option<bool>,
    /// WireGuard configuration for coordinator
    pub wireguard: Option<WireGuardConfig>,
}

/// WireGuard VPN configuration for the coordinator node.
///
/// Values can be:
/// - Raw strings (not recommended for private keys)
/// - `secret:NAME` - fetched from Google Secret Manager at VM boot
/// - `env:VAR_NAME` - read from environment variable (source USB secrets.sh first)
///
/// # Example with Google Secret Manager
/// ```toml
/// [pools.dev.wireguard]
/// endpoint = "vpn.example.com:51820"
/// client_address = "10.10.0.50/32"
/// peer_public_key = "secret:wg-peer-pubkey"
/// client_private_key = "secret:wg-coord-privkey"
/// ```
///
/// # Example with environment variables (USB stick)
/// ```toml
/// [pools.dev.wireguard]
/// endpoint = "env:GCP_WG_ENDPOINT"
/// client_address = "env:GCP_WG_ADDRESS_1"
/// peer_public_key = "env:GCP_WG_PEER_PUBLIC_KEY"
/// client_private_key = "env:GCP_WG_PRIVATE_KEY_1"
/// ```
///
/// Then source your USB secrets before running:
/// ```bash
/// source /Volumes/USB_NAME/bootstrap/secrets.sh
/// hail-decoder pool create dev --with-coordinator
/// ```
#[derive(Debug, Deserialize, Clone)]
pub struct WireGuardConfig {
    /// WireGuard server endpoint (host:port)
    pub endpoint: String,
    /// Client IP address with CIDR (e.g., "10.10.0.50/32")
    pub client_address: String,
    /// Allowed IPs to route through VPN (default: "10.10.0.0/24")
    #[serde(default = "default_allowed_ips")]
    pub allowed_ips: String,
    /// Server's public key (can be `secret:name` or `env:VAR`)
    pub peer_public_key: String,
    /// Client's private key (should be `secret:name` or `env:VAR`)
    pub client_private_key: String,
}

impl WireGuardConfig {
    /// Resolve all `env:VAR_NAME` references in the config.
    /// Returns a new config with environment variables expanded.
    /// Called at pool creation time (on the local machine).
    pub fn resolve_env_vars(&self) -> Result<WireGuardConfig, String> {
        Ok(WireGuardConfig {
            endpoint: resolve_env_value(&self.endpoint)?,
            client_address: resolve_env_value(&self.client_address)?,
            allowed_ips: resolve_env_value(&self.allowed_ips)?,
            peer_public_key: resolve_env_value(&self.peer_public_key)?,
            client_private_key: resolve_env_value(&self.client_private_key)?,
        })
    }
}

/// Resolve a value that may have an `env:` prefix.
/// - `env:VAR_NAME` -> reads from environment variable
/// - `secret:name` -> passed through (resolved at VM boot)
/// - other -> passed through as-is
fn resolve_env_value(value: &str) -> Result<String, String> {
    if let Some(var_name) = value.strip_prefix("env:") {
        std::env::var(var_name).map_err(|_| {
            format!(
                "Environment variable '{}' not set. Source your USB secrets.sh first.",
                var_name
            )
        })
    } else {
        Ok(value.to_string())
    }
}

fn default_allowed_ips() -> String {
    "10.10.0.0/24".to_string()
}

impl Config {
    /// Load configuration from the default search paths.
    ///
    /// Searches in order:
    /// 1. `$XDG_CONFIG_HOME/hail-decoder/config.toml`
    /// 2. `~/.config/hail-decoder/config.toml`
    /// 3. `./hail-decoder.toml`
    ///
    /// Returns default config if no file is found.
    pub fn load() -> Self {
        Self::load_from_path(None)
    }

    /// Load configuration from a specific path, or search default paths if None.
    pub fn load_from_path(path: Option<&str>) -> Self {
        // If explicit path provided, use it
        if let Some(p) = path {
            return Self::load_file(p).unwrap_or_else(|e| {
                eprintln!("Warning: Failed to load config from {}: {}", p, e);
                Config::default()
            });
        }

        // Search default paths
        let search_paths = Self::config_search_paths();
        for path in search_paths {
            if path.exists() {
                match Self::load_file(path.to_string_lossy().as_ref()) {
                    Ok(config) => return config,
                    Err(e) => {
                        eprintln!(
                            "Warning: Failed to load config from {}: {}",
                            path.display(),
                            e
                        );
                    }
                }
            }
        }

        Config::default()
    }

    /// Get the list of paths to search for config files.
    fn config_search_paths() -> Vec<PathBuf> {
        let mut paths = Vec::new();

        // ~/.config/hail-decoder/config.toml (Linux/macOS convention)
        if let Some(home) = dirs::home_dir() {
            paths.push(home.join(".config").join("hail-decoder").join("config.toml"));
        }

        // XDG config home (~/Library/Application Support on macOS)
        if let Some(config_dir) = dirs::config_dir() {
            paths.push(config_dir.join("hail-decoder").join("config.toml"));
        }

        // Current directory
        paths.push(PathBuf::from("hail-decoder.toml"));

        paths
    }

    /// Load configuration from a specific file path.
    fn load_file(path: &str) -> Result<Self, String> {
        let content =
            std::fs::read_to_string(path).map_err(|e| format!("Failed to read file: {}", e))?;
        toml::from_str(&content).map_err(|e| format!("Failed to parse TOML: {}", e))
    }

    /// Get a pool profile by name, with defaults applied.
    pub fn get_pool(&self, name: &str) -> Option<ResolvedPoolConfig> {
        self.pools.get(name).map(|profile| {
            // Auto-enable coordinator if WireGuard is configured
            let with_coordinator = profile.with_coordinator.unwrap_or(false)
                || profile.wireguard.is_some();

            ResolvedPoolConfig {
                name: name.to_string(),
                machine_type: profile
                    .machine_type
                    .clone()
                    .unwrap_or_else(|| "c3-highcpu-22".to_string()),
                starting_workers: profile.starting_workers.unwrap_or(0),
                workers: profile.workers.unwrap_or(4),
                spot: profile.spot.unwrap_or(false),
                zone: profile
                    .zone
                    .clone()
                    .or_else(|| self.defaults.zone.clone())
                    .unwrap_or_else(|| "us-central1-a".to_string()),
                network: profile.network.clone().or_else(|| self.defaults.network.clone()),
                subnet: profile.subnet.clone().or_else(|| self.defaults.subnet.clone()),
                project: self.defaults.project.clone(),
                with_coordinator,
                wireguard: profile.wireguard.clone(),
            }
        })
    }
}

/// A fully resolved pool configuration with all defaults applied.
#[derive(Debug, Clone)]
pub struct ResolvedPoolConfig {
    /// Pool name
    pub name: String,
    /// Machine type
    pub machine_type: String,
    /// Number of workers to create initially (default: 0)
    pub starting_workers: usize,
    /// Target number of workers for autoscaling (default: 4)
    pub workers: usize,
    /// Use spot instances
    pub spot: bool,
    /// GCP zone
    pub zone: String,
    /// VPC network
    pub network: Option<String>,
    /// Subnet
    pub subnet: Option<String>,
    /// GCP project (may need CLI override)
    pub project: Option<String>,
    /// Create a dedicated coordinator node
    pub with_coordinator: bool,
    /// WireGuard configuration
    pub wireguard: Option<WireGuardConfig>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert!(config.pools.is_empty());
        assert!(config.defaults.project.is_none());
    }

    #[test]
    fn test_parse_config() {
        let toml = r#"
[defaults]
project = "my-project"
zone = "us-central1-a"

[pools.dev]
machine_type = "n1-standard-4"
workers = 2
spot = true

[pools.dev.wireguard]
endpoint = "vpn.example.com:51820"
client_address = "10.10.0.50/32"
peer_public_key = "secret:wg-peer-pubkey"
client_private_key = "secret:wg-coord-privkey"
"#;
        let config: Config = toml::from_str(toml).unwrap();

        assert_eq!(config.defaults.project, Some("my-project".to_string()));
        assert_eq!(config.defaults.zone, Some("us-central1-a".to_string()));

        let dev = config.pools.get("dev").unwrap();
        assert_eq!(dev.machine_type, Some("n1-standard-4".to_string()));
        assert_eq!(dev.workers, Some(2));
        assert_eq!(dev.spot, Some(true));

        let wg = dev.wireguard.as_ref().unwrap();
        assert_eq!(wg.endpoint, "vpn.example.com:51820");
        assert_eq!(wg.client_address, "10.10.0.50/32");
        assert_eq!(wg.peer_public_key, "secret:wg-peer-pubkey");
        assert_eq!(wg.client_private_key, "secret:wg-coord-privkey");
        assert_eq!(wg.allowed_ips, "10.10.0.0/24"); // default
    }

    #[test]
    fn test_resolved_pool_config() {
        let toml = r#"
[defaults]
project = "my-project"
zone = "us-east1-b"

[pools.test]
starting_workers = 2
workers = 8
"#;
        let config: Config = toml::from_str(toml).unwrap();
        let resolved = config.get_pool("test").unwrap();

        assert_eq!(resolved.name, "test");
        assert_eq!(resolved.machine_type, "c3-highcpu-22"); // default
        assert_eq!(resolved.starting_workers, 2);
        assert_eq!(resolved.workers, 8); // autoscale target
        assert!(!resolved.spot); // default
        assert_eq!(resolved.zone, "us-east1-b"); // from defaults
        assert_eq!(resolved.project, Some("my-project".to_string()));
    }

    #[test]
    fn test_resolved_pool_config_defaults() {
        let toml = r#"
[pools.minimal]
"#;
        let config: Config = toml::from_str(toml).unwrap();
        let resolved = config.get_pool("minimal").unwrap();

        assert_eq!(resolved.starting_workers, 0); // default: coordinator-only
        assert_eq!(resolved.workers, 4); // default autoscale target
    }

    #[test]
    fn test_wireguard_resolve_env_vars() {
        // Set test environment variables
        std::env::set_var("TEST_WG_ENDPOINT", "vpn.test.com:51820");
        std::env::set_var("TEST_WG_PRIVKEY", "test-private-key");

        let wg = WireGuardConfig {
            endpoint: "env:TEST_WG_ENDPOINT".to_string(),
            client_address: "10.10.0.50/32".to_string(), // raw value
            allowed_ips: "10.10.0.0/24".to_string(),
            peer_public_key: "raw-public-key".to_string(), // raw value
            client_private_key: "env:TEST_WG_PRIVKEY".to_string(),
        };

        let resolved = wg.resolve_env_vars().unwrap();

        assert_eq!(resolved.endpoint, "vpn.test.com:51820");
        assert_eq!(resolved.client_address, "10.10.0.50/32");
        assert_eq!(resolved.peer_public_key, "raw-public-key");
        assert_eq!(resolved.client_private_key, "test-private-key");

        // Clean up
        std::env::remove_var("TEST_WG_ENDPOINT");
        std::env::remove_var("TEST_WG_PRIVKEY");
    }

    #[test]
    fn test_wireguard_missing_env_var() {
        let wg = WireGuardConfig {
            endpoint: "env:NONEXISTENT_VAR_12345".to_string(),
            client_address: "10.10.0.50/32".to_string(),
            allowed_ips: "10.10.0.0/24".to_string(),
            peer_public_key: "key".to_string(),
            client_private_key: "key".to_string(),
        };

        let result = wg.resolve_env_vars();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("NONEXISTENT_VAR_12345"));
    }

    #[test]
    fn test_wireguard_secret_prefix_passthrough() {
        let wg = WireGuardConfig {
            endpoint: "vpn.example.com:51820".to_string(),
            client_address: "10.10.0.50/32".to_string(),
            allowed_ips: "10.10.0.0/24".to_string(),
            peer_public_key: "secret:wg-peer-pubkey".to_string(),
            client_private_key: "secret:wg-privkey".to_string(),
        };

        // secret: prefix should pass through unchanged (resolved at VM boot)
        let resolved = wg.resolve_env_vars().unwrap();
        assert_eq!(resolved.peer_public_key, "secret:wg-peer-pubkey");
        assert_eq!(resolved.client_private_key, "secret:wg-privkey");
    }
}

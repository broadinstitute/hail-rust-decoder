//! VM startup script generation for worker nodes.
//!
//! This module generates the bash script that runs on VM boot to install
//! dependencies required by the hail-decoder binary.

/// Generate the startup script for worker VMs.
///
/// The script installs minimal dependencies needed to run the Rust binary:
/// - libssl-dev (for TLS/HTTPS support)
/// - ca-certificates (for certificate verification)
///
/// The script is designed to be minimal and fast, avoiding unnecessary packages.
pub fn generate_startup_script() -> String {
    r#"#!/bin/bash
set -e

# Log startup
echo "=== hail-decoder worker VM startup ==="
echo "Timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)"

# Install dependencies required for the Rust binary
apt-get update -qq
apt-get install -y -qq libssl-dev ca-certificates

# Create directory for the binary
mkdir -p /usr/local/bin

# Create a marker file to indicate VM is ready
touch /tmp/hail-decoder-ready

echo "=== Worker VM initialized ==="
"#
    .to_string()
}

/// Generate a startup script with custom commands appended.
///
/// Useful for adding project-specific setup steps.
pub fn generate_startup_script_with_extras(extra_commands: &str) -> String {
    let mut script = generate_startup_script();
    // Remove the final echo and add custom commands before it
    script = script.trim_end_matches("echo \"=== Worker VM initialized ===\"\n").to_string();
    script.push_str("\n# Custom setup\n");
    script.push_str(extra_commands);
    script.push_str("\necho \"=== Worker VM initialized ===\"\n");
    script
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_startup_script_contains_essentials() {
        let script = generate_startup_script();
        assert!(script.contains("#!/bin/bash"));
        assert!(script.contains("apt-get"));
        assert!(script.contains("libssl-dev"));
        assert!(script.contains("ca-certificates"));
        assert!(script.contains("/usr/local/bin"));
        assert!(script.contains("hail-decoder-ready"));
    }

    #[test]
    fn test_startup_script_with_extras() {
        let script = generate_startup_script_with_extras("echo 'Custom step'");
        assert!(script.contains("Custom step"));
        assert!(script.contains("# Custom setup"));
    }
}

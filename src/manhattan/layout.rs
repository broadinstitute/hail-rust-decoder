//! Layout engine for genome-wide and per-chromosome plots.
//!
//! Maps genomic coordinates (contig + base-pair position) to pixel coordinates.

use std::collections::HashMap;

/// Maps chromosome names and base-pair positions to pixel X coordinates.
pub struct ChromosomeLayout {
    /// Map of contig name to (start_px, width_px)
    offsets: HashMap<String, (f32, f32)>,
    /// Pixels per base pair (global scale)
    px_per_bp: f64,
    /// Ordered list of (name, start_px, end_px, color) for the sidecar
    pub chromosome_info: Vec<ChromosomeInfo>,
}

/// Per-chromosome layout metadata for the sidecar JSON.
pub struct ChromosomeInfo {
    pub name: String,
    pub x_start_px: f32,
    pub x_end_px: f32,
    pub color: String,
}

/// Alternating chromosome colors (classic Manhattan style).
const CHROM_COLORS: [&str; 2] = ["#404040", "#4682B4"];

impl ChromosomeLayout {
    /// Build a layout from an ordered list of (contig_name, length_bp).
    ///
    /// `width_px` is the total image width. `gap_px` is the visual gap between
    /// adjacent chromosomes.
    pub fn new(contigs: &[(String, u32)], width_px: u32, gap_px: u32) -> Self {
        let total_bp: u64 = contigs.iter().map(|(_, len)| *len as u64).sum();
        let total_gaps = contigs.len().saturating_sub(1) as u32 * gap_px;
        let available_width = width_px.saturating_sub(total_gaps);

        let px_per_bp = if total_bp > 0 {
            available_width as f64 / total_bp as f64
        } else {
            1.0
        };

        let mut offsets = HashMap::new();
        let mut chromosome_info = Vec::new();
        let mut current_x: f32 = 0.0;

        for (i, (name, len)) in contigs.iter().enumerate() {
            let width = (*len as f64 * px_per_bp) as f32;
            offsets.insert(name.clone(), (current_x, width));

            let color = CHROM_COLORS[i % 2].to_string();
            chromosome_info.push(ChromosomeInfo {
                name: name.clone(),
                x_start_px: current_x,
                x_end_px: current_x + width,
                color,
            });

            current_x += width + gap_px as f32;
        }

        Self {
            offsets,
            px_per_bp,
            chromosome_info,
        }
    }

    /// Map a genomic coordinate to a pixel X position.
    pub fn get_x(&self, contig: &str, pos: i32) -> Option<f32> {
        let &(start_px, _) = self.offsets.get(contig)?;
        Some(start_px + (pos as f64 * self.px_per_bp) as f32)
    }

    /// Return the color for a given chromosome.
    pub fn get_color(&self, contig: &str) -> &str {
        for info in &self.chromosome_info {
            if info.name == contig {
                return &info.color;
            }
        }
        CHROM_COLORS[0]
    }
}

/// Maps -log10(pvalue) values to pixel Y coordinates using a log-log scale.
///
/// The scale is linear from 0 to `log_threshold` (default 10), then
/// logarithmic above that. This compresses extreme values while preserving
/// detail in the 0-10 range where most variants cluster.
pub struct YScale {
    height: f32,
    /// Threshold where we switch from linear to log scale
    log_threshold: f64,
    /// Fraction of plot height for the linear portion (0-log_threshold)
    linear_fraction: f64,
    /// Maximum -log10(p) value to display (for log portion scaling)
    max_log_val: f64,
}

impl YScale {
    /// Create a log-log Y scale.
    ///
    /// - `height`: total plot height in pixels
    /// - `max_neg_log_p`: the maximum -log10(p) value in the data (for scaling)
    pub fn new(height: u32, max_neg_log_p: f64) -> Self {
        let log_threshold = 10.0;
        let linear_fraction = 0.6; // 60% of height for 0-10 range

        // For the log portion, we need to know the max value
        // Use at least 20 to have reasonable scaling even if max is lower
        let max_log_val = max_neg_log_p.max(20.0);

        Self {
            height: height as f32,
            log_threshold,
            linear_fraction,
            max_log_val,
        }
    }

    /// Map a -log10(pvalue) to a pixel Y position (0 = top of image).
    pub fn get_y(&self, val: f64) -> f32 {
        let linear_height = self.height as f64 * self.linear_fraction;
        let log_height = self.height as f64 * (1.0 - self.linear_fraction);

        if val <= self.log_threshold {
            // Linear portion: maps [0, log_threshold] -> [height, height - linear_height]
            let normalized = val / self.log_threshold;
            self.height - (normalized * linear_height) as f32
        } else {
            // Log portion: maps [log_threshold, max_log_val] -> [height - linear_height, 0]
            // Using log scale for compression
            let log_val = (val / self.log_threshold).ln();
            let log_max = (self.max_log_val / self.log_threshold).ln();
            let normalized = (log_val / log_max).min(1.0); // Clamp to top edge

            // Position in the log portion (0 = bottom of log region, 1 = top)
            let y_in_log = normalized * log_height;
            ((self.height as f64 - linear_height - y_in_log) as f32).max(0.0)
        }
    }

    /// Return the pixel Y position for a given p-value threshold.
    pub fn threshold_y(&self, pvalue: f64) -> f32 {
        self.get_y(-pvalue.log10())
    }
}

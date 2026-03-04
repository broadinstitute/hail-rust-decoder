/**
 * TypeScript interfaces matching the Rust ManhattanSidecar struct from src/plot/data.rs.
 * These types define the contract between the genohype backend and the viewer frontend.
 */

/**
 * A variant that exceeds the significance threshold.
 * Included in the sidecar JSON for interactive overlays.
 */
export interface SignificantHit {
  variant_id: string;
  pvalue: number;
  x_px: number; // Original PNG pixel coordinates
  y_px: number;
  x_normalized: number; // 0.0 to 1.0 (for responsive positioning)
  y_normalized: number; // 0.0 to 1.0
  annotations?: Record<string, unknown>;
}

/**
 * Per-chromosome metadata for rendering labels and understanding plot regions.
 */
export interface ChromosomeInfo {
  name: string;
  x_start_px: number;
  x_end_px: number;
  color: string; // Hex color used in PNG (e.g., "#404040")
}

/**
 * Significance threshold line metadata.
 */
export interface ThresholdInfo {
  pvalue: number; // e.g., 5e-8
  y_px: number; // Pixel Y position of threshold line in PNG
}

/**
 * Image dimensions metadata.
 */
export interface ImageDimensions {
  width: number; // PNG natural width (e.g., 3000)
  height: number; // PNG natural height (e.g., 800)
}

/**
 * Y-axis scale metadata for the hybrid linear-log scale.
 * The scale is linear from 0 to log_threshold, then logarithmic above.
 */
export interface YAxisScale {
  /** Threshold where scale switches from linear to log (-log10(p) value, typically 10) */
  log_threshold: number;
  /** Fraction of plot height used for the linear portion (0 to log_threshold) */
  linear_fraction: number;
  /** Maximum -log10(p) value in the data (used for log portion scaling) */
  max_neg_log_p: number;
}

/**
 * Top-level sidecar JSON structure emitted alongside the Manhattan plot PNG.
 */
export interface ManhattanSidecar {
  image: ImageDimensions;
  chromosomes: ChromosomeInfo[];
  threshold: ThresholdInfo;
  y_axis: YAxisScale;
  significant_hits: SignificantHit[];
}

//! Data structures for Manhattan plot points, significant hits, and the sidecar JSON.

use crate::codec::EncodedValue;
use serde::{Deserialize, Serialize};

/// Source of a variant (for distinguishing exome vs genome in combined analyses).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum VariantSource {
    Exome,
    Genome,
}

/// A variant buffered for locus plot generation.
/// Contains minimal data needed for rendering and JSON export.
#[derive(Debug, Clone, Serialize)]
pub struct BufferedVariant {
    pub contig: String,
    pub position: i32,
    pub alleles: Vec<String>,
    pub pvalue: f64,
    pub beta: Option<f64>,
    pub source: VariantSource,
    pub gene_symbol: Option<String>,
    pub consequence: Option<String>,
}

/// A genomic region of interest for locus plot generation.
#[derive(Debug, Clone)]
pub struct LocusRegion {
    pub contig: String,
    pub start: i32,
    pub end: i32,
    /// Description of signals driving this region (e.g., "rs123 (exome)", "PCSK9 (burden)")
    pub signals: Vec<String>,
}

/// A single variant extracted from a table row, ready for plotting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlotPoint {
    pub contig: String,
    pub position: i32,
    pub pvalue: f64,
    pub neg_log10_p: f64,
}

/// A variant that exceeds the significance threshold. Included in the sidecar JSON.
#[derive(Debug, Serialize)]
pub struct SignificantHit {
    pub variant_id: String,
    pub pvalue: f64,
    pub x_px: f32,
    pub y_px: f32,
    pub x_normalized: f32,
    pub y_normalized: f32,
    #[serde(skip_serializing_if = "serde_json::Value::is_null")]
    pub annotations: serde_json::Value,
}

/// Per-chromosome metadata in the sidecar.
#[derive(Debug, Serialize)]
pub struct SidecarChromosome {
    pub name: String,
    pub x_start_px: f32,
    pub x_end_px: f32,
    pub color: String,
}

/// Threshold metadata in the sidecar.
#[derive(Debug, Serialize)]
pub struct SidecarThreshold {
    pub pvalue: f64,
    pub y_px: f32,
}

/// Y-axis scale metadata for the hybrid linear-log scale.
#[derive(Debug, Serialize)]
pub struct SidecarYAxis {
    /// Threshold where scale switches from linear to log (-log10(p) value, typically 10)
    pub log_threshold: f64,
    /// Fraction of plot height used for the linear portion (0 to log_threshold)
    pub linear_fraction: f64,
    /// Maximum -log10(p) value in the data (used for log portion scaling)
    pub max_neg_log_p: f64,
}

/// Top-level sidecar JSON emitted alongside the PNG.
#[derive(Debug, Serialize)]
pub struct ManhattanSidecar {
    pub image: SidecarImage,
    pub chromosomes: Vec<SidecarChromosome>,
    pub threshold: SidecarThreshold,
    pub y_axis: SidecarYAxis,
    pub significant_hits: Vec<SignificantHit>,
}

/// Image dimensions metadata.
#[derive(Debug, Serialize)]
pub struct SidecarImage {
    pub width: u32,
    pub height: u32,
}

/// Extract a `PlotPoint` from a table row.
///
/// Expects the row to have a `locus` struct with `contig` (string) and
/// `position` (i32), plus a float field named `y_field` for the p-value.
pub fn extract_plot_data(row: &EncodedValue, y_field: &str) -> Option<PlotPoint> {
    let locus = get_nested_field(row, &["locus"])?;
    let contig = get_nested_field(locus, &["contig"])?.as_string()?;
    let position = get_nested_field(locus, &["position"])?.as_i32()?;

    let p_val_obj = get_nested_field(row, &[y_field])?;
    let pvalue = match p_val_obj {
        EncodedValue::Float64(v) => *v,
        EncodedValue::Float32(v) => *v as f64,
        _ => return None,
    };

    // Filter invalid p-values
    if pvalue <= 0.0 || pvalue > 1.0 || !pvalue.is_finite() {
        return None;
    }

    Some(PlotPoint {
        contig,
        position,
        pvalue,
        neg_log10_p: -pvalue.log10(),
    })
}

/// Navigate into a nested `EncodedValue::Struct` by field names.
fn get_nested_field<'a>(value: &'a EncodedValue, path: &[&str]) -> Option<&'a EncodedValue> {
    let mut current = value;
    for &field_name in path {
        if let EncodedValue::Struct(fields) = current {
            current = fields.iter().find(|(n, _)| n == field_name).map(|(_, v)| v)?;
        } else {
            return None;
        }
    }
    Some(current)
}

// =============================================================================
// Manifest types for phenotype pipeline output
// =============================================================================

/// Top-level manifest.json for a phenotype pipeline run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Phenotype name (derived from output path or explicitly set)
    pub phenotype: String,
    /// Ancestry group (e.g., "META", "EUR", "AFR")
    #[serde(default)]
    pub ancestry: Option<String>,
    /// ISO timestamp when the pipeline completed
    pub created_at: String,

    /// Input table paths
    pub inputs: ManifestInputs,
    /// Manhattan plot outputs
    pub manhattans: ManifestManhattans,
    /// Significant hit outputs
    pub significant_hits: ManifestSignificantHits,
    /// Locus zoom regions
    pub loci: Vec<ManifestLocus>,
    /// Pipeline statistics
    pub stats: ManifestStats,
}

/// Input paths used in the pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestInputs {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exome_results: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub genome_results: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gene_burden: Option<String>,
}

/// Manhattan plot outputs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestManhattans {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exome: Option<ManifestManhattan>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub genome: Option<ManifestManhattan>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gene: Option<ManifestManhattan>,
}

/// Info about a single Manhattan plot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestManhattan {
    /// Path to PNG file
    pub png: String,
    /// Number of variants/genes scanned
    pub count: u64,
}

/// Significant hit outputs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestSignificantHits {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exome: Option<ManifestSigHits>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub genome: Option<ManifestSigHits>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gene: Option<ManifestSigHits>,
}

/// Info about significant hits for one source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestSigHits {
    /// Path to Parquet file with annotated hits
    pub path: String,
    /// Number of significant hits
    pub count: u64,
    /// Top hit info
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_hit: Option<ManifestTopHit>,
}

/// Top hit summary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestTopHit {
    /// Variant ID (e.g., "2:21234567:A:G") or gene ID
    pub id: String,
    /// P-value
    pub pvalue: f64,
    /// Gene symbol if available
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gene: Option<String>,
    /// Consequence if available
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consequence: Option<String>,
}

/// A locus region with associated outputs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestLocus {
    /// Unique ID for the locus (e.g., "chr6_32000000_34000000")
    pub id: String,
    /// Genomic region
    pub region: ManifestRegion,
    /// Source that drove this locus (exome, genome, or gene)
    pub source: String,
    /// Lead variant ID
    pub lead_variant: String,
    /// Lead variant p-value
    pub lead_pvalue: f64,
    /// Lead variant gene (if available)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lead_gene: Option<String>,
    /// Path to locus plot PNG
    pub plot: String,
    /// Exome variants in this region
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exome_variants: Option<ManifestLocusVariants>,
    /// Genome variants in this region
    #[serde(skip_serializing_if = "Option::is_none")]
    pub genome_variants: Option<ManifestLocusVariants>,
    /// Genes overlapping this region
    pub genes: Vec<String>,
}

/// A genomic region.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestRegion {
    pub contig: String,
    pub start: i64,
    pub end: i64,
}

/// Locus variant file info.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestLocusVariants {
    /// Path to Parquet file
    pub path: String,
    /// Number of variants
    pub count: u64,
}

/// Pipeline execution statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestStats {
    /// Duration of scan phase in seconds
    pub scan_duration_sec: f64,
    /// Duration of aggregate phase in seconds
    pub aggregate_duration_sec: f64,
    /// Total number of loci generated
    pub total_loci: usize,
}

// =============================================================================
// Significant hit row for Parquet output (scan phase)
// =============================================================================

/// A significant variant hit to be written to Parquet during scan phase.
/// This is the flat schema used for partition-level sig.parquet files.
#[derive(Debug, Clone)]
pub struct SigHitRow {
    /// Chromosome (e.g., "1", "X")
    pub contig: String,
    /// Position (1-based)
    pub position: i32,
    /// Reference allele
    pub ref_allele: String,
    /// Alternate allele
    pub alt_allele: String,
    /// P-value
    pub pvalue: f64,
    /// Effect size (beta)
    pub beta: Option<f64>,
    /// Standard error
    pub se: Option<f64>,
    /// Allele frequency
    pub af: Option<f64>,
    /// Gene symbol (from annotation merge-join during scan)
    pub gene: Option<String>,
    /// VEP consequence (from annotation merge-join during scan)
    pub consequence: Option<String>,
}

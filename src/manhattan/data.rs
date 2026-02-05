//! Data structures for Manhattan plot points, significant hits, and the sidecar JSON.

use crate::codec::EncodedValue;
use serde::Serialize;

/// A single variant extracted from a table row, ready for plotting.
#[derive(Debug)]
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

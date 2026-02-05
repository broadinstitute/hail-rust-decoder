//! Gene structures and burden table processing for Manhattan plots.
//!
//! This module provides:
//! - Gene definitions loaded from gnomAD/GENCODE Hail tables
//! - Gene burden table processing for gene-level Manhattan plots
//! - Identification of significant regions of interest

use crate::codec::EncodedValue;
use crate::manhattan::data::PlotPoint;
use crate::manhattan::layout::{ChromosomeLayout, YScale};
use crate::manhattan::reference::get_contig_lengths;
use crate::manhattan::render::ManhattanRenderer;
use crate::query::{IntervalList, QueryEngine};
use crate::Result;
use serde::Serialize;
use std::collections::HashMap;

/// P-value fields to check for significance in gene burden tables.
const GENE_PVALUE_FIELDS: &[&str] = &["Pvalue", "Pvalue_SKAT", "Pvalue_Burden"];

/// A single significant test result from gene burden analysis.
#[derive(Debug, Clone, Serialize)]
pub struct SignificantTest {
    pub annotation: String,
    pub max_maf: f64,
    pub pvalue_field: String,
    pub pvalue: f64,
}

/// A gene with significant burden signal.
#[derive(Debug, Clone, Serialize)]
pub struct SignificantGene {
    pub gene_id: String,
    pub gene_symbol: String,
    pub best_pvalue: f64,
    pub best_test: String,
    pub interval: (String, i32, i32), // chrom, start, end
    pub plot_position: i32,           // TSS for manhattan plot
    pub significant_tests: Vec<SignificantTest>,
}

/// A gene definition from gnomAD/GENCODE.
#[derive(Debug, Clone, Serialize)]
pub struct Gene {
    pub id: String,
    pub symbol: String,
    pub chrom: String,
    pub start: i32,
    pub stop: i32,
    pub strand: String,
}

/// Helper to manage gene lookups.
pub struct GeneMap {
    pub genes: Vec<Gene>,
}

impl GeneMap {
    pub fn new() -> Self {
        Self { genes: Vec::new() }
    }

    /// Load genes from a gnomAD genes Hail table.
    pub fn load(path: &str) -> Result<Self> {
        let engine = QueryEngine::open_path(path)?;
        let mut genes = Vec::new();

        // Scan the table
        let iter = engine.query_iter(&[])?;

        for row_res in iter {
            let row = row_res?;
            if let EncodedValue::Struct(fields) = row {
                let get_str = |name: &str| -> Option<String> {
                    fields
                        .iter()
                        .find(|(n, _)| n == name)
                        .and_then(|(_, v)| v.as_string())
                };
                let get_int = |name: &str| -> Option<i32> {
                    fields
                        .iter()
                        .find(|(n, _)| n == name)
                        .and_then(|(_, v)| v.as_i32())
                };

                let id = get_str("gene_id");
                let symbol = get_str("gencode_symbol").or_else(|| get_str("gene_symbol"));
                let chrom = get_str("chrom").or_else(|| get_str("contig"));
                let start = get_int("start");
                let stop = get_int("stop");
                let strand = get_str("strand");

                if let (Some(id), Some(symbol), Some(chrom), Some(start), Some(stop), Some(strand)) =
                    (id, symbol, chrom, start, stop, strand)
                {
                    genes.push(Gene {
                        id,
                        symbol,
                        chrom,
                        start,
                        stop,
                        strand,
                    });
                }
            }
        }

        Ok(Self { genes })
    }

    /// Find a gene by symbol.
    pub fn find_by_symbol(&self, symbol: &str) -> Option<&Gene> {
        self.genes.iter().find(|g| g.symbol == symbol)
    }

    /// Query genes overlapping a region.
    pub fn query(&self, chrom: &str, start: i32, stop: i32) -> Vec<&Gene> {
        self.genes
            .iter()
            .filter(|g| g.chrom == chrom && g.start <= stop && g.stop >= start)
            .collect()
    }
}

impl Default for GeneMap {
    fn default() -> Self {
        Self::new()
    }
}

/// Process a gene burden table to generate plot and regions of interest.
///
/// # Arguments
/// * `path` - Path to the gene burden Hail table
/// * `pval_threshold` - P-value threshold for significance
/// * `width` - Image width in pixels
/// * `height` - Image height in pixels
/// * `gene_map` - Optional gene map for precise gene bounds
///
/// # Returns
/// A tuple of (PNG bytes, JSON sidecar string, IntervalList of significant regions)
pub fn process_gene_burden(
    path: &str,
    pval_threshold: f64,
    width: u32,
    height: u32,
    gene_map: Option<&GeneMap>,
) -> Result<(Vec<u8>, String, IntervalList)> {
    let engine = QueryEngine::open_path(path)?;

    let all_contigs = get_contig_lengths(&engine);
    // Filter to autosomes + X/Y/M (skip random/unplaced contigs)
    let contigs: Vec<(String, u32)> = all_contigs
        .into_iter()
        .filter(|(name, _)| name.len() <= 5) // Simple heuristic to skip random contigs
        .collect();

    let layout = ChromosomeLayout::new(&contigs, width, 4);
    let mut renderer = ManhattanRenderer::new(width, height);

    // Y-Scale config (cap at 50 for gene burden usually sufficient)
    let y_scale = YScale::new(height, 50.0);
    renderer.render_threshold_line(y_scale.threshold_y(pval_threshold), width);

    let mut significant_regions = IntervalList::new();
    let iter = engine.query_iter(&[])?;

    // Gene burden table fields: usually Pvalue, gene_id/symbol, genomic coords
    // We expect CHR, POS or interval
    for row_res in iter {
        let row = row_res?;

        // Extract plotting data.
        // Note: Burden tables vary. We'll look for standard fields.
        let mut point: Option<PlotPoint> = None;
        let mut gene_symbol: Option<String> = None;

        if let EncodedValue::Struct(fields) = &row {
            // Find P-value
            let pval = fields
                .iter()
                .find(|(n, _)| n == "Pvalue")
                .and_then(|(_, v)| match v {
                    EncodedValue::Float64(f) => Some(*f),
                    EncodedValue::Float32(f) => Some(*f as f64),
                    _ => None,
                });

            // Find Position (CHR/POS or interval)
            // Simplified: look for CHR and POS
            let chr = fields
                .iter()
                .find(|(n, _)| n == "CHR" || n == "chromosome")
                .and_then(|(_, v)| v.as_string());
            let pos = fields
                .iter()
                .find(|(n, _)| n == "POS" || n == "position")
                .and_then(|(_, v)| v.as_i32());

            gene_symbol = fields
                .iter()
                .find(|(n, _)| n == "gene_symbol" || n == "symbol")
                .and_then(|(_, v)| v.as_string());

            if let (Some(p), Some(c), Some(pos)) = (pval, chr, pos) {
                if p > 0.0 && p.is_finite() {
                    point = Some(PlotPoint {
                        contig: c,
                        position: pos,
                        pvalue: p,
                        neg_log10_p: -p.log10(),
                    });
                }
            }
        }

        if let Some(pt) = point {
            // Render
            // Normalize contig name (strip chr)
            let contig_name = if pt.contig.starts_with("chr") {
                pt.contig[3..].to_string()
            } else {
                pt.contig.clone()
            };

            if let Some(x) = layout.get_x(&contig_name, pt.position) {
                let y = y_scale.get_y(pt.neg_log10_p);
                let color = layout.get_color(&contig_name);
                renderer.render_point(x, y, color, 0.8);
            }

            // Check significance
            if pt.pvalue < pval_threshold {
                // Determine gene bounds
                let padding = 100_000; // 100kb padding

                // If we have a gene map and symbol, use precise bounds
                if let (Some(map), Some(sym)) = (gene_map, &gene_symbol) {
                    if let Some(gene) = map.find_by_symbol(sym) {
                        let start = (gene.start - padding).max(1);
                        let stop = gene.stop + padding;
                        significant_regions.add(gene.chrom.clone(), start, stop);
                        continue;
                    }
                }

                // Fallback: use point position +/- padding
                let start = (pt.position - padding).max(1);
                let stop = pt.position + padding;
                significant_regions.add(pt.contig.clone(), start, stop);
            }
        }
    }

    significant_regions.optimize();

    let png_data = renderer.encode_png()?;
    // Minimal sidecar for now
    let sidecar = serde_json::json!({
        "image": {"width": width, "height": height},
        "threshold": pval_threshold,
        "significant_regions": significant_regions.len()
    })
    .to_string();

    Ok((png_data, sidecar, significant_regions))
}

/// Helper to extract f64 from EncodedValue.
fn encoded_as_f64(val: &EncodedValue) -> Option<f64> {
    match val {
        EncodedValue::Float64(f) => Some(*f),
        EncodedValue::Float32(f) => Some(*f as f64),
        EncodedValue::Int64(i) => Some(*i as f64),
        EncodedValue::Int32(i) => Some(*i as f64),
        _ => None,
    }
}

/// Process a gene burden table with compound keys and multiple p-value fields.
///
/// This handles the complex gene burden schema with:
/// - Compound key: (gene_id, gene_symbol, annotation, max_MAF)
/// - Multiple p-value fields: Pvalue, Pvalue_SKAT, Pvalue_Burden
/// - Multiple annotation types: pLoF, missenseLC, synonymous, etc.
///
/// # Arguments
/// * `path` - Path to the gene burden Hail table
/// * `pval_threshold` - P-value threshold for significance
///
/// # Returns
/// A tuple of (SignificantGenes, IntervalList of significant regions)
pub fn process_complex_gene_burden(
    path: &str,
    pval_threshold: f64,
) -> Result<(Vec<SignificantGene>, IntervalList)> {
    let engine = QueryEngine::open_path(path)?;
    let mut gene_groups: HashMap<String, Vec<EncodedValue>> = HashMap::new();

    // 1. Scan and group by gene_id
    let iter = engine.query_iter(&[])?;
    for row_res in iter {
        let row = row_res?;
        if let EncodedValue::Struct(fields) = &row {
            // Extract gene_id to group rows
            if let Some((_, val)) = fields.iter().find(|(n, _)| n == "gene_id") {
                if let Some(gene_id) = val.as_string() {
                    gene_groups.entry(gene_id).or_default().push(row);
                }
            }
        }
    }

    let mut significant_genes = Vec::new();
    let mut regions = IntervalList::new();

    // 2. Process each gene group
    for (gene_id, rows) in gene_groups {
        let mut best_pvalue = 1.0;
        let mut best_test_desc = String::new();
        let mut symbol = gene_id.clone();
        let mut interval_coords: Option<(String, i32, i32)> = None;
        let mut plot_pos = 0i32;
        let mut significant_tests = Vec::new();

        for row in &rows {
            if let EncodedValue::Struct(fields) = row {
                // Extract gene_symbol (only if not yet found)
                if symbol == gene_id {
                    if let Some((_, v)) = fields.iter().find(|(n, _)| n == "gene_symbol") {
                        if let Some(s) = v.as_string() {
                            symbol = s;
                        }
                    }
                }

                // Extract annotation and MAF for description
                let annot = fields
                    .iter()
                    .find(|(n, _)| n == "annotation")
                    .and_then(|(_, v)| v.as_string())
                    .unwrap_or_default();

                let maf = fields
                    .iter()
                    .find(|(n, _)| n == "max_MAF")
                    .and_then(|(_, v)| encoded_as_f64(v))
                    .unwrap_or(-1.0);

                // Check all p-value fields
                for &field_name in GENE_PVALUE_FIELDS {
                    if let Some((_, val)) = fields.iter().find(|(n, _)| n == field_name) {
                        if let Some(p) = encoded_as_f64(val) {
                            if p > 0.0 && p.is_finite() {
                                // Track significant tests
                                if p < pval_threshold {
                                    significant_tests.push(SignificantTest {
                                        annotation: annot.clone(),
                                        max_maf: maf,
                                        pvalue_field: field_name.to_string(),
                                        pvalue: p,
                                    });
                                }
                                // Track best pvalue
                                if p < best_pvalue {
                                    best_pvalue = p;
                                    best_test_desc =
                                        format!("{}:{}:maf{:.4}", annot, field_name, maf);
                                }
                            }
                        }
                    }
                }

                // Capture interval/position if not yet found
                if interval_coords.is_none() {
                    // Try CHR/POS fields
                    let chr = fields
                        .iter()
                        .find(|(n, _)| n == "CHR")
                        .and_then(|(_, v)| v.as_string());
                    let pos = fields
                        .iter()
                        .find(|(n, _)| n == "POS")
                        .and_then(|(_, v)| v.as_i32());

                    if let (Some(c), Some(p)) = (chr, pos) {
                        plot_pos = p;
                        // Pad by 100kb for region of interest
                        interval_coords = Some((c, (p - 100_000).max(1), p + 100_000));
                    }
                }
            }
        }

        // Only include genes with at least one significant test
        if !significant_tests.is_empty() {
            if let Some((chrom, start, end)) = interval_coords {
                significant_genes.push(SignificantGene {
                    gene_id: gene_id.clone(),
                    gene_symbol: symbol,
                    best_pvalue,
                    best_test: best_test_desc,
                    interval: (chrom.clone(), start, end),
                    plot_position: plot_pos,
                    significant_tests,
                });
                regions.add(chrom, start, end);
            }
        }
    }

    regions.optimize();
    Ok((significant_genes, regions))
}

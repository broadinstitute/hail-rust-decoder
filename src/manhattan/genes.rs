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

/// A gene definition from gnomAD/GENCODE.
#[derive(Debug, Clone)]
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

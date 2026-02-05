//! Integrated pipeline for multi-table Manhattan plot and locus zoom generation.
//!
//! This module orchestrates the full workflow:
//! 1. Load gene map and process gene burden table (if provided)
//! 2. Scan exome variants with annotation merge-join
//! 3. Scan genome variants with annotation merge-join
//! 4. Compute locus regions from significant signals
//! 5. Generate locus plots and JSON exports

use crate::codec::EncodedValue;
use crate::manhattan::data::{BufferedVariant, VariantSource};
use crate::manhattan::genes::{process_complex_gene_burden, GeneMap, SignificantGene};
use crate::manhattan::layout::{ChromosomeLayout, YScale};
use crate::manhattan::locus::{DataSource, LocusPlotConfig, LocusRenderer, RenderVariant};
use crate::manhattan::reference::get_contig_lengths;
use crate::manhattan::render::ManhattanRenderer;
use crate::query::join::{JoinedRow, SortedMergeIterator};
use crate::query::{IntervalList, QueryEngine};
use crate::Result;
use indicatif::{ProgressBar, ProgressStyle};
use std::fs;
use std::path::Path;

/// Configuration for the integrated pipeline.
///
/// This is separate from CLI args to allow the pipeline to be called
/// from library code without depending on the binary's CLI module.
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    // Data inputs
    pub exome: Option<String>,
    pub exome_annotations: Option<String>,
    pub genome: Option<String>,
    pub genome_annotations: Option<String>,
    pub gene_burden: Option<String>,
    pub genes: Option<String>,

    // Thresholds
    pub threshold: f64,
    pub gene_threshold: f64,
    pub locus_threshold: f64,
    pub locus_window: i32,
    pub locus_plots: bool,

    // Output
    pub output: Option<String>,
    pub width: u32,
    pub height: u32,
    pub y_field: String,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            exome: None,
            exome_annotations: None,
            genome: None,
            genome_annotations: None,
            gene_burden: None,
            genes: None,
            threshold: 5e-8,
            gene_threshold: 2.5e-6,
            locus_threshold: 0.01,
            locus_window: 1_000_000,
            locus_plots: false,
            output: None,
            width: 3000,
            height: 800,
            y_field: "Pvalue".to_string(),
        }
    }
}

/// Run the integrated multi-table pipeline.
///
/// This replaces the simple single-table manhattan command when multiple
/// inputs are provided (exome, genome, burden tables).
pub fn run_integrated_pipeline(config: &PipelineConfig) -> Result<()> {
    let output_base = Path::new(config.output.as_deref().unwrap_or("."));
    fs::create_dir_all(output_base)?;

    // 1. Load Gene Map if provided
    let gene_map = if let Some(path) = &config.genes {
        println!("Loading genes from: {}", path);
        Some(GeneMap::load(path)?)
    } else {
        None
    };

    // 2. Process Gene Burden (if provided)
    let mut interest_regions = IntervalList::new();
    let mut sig_genes: Vec<SignificantGene> = Vec::new();

    if let Some(burden_path) = &config.gene_burden {
        println!("Processing gene burden: {}", burden_path);
        let (genes, regions) = process_complex_gene_burden(burden_path, config.gene_threshold)?;
        println!("Found {} significant genes", genes.len());

        // Write significant genes JSON
        let genes_json = serde_json::to_string_pretty(&genes)?;
        fs::write(output_base.join("significant_genes.json"), &genes_json)?;

        sig_genes = genes;
        interest_regions.merge(regions);
    }

    // Prepare Buffers
    let mut exome_buffer: Vec<BufferedVariant> = Vec::new();
    let mut genome_buffer: Vec<BufferedVariant> = Vec::new();
    let mut sig_variants: Vec<(String, i32, f64, VariantSource)> = Vec::new();

    // We need at least one table to establish layout
    let layout_path = config
        .exome
        .as_ref()
        .or(config.genome.as_ref());

    let (chrom_layout, y_scale) = if let Some(path) = layout_path {
        let layout_engine = QueryEngine::open_path(path)?;
        let all_contigs = get_contig_lengths(&layout_engine);

        // Filter to standard chromosomes
        let contigs: Vec<(String, u32)> = all_contigs
            .into_iter()
            .filter(|(name, _)| name.len() <= 5)
            .collect();

        let layout = ChromosomeLayout::new(&contigs, config.width, 4);
        let scale = YScale::new(config.height, 50.0);
        (layout, scale)
    } else {
        return Err(crate::HailError::InvalidFormat(
            "No variant tables provided".into(),
        ));
    };

    // 3. Scan Exomes (if provided) - NO annotations during main scan for speed
    if let Some(res_path) = &config.exome {
        println!("Scanning Exome variants: {}", res_path);

        scan_variant_table(
            res_path,
            None, // Skip annotations during main scan
            VariantSource::Exome,
            &chrom_layout,
            &y_scale,
            config,
            &interest_regions,
            &mut exome_buffer,
            &mut sig_variants,
            output_base.join("exome_manhattan.png").to_str().unwrap(),
        )?;
    }

    // 4. Scan Genomes (if provided) - NO annotations during main scan for speed
    if let Some(res_path) = &config.genome {
        println!("Scanning Genome variants: {}", res_path);

        scan_variant_table(
            res_path,
            None, // Skip annotations during main scan
            VariantSource::Genome,
            &chrom_layout,
            &y_scale,
            config,
            &interest_regions,
            &mut genome_buffer,
            &mut sig_variants,
            output_base.join("genome_manhattan.png").to_str().unwrap(),
        )?;
    }

    // 4b. Enrich buffered variants with annotations (targeted lookups)
    if !exome_buffer.is_empty() {
        if let Some(annot_path) = &config.exome_annotations {
            println!("Enriching {} exome variants with annotations...", exome_buffer.len());
            enrich_variants_with_annotations(&mut exome_buffer, annot_path)?;
        }
    }
    if !genome_buffer.is_empty() {
        if let Some(annot_path) = &config.genome_annotations {
            println!("Enriching {} genome variants with annotations...", genome_buffer.len());
            enrich_variants_with_annotations(&mut genome_buffer, annot_path)?;
        }
    }

    // 5. Compute Locus Regions
    if config.locus_plots {
        println!("Defining locus regions...");

        // Add significant variant regions (± window)
        for (chrom, pos, _, _) in &sig_variants {
            let start = (pos - config.locus_window).max(1);
            let end = pos + config.locus_window;
            interest_regions.add(chrom.clone(), start, end);
        }

        interest_regions.optimize();
        println!("Total locus regions: {}", interest_regions.len());

        // 6. Generate Locus Outputs
        let loci_dir = output_base.join("loci");
        fs::create_dir_all(&loci_dir)?;

        // Collect contigs to iterate
        let contigs: Vec<String> = interest_regions.contigs().cloned().collect();

        for contig in &contigs {
            if let Some(ranges) = interest_regions.intervals_for_contig(contig) {
                for range in ranges {
                    let start = *range.start();
                    let end = *range.end();

                    // Filter buffers for this region
                    let exome_subset: Vec<&BufferedVariant> = exome_buffer
                        .iter()
                        .filter(|v| v.contig == *contig && v.position >= start && v.position <= end)
                        .collect();

                    let genome_subset: Vec<&BufferedVariant> = genome_buffer
                        .iter()
                        .filter(|v| v.contig == *contig && v.position >= start && v.position <= end)
                        .collect();

                    if exome_subset.is_empty() && genome_subset.is_empty() {
                        continue;
                    }

                    // Create region output directory
                    let region_name = format!("{}_{}_{}", contig, start, end);
                    let region_dir = loci_dir.join(&region_name);
                    fs::create_dir_all(&region_dir)?;

                    // Write variants JSON
                    let exome_json = serde_json::to_string_pretty(&exome_subset)?;
                    fs::write(region_dir.join("exome.json"), exome_json)?;

                    let genome_json = serde_json::to_string_pretty(&genome_subset)?;
                    fs::write(region_dir.join("genome.json"), genome_json)?;

                    // If genes loaded, extract genes in region
                    if let Some(map) = &gene_map {
                        let genes = map.query(contig, start, end);
                        let genes_json = serde_json::to_string_pretty(&genes)?;
                        fs::write(region_dir.join("genes.json"), genes_json)?;
                    }

                    // Render locus plot
                    let png_data = render_locus_plot(
                        &exome_subset,
                        &genome_subset,
                        contig,
                        start,
                        end,
                        config.threshold,
                    )?;
                    fs::write(region_dir.join("plot.png"), png_data)?;

                    println!("  Generated locus: {}", region_name);
                }
            }
        }
    }

    // Write summary
    let summary = serde_json::json!({
        "significant_genes": sig_genes.len(),
        "significant_variants": sig_variants.len(),
        "exome_variants_buffered": exome_buffer.len(),
        "genome_variants_buffered": genome_buffer.len(),
        "locus_regions": interest_regions.len(),
    });
    fs::write(
        output_base.join("summary.json"),
        serde_json::to_string_pretty(&summary)?,
    )?;

    println!("Pipeline complete. Output: {:?}", output_base);
    Ok(())
}

/// Scan a variant table (with optional annotation join), render Manhattan, and buffer variants.
#[allow(clippy::too_many_arguments)]
fn scan_variant_table(
    results_path: &str,
    annotations_path: Option<&str>,
    source: VariantSource,
    layout: &ChromosomeLayout,
    y_scale: &YScale,
    config: &PipelineConfig,
    interest_regions: &IntervalList,
    buffer: &mut Vec<BufferedVariant>,
    sig_variants: &mut Vec<(String, i32, f64, VariantSource)>,
    output_png: &str,
) -> Result<()> {
    let results_engine = QueryEngine::open_path(results_path)?;
    let mut renderer = ManhattanRenderer::new(config.width, config.height);

    // Render threshold line
    renderer.render_threshold_line(y_scale.threshold_y(config.threshold), config.width);

    // Progress bar
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg} {pos} rows")
            .unwrap(),
    );
    pb.set_message(match source {
        VariantSource::Exome => "Exome:",
        VariantSource::Genome => "Genome:",
    });

    let y_field = &config.y_field;
    let locus_threshold = config.locus_threshold;
    let variant_threshold = config.threshold;

    if let Some(annot_path) = annotations_path {
        // Merge-join with annotations
        let annot_engine = QueryEngine::open_path(annot_path)?;

        let keys = vec!["locus".to_string(), "alleles".to_string()];
        // Use sorted iterators for merge-join (sequential partition iteration)
        let results_iter = results_engine.query_iter_sorted(&[])?;
        let annot_iter = annot_engine.query_iter_sorted(&[])?;
        let join_iter = SortedMergeIterator::new(results_iter, annot_iter, keys);

        let mut matched = 0usize;
        let mut unmatched = 0usize;
        for (i, join_res) in join_iter.enumerate() {
            let row = join_res?;

            // Track match status
            if row.right.is_some() {
                matched += 1;
            } else {
                unmatched += 1;
            }

            process_joined_row(
                &row,
                source,
                layout,
                y_scale,
                y_field,
                locus_threshold,
                variant_threshold,
                interest_regions,
                buffer,
                sig_variants,
                &mut renderer,
            );

            if i % 10_000 == 0 {
                pb.set_position(i as u64);
            }
        }
        println!("  Merge complete: {} matched, {} unmatched", matched, unmatched);
    } else {
        // No annotations - just scan results
        let results_iter = results_engine.query_iter(&[])?;

        for (i, row_res) in results_iter.enumerate() {
            let row = row_res?;
            process_single_row(
                &row,
                source,
                layout,
                y_scale,
                y_field,
                locus_threshold,
                variant_threshold,
                interest_regions,
                buffer,
                sig_variants,
                &mut renderer,
            );

            if i % 10_000 == 0 {
                pb.set_position(i as u64);
            }
        }
    }

    pb.finish_with_message("complete");

    // Save Manhattan PNG
    let png_data = renderer.encode_png()?;
    fs::write(output_png, png_data)?;
    println!("Saved: {}", output_png);

    Ok(())
}

/// Process a joined row (results + annotations).
#[allow(clippy::too_many_arguments)]
fn process_joined_row(
    row: &JoinedRow,
    source: VariantSource,
    layout: &ChromosomeLayout,
    y_scale: &YScale,
    y_field: &str,
    locus_threshold: f64,
    variant_threshold: f64,
    interest_regions: &IntervalList,
    buffer: &mut Vec<BufferedVariant>,
    sig_variants: &mut Vec<(String, i32, f64, VariantSource)>,
    renderer: &mut ManhattanRenderer,
) {
    // Extract from left (results)
    let (contig, position, pvalue, beta, alleles) = match extract_variant_fields(&row.left, y_field)
    {
        Some(v) => v,
        None => return,
    };

    // Extract from right (annotations) if present
    let (gene_symbol, consequence) = if let Some(ref annot) = row.right {
        extract_annotation_fields(annot)
    } else {
        (None, None)
    };

    // Render point
    render_variant(
        &contig, position, pvalue, layout, y_scale, renderer,
    );

    // Check significance
    if pvalue < variant_threshold {
        sig_variants.push((contig.clone(), position, pvalue, source));
    }

    // Buffer if interesting
    let should_buffer =
        pvalue < locus_threshold || interest_regions.contains(&contig, position);

    if should_buffer {
        buffer.push(BufferedVariant {
            contig,
            position,
            alleles,
            pvalue,
            beta,
            source,
            gene_symbol,
            consequence,
        });
    }
}

/// Process a single row (no annotations).
#[allow(clippy::too_many_arguments)]
fn process_single_row(
    row: &EncodedValue,
    source: VariantSource,
    layout: &ChromosomeLayout,
    y_scale: &YScale,
    y_field: &str,
    locus_threshold: f64,
    variant_threshold: f64,
    interest_regions: &IntervalList,
    buffer: &mut Vec<BufferedVariant>,
    sig_variants: &mut Vec<(String, i32, f64, VariantSource)>,
    renderer: &mut ManhattanRenderer,
) {
    let (contig, position, pvalue, beta, alleles) = match extract_variant_fields(row, y_field) {
        Some(v) => v,
        None => return,
    };

    // Render point
    render_variant(
        &contig, position, pvalue, layout, y_scale, renderer,
    );

    // Check significance
    if pvalue < variant_threshold {
        sig_variants.push((contig.clone(), position, pvalue, source));
    }

    // Buffer if interesting
    let should_buffer =
        pvalue < locus_threshold || interest_regions.contains(&contig, position);

    if should_buffer {
        buffer.push(BufferedVariant {
            contig,
            position,
            alleles,
            pvalue,
            beta,
            source,
            gene_symbol: None,
            consequence: None,
        });
    }
}

/// Render a single variant point on the Manhattan plot.
fn render_variant(
    contig: &str,
    position: i32,
    pvalue: f64,
    layout: &ChromosomeLayout,
    y_scale: &YScale,
    renderer: &mut ManhattanRenderer,
) {
    // Normalize contig (strip chr prefix)
    let contig_name = if contig.starts_with("chr") {
        &contig[3..]
    } else {
        contig
    };

    if let Some(x) = layout.get_x(contig_name, position) {
        let neg_log_p = -pvalue.log10();
        let y = y_scale.get_y(neg_log_p);
        let color = layout.get_color(contig_name);
        renderer.render_point(x, y, color, 0.6);
    }
}

/// Extract variant fields from a row.
fn extract_variant_fields(
    row: &EncodedValue,
    y_field: &str,
) -> Option<(String, i32, f64, Option<f64>, Vec<String>)> {
    if let EncodedValue::Struct(fields) = row {
        // Extract locus
        let locus = fields.iter().find(|(n, _)| n == "locus").map(|(_, v)| v)?;

        let (contig, position) = if let EncodedValue::Struct(locus_fields) = locus {
            let contig = locus_fields
                .iter()
                .find(|(n, _)| n == "contig")
                .and_then(|(_, v)| v.as_string())?;
            let pos = locus_fields
                .iter()
                .find(|(n, _)| n == "position")
                .and_then(|(_, v)| v.as_i32())?;
            (contig, pos)
        } else {
            return None;
        };

        // Extract p-value
        let pvalue = fields
            .iter()
            .find(|(n, _)| n == y_field)
            .and_then(|(_, v)| match v {
                EncodedValue::Float64(f) => Some(*f),
                EncodedValue::Float32(f) => Some(*f as f64),
                _ => None,
            })?;

        // Skip invalid p-values
        if pvalue <= 0.0 || pvalue > 1.0 || !pvalue.is_finite() {
            return None;
        }

        // Extract beta (optional)
        let beta = fields
            .iter()
            .find(|(n, _)| n == "BETA")
            .and_then(|(_, v)| match v {
                EncodedValue::Float64(f) => Some(*f),
                EncodedValue::Float32(f) => Some(*f as f64),
                _ => None,
            });

        // Extract alleles
        let alleles = fields
            .iter()
            .find(|(n, _)| n == "alleles")
            .and_then(|(_, v)| {
                if let EncodedValue::Array(arr) = v {
                    Some(
                        arr.iter()
                            .filter_map(|a| a.as_string())
                            .collect::<Vec<String>>(),
                    )
                } else {
                    None
                }
            })
            .unwrap_or_default();

        Some((contig, position, pvalue, beta, alleles))
    } else {
        None
    }
}

/// Extract annotation fields from an annotation row.
fn extract_annotation_fields(row: &EncodedValue) -> (Option<String>, Option<String>) {
    if let EncodedValue::Struct(fields) = row {
        let gene_symbol = fields
            .iter()
            .find(|(n, _)| n == "gene_symbol")
            .and_then(|(_, v)| v.as_string());

        let consequence = fields
            .iter()
            .find(|(n, _)| n == "most_severe_csq_variant" || n == "consequence")
            .and_then(|(_, v)| v.as_string());

        (gene_symbol, consequence)
    } else {
        (None, None)
    }
}

/// Render a locus plot for a specific region.
fn render_locus_plot(
    exome_variants: &[&BufferedVariant],
    genome_variants: &[&BufferedVariant],
    _contig: &str,
    start: i32,
    end: i32,
    threshold: f64,
) -> Result<Vec<u8>> {
    let config = LocusPlotConfig {
        width: 800,
        height: 400,
        start_pos: start,
        end_pos: end,
        y_max: 30.0,
    };

    let mut renderer = LocusRenderer::new(config);
    renderer.draw_threshold_line(threshold);

    // Convert buffered variants to render variants
    let mut render_variants: Vec<RenderVariant> = Vec::new();

    for v in genome_variants {
        render_variants.push(RenderVariant {
            position: v.position,
            pvalue: v.pvalue,
            source: DataSource::Genome,
            is_significant: v.pvalue < threshold,
        });
    }

    for v in exome_variants {
        render_variants.push(RenderVariant {
            position: v.position,
            pvalue: v.pvalue,
            source: DataSource::Exome,
            is_significant: v.pvalue < threshold,
        });
    }

    renderer.draw_variants(&render_variants);
    renderer.encode_png()
}

/// Enrich buffered variants with annotations via targeted lookups.
///
/// This is much faster than merge-join for small numbers of variants
/// because it uses index lookups instead of scanning the entire table.
fn enrich_variants_with_annotations(
    variants: &mut [BufferedVariant],
    annot_path: &str,
) -> Result<()> {
    let mut annot_engine = QueryEngine::open_path(annot_path)?;

    for variant in variants.iter_mut() {
        // Build a key for lookup
        let key = EncodedValue::Struct(vec![
            (
                "locus".to_string(),
                EncodedValue::Struct(vec![
                    ("contig".to_string(), EncodedValue::Binary(variant.contig.as_bytes().to_vec())),
                    ("position".to_string(), EncodedValue::Int32(variant.position)),
                ]),
            ),
            (
                "alleles".to_string(),
                EncodedValue::Array(
                    variant.alleles.iter()
                        .map(|a| EncodedValue::Binary(a.as_bytes().to_vec()))
                        .collect()
                ),
            ),
        ]);

        // Try point lookup
        if let Ok(Some(annot_row)) = annot_engine.lookup(&key) {
            let (gene_symbol, consequence) = extract_annotation_fields(&annot_row);
            variant.gene_symbol = gene_symbol;
            variant.consequence = consequence;
        }
    }

    Ok(())
}

// =============================================================================
// Distributed Processing Support
// =============================================================================

use crate::io::{is_cloud_path, StreamingCloudWriter};
use crate::manhattan::data::{extract_plot_data, PlotPoint};
use std::io::{BufWriter, Write};

/// Result of a distributed scan containing extracted plot points.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct DistributedScanResult {
    /// All extracted plot points from scanned partitions
    pub points: Vec<PlotPoint>,
    /// Total rows processed
    pub rows_processed: usize,
}

/// Run a distributed scan for a specific set of partitions.
///
/// This function is designed to be called by workers in a distributed pool.
/// Instead of rendering a full Manhattan plot, it extracts raw PlotPoint data
/// and writes it to an intermediate file for later aggregation.
///
/// # Arguments
/// * `config` - Pipeline configuration (determines which tables to scan)
/// * `partitions` - List of partition indices to process
/// * `output_file` - Path to write the intermediate JSON output
///
/// # Returns
/// Number of rows processed.
pub fn run_distributed_scan(
    config: &PipelineConfig,
    partitions: &[usize],
    output_file: &str,
) -> Result<usize> {
    let y_field = &config.y_field;
    let mut all_points: Vec<PlotPoint> = Vec::new();
    let mut total_rows: usize = 0;

    // Scan exome table partitions
    if let Some(exome_path) = &config.exome {
        let (points, rows) = scan_table_partitions(exome_path, partitions, y_field)?;
        all_points.extend(points);
        total_rows += rows;
    }

    // Scan genome table partitions
    if let Some(genome_path) = &config.genome {
        let (points, rows) = scan_table_partitions(genome_path, partitions, y_field)?;
        all_points.extend(points);
        total_rows += rows;
    }

    // Write results to output file
    let result = DistributedScanResult {
        points: all_points,
        rows_processed: total_rows,
    };

    write_scan_result(&result, output_file)?;

    Ok(total_rows)
}

/// Scan specific partitions from a table and extract PlotPoints.
fn scan_table_partitions(
    table_path: &str,
    partitions: &[usize],
    y_field: &str,
) -> Result<(Vec<PlotPoint>, usize)> {
    use rayon::prelude::*;

    let table_path = table_path.to_string();
    let y_field = y_field.to_string();

    // Process partitions in parallel
    let results: Vec<Result<(Vec<PlotPoint>, usize)>> = partitions
        .par_iter()
        .map(|&partition_id| {
            let engine = QueryEngine::open_path(&table_path)?;
            let iter = engine.scan_partition_iter(partition_id, &[])?;

            let mut points = Vec::new();
            let mut rows = 0;

            for row_result in iter {
                let row = row_result?;
                rows += 1;

                if let Some(point) = extract_plot_data(&row, &y_field) {
                    points.push(point);
                }
            }

            Ok((points, rows))
        })
        .collect();

    // Aggregate results
    let mut all_points = Vec::new();
    let mut total_rows = 0;

    for result in results {
        let (points, rows) = result?;
        all_points.extend(points);
        total_rows += rows;
    }

    Ok((all_points, total_rows))
}

/// Write scan result to a JSON file (supports cloud and local paths).
fn write_scan_result(result: &DistributedScanResult, output_path: &str) -> Result<()> {
    let json_data = serde_json::to_vec(result).map_err(|e| {
        crate::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Failed to serialize scan result: {}", e),
        ))
    })?;

    if is_cloud_path(output_path) {
        let mut writer = StreamingCloudWriter::new(output_path)?;
        writer.write_all(&json_data)?;
        writer.finish()?;
    } else {
        // Ensure parent directory exists
        if let Some(parent) = std::path::Path::new(output_path).parent() {
            fs::create_dir_all(parent)?;
        }
        let file = std::fs::File::create(output_path)?;
        let mut writer = BufWriter::new(file);
        writer.write_all(&json_data)?;
        writer.flush()?;
    }

    Ok(())
}

// =============================================================================
// Shard Aggregation (--from-shards mode)
// =============================================================================

use crate::io::{get_file_size, range_read};
use crate::manhattan::data::{
    ManhattanSidecar, SidecarChromosome, SidecarImage, SidecarThreshold, SidecarYAxis,
    SignificantHit,
};

/// Aggregate distributed scan shards and render final Manhattan plot.
///
/// This function reads all `part-*.json` files from the shards directory,
/// combines the PlotPoints, and renders the final PNG with sidecar JSON.
pub fn aggregate_shards_and_render(
    shards_path: &str,
    output_prefix: &str,
    width: u32,
    height: u32,
    threshold: f64,
) -> Result<()> {
    println!("Aggregating shards from: {}", shards_path);

    // 1. List and read all shard files
    let shard_files = list_shard_files(shards_path)?;
    println!("Found {} shard files", shard_files.len());

    if shard_files.is_empty() {
        return Err(crate::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("No part-*.json files found in {}", shards_path),
        )));
    }

    // 2. Read and aggregate all PlotPoints
    let mut all_points: Vec<PlotPoint> = Vec::new();
    let mut total_rows: usize = 0;

    let pb = ProgressBar::new(shard_files.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} shards")
            .unwrap()
            .progress_chars("#>-"),
    );

    for shard_file in &shard_files {
        let result = read_shard_file(shard_file)?;
        all_points.extend(result.points);
        total_rows += result.rows_processed;
        pb.inc(1);
    }
    pb.finish_with_message("done");

    println!(
        "Aggregated {} plot points from {} total rows",
        all_points.len(),
        total_rows
    );

    // 3. Build chromosome layout from the data
    let (layout, y_scale, max_neg_log_p) = build_layout_from_points(&all_points, width, height);

    // 4. Render the Manhattan plot
    let mut renderer = ManhattanRenderer::new(width, height);

    // Draw threshold line
    let threshold_y = y_scale.get_y(-threshold.log10());
    renderer.render_threshold_line(threshold_y, width);

    // Draw all points
    let mut significant_hits: Vec<SignificantHit> = Vec::new();

    for point in &all_points {
        // Normalize contig name (strip "chr" prefix)
        let contig_name = if point.contig.starts_with("chr") {
            &point.contig[3..]
        } else {
            &point.contig
        };

        if let Some(x) = layout.get_x(contig_name, point.position) {
            let y = y_scale.get_y(point.neg_log10_p);
            let color = layout.get_color(contig_name);
            renderer.render_point(x, y, color, 0.6);

            // Track significant hits
            if point.pvalue < threshold {
                significant_hits.push(SignificantHit {
                    variant_id: format!("{}:{}", point.contig, point.position),
                    pvalue: point.pvalue,
                    x_px: x,
                    y_px: y,
                    x_normalized: x / width as f32,
                    y_normalized: y / height as f32,
                    annotations: serde_json::Value::Null,
                });
            }
        }
    }

    // 5. Write output PNG
    let png_path = format!("{}.png", output_prefix);
    let png_data = renderer.encode_png()?;

    if is_cloud_path(&png_path) {
        let mut writer = StreamingCloudWriter::new(&png_path)?;
        writer.write_all(&png_data)?;
        writer.finish()?;
    } else {
        fs::write(&png_path, &png_data)?;
    }
    println!("Wrote PNG: {}", png_path);

    // 6. Write sidecar JSON
    let sidecar = ManhattanSidecar {
        image: SidecarImage { width, height },
        chromosomes: layout
            .chromosome_info
            .iter()
            .map(|c| SidecarChromosome {
                name: c.name.clone(),
                x_start_px: c.x_start_px,
                x_end_px: c.x_end_px,
                color: c.color.clone(),
            })
            .collect(),
        threshold: SidecarThreshold {
            pvalue: threshold,
            y_px: threshold_y,
        },
        y_axis: SidecarYAxis {
            log_threshold: 10.0,
            linear_fraction: 0.5,
            max_neg_log_p,
        },
        significant_hits,
    };

    let json_path = format!("{}.json", output_prefix);
    let json_data = serde_json::to_string_pretty(&sidecar).map_err(|e| {
        crate::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Failed to serialize sidecar: {}", e),
        ))
    })?;

    if is_cloud_path(&json_path) {
        let mut writer = StreamingCloudWriter::new(&json_path)?;
        writer.write_all(json_data.as_bytes())?;
        writer.finish()?;
    } else {
        fs::write(&json_path, &json_data)?;
    }
    println!("Wrote sidecar: {}", json_path);

    println!(
        "Manhattan plot complete: {} significant hits (p < {})",
        sidecar.significant_hits.len(),
        threshold
    );

    Ok(())
}

/// List all part-*.json files in a directory (supports cloud and local).
fn list_shard_files(dir_path: &str) -> Result<Vec<String>> {
    if is_cloud_path(dir_path) {
        // Use gsutil to list files
        let dir = dir_path.trim_end_matches('/');
        let output = std::process::Command::new("gsutil")
            .args(["ls", &format!("{}/part-*.json", dir)])
            .output()
            .map_err(|e| {
                crate::HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to run gsutil: {}", e),
                ))
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(crate::HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("gsutil ls failed: {}", stderr),
            )));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let files: Vec<String> = stdout
            .lines()
            .filter(|l| l.ends_with(".json"))
            .map(|s| s.to_string())
            .collect();

        Ok(files)
    } else {
        // Local directory
        let mut files = Vec::new();
        for entry in fs::read_dir(dir_path)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with("part-") && name.ends_with(".json") {
                    files.push(path.to_string_lossy().to_string());
                }
            }
        }
        files.sort();
        Ok(files)
    }
}

/// Read a single shard file and deserialize to DistributedScanResult.
fn read_shard_file(path: &str) -> Result<DistributedScanResult> {
    let data = if is_cloud_path(path) {
        // Read entire cloud file
        let file_size = get_file_size(path)?;
        range_read(path, 0, file_size as usize)?
    } else {
        fs::read(path)?
    };

    serde_json::from_slice(&data).map_err(|e| {
        crate::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Failed to parse shard {}: {}", path, e),
        ))
    })
}

/// Build chromosome layout and Y scale from aggregated PlotPoints.
fn build_layout_from_points(
    points: &[PlotPoint],
    width: u32,
    height: u32,
) -> (ChromosomeLayout, YScale, f64) {
    use std::collections::HashMap;

    // Collect chromosome extents from the data
    let mut chrom_extents: HashMap<String, (i32, i32)> = HashMap::new();

    for point in points {
        // Normalize contig name
        let contig = if point.contig.starts_with("chr") {
            point.contig[3..].to_string()
        } else {
            point.contig.clone()
        };

        let entry = chrom_extents.entry(contig).or_insert((i32::MAX, i32::MIN));
        entry.0 = entry.0.min(point.position);
        entry.1 = entry.1.max(point.position);
    }

    // Sort chromosomes in standard order
    let mut chroms: Vec<(String, u32)> = chrom_extents
        .into_iter()
        .map(|(name, (_, max))| (name, max as u32))
        .collect();

    chroms.sort_by(|a, b| {
        let a_num: Option<u32> = a.0.parse().ok();
        let b_num: Option<u32> = b.0.parse().ok();
        match (a_num, b_num) {
            (Some(an), Some(bn)) => an.cmp(&bn),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => a.0.cmp(&b.0),
        }
    });

    // Filter to standard chromosomes (1-22, X, Y)
    let chroms: Vec<(String, u32)> = chroms
        .into_iter()
        .filter(|(name, _)| {
            name.len() <= 2
                || name == "X"
                || name == "Y"
                || name == "MT"
                || name.parse::<u32>().is_ok()
        })
        .collect();

    let layout = ChromosomeLayout::new(&chroms, width, 4);

    // Find max -log10(p) for Y scale
    let max_neg_log_p = points
        .iter()
        .map(|p| p.neg_log10_p)
        .fold(0.0_f64, |a, b| a.max(b));

    let y_scale = YScale::new(height, max_neg_log_p.max(10.0));

    (layout, y_scale, max_neg_log_p)
}

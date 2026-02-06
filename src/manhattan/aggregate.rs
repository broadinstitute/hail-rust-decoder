//! Manhattan aggregate phase (Phase 2 of V2 pipeline).
//!
//! This module handles the aggregation of scan phase outputs:
//! 1. Compositing partial PNGs into final Manhattan plots
//! 2. Processing gene burden table
//! 3. Merging significant hits from scan phase
//! 4. Generating locus plots for significant regions
//! 5. Writing manifest.json

use crate::distributed::message::ManhattanAggregateSpec;
use crate::error::Result;
use crate::io::is_cloud_path;
use crate::manhattan::data::{
    LocusDefinitionRow, LocusVariantRow, Manifest, ManifestInputs, ManifestLocus,
    ManifestLocusVariants, ManifestManhattan, ManifestManhattans, ManifestRegion, ManifestSigHits,
    ManifestSignificantHits, ManifestStats, ManifestTopHit,
};
use crate::manhattan::genes::{render_gene_manhattan, scan_gene_burden_to_parquet, scan_qq_to_parquet};
use crate::manhattan::layout::ChromosomeLayout;
use crate::manhattan::loci_writer::{LocusDefinitionWriter, LocusVariantWriter};
use crate::manhattan::locus::{LocusPlotConfig, LocusRenderer, RenderVariant};
use crate::manhattan::data::VariantSource;
use crate::manhattan::reference::calculate_xpos;
use crate::query::{IntervalList, QueryEngine};
use arrow::array::{Array, Float64Array, Int32Array, StringArray};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;
use std::time::Instant;

/// Generate locus plots from an existing Manhattan output directory (standalone CLI command).
///
/// This allows generating locus plots after the scan/merge phase has completed,
/// without re-running the full pipeline.
pub fn generate_loci_standalone(
    output_dir: &str,
    exome_table: Option<&str>,
    genome_table: Option<&str>,
    gene_burden_table: Option<&str>,
    locus_window: i32,
    threshold: f64,
    gene_threshold: f64,
    num_threads: usize,
) -> Result<Vec<ManifestLocus>> {
    let output_base = output_dir.trim_end_matches('/');

    // Extract phenotype from path; use default ancestry
    let phenotype = extract_phenotype_name(output_base);
    let ancestry = "meta".to_string();

    let loci = generate_loci_from_parquet(
        output_base,
        exome_table,
        genome_table,
        gene_burden_table,
        locus_window,
        threshold,
        gene_threshold,
        num_threads,
        &phenotype,
        &ancestry,
    )?;

    // Update manifest.json with loci info
    update_manifest_with_loci(output_base, &loci)?;

    Ok(loci)
}

/// Update the manifest.json file with loci information.
fn update_manifest_with_loci(output_base: &str, loci: &[ManifestLocus]) -> Result<()> {
    use crate::io::is_cloud_path;

    let manifest_path = format!("{}/manifest.json", output_base);

    // Read existing manifest
    let manifest_content = if is_cloud_path(&manifest_path) {
        use crate::io::{get_file_size, range_read};
        let size = get_file_size(&manifest_path)?;
        let data = range_read(&manifest_path, 0, size as usize)?;
        String::from_utf8(data).map_err(|e| crate::HailError::InvalidFormat(e.to_string()))?
    } else {
        std::fs::read_to_string(&manifest_path)?
    };

    // Parse and update
    let mut manifest: serde_json::Value = serde_json::from_str(&manifest_content)?;

    manifest["loci"] = serde_json::to_value(loci)?;
    manifest["stats"]["total_loci"] = serde_json::json!(loci.len());

    // Write back
    let updated = serde_json::to_string_pretty(&manifest)?;

    if is_cloud_path(&manifest_path) {
        use crate::io::CloudWriter;
        use std::io::Write;
        let mut writer = CloudWriter::new(&manifest_path)?;
        writer.write_all(updated.as_bytes())?;
        writer.finish()?;
    } else {
        std::fs::write(&manifest_path, &updated)?;
    }

    println!("  Updated manifest.json with {} loci", loci.len());
    Ok(())
}

/// Run the Manhattan aggregation phase.
///
/// This is called by the worker when assigned a ManhattanAggregate job.
/// Returns (row_count, summary_json) where summary_json contains stats about the aggregation.
pub fn run_aggregation(spec: &ManhattanAggregateSpec) -> Result<(usize, serde_json::Value)> {
    use crate::io::is_cloud_path;

    let start = Instant::now();
    let scan_duration = 0.0; // We don't know the scan duration here

    println!("Starting Manhattan aggregation phase...");

    let output_base = spec.output_path.trim_end_matches('/');

    // Step 1: Composite PNGs
    println!("  Compositing partial PNGs...");
    let exome_count = if spec.exome_results.is_some() {
        composite_source_pngs(output_base, "exome", spec.width, spec.height)?
    } else {
        0
    };

    let genome_count = if spec.genome_results.is_some() {
        composite_source_pngs(output_base, "genome", spec.width, spec.height)?
    } else {
        0
    };

    // Step 2: Process gene burden (if provided)
    let (gene_count, _gene_sig_regions) = if let Some(ref gene_burden_path) = spec.gene_burden {
        println!("  Processing gene burden table...");

        let phenotype = extract_phenotype_name(output_base);
        let ancestry = "meta";

        // Export to Parquet
        let parquet_path = format!("{}/gene_associations.parquet", output_base);
        let scan_result = scan_gene_burden_to_parquet(
            gene_burden_path,
            &phenotype,
            ancestry,
            &parquet_path,
            spec.gene_threshold,
            None, // No MAF filter during aggregation
        )?;

        println!(
            "    Exported {} gene rows, {} significant genes",
            scan_result.total_rows,
            scan_result.significant_genes.len()
        );

        // Write significant genes JSON
        let genes_json = serde_json::to_string_pretty(&scan_result.significant_genes)?;
        let genes_path = format!("{}/significant_genes.json", output_base);
        if is_cloud_path(&genes_path) {
            use crate::io::CloudWriter;
            use std::io::Write;
            let mut writer = CloudWriter::new(&genes_path)?;
            writer.write_all(genes_json.as_bytes())?;
            writer.finish()?;
        } else {
            std::fs::write(&genes_path, &genes_json)?;
        }

        // Build layout from reference genome (GRCh38)
        let contigs = crate::manhattan::reference::get_contig_lengths(
            // Dummy engine - we just need the default contig lengths
            &QueryEngine::open_path(gene_burden_path)?,
        );
        let layout = ChromosomeLayout::new(&contigs, spec.width, 4);

        // Render gene Manhattan plot
        if !scan_result.plot_points.is_empty() {
            let gene_png = render_gene_manhattan(
                &scan_result.plot_points,
                spec.width,
                spec.height,
                spec.gene_threshold,
                &layout,
            )?;

            let png_path = format!("{}/gene_manhattan.png", output_base);
            if is_cloud_path(&png_path) {
                use crate::io::CloudWriter;
                use std::io::Write;
                let mut writer = CloudWriter::new(&png_path)?;
                writer.write_all(&gene_png)?;
                writer.finish()?;
            } else {
                std::fs::write(&png_path, &gene_png)?;
            }
        }

        // Collect significant gene regions for locus plots
        let mut gene_regions = IntervalList::new();
        for gene in &scan_result.significant_genes {
            gene_regions.add(
                gene.interval.0.clone(),
                gene.interval.1,
                gene.interval.2,
            );
        }

        (scan_result.total_rows as u64, gene_regions)
    } else {
        (0u64, IntervalList::new())
    };

    // Step 2b: Process QQ tables (expected p-values for QQ plots)
    let phenotype = extract_phenotype_name(output_base);
    let ancestry = "meta"; // Default ancestry

    let mut qq_stats_map = serde_json::Map::new();

    if let Some(ref exome_exp_p_path) = spec.exome_exp_p {
        println!("  Processing exome QQ table...");
        let parquet_path = format!("{}/qq_exome.parquet", output_base);
        match scan_qq_to_parquet(
            exome_exp_p_path,
            &phenotype,
            ancestry,
            "exomes",
            &parquet_path,
        ) {
            Ok(result) => {
                println!("    Exported {} QQ points for exome", result.total_rows);
                qq_stats_map.insert("exome".to_string(), serde_json::to_value(&result.stats).unwrap_or_default());
            }
            Err(e) => {
                eprintln!("    Warning: Failed to process exome QQ table: {}", e);
            }
        }
    }

    if let Some(ref genome_exp_p_path) = spec.genome_exp_p {
        println!("  Processing genome QQ table...");
        let parquet_path = format!("{}/qq_genome.parquet", output_base);
        match scan_qq_to_parquet(
            genome_exp_p_path,
            &phenotype,
            ancestry,
            "genomes",
            &parquet_path,
        ) {
            Ok(result) => {
                println!("    Exported {} QQ points for genome", result.total_rows);
                qq_stats_map.insert("genome".to_string(), serde_json::to_value(&result.stats).unwrap_or_default());
            }
            Err(e) => {
                eprintln!("    Warning: Failed to process genome QQ table: {}", e);
            }
        }
    }

    // Write QQ stats JSON if we have any
    if !qq_stats_map.is_empty() {
        let qq_stats_json = serde_json::to_string_pretty(&qq_stats_map)?;
        let stats_path = format!("{}/qq_stats.json", output_base);
        if is_cloud_path(&stats_path) {
            use crate::io::CloudWriter;
            use std::io::Write;
            let mut writer = CloudWriter::new(&stats_path)?;
            writer.write_all(qq_stats_json.as_bytes())?;
            writer.finish()?;
        } else {
            std::fs::write(&stats_path, &qq_stats_json)?;
        }
    }

    // Step 3: Merge significant hits (combined exome + genome into one file)
    println!("  Merging significant hits (combined exome + genome)...");
    let has_exome = spec.exome_results.is_some();
    let has_genome = spec.genome_results.is_some();
    let (combined_sig_count, _combined_top_hit) =
        merge_and_combine_hits(output_base, has_exome, has_genome)?;

    // For backward compatibility, also generate per-source files
    let (exome_sig_count, exome_top_hit) = if has_exome {
        merge_significant_hits(output_base, "exome")?
    } else {
        (0, None)
    };

    let (genome_sig_count, genome_top_hit) = if has_genome {
        merge_significant_hits(output_base, "genome")?
    } else {
        (0, None)
    };

    // Extract phenotype and ancestry from spec or path
    let phenotype = extract_phenotype_name(output_base);
    let ancestry = "meta".to_string(); // Default ancestry; could be extracted from spec if available

    // Step 4: Compute locus regions and generate plots (if enabled)
    let loci = if spec.locus_plots {
        println!("  Generating locus plots...");
        generate_locus_plots(spec, output_base, &phenotype, &ancestry)?
    } else {
        vec![]
    };

    println!(
        "  Combined significant hits: {} total, {} exome, {} genome",
        combined_sig_count, exome_sig_count, genome_sig_count
    );

    // Step 5: Write manifest.json
    println!("  Writing manifest.json...");
    let aggregate_duration = start.elapsed().as_secs_f64();

    let manifest = Manifest {
        phenotype: phenotype.clone(),
        ancestry: Some(ancestry.clone()),
        created_at: chrono_now_iso(),
        inputs: ManifestInputs {
            exome_results: spec.exome_results.clone(),
            genome_results: spec.genome_results.clone(),
            gene_burden: spec.gene_burden.clone(),
        },
        manhattans: ManifestManhattans {
            exome: if spec.exome_results.is_some() {
                Some(ManifestManhattan {
                    png: format!("{}/exome_manhattan.png", output_base),
                    count: exome_count,
                })
            } else {
                None
            },
            genome: if spec.genome_results.is_some() {
                Some(ManifestManhattan {
                    png: format!("{}/genome_manhattan.png", output_base),
                    count: genome_count,
                })
            } else {
                None
            },
            gene: if spec.gene_burden.is_some() {
                Some(ManifestManhattan {
                    png: format!("{}/gene_manhattan.png", output_base),
                    count: gene_count,
                })
            } else {
                None
            },
        },
        significant_hits: ManifestSignificantHits {
            // Consolidated output
            exome: if spec.exome_results.is_some() {
                Some(ManifestSigHits {
                    path: format!("{}/significant.parquet", output_base),
                    count: exome_sig_count,
                    top_hit: exome_top_hit,
                })
            } else {
                None
            },
            genome: if spec.genome_results.is_some() {
                Some(ManifestSigHits {
                    path: format!("{}/significant.parquet", output_base),
                    count: genome_sig_count,
                    top_hit: genome_top_hit,
                })
            } else {
                None
            },
            gene: None,
        },
        loci: loci.clone(),
        stats: ManifestStats {
            scan_duration_sec: scan_duration,
            aggregate_duration_sec: aggregate_duration,
            total_loci: loci.len(),
        },
    };

    let manifest_path = format!("{}/manifest.json", output_base);
    let manifest_json = serde_json::to_string_pretty(&manifest)?;

    if is_cloud_path(&manifest_path) {
        use crate::io::CloudWriter;
        use std::io::Write;
        let mut writer = CloudWriter::new(&manifest_path)?;
        writer.write_all(manifest_json.as_bytes())?;
        writer.finish()?;
    } else {
        std::fs::write(&manifest_path, &manifest_json)?;
    }

    // Step 6: Cleanup intermediate files (if requested)
    if spec.cleanup {
        println!("  Cleaning up intermediate files...");
        cleanup_intermediates(output_base)?;
    }

    println!(
        "Manhattan aggregation complete in {:.1}s",
        aggregate_duration
    );

    // Build summary for return
    let summary = serde_json::json!({
        "phenotype": extract_phenotype_name(output_base),
        "exome_sig_count": exome_sig_count,
        "genome_sig_count": genome_sig_count,
        "total_loci": loci.len(),
        "aggregate_duration_sec": aggregate_duration,
    });

    Ok(((exome_count + genome_count) as usize, summary))
}

/// Composite partial PNGs for a source (exome or genome).
fn composite_source_pngs(output_base: &str, source: &str, width: u32, height: u32) -> Result<u64> {
    use crate::manhattan::pipeline::composite_partial_pngs;

    let parts_dir = format!("{}/{}", output_base, source);
    let output_path = format!("{}/{}_manhattan.png", output_base, source);

    // Use existing composite function
    // Note: threshold is not used for compositing, pass 0.0
    composite_partial_pngs(&parts_dir, &output_path, width, height, 0.0)?;

    // Count total variants by counting files (rough estimate)
    // TODO: Track actual counts during scan phase
    Ok(0)
}

/// Merge significant hits from scan phase parquet files.
///
/// Optimized approach:
/// - Parallel file reads with rayon
/// - Find top hit by scanning for min pvalue (no full sort needed)
/// - Concatenate batches without sorting for output (partitions are already sorted)
fn merge_significant_hits(
    output_base: &str,
    source: &str,
) -> Result<(u64, Option<ManifestTopHit>)> {
    use crate::io::is_cloud_path;
    use rayon::prelude::*;
    use std::path::Path;
    use std::time::Instant;

    let sig_dir = format!("{}/{}", output_base, source);
    let output_file = format!("{}/{}_significant.parquet", output_base, source);

    // Collect all sig parquet files
    let sig_files = if is_cloud_path(&sig_dir) {
        list_cloud_parquet_files(&sig_dir, "-sig.parquet")?
    } else {
        let path = Path::new(&sig_dir);
        if !path.exists() {
            return Ok((0, None));
        }
        list_local_parquet_files(&sig_dir, "-sig.parquet")?
    };

    if sig_files.is_empty() {
        return Ok((0, None));
    }

    let start = Instant::now();
    println!("    Reading {} sig.parquet files in parallel...", sig_files.len());

    // Read all parquet files in parallel
    let results: Vec<Result<(Vec<RecordBatch>, Option<TopHitCandidate>)>> = sig_files
        .par_iter()
        .map(|file_path| {
            let batches = read_parquet_file(file_path)?;
            // Find top hit candidate in this file while we have it in memory
            let top_candidate = find_top_hit_in_batches(&batches);
            Ok((batches, top_candidate))
        })
        .collect();

    // Collect batches and find global top hit
    let mut all_batches: Vec<RecordBatch> = Vec::new();
    let mut schema = None;
    let mut global_top: Option<TopHitCandidate> = None;

    for result in results {
        let (batches, top_candidate) = result?;
        for batch in batches {
            if schema.is_none() {
                schema = Some(batch.schema());
            }
            all_batches.push(batch);
        }
        // Update global top hit if this file has a better one
        if let Some(candidate) = top_candidate {
            global_top = Some(match global_top {
                None => candidate,
                Some(current) if candidate.pvalue < current.pvalue => candidate,
                Some(current) => current,
            });
        }
    }

    let read_time = start.elapsed();
    println!("    Read {} batches in {:.1}s", all_batches.len(), read_time.as_secs_f64());

    if all_batches.is_empty() {
        return Ok((0, None));
    }

    let schema = schema.unwrap();
    let total_count: u64 = all_batches.iter().map(|b| b.num_rows() as u64).sum();

    if total_count == 0 {
        return Ok((0, None));
    }

    // Write concatenated output (no sorting - files are already partition-sorted)
    let write_start = Instant::now();
    write_parquet_batches(&output_file, &schema, &all_batches)?;
    println!("    Wrote {} rows in {:.1}s", total_count, write_start.elapsed().as_secs_f64());

    // Convert top candidate to ManifestTopHit
    let top_hit = global_top.map(|c| ManifestTopHit {
        id: format!("{}:{}:{}:{}", c.contig, c.position, c.ref_allele, c.alt_allele),
        pvalue: c.pvalue,
        gene: None,
        consequence: None,
    });

    Ok((total_count, top_hit))
}

/// Merge and combine significant hits from both exome and genome into a single file.
///
/// This function reads all `*-sig.parquet` files from both the `exome/` and `genome/`
/// directories and writes them to a single `significant.parquet` at the output root.
/// Since the scan phase now includes `sequencing_type` in the output, we can safely
/// merge them into one file.
fn merge_and_combine_hits(
    output_base: &str,
    has_exome: bool,
    has_genome: bool,
) -> Result<(u64, Option<ManifestTopHit>)> {
    use crate::io::is_cloud_path;
    use rayon::prelude::*;
    use std::path::Path;
    use std::time::Instant;

    let output_file = format!("{}/significant.parquet", output_base);

    // Collect all sig parquet files from both sources
    let mut sig_files: Vec<String> = Vec::new();

    if has_exome {
        let exome_dir = format!("{}/exome", output_base);
        if is_cloud_path(&exome_dir) {
            if let Ok(files) = list_cloud_parquet_files(&exome_dir, "-sig.parquet") {
                sig_files.extend(files);
            }
        } else if Path::new(&exome_dir).exists() {
            if let Ok(files) = list_local_parquet_files(&exome_dir, "-sig.parquet") {
                sig_files.extend(files);
            }
        }
    }

    if has_genome {
        let genome_dir = format!("{}/genome", output_base);
        if is_cloud_path(&genome_dir) {
            if let Ok(files) = list_cloud_parquet_files(&genome_dir, "-sig.parquet") {
                sig_files.extend(files);
            }
        } else if Path::new(&genome_dir).exists() {
            if let Ok(files) = list_local_parquet_files(&genome_dir, "-sig.parquet") {
                sig_files.extend(files);
            }
        }
    }

    if sig_files.is_empty() {
        return Ok((0, None));
    }

    let start = Instant::now();
    println!(
        "    Reading {} sig.parquet files from exome+genome in parallel...",
        sig_files.len()
    );

    // Read all parquet files in parallel
    let results: Vec<Result<(Vec<RecordBatch>, Option<TopHitCandidate>)>> = sig_files
        .par_iter()
        .map(|file_path| {
            let batches = read_parquet_file(file_path)?;
            // Find top hit candidate in this file while we have it in memory
            let top_candidate = find_top_hit_in_batches(&batches);
            Ok((batches, top_candidate))
        })
        .collect();

    // Collect batches and find global top hit
    let mut all_batches: Vec<RecordBatch> = Vec::new();
    let mut schema = None;
    let mut global_top: Option<TopHitCandidate> = None;

    for result in results {
        let (batches, top_candidate) = result?;
        for batch in batches {
            if schema.is_none() {
                schema = Some(batch.schema());
            }
            all_batches.push(batch);
        }
        // Update global top hit if this file has a better one
        if let Some(candidate) = top_candidate {
            global_top = Some(match global_top {
                None => candidate,
                Some(current) if candidate.pvalue < current.pvalue => candidate,
                Some(current) => current,
            });
        }
    }

    let read_time = start.elapsed();
    println!(
        "    Read {} batches in {:.1}s",
        all_batches.len(),
        read_time.as_secs_f64()
    );

    if all_batches.is_empty() {
        return Ok((0, None));
    }

    let schema = schema.unwrap();
    let total_count: u64 = all_batches.iter().map(|b| b.num_rows() as u64).sum();

    if total_count == 0 {
        return Ok((0, None));
    }

    // Write concatenated output (no sorting - files are already partition-sorted)
    let write_start = Instant::now();
    write_parquet_batches(&output_file, &schema, &all_batches)?;
    println!(
        "    Wrote {} rows to significant.parquet in {:.1}s",
        total_count,
        write_start.elapsed().as_secs_f64()
    );

    // Convert top candidate to ManifestTopHit
    let top_hit = global_top.map(|c| ManifestTopHit {
        id: format!("{}:{}:{}:{}", c.contig, c.position, c.ref_allele, c.alt_allele),
        pvalue: c.pvalue,
        gene: None,
        consequence: None,
    });

    Ok((total_count, top_hit))
}

/// Candidate for top hit found during parallel scan.
#[derive(Clone)]
struct TopHitCandidate {
    contig: String,
    position: i32,
    ref_allele: String,
    alt_allele: String,
    pvalue: f64,
}

/// Find the top hit (lowest pvalue) in a set of batches.
fn find_top_hit_in_batches(batches: &[RecordBatch]) -> Option<TopHitCandidate> {
    let mut best: Option<TopHitCandidate> = None;

    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }

        let schema = batch.schema();

        // Get column indices
        let pvalue_idx = schema.fields().iter().position(|f| f.name() == "pvalue")?;
        let contig_idx = schema.fields().iter().position(|f| f.name() == "contig")?;
        let position_idx = schema.fields().iter().position(|f| f.name() == "position")?;
        let ref_idx = schema.fields().iter().position(|f| f.name() == "ref")?;
        let alt_idx = schema.fields().iter().position(|f| f.name() == "alt")?;

        let pvalue_col = batch.column(pvalue_idx).as_any().downcast_ref::<Float64Array>()?;
        let contig_col = batch.column(contig_idx).as_any().downcast_ref::<StringArray>()?;
        let position_col = batch.column(position_idx).as_any().downcast_ref::<arrow::array::Int32Array>()?;
        let ref_col = batch.column(ref_idx).as_any().downcast_ref::<StringArray>()?;
        let alt_col = batch.column(alt_idx).as_any().downcast_ref::<StringArray>()?;

        for i in 0..batch.num_rows() {
            if pvalue_col.is_null(i) {
                continue;
            }
            let pvalue = pvalue_col.value(i);

            let dominated = best.as_ref().map(|b| pvalue >= b.pvalue).unwrap_or(false);
            if dominated {
                continue;
            }

            best = Some(TopHitCandidate {
                contig: contig_col.value(i).to_string(),
                position: position_col.value(i),
                ref_allele: ref_col.value(i).to_string(),
                alt_allele: alt_col.value(i).to_string(),
                pvalue,
            });
        }
    }

    best
}

/// List parquet files matching a suffix in a local directory.
fn list_local_parquet_files(dir: &str, suffix: &str) -> Result<Vec<String>> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.ends_with(suffix) {
                files.push(path.to_string_lossy().to_string());
            }
        }
    }
    files.sort();
    Ok(files)
}

/// List parquet files matching a suffix in a cloud directory.
fn list_cloud_parquet_files(dir: &str, suffix: &str) -> Result<Vec<String>> {
    use crate::HailError;
    use object_store::path::Path as ObjPath;
    use object_store::ObjectStore;
    use url::Url;

    let url = Url::parse(dir)
        .map_err(|e| HailError::InvalidFormat(format!("Invalid URL: {}", e)))?;

    let (store, prefix, base_url): (Arc<dyn object_store::ObjectStore>, ObjPath, String) = match url.scheme() {
        #[cfg(feature = "gcp")]
        "gs" => {
            let bucket = url.host_str()
                .ok_or_else(|| HailError::InvalidFormat("Missing bucket in GCS URL".to_string()))?;
            let path = url.path().trim_start_matches('/');
            (crate::io::get_gcs_client(bucket)?, ObjPath::from(path), format!("gs://{}/", bucket))
        }
        #[cfg(feature = "aws")]
        "s3" => {
            let bucket = url.host_str()
                .ok_or_else(|| HailError::InvalidFormat("Missing bucket in S3 URL".to_string()))?;
            let path = url.path().trim_start_matches('/');
            let s3 = object_store::aws::AmazonS3Builder::new()
                .with_bucket_name(bucket)
                .build()
                .map_err(|e| HailError::InvalidFormat(format!("Failed to create S3 client: {}", e)))?;
            (Arc::new(s3), ObjPath::from(path), format!("s3://{}/", bucket))
        }
        scheme => {
            return Err(HailError::InvalidFormat(format!("Unsupported URL scheme: {}", scheme)));
        }
    };

    // Use blocking runtime for object_store async operations
    let rt = tokio::runtime::Runtime::new()?;
    let list_result = rt.block_on(async {
        let mut files = Vec::new();
        let stream = store.list(Some(&prefix));
        use futures::StreamExt;
        let results: Vec<_> = stream.collect().await;
        for result in results {
            if let Ok(meta) = result {
                let path = meta.location.to_string();
                if path.ends_with(suffix) {
                    // Reconstruct full URL
                    let full_path = format!("{}{}", base_url, path);
                    files.push(full_path);
                }
            }
        }
        files
    });

    let mut files = list_result;
    files.sort();
    Ok(files)
}

/// Read all record batches from a parquet file.
fn read_parquet_file(path: &str) -> Result<Vec<RecordBatch>> {
    use crate::io::is_cloud_path;

    if is_cloud_path(path) {
        read_cloud_parquet_file(path)
    } else {
        read_local_parquet_file(path)
    }
}

/// Read parquet from local filesystem.
fn read_local_parquet_file(path: &str) -> Result<Vec<RecordBatch>> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let batches: Vec<RecordBatch> = reader.collect::<std::result::Result<_, _>>()?;
    Ok(batches)
}

/// Read parquet from cloud storage.
fn read_cloud_parquet_file(path: &str) -> Result<Vec<RecordBatch>> {
    use crate::io::{get_file_size, range_read};

    // Download entire file to memory (sig.parquet files are small)
    let file_size = get_file_size(path)?;
    let data = range_read(path, 0, file_size as usize)?;

    // bytes::Bytes implements ChunkReader
    let bytes = bytes::Bytes::from(data);
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
    let reader = builder.build()?;

    let batches: Vec<RecordBatch> = reader.collect::<std::result::Result<_, _>>()?;
    Ok(batches)
}

/// Sort record batches by pvalue column.
fn sort_batches_by_pvalue(batches: &[RecordBatch]) -> Result<Vec<RecordBatch>> {
    use arrow::compute::concat_batches;
    use arrow::compute::sort_to_indices;
    use arrow::compute::take;

    if batches.is_empty() {
        return Ok(vec![]);
    }

    let schema = batches[0].schema();

    // Concatenate all batches
    let combined = concat_batches(&schema, batches)?;

    // Get pvalue column index
    let pvalue_idx = schema
        .fields()
        .iter()
        .position(|f| f.name() == "pvalue")
        .ok_or_else(|| crate::HailError::InvalidFormat("Missing pvalue column".into()))?;

    let pvalue_col = combined.column(pvalue_idx);

    // Sort indices by pvalue (ascending)
    let sort_options = arrow::compute::SortOptions {
        descending: false,
        nulls_first: false,
    };
    let indices = sort_to_indices(pvalue_col, Some(sort_options), None)?;

    // Apply sort to all columns
    let sorted_columns: Vec<Arc<dyn Array>> = combined
        .columns()
        .iter()
        .map(|col| take(col.as_ref(), &indices, None).map(Arc::from))
        .collect::<std::result::Result<_, _>>()?;

    let sorted_batch = RecordBatch::try_new(schema, sorted_columns)?;
    Ok(vec![sorted_batch])
}

/// Write record batches to a parquet file.
fn write_parquet_batches(
    path: &str,
    schema: &Arc<arrow::datatypes::Schema>,
    batches: &[RecordBatch],
) -> Result<()> {
    use crate::io::is_cloud_path;

    if is_cloud_path(path) {
        write_cloud_parquet_batches(path, schema, batches)
    } else {
        write_local_parquet_batches(path, schema, batches)
    }
}

/// Write parquet to local filesystem.
fn write_local_parquet_batches(
    path: &str,
    schema: &Arc<arrow::datatypes::Schema>,
    batches: &[RecordBatch],
) -> Result<()> {
    let file = File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
        .build();

    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

    for batch in batches {
        writer.write(batch)?;
    }

    writer.close()?;
    Ok(())
}

/// Write parquet to cloud storage.
fn write_cloud_parquet_batches(
    path: &str,
    schema: &Arc<arrow::datatypes::Schema>,
    batches: &[RecordBatch],
) -> Result<()> {
    use crate::io::CloudWriter;

    let cloud_writer = CloudWriter::new(path)?;
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
        .build();

    let mut writer = ArrowWriter::try_new(cloud_writer, schema.clone(), Some(props))?;

    for batch in batches {
        writer.write(batch)?;
    }

    let cloud_writer = writer.into_inner()?;
    cloud_writer.finish()?;
    Ok(())
}

/// Extract top hit from sorted batches.
fn extract_top_hit(batches: &[RecordBatch]) -> Option<ManifestTopHit> {
    if batches.is_empty() {
        return None;
    }

    let batch = &batches[0];
    if batch.num_rows() == 0 {
        return None;
    }

    // Get columns by name
    let schema = batch.schema();
    let get_string = |name: &str| -> Option<String> {
        let idx = schema.fields().iter().position(|f| f.name() == name)?;
        let col = batch.column(idx);
        let arr = col.as_any().downcast_ref::<StringArray>()?;
        if arr.is_null(0) {
            None
        } else {
            Some(arr.value(0).to_string())
        }
    };

    let get_f64 = |name: &str| -> Option<f64> {
        let idx = schema.fields().iter().position(|f| f.name() == name)?;
        let col = batch.column(idx);
        let arr = col.as_any().downcast_ref::<Float64Array>()?;
        if arr.is_null(0) {
            None
        } else {
            Some(arr.value(0))
        }
    };

    let contig = get_string("contig")?;
    let position = {
        let idx = schema.fields().iter().position(|f| f.name() == "position")?;
        let col = batch.column(idx);
        let arr = col
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()?;
        arr.value(0)
    };
    let ref_allele = get_string("ref")?;
    let alt_allele = get_string("alt")?;
    let pvalue = get_f64("pvalue")?;

    Some(ManifestTopHit {
        id: format!("{}:{}:{}:{}", contig, position, ref_allele, alt_allele),
        pvalue,
        gene: None,
        consequence: None,
    })
}

/// Clean up intermediate partition files.
fn cleanup_intermediates(output_base: &str) -> Result<()> {
    use crate::io::is_cloud_path;

    // Delete exome/part-*.png and exome/part-*-sig.parquet
    // Delete genome/part-*.png and genome/part-*-sig.parquet

    if is_cloud_path(output_base) {
        // For cloud, we'd need to list and delete
        // TODO: Implement cloud cleanup
        println!("    Cloud cleanup not yet implemented");
    } else {
        for source in &["exome", "genome"] {
            let dir = format!("{}/{}", output_base, source);
            if std::path::Path::new(&dir).exists() {
                std::fs::remove_dir_all(&dir)?;
            }
        }
    }

    Ok(())
}

/// Extract phenotype name from output path.
fn extract_phenotype_name(output_path: &str) -> String {
    output_path
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or("unknown")
        .to_string()
}

/// Get current timestamp in ISO format.
fn chrono_now_iso() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = duration.as_secs();
    // Simple ISO format without chrono dependency
    format!("{}", secs) // TODO: Proper ISO format
}

// =============================================================================
// Locus Plot Generation
// =============================================================================

/// A significant hit position extracted from merged parquet.
#[derive(Debug, Clone)]
pub struct SigPosition {
    pub contig: String,
    pub position: i32,
    pub pvalue: f64,
    pub source: String, // "exome" or "genome"
}

/// Generate locus plots for significant regions (called from aggregation phase).
fn generate_locus_plots(
    spec: &ManhattanAggregateSpec,
    output_base: &str,
    phenotype: &str,
    ancestry: &str,
) -> Result<Vec<ManifestLocus>> {
    generate_loci_from_parquet(
        output_base,
        spec.exome_results.as_deref(),
        spec.genome_results.as_deref(),
        spec.gene_burden.as_deref(),
        spec.locus_window,
        spec.threshold,
        spec.gene_threshold,
        8, // Default thread count for aggregation
        phenotype,
        ancestry,
    )
}

/// Core locus generation logic shared by aggregation and standalone CLI.
///
/// Now writes consolidated loci.parquet and loci_variants.parquet files.
fn generate_loci_from_parquet(
    output_base: &str,
    exome_table: Option<&str>,
    genome_table: Option<&str>,
    gene_burden_table: Option<&str>,
    locus_window: i32,
    threshold: f64,
    gene_threshold: f64,
    num_threads: usize,
    phenotype: &str,
    ancestry: &str,
) -> Result<Vec<ManifestLocus>> {
    use crate::io::is_cloud_path;
    use crate::manhattan::genes::process_complex_gene_burden;
    use rayon::prelude::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Step 1: Extract significant positions from merged parquet file
    println!("    Extracting significant positions...");
    let sig_path = format!("{}/significant.parquet", output_base);

    let mut sig_positions = if std::path::Path::new(&sig_path).exists() || is_cloud_path(&sig_path) {
        let positions = extract_sig_positions(&sig_path)?;
        println!("      Found {} significant hits from significant.parquet", positions.len());
        positions
    } else {
        println!("      No significant.parquet found, skipping variant hits");
        Vec::new()
    };

    // Step 1b: Extract significant genes from gene burden table
    let mut gene_regions: Vec<(String, i32, i32)> = Vec::new();
    if let Some(gene_burden_path) = gene_burden_table {
        println!("    Processing gene burden table for significant genes...");
        match process_complex_gene_burden(gene_burden_path, gene_threshold) {
            Ok((sig_genes, _intervals)) => {
                println!("      Found {} significant genes", sig_genes.len());
                for gene in &sig_genes {
                    let (chrom, start, end) = &gene.interval;
                    // Add gene as a "position" for region computation
                    // Use the gene midpoint as the position, but we'll handle the full span in region computation
                    sig_positions.push(SigPosition {
                        contig: chrom.clone(),
                        position: (start + end) / 2, // midpoint
                        pvalue: gene.best_pvalue,
                        source: "gene".to_string(),
                    });
                    // Also track the full gene bounds for proper region expansion
                    gene_regions.push((chrom.clone(), *start, *end));
                }
            }
            Err(e) => {
                eprintln!("      Warning: failed to process gene burden: {}", e);
            }
        }
    }

    if sig_positions.is_empty() {
        println!("    No significant hits found, skipping locus plots");
        return Ok(vec![]);
    }

    // Step 2: Compute locus regions (union + merge overlapping)
    println!("    Computing locus regions (window: {}bp)...", locus_window);
    let regions = compute_locus_regions_with_genes(&sig_positions, &gene_regions, locus_window);
    println!("      Found {} merged locus regions", regions.len());

    if regions.is_empty() {
        return Ok(vec![]);
    }

    // Step 3: Create loci directory (for local paths)
    let loci_dir = format!("{}/loci", output_base);
    if !is_cloud_path(&loci_dir) {
        std::fs::create_dir_all(&loci_dir)?;
    }

    // Step 4: Generate plots for each region in parallel
    let completed = AtomicUsize::new(0);
    let total_regions = regions.len();

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build()
        .unwrap_or_else(|_| rayon::ThreadPoolBuilder::new().build().unwrap());

    println!(
        "    Generating {} locus plots ({} threads)...",
        total_regions, num_threads
    );

    // Generate loci in parallel, collecting rows for parquet output
    let results: Vec<
        Result<Option<(ManifestLocus, LocusDefinitionRow, Vec<LocusVariantRow>)>>,
    > = pool.install(|| {
        regions
            .par_iter()
            .map(|(contig, start, end)| {
                let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
                if done == 1 || done % 20 == 0 || done == total_regions {
                    println!(
                        "      Progress: {}/{} - {}:{}-{}",
                        done, total_regions, contig, start, end
                    );
                }

                generate_single_locus_core(
                    &loci_dir,
                    &sig_positions,
                    exome_table,
                    genome_table,
                    contig,
                    *start,
                    *end,
                    threshold,
                    phenotype,
                    ancestry,
                )
            })
            .collect()
    });

    // Collect successful results
    let mut manifest_loci = Vec::new();
    let mut locus_definitions: Vec<LocusDefinitionRow> = Vec::new();
    let mut locus_variants: Vec<LocusVariantRow> = Vec::new();
    let mut errors = 0;

    for result in results {
        match result {
            Ok(Some((locus, def_row, var_rows))) => {
                manifest_loci.push(locus);
                locus_definitions.push(def_row);
                locus_variants.extend(var_rows);
            }
            Ok(None) => {}
            Err(e) => {
                errors += 1;
                eprintln!("      Warning: locus generation failed: {}", e);
            }
        }
    }

    if errors > 0 {
        println!("      Completed with {} errors", errors);
    }

    // Write loci.parquet and loci_variants.parquet
    if !locus_definitions.is_empty() {
        println!(
            "    Writing {} locus definitions to loci.parquet...",
            locus_definitions.len()
        );
        write_loci_parquet(output_base, &locus_definitions, &locus_variants)?;
    }

    // Sort by chromosome and position for consistent output
    manifest_loci.sort_by(|a, b| {
        let chr_a = parse_chrom_order(&a.region.contig);
        let chr_b = parse_chrom_order(&b.region.contig);
        chr_a.cmp(&chr_b).then(a.region.start.cmp(&b.region.start))
    });

    Ok(manifest_loci)
}

/// Generate a single locus plot and its associated data.
///
/// Returns a tuple of (ManifestLocus, LocusDefinitionRow, Vec<LocusVariantRow>)
/// for use in manifest.json and consolidated parquet output.
fn generate_single_locus_core(
    loci_dir: &str,
    sig_positions: &[SigPosition],
    exome_table: Option<&str>,
    genome_table: Option<&str>,
    contig: &str,
    start: i32,
    end: i32,
    threshold: f64,
    phenotype: &str,
    ancestry: &str,
) -> Result<Option<(ManifestLocus, LocusDefinitionRow, Vec<LocusVariantRow>)>> {
    let region_id = format!("{}_{}_{}",
        contig.replace("chr", ""),
        start,
        end
    );

    // Find lead variant in this region
    let lead = find_lead_variant(sig_positions, contig, start, end);

    // Read variants from original Hail tables for this region
    let exome_variants = if let Some(table_path) = exome_table {
        read_locus_variants(table_path, contig, start, end, VariantSource::Exome)
            .unwrap_or_default()
    } else {
        vec![]
    };

    let genome_variants = if let Some(table_path) = genome_table {
        read_locus_variants(table_path, contig, start, end, VariantSource::Genome)
            .unwrap_or_default()
    } else {
        vec![]
    };

    if exome_variants.is_empty() && genome_variants.is_empty() {
        return Ok(None);
    }

    // Render locus plot
    let all_variants: Vec<RenderVariant> = exome_variants
        .iter()
        .chain(genome_variants.iter())
        .cloned()
        .collect();

    let png_data = render_locus_plot(&all_variants, start, end, threshold)?;

    // Write plot file (this remains as a file)
    let plot_path = format!("{}/{}/plot.png", loci_dir, region_id);
    write_locus_file(&plot_path, &png_data)?;

    // Build LocusVariantRow records (replaces JSON files)
    let variant_rows: Vec<LocusVariantRow> = exome_variants
        .iter()
        .map(|v| LocusVariantRow {
            locus_id: region_id.clone(),
            phenotype: phenotype.to_string(),
            ancestry: ancestry.to_string(),
            sequencing_type: "exome".to_string(),
            contig: contig.to_string(),
            xpos: calculate_xpos(contig, v.position),
            position: v.position,
            pvalue: v.pvalue,
            neg_log10_p: if v.pvalue > 0.0 {
                -v.pvalue.log10() as f32
            } else {
                0.0
            },
            is_significant: v.is_significant,
        })
        .chain(genome_variants.iter().map(|v| LocusVariantRow {
            locus_id: region_id.clone(),
            phenotype: phenotype.to_string(),
            ancestry: ancestry.to_string(),
            sequencing_type: "genome".to_string(),
            contig: contig.to_string(),
            xpos: calculate_xpos(contig, v.position),
            position: v.position,
            pvalue: v.pvalue,
            neg_log10_p: if v.pvalue > 0.0 {
                -v.pvalue.log10() as f32
            } else {
                0.0
            },
            is_significant: v.is_significant,
        }))
        .collect();

    // Build LocusDefinitionRow
    let source_str = lead
        .as_ref()
        .map(|l| l.source.clone())
        .unwrap_or_else(|| "unknown".to_string());
    let lead_variant_str = lead
        .as_ref()
        .map(|l| format!("{}:{}::", l.contig, l.position)) // Note: ref/alt not available from SigPosition
        .unwrap_or_else(|| "unknown".to_string());
    let lead_pvalue = lead.as_ref().map(|l| l.pvalue).unwrap_or(1.0);

    let definition_row = LocusDefinitionRow {
        locus_id: region_id.clone(),
        phenotype: phenotype.to_string(),
        ancestry: ancestry.to_string(),
        contig: contig.to_string(),
        start,
        stop: end,
        xstart: calculate_xpos(contig, start),
        xstop: calculate_xpos(contig, end),
        source: source_str.clone(),
        lead_variant: lead_variant_str.clone(),
        lead_pvalue,
        exome_count: exome_variants.len() as u32,
        genome_count: genome_variants.len() as u32,
    };

    // Build ManifestLocus (still needed for manifest.json)
    let manifest_locus = ManifestLocus {
        id: region_id.clone(),
        region: ManifestRegion {
            contig: contig.to_string(),
            start: start as i64,
            end: end as i64,
        },
        source: source_str,
        lead_variant: lead_variant_str,
        lead_pvalue,
        lead_gene: None,
        plot: plot_path,
        exome_variants: if !exome_variants.is_empty() {
            Some(ManifestLocusVariants {
                path: format!("loci_variants.parquet (locus_id={})", region_id),
                count: exome_variants.len() as u64,
            })
        } else {
            None
        },
        genome_variants: if !genome_variants.is_empty() {
            Some(ManifestLocusVariants {
                path: format!("loci_variants.parquet (locus_id={})", region_id),
                count: genome_variants.len() as u64,
            })
        } else {
            None
        },
        genes: vec![],
    };

    Ok(Some((manifest_locus, definition_row, variant_rows)))
}

/// Extract significant positions from the consolidated significant.parquet file.
pub fn extract_sig_positions(parquet_path: &str) -> Result<Vec<SigPosition>> {
    use crate::io::is_cloud_path;

    let batches = if is_cloud_path(parquet_path) {
        read_cloud_parquet_file(parquet_path)?
    } else {
        if !std::path::Path::new(parquet_path).exists() {
            return Ok(vec![]);
        }
        read_local_parquet_file(parquet_path)?
    };

    let mut positions = Vec::new();

    for batch in &batches {
        if batch.num_rows() == 0 {
            continue;
        }

        let schema = batch.schema();
        let contig_idx = schema.fields().iter().position(|f| f.name() == "contig");
        let position_idx = schema.fields().iter().position(|f| f.name() == "position");
        let pvalue_idx = schema.fields().iter().position(|f| f.name() == "pvalue");
        let seq_type_idx = schema.fields().iter().position(|f| f.name() == "sequencing_type");

        if contig_idx.is_none() || position_idx.is_none() || pvalue_idx.is_none() || seq_type_idx.is_none() {
            continue;
        }

        let contig_col = batch.column(contig_idx.unwrap()).as_any().downcast_ref::<StringArray>();
        let position_col = batch.column(position_idx.unwrap()).as_any().downcast_ref::<Int32Array>();
        let pvalue_col = batch.column(pvalue_idx.unwrap()).as_any().downcast_ref::<Float64Array>();
        let seq_type_col = batch.column(seq_type_idx.unwrap()).as_any().downcast_ref::<StringArray>();

        if let (Some(contig_arr), Some(pos_arr), Some(pval_arr), Some(seq_arr)) =
            (contig_col, position_col, pvalue_col, seq_type_col)
        {
            for i in 0..batch.num_rows() {
                if contig_arr.is_null(i) || pos_arr.is_null(i) || pval_arr.is_null(i) || seq_arr.is_null(i) {
                    continue;
                }

                positions.push(SigPosition {
                    contig: contig_arr.value(i).to_string(),
                    position: pos_arr.value(i),
                    pvalue: pval_arr.value(i),
                    source: seq_arr.value(i).to_string(),
                });
            }
        }
    }

    Ok(positions)
}

/// Compute locus regions by expanding significant positions and merging overlapping.
fn compute_locus_regions(
    positions: &[SigPosition],
    window: i32,
) -> Vec<(String, i32, i32)> {
    // Group positions by chromosome
    let mut by_chrom: HashMap<String, Vec<(i32, f64)>> = HashMap::new();
    for pos in positions {
        by_chrom
            .entry(pos.contig.clone())
            .or_default()
            .push((pos.position, pos.pvalue));
    }

    let mut regions = Vec::new();

    for (contig, mut chrom_positions) in by_chrom {
        // Sort by position
        chrom_positions.sort_by_key(|(pos, _)| *pos);

        // Expand and merge
        let mut current_start: Option<i32> = None;
        let mut current_end: Option<i32> = None;

        for (pos, _pvalue) in chrom_positions {
            let expanded_start = (pos - window).max(1);
            let expanded_end = pos + window;

            match (current_start, current_end) {
                (Some(_start), Some(end)) if expanded_start <= end => {
                    // Overlapping - extend current region
                    current_end = Some(expanded_end.max(end));
                }
                (Some(start), Some(end)) => {
                    // Non-overlapping - emit current and start new
                    regions.push((contig.clone(), start, end));
                    current_start = Some(expanded_start);
                    current_end = Some(expanded_end);
                }
                _ => {
                    // First region
                    current_start = Some(expanded_start);
                    current_end = Some(expanded_end);
                }
            }
        }

        // Emit final region
        if let (Some(start), Some(end)) = (current_start, current_end) {
            regions.push((contig, start, end));
        }
    }

    // Sort by chromosome and position
    regions.sort_by(|a, b| {
        let chr_a = parse_chrom_order(&a.0);
        let chr_b = parse_chrom_order(&b.0);
        chr_a.cmp(&chr_b).then(a.1.cmp(&b.1))
    });

    regions
}

/// Compute locus regions including gene bounds.
///
/// For variant positions, expands by ±window.
/// For gene regions, expands the full gene bounds by ±window.
fn compute_locus_regions_with_genes(
    positions: &[SigPosition],
    gene_regions: &[(String, i32, i32)],
    window: i32,
) -> Vec<(String, i32, i32)> {
    // Collect all expanded regions
    let mut all_regions: Vec<(String, i32, i32)> = Vec::new();

    // Add variant position regions (expanded by window)
    for pos in positions {
        let expanded_start = (pos.position - window).max(1);
        let expanded_end = pos.position + window;
        all_regions.push((pos.contig.clone(), expanded_start, expanded_end));
    }

    // Add gene regions (gene bounds expanded by window)
    for (chrom, start, end) in gene_regions {
        let expanded_start = (start - window).max(1);
        let expanded_end = end + window;
        all_regions.push((chrom.clone(), expanded_start, expanded_end));
    }

    // Group by chromosome
    let mut by_chrom: HashMap<String, Vec<(i32, i32)>> = HashMap::new();
    for (chrom, start, end) in all_regions {
        by_chrom.entry(chrom).or_default().push((start, end));
    }

    // Merge overlapping regions per chromosome
    let mut merged_regions = Vec::new();

    for (contig, mut intervals) in by_chrom {
        // Sort by start position
        intervals.sort_by_key(|(start, _)| *start);

        let mut current_start: Option<i32> = None;
        let mut current_end: Option<i32> = None;

        for (start, end) in intervals {
            match (current_start, current_end) {
                (Some(_cs), Some(ce)) if start <= ce => {
                    // Overlapping - extend current region
                    current_end = Some(end.max(ce));
                }
                (Some(cs), Some(ce)) => {
                    // Non-overlapping - emit current and start new
                    merged_regions.push((contig.clone(), cs, ce));
                    current_start = Some(start);
                    current_end = Some(end);
                }
                _ => {
                    // First region
                    current_start = Some(start);
                    current_end = Some(end);
                }
            }
        }

        // Emit final region
        if let (Some(start), Some(end)) = (current_start, current_end) {
            merged_regions.push((contig, start, end));
        }
    }

    // Sort by chromosome and position
    merged_regions.sort_by(|a, b| {
        let chr_a = parse_chrom_order(&a.0);
        let chr_b = parse_chrom_order(&b.0);
        chr_a.cmp(&chr_b).then(a.1.cmp(&b.1))
    });

    merged_regions
}

/// Parse chromosome for sorting (1-22, then X, Y, MT).
fn parse_chrom_order(chrom: &str) -> (i32, String) {
    let c = chrom.trim_start_matches("chr");
    match c.parse::<i32>() {
        Ok(n) => (n, String::new()),
        Err(_) => (100, c.to_string()), // X, Y, MT sort after numbered
    }
}

/// Find the lead variant (lowest p-value) in a region.
fn find_lead_variant(
    positions: &[SigPosition],
    contig: &str,
    start: i32,
    end: i32,
) -> Option<SigPosition> {
    positions
        .iter()
        .filter(|p| p.contig == contig && p.position >= start && p.position <= end)
        .min_by(|a, b| a.pvalue.partial_cmp(&b.pvalue).unwrap_or(std::cmp::Ordering::Equal))
        .cloned()
}

/// Read variants from a Hail table for a specific genomic region.
fn read_locus_variants(
    table_path: &str,
    contig: &str,
    start: i32,
    end: i32,
    source: VariantSource,
) -> Result<Vec<RenderVariant>> {
    use crate::query::QueryEngine;
    use std::time::Instant;

    let t0 = Instant::now();
    let engine = QueryEngine::open_path(table_path)?;
    let open_time = t0.elapsed();
    if open_time.as_secs() > 1 {
        eprintln!("      [slow] QueryEngine::open_path took {:.1}s for {}",
            open_time.as_secs_f64(), table_path);
    }

    // Create interval list for this region
    let mut intervals = IntervalList::new();
    intervals.add(contig.to_string(), start, end);

    // Also try with "chr" prefix if not present, or without if present
    let alt_contig = if contig.starts_with("chr") {
        contig.trim_start_matches("chr").to_string()
    } else {
        format!("chr{}", contig)
    };
    intervals.add(alt_contig, start, end);

    let intervals = Arc::new(intervals);

    // Query with interval filter
    let t1 = Instant::now();
    let iter = engine.query_iter_with_intervals(&[], Some(intervals))?;

    let mut variants = Vec::new();
    let threshold = 5e-8; // Genome-wide significance

    let mut row_count = 0;
    for row_result in iter {
        row_count += 1;
        let row = row_result?;

        // Extract locus and pvalue
        if let Some((pos, pvalue)) = extract_locus_pvalue(&row) {
            if pvalue > 0.0 && pvalue <= 1.0 && pvalue.is_finite() {
                variants.push(RenderVariant {
                    position: pos,
                    pvalue,
                    source,
                    is_significant: pvalue < threshold,
                });
            }
        }
    }

    let query_time = t1.elapsed();
    if query_time.as_secs() > 2 {
        eprintln!("      [slow] Interval query took {:.1}s, {} rows for {}:{}-{}",
            query_time.as_secs_f64(), row_count, contig, start, end);
    }

    Ok(variants)
}

/// Extract position and p-value from an encoded row.
fn extract_locus_pvalue(row: &crate::codec::EncodedValue) -> Option<(i32, f64)> {
    use crate::codec::EncodedValue;

    fn get_field<'a>(value: &'a EncodedValue, path: &[&str]) -> Option<&'a EncodedValue> {
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

    let position = get_field(row, &["locus", "position"])?.as_i32()?;

    // Try common p-value field names
    let pvalue = get_field(row, &["Pvalue"])
        .or_else(|| get_field(row, &["pvalue"]))
        .or_else(|| get_field(row, &["p_value"]))
        .or_else(|| get_field(row, &["P"]))
        .and_then(|v| match v {
            EncodedValue::Float64(f) => Some(*f),
            EncodedValue::Float32(f) => Some(*f as f64),
            _ => None,
        })?;

    Some((position, pvalue))
}

/// Render a locus plot and return PNG bytes.
fn render_locus_plot(
    variants: &[RenderVariant],
    start: i32,
    end: i32,
    threshold: f64,
) -> Result<Vec<u8>> {
    // Calculate y_max from data
    let y_max = variants
        .iter()
        .filter(|v| v.pvalue > 0.0 && v.pvalue.is_finite())
        .map(|v| -v.pvalue.log10())
        .fold(10.0f64, |a, b| a.max(b))
        * 1.1; // 10% padding

    let config = LocusPlotConfig {
        width: 800,
        height: 400,
        start_pos: start,
        end_pos: end,
        y_max,
    };

    let mut renderer = LocusRenderer::new(config);
    renderer.draw_threshold_line(threshold);
    renderer.draw_variants(variants);

    renderer.encode_png()
}

/// Write a file (handles both local and cloud paths).
fn write_locus_file(path: &str, data: &[u8]) -> Result<()> {
    use crate::io::is_cloud_path;

    if is_cloud_path(path) {
        use crate::io::CloudWriter;
        use std::io::Write;

        // Ensure parent directory structure is implied in the path
        let mut writer = CloudWriter::new(path)?;
        writer.write_all(data)?;
        writer.finish()?;
    } else {
        // Create parent directory
        if let Some(parent) = std::path::Path::new(path).parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, data)?;
    }

    Ok(())
}

/// Write loci.parquet and loci_variants.parquet files.
fn write_loci_parquet(
    output_base: &str,
    definitions: &[LocusDefinitionRow],
    variants: &[LocusVariantRow],
) -> Result<()> {
    use crate::io::is_cloud_path;

    let loci_path = format!("{}/loci.parquet", output_base);
    let variants_path = format!("{}/loci_variants.parquet", output_base);

    // Write loci definitions
    if is_cloud_path(&loci_path) {
        use crate::io::CloudWriter;
        let cloud_writer = CloudWriter::new(&loci_path)?;
        let mut writer = LocusDefinitionWriter::from_writer(cloud_writer)?;
        writer.write_batch(definitions)?;
        let cloud_writer = writer.into_inner()?;
        cloud_writer.finish()?;
    } else {
        let mut writer = LocusDefinitionWriter::new(&loci_path)?;
        writer.write_batch(definitions)?;
        let count = writer.finish()?;
        println!("      Wrote {} locus definitions to loci.parquet", count);
    }

    // Write loci variants
    if !variants.is_empty() {
        if is_cloud_path(&variants_path) {
            use crate::io::CloudWriter;
            let cloud_writer = CloudWriter::new(&variants_path)?;
            let mut writer = LocusVariantWriter::from_writer(cloud_writer)?;
            writer.write_batch(variants)?;
            let cloud_writer = writer.into_inner()?;
            cloud_writer.finish()?;
        } else {
            let mut writer = LocusVariantWriter::new(&variants_path)?;
            writer.write_batch(variants)?;
            let count = writer.finish()?;
            println!("      Wrote {} locus variants to loci_variants.parquet", count);
        }
    }

    Ok(())
}

//! Manhattan aggregate phase (Phase 2 of V2 pipeline).
//!
//! This module handles the aggregation of scan phase outputs:
//! 1. Compositing partial PNGs into final Manhattan plots
//! 2. Processing gene burden table
//! 3. Merging pre-annotated significant hits (annotations added during scan phase)
//! 4. Generating locus plots for significant regions
//! 5. Writing manifest.json

use crate::distributed::message::ManhattanAggregateSpec;
use crate::error::Result;
use crate::manhattan::data::{
    Manifest, ManifestInputs, ManifestManhattan, ManifestManhattans, ManifestSigHits,
    ManifestSignificantHits, ManifestStats, ManifestTopHit,
};
use arrow::array::{Array, Float64Array, StringArray};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::sync::Arc;
use std::time::Instant;

/// Run the Manhattan aggregation phase.
///
/// This is called by the worker when assigned a ManhattanAggregate job.
pub fn run_aggregation(spec: &ManhattanAggregateSpec) -> Result<usize> {
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
    let gene_count = if let Some(ref _gene_burden_path) = spec.gene_burden {
        // TODO: Call process_complex_gene_burden and render gene manhattan
        println!("  Processing gene burden table...");
        0u64
    } else {
        0
    };

    // Step 3: Merge pre-annotated significant hits
    // Annotations were added during the scan phase via streaming merge-join
    println!("  Merging significant hits...");
    let (exome_sig_count, exome_top_hit) = if spec.exome_results.is_some() {
        merge_significant_hits(output_base, "exome")?
    } else {
        (0, None)
    };

    let (genome_sig_count, genome_top_hit) = if spec.genome_results.is_some() {
        merge_significant_hits(output_base, "genome")?
    } else {
        (0, None)
    };

    // Step 4: Compute locus regions and generate plots (if enabled)
    let loci = if spec.locus_plots {
        println!("  Generating locus plots...");
        // TODO: Implement locus region computation and plotting
        // This would query the original Hail tables using QueryEngine::query_iter_with_intervals
        vec![]
    } else {
        vec![]
    };

    // Step 5: Write manifest.json
    println!("  Writing manifest.json...");
    let aggregate_duration = start.elapsed().as_secs_f64();

    let manifest = Manifest {
        phenotype: extract_phenotype_name(output_base),
        ancestry: None,
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
            exome: if spec.exome_results.is_some() {
                Some(ManifestSigHits {
                    path: format!("{}/exome_significant.parquet", output_base),
                    count: exome_sig_count,
                    top_hit: exome_top_hit,
                })
            } else {
                None
            },
            genome: if spec.genome_results.is_some() {
                Some(ManifestSigHits {
                    path: format!("{}/genome_significant.parquet", output_base),
                    count: genome_sig_count,
                    top_hit: genome_top_hit,
                })
            } else {
                None
            },
            gene: None, // TODO: Gene significant hits
        },
        loci,
        stats: ManifestStats {
            scan_duration_sec: scan_duration,
            aggregate_duration_sec: aggregate_duration,
            total_loci: 0,
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

    Ok((exome_count + genome_count) as usize)
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

/// Merge pre-annotated significant hits from scan phase parquet files.
///
/// The sig.parquet files already contain gene and consequence columns from
/// the streaming merge-join performed during scan phase.
fn merge_significant_hits(
    output_base: &str,
    source: &str,
) -> Result<(u64, Option<ManifestTopHit>)> {
    use crate::io::is_cloud_path;
    use std::path::Path;

    let sig_dir = format!("{}/{}", output_base, source);
    let output_file = format!("{}/{}_significant.parquet", output_base, source);

    // Collect all sig parquet files
    let sig_files = if is_cloud_path(&sig_dir) {
        // For cloud paths, list objects
        list_cloud_parquet_files(&sig_dir, "-sig.parquet")?
    } else {
        // For local paths, use filesystem
        let path = Path::new(&sig_dir);
        if !path.exists() {
            return Ok((0, None));
        }
        list_local_parquet_files(&sig_dir, "-sig.parquet")?
    };

    if sig_files.is_empty() {
        return Ok((0, None));
    }

    // Read and merge all parquet files
    let mut all_batches: Vec<RecordBatch> = Vec::new();
    let mut schema = None;

    for file_path in &sig_files {
        let batches = read_parquet_file(file_path)?;
        for batch in batches {
            if schema.is_none() {
                schema = Some(batch.schema());
            }
            all_batches.push(batch);
        }
    }

    if all_batches.is_empty() {
        return Ok((0, None));
    }

    let schema = schema.unwrap();

    // Count total rows
    let total_count: u64 = all_batches.iter().map(|b| b.num_rows() as u64).sum();

    if total_count == 0 {
        return Ok((0, None));
    }

    // Sort by pvalue and write output
    // For simplicity, collect all rows, sort, and write
    let sorted_batches = sort_batches_by_pvalue(&all_batches)?;

    // Write merged output
    write_parquet_batches(&output_file, &schema, &sorted_batches)?;

    // Extract top hit
    let top_hit = extract_top_hit(&sorted_batches);

    Ok((total_count, top_hit))
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
            let gcs = object_store::gcp::GoogleCloudStorageBuilder::new()
                .with_bucket_name(bucket)
                .build()
                .map_err(|e| HailError::InvalidFormat(format!("Failed to create GCS client: {}", e)))?;
            (Arc::new(gcs), ObjPath::from(path), format!("gs://{}/", bucket))
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
    let gene = get_string("gene");
    let consequence = get_string("consequence");

    Some(ManifestTopHit {
        id: format!("{}:{}:{}:{}", contig, position, ref_allele, alt_allele),
        pvalue,
        gene,
        consequence,
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

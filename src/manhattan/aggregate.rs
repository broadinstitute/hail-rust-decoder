//! Manhattan aggregate phase (Phase 2 of V2 pipeline).
//!
//! This module handles the aggregation of scan phase outputs:
//! 1. Compositing partial PNGs into final Manhattan plots
//! 2. Processing gene burden table
//! 3. Joining significant hits with annotations using DuckDB
//! 4. Generating locus plots for significant regions
//! 5. Writing manifest.json

#[cfg(feature = "duckdb")]
use duckdb::Connection;

use crate::distributed::message::ManhattanAggregateSpec;
use crate::error::Result;
use crate::manhattan::data::{
    Manifest, ManifestInputs, ManifestManhattan, ManifestManhattans, ManifestSigHits,
    ManifestSignificantHits, ManifestStats, ManifestTopHit,
};
use std::time::Instant;

/// DuckDB wrapper for clean access in aggregate phase.
#[cfg(feature = "duckdb")]
pub struct Database {
    conn: Connection,
}

#[cfg(feature = "duckdb")]
impl Database {
    /// Create a new in-memory DuckDB connection.
    pub fn new() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        Ok(Self { conn })
    }

    /// Register a parquet file/directory as a view.
    pub fn register_parquet(&self, name: &str, path: &str) -> Result<()> {
        self.conn.execute(
            &format!(
                "CREATE OR REPLACE VIEW {} AS SELECT * FROM read_parquet('{}')",
                name, path
            ),
            [],
        )?;
        Ok(())
    }

    /// Execute a query and write results to a parquet file.
    pub fn query_to_parquet(&self, sql: &str, output: &str) -> Result<()> {
        self.conn.execute(
            &format!("COPY ({}) TO '{}' (FORMAT PARQUET)", sql, output),
            [],
        )?;
        Ok(())
    }

    /// Execute a query and return the row count.
    pub fn count(&self, sql: &str) -> Result<u64> {
        let mut stmt = self.conn.prepare(&format!("SELECT COUNT(*) FROM ({})", sql))?;
        let count: i64 = stmt.query_row([], |row| row.get(0))?;
        Ok(count as u64)
    }

    /// Get the connection for advanced queries.
    pub fn conn(&self) -> &Connection {
        &self.conn
    }
}

/// Run the Manhattan aggregation phase.
///
/// This is called by the worker when assigned a ManhattanAggregate job.
#[cfg(feature = "duckdb")]
pub fn run_aggregation(spec: &ManhattanAggregateSpec) -> Result<usize> {
    use crate::io::is_cloud_path;
    use std::path::Path;

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

    // Step 3: Join significant hits with annotations using DuckDB
    println!("  Joining significant hits with annotations...");
    let (exome_sig_count, exome_top_hit) = if spec.exome_results.is_some() {
        join_and_annotate_hits(
            output_base,
            "exome",
            spec.exome_annotations.as_deref(),
        )?
    } else {
        (0, None)
    };

    let (genome_sig_count, genome_top_hit) = if spec.genome_results.is_some() {
        join_and_annotate_hits(
            output_base,
            "genome",
            spec.genome_annotations.as_deref(),
        )?
    } else {
        (0, None)
    };

    // Step 4: Compute locus regions and generate plots (if enabled)
    let loci = if spec.locus_plots {
        println!("  Generating locus plots...");
        // TODO: Implement locus region computation and plotting
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
#[cfg(feature = "duckdb")]
fn composite_source_pngs(
    output_base: &str,
    source: &str,
    width: u32,
    height: u32,
) -> Result<u64> {
    use crate::io::is_cloud_path;
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

/// Join significant hits with annotations using DuckDB.
#[cfg(feature = "duckdb")]
fn join_and_annotate_hits(
    output_base: &str,
    source: &str,
    annotations_path: Option<&str>,
) -> Result<(u64, Option<ManifestTopHit>)> {
    use crate::io::is_cloud_path;
    use std::path::Path;

    let sig_dir = format!("{}/{}", output_base, source);
    let sig_pattern = format!("{}/part-*-sig.parquet", sig_dir);
    let output_file = format!("{}/{}_significant.parquet", output_base, source);

    // Check if we have any sig files
    // For cloud paths, we'll just try the query
    // For local paths, check if directory has files
    if !is_cloud_path(&sig_dir) {
        let path = Path::new(&sig_dir);
        if !path.exists() {
            return Ok((0, None));
        }
    }

    let db = Database::new()?;

    // Register significant hits from all partitions
    db.conn.execute(
        &format!(
            "CREATE VIEW sig_hits AS SELECT * FROM read_parquet('{}')",
            sig_pattern
        ),
        [],
    )?;

    // Get count
    let count = db.count("SELECT * FROM sig_hits").unwrap_or(0);
    if count == 0 {
        return Ok((0, None));
    }

    // Build the output query
    let query = if let Some(annot_path) = annotations_path {
        // Register annotations
        let annot_pattern = if annot_path.ends_with('/') || !annot_path.ends_with(".parquet") {
            format!("{}/*.parquet", annot_path.trim_end_matches('/'))
        } else {
            annot_path.to_string()
        };

        db.conn.execute(
            &format!(
                "CREATE VIEW annotations AS SELECT * FROM read_parquet('{}')",
                annot_pattern
            ),
            [],
        )?;

        // Join with annotations
        // Note: Annotation parquet may have locus as nested struct
        // We handle both flat and nested schemas
        format!(
            r#"
            SELECT
                h.*,
                a.gene_symbol,
                a.consequence
            FROM sig_hits h
            LEFT JOIN annotations a
                ON h.contig = COALESCE(a.contig, a.locus.contig)
                AND h.position = COALESCE(a.position, a.locus.position)
                AND h.ref = COALESCE(a.ref, a.alleles[1])
                AND h.alt = COALESCE(a.alt, a.alleles[2])
            ORDER BY h.pvalue ASC
            "#
        )
    } else {
        // No annotations, just output sorted hits
        "SELECT * FROM sig_hits ORDER BY pvalue ASC".to_string()
    };

    // Write to output parquet
    db.query_to_parquet(&query, &output_file)?;

    // Get top hit
    let top_hit = get_top_hit(&db, annotations_path.is_some())?;

    Ok((count, top_hit))
}

/// Get the top hit from the sig_hits view.
#[cfg(feature = "duckdb")]
fn get_top_hit(db: &Database, has_annotations: bool) -> Result<Option<ManifestTopHit>> {
    let query = if has_annotations {
        r#"
        SELECT
            contig || ':' || position || ':' || ref || ':' || alt as id,
            pvalue,
            gene_symbol,
            consequence
        FROM sig_hits h
        LEFT JOIN annotations a
            ON h.contig = COALESCE(a.contig, a.locus.contig)
            AND h.position = COALESCE(a.position, a.locus.position)
        ORDER BY pvalue ASC
        LIMIT 1
        "#
    } else {
        r#"
        SELECT
            contig || ':' || position || ':' || ref || ':' || alt as id,
            pvalue,
            NULL as gene_symbol,
            NULL as consequence
        FROM sig_hits
        ORDER BY pvalue ASC
        LIMIT 1
        "#
    };

    let mut stmt = db.conn.prepare(query)?;
    let result = stmt.query_row([], |row| {
        Ok(ManifestTopHit {
            id: row.get(0)?,
            pvalue: row.get(1)?,
            gene: row.get::<_, Option<String>>(2)?,
            consequence: row.get::<_, Option<String>>(3)?,
        })
    });

    match result {
        Ok(hit) => Ok(Some(hit)),
        Err(duckdb::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Clean up intermediate partition files.
#[cfg(feature = "duckdb")]
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

// Stub for non-duckdb builds
#[cfg(not(feature = "duckdb"))]
pub fn run_aggregation(_spec: &ManhattanAggregateSpec) -> Result<usize> {
    Err(crate::HailError::InvalidFormat(
        "Manhattan aggregate requires the 'duckdb' feature".into(),
    ))
}

//! Reference genome contig length helpers.
//!
//! Provides fallback chromosome lengths for GRCh37 and GRCh38 when the
//! table metadata doesn't include reference genome information.

use std::collections::HashMap;

use crate::query::QueryEngine;

/// Try to determine contig lengths from the engine metadata, falling back
/// to GRCh38 defaults.
pub fn get_contig_lengths(_engine: &QueryEngine) -> Vec<(String, u32)> {
    // TODO: Try to extract from engine globals / reference genome metadata.
    // For now, fall back to GRCh38 standard lengths.
    get_grch38_autosomes_and_x()
}

/// GRCh38 chromosome lengths for autosomes + X.
/// Returns an ordered Vec suitable for layout.
fn get_grch38_autosomes_and_x() -> Vec<(String, u32)> {
    vec![
        ("1".into(), 248956422),
        ("2".into(), 242193529),
        ("3".into(), 198295559),
        ("4".into(), 190214555),
        ("5".into(), 181538259),
        ("6".into(), 170805979),
        ("7".into(), 159345973),
        ("8".into(), 145138636),
        ("9".into(), 138394717),
        ("10".into(), 133797422),
        ("11".into(), 135086622),
        ("12".into(), 133275309),
        ("13".into(), 114364328),
        ("14".into(), 107043718),
        ("15".into(), 101991189),
        ("16".into(), 90338345),
        ("17".into(), 83257441),
        ("18".into(), 80373285),
        ("19".into(), 58617616),
        ("20".into(), 64444167),
        ("21".into(), 46709983),
        ("22".into(), 50818468),
        ("X".into(), 156040895),
    ]
}

/// Build a lookup map from the ordered contig list, including "chr"-prefixed aliases.
pub fn contig_length_map(contigs: &[(String, u32)]) -> HashMap<String, u32> {
    let mut m = HashMap::new();
    for (name, len) in contigs {
        m.insert(name.clone(), *len);
        // Also register "chr" prefixed version for VCFs that use "chr1" format
        if !name.starts_with("chr") {
            m.insert(format!("chr{}", name), *len);
        }
    }
    m
}

CREATE TABLE IF NOT EXISTS loci_variants (
    -- Identity
    locus_id             String,
    phenotype            LowCardinality(String),
    ancestry             LowCardinality(String),
    sequencing_type      LowCardinality(String),

    -- Variant position
    contig               LowCardinality(String),
    xpos                 Int64,
    position             Int32,
    ref                  String,
    alt                  String,

    -- Association stats
    pvalue               Float64,
    neg_log10_p          Float32,
    is_significant       Bool
)
ENGINE = MergeTree()
PARTITION BY phenotype
ORDER BY (locus_id, sequencing_type, xpos, ref, alt)
SETTINGS index_granularity = 8192

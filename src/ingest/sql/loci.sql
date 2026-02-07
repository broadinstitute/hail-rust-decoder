CREATE TABLE IF NOT EXISTS loci (
    -- Identity
    locus_id             String,
    phenotype            String,
    ancestry             LowCardinality(String),

    -- Region
    contig               LowCardinality(String),
    start                Int32,
    stop                 Int32,
    xstart               Int64,
    xstop                Int64,

    -- Lead variant info
    source               LowCardinality(String),
    lead_variant         String,
    lead_pvalue          Float64,

    -- Variant counts
    exome_count          UInt32,
    genome_count         UInt32,

    -- Plot URI
    plot_gcs_uri         String
)
ENGINE = ReplacingMergeTree()
ORDER BY (phenotype, ancestry, locus_id)
SETTINGS index_granularity = 8192

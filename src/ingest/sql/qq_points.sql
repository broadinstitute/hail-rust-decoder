CREATE TABLE IF NOT EXISTS qq_points (
    -- Identity
    phenotype            String,
    ancestry             LowCardinality(String),
    sequencing_type      LowCardinality(String),

    -- Variant key (for annotation joins)
    contig               LowCardinality(String),
    position             Int32,
    ref                  String,
    alt                  String,

    -- QQ values (pre-computed -log10)
    pvalue_log10           Float64,
    pvalue_expected_log10  Float64
)
ENGINE = MergeTree()
PARTITION BY phenotype
ORDER BY (ancestry, sequencing_type, pvalue_expected_log10)
SETTINGS index_granularity = 8192

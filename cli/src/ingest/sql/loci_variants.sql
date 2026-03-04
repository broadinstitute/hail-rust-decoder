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
    is_significant       Bool,

    -- Effect size fields (nullable)
    beta                 Nullable(Float64),
    se                   Nullable(Float64),
    af                   Nullable(Float64),

    -- Case/control breakdown fields (nullable)
    ac_cases             Nullable(Float64),
    ac_controls          Nullable(Float64),
    af_cases             Nullable(Float64),
    af_controls          Nullable(Float64),
    association_ac       Nullable(Float64)
)
ENGINE = MergeTree()
PARTITION BY phenotype
ORDER BY (locus_id, sequencing_type, xpos, ref, alt)
SETTINGS index_granularity = 8192
